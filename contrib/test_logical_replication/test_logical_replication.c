#include "postgres.h"

#include "access/timeline.h"
#include "access/xlog_internal.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/snapbuild.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "miscadmin.h"
#include "funcapi.h"

PG_MODULE_MAGIC;

Datum init_logical_replication(PG_FUNCTION_ARGS);
Datum start_logical_replication(PG_FUNCTION_ARGS);
Datum stop_logical_replication(PG_FUNCTION_ARGS);

static Tuplestorestate *tupstore = NULL;
static TupleDesc tupdesc;

extern void XLogRead(char *buf, TimeLineID tli, XLogRecPtr startptr, Size count);

static int
test_read_page(XLogReaderState* state, XLogRecPtr targetPagePtr, int reqLen,
                 char* cur_page, TimeLineID *pageTLI)
{
    XLogRecPtr flushptr, loc;
    int count;

	loc = targetPagePtr + reqLen;
	while (1) {
		flushptr = GetFlushRecPtr();
		if (loc <= flushptr)
			break;
		pg_usleep(1000L);
	}

    /* more than one block available */
    if (targetPagePtr + XLOG_BLCKSZ <= flushptr)
        count = XLOG_BLCKSZ;
    /* not enough data there */
    else if (targetPagePtr + reqLen > flushptr)
        return -1;
    /* part of the page available */
    else
        count = flushptr - targetPagePtr;

    /* FIXME: more sensible/efficient implementation */
    XLogRead(cur_page, ThisTimeLineID, targetPagePtr, XLOG_BLCKSZ);

    return count;
}

static void store_tuple(XLogRecPtr ptr, TransactionId xid, StringInfo si)
{
	Datum values[3];
	bool nulls[3];
	char buf[60];

	sprintf(buf, "%X/%X", (uint32)(ptr >> 32), (uint32)ptr);

	memset(nulls, 0, sizeof(nulls));
	values[0] = CStringGetTextDatum(buf);
	values[1] = Int64GetDatum(xid);
	values[2] = CStringGetTextDatum(si->data);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

static void
begin_txn_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn)
{
	ReaderApplyState *state = cache->private_data;
	bool send;

	resetStringInfo(state->out);

	send = state->begin_cb(state->user_private, state->out, txn);

	if (send)
		store_tuple(txn->lsn, txn->xid, state->out);
}

static void
commit_txn_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
	ReaderApplyState *state = cache->private_data;
	bool send;

	resetStringInfo(state->out);

	send = state->commit_cb(state->user_private, state->out, txn, commit_lsn);

	if (send)
		store_tuple(commit_lsn, txn->xid, state->out);
}

static void
change_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn, ReorderBufferChange* change)
{
	ReaderApplyState *state = cache->private_data;
	bool send;
	HeapTuple table;
	Oid reloid;

	resetStringInfo(state->out);

	table = LookupRelationByRelFileNode(&change->relnode);
	Assert(table);
	reloid = HeapTupleHeaderGetOid(table->t_data);
	ReleaseSysCache(table);

	send = state->change_cb(state->user_private, state->out, txn,
							reloid, change);

	if (send)
		store_tuple(change->lsn, txn->xid, state->out);
}


PG_FUNCTION_INFO_V1(init_logical_replication);

Datum
init_logical_replication(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	Name plugin = PG_GETARG_NAME(1);

	char		xpos[MAXFNAMELEN];
	XLogReaderState *logical_reader;

	TupleDesc   tupdesc;
	HeapTuple   tuple;
	Datum       result;
	Datum       values[2];
	bool        nulls[2];

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Acquire a logical replication slot */
	CheckLogicalReplicationRequirements();
	LogicalDecodingAcquireFreeSlot(NameStr(*name), NameStr(*plugin));

	/*
	 * Use the same initial_snapshot_reader, but with our own read_page
	 * callback that does not depend on walsender.
	 */
	logical_reader = initial_snapshot_reader(MyLogicalDecodingSlot->last_required_checkpoint,
											 MyLogicalDecodingSlot->xmin,
											 NameStr(MyLogicalDecodingSlot->plugin));
	logical_reader->read_page = test_read_page;

	/* Wait for a consistent starting point */
	for (;;)
	{
		XLogRecord *record;
		XLogRecordBuffer buf;
		ReaderApplyState* apply_state = logical_reader->private_data;
		char *err = NULL;

		/* the read_page callback waits for new WAL */
		record = XLogReadRecord(logical_reader, InvalidXLogRecPtr, &err);
		if (err)
			elog(ERROR, "%s", err);

		Assert(record);

		buf.origptr = logical_reader->ReadRecPtr;
		buf.record = *record;
		buf.record_data = XLogRecGetData(record);
		DecodeRecordIntoReorderBuffer(logical_reader, apply_state, &buf);

		if (initial_snapshot_ready(logical_reader))
			break;
	}

	/* Extract the values we want and build a tuple to return */
	MyLogicalDecodingSlot->confirmed_flush = logical_reader->EndRecPtr;
	snprintf(xpos, sizeof(xpos), "%X/%X",
			 (uint32) (MyLogicalDecodingSlot->confirmed_flush >> 32),
			 (uint32) MyLogicalDecodingSlot->confirmed_flush);

	values[0] = CStringGetTextDatum(NameStr(MyLogicalDecodingSlot->name));
	values[1] = CStringGetTextDatum(xpos);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	LogicalDecodingReleaseSlot();

	PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(start_logical_replication);

Datum
start_logical_replication(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	XLogRecPtr now;
	XLogReaderState *logical_reader;
	ReaderApplyState *apply_state;
	ReorderBuffer *reorder;

	ResourceOwner old_resowner = CurrentResourceOwner;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * XXX: It's impolite to ignore our argument and keep decoding
	 * until the current position.
	 */
	now = GetFlushRecPtr();

	/*
	 * We need to create a normal_snapshot_reader, but adjust it to use
	 * our page_read callback, and also make its reorder buffer use our
	 * callback wrappers that don't depend on walsender.
	 */

	CheckLogicalReplicationRequirements();
	LogicalDecodingReAcquireSlot(NameStr(*name));
	logical_reader = normal_snapshot_reader(MyLogicalDecodingSlot->last_required_checkpoint,
											MyLogicalDecodingSlot->xmin,
											NameStr(MyLogicalDecodingSlot->plugin),
											MyLogicalDecodingSlot->confirmed_flush);

	logical_reader->read_page = test_read_page;
	apply_state = (ReaderApplyState *)logical_reader->private_data;

	reorder = apply_state->reorderbuffer;
	reorder->begin = begin_txn_wrapper;
	reorder->apply_change = change_wrapper;
	reorder->commit = commit_txn_wrapper;

	elog(DEBUG1, "Starting logical replication from %X/%X to %X/%x",
		 (uint32)(MyLogicalDecodingSlot->last_required_checkpoint>>32), (uint32)MyLogicalDecodingSlot->last_required_checkpoint,
		 (uint32)(now>>32), (uint32)now);

	CurrentResourceOwner = ResourceOwnerCreate(CurrentResourceOwner, "logical decoding");

	for (;;)
	{
		XLogRecord *record;
		char *errm = NULL;

		record = XLogReadRecord(logical_reader, InvalidXLogRecPtr, &errm);
		if (errm)
			elog(ERROR, "%s", errm);

		if (record != NULL)
		{
			XLogRecPtr rp;
			XLogRecordBuffer buf;
			ReaderApplyState* apply_state = logical_reader->private_data;

			buf.origptr = logical_reader->ReadRecPtr;
			buf.record = *record;
			buf.record_data = XLogRecGetData(record);

			/*
			 * The {begin_txn,change,commit_txn}_wrapper callbacks above
			 * will store the description into our tuplestore.
			 */
			DecodeRecordIntoReorderBuffer(logical_reader, apply_state, &buf);

			rp = logical_reader->EndRecPtr;
			if (rp >= now)
			{
				elog(DEBUG1, "Reached endpoint (wanted: %X/%X, got: %X/%X)",
					 (uint32)(now>>32), (uint32)now,
					 (uint32)(rp>>32), (uint32)rp);
				break;
			}
		}
	}

	tuplestore_donestoring(tupstore);

	CurrentResourceOwner = old_resowner;

	/* Next time, start where we left off */
	MyLogicalDecodingSlot->confirmed_flush = logical_reader->EndRecPtr;

	LogicalDecodingReleaseSlot();

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(stop_logical_replication);

Datum
stop_logical_replication(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	CheckLogicalReplicationRequirements();
	LogicalDecodingFreeSlot(NameStr(*name));

	PG_RETURN_INT32(0);
}
