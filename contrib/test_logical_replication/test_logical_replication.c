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
#include "utils/syscache.h"
#include "nodes/pg_list.h"
#include "funcapi.h"

PG_MODULE_MAGIC;

Datum init_logical_replication(PG_FUNCTION_ARGS);
Datum start_logical_replication(PG_FUNCTION_ARGS);
Datum stop_logical_replication(PG_FUNCTION_ARGS);

static const char *slot_name = NULL;
static XLogRecPtr startpoint = 0;
static List *output = NULL;

extern void XLogRead(char *buf, TimeLineID tli, XLogRecPtr startptr, Size count);

PG_FUNCTION_INFO_V1(init_logical_replication);

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

static void save_line(StringInfo si)
{
	StringInfo ti = makeStringInfo();
	appendStringInfoString(ti, si->data);
	output = lappend(output, ti);
}

static void
begin_txn_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn)
{
	ReaderApplyState *state = cache->private_data;
	bool send;

	resetStringInfo(state->out);

	send = state->begin_cb(state->user_private, state->out, txn);

	if (send)
	{
		save_line(state->out);
		elog(DEBUG1, "Decoded complete BEGIN: %s", state->out->data);
	}
}

static void
commit_txn_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
	ReaderApplyState *state = cache->private_data;
	bool send;

	resetStringInfo(state->out);

	send = state->commit_cb(state->user_private, state->out, txn, commit_lsn);

	if (send)
	{
		save_line(state->out);
		elog(DEBUG1, "Decoded complete COMMIT: %s", state->out->data);
	}
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
	{
		save_line(state->out);
		elog(DEBUG1, "Decoded complete change: %s", state->out->data);
	}
}

Datum
init_logical_replication(PG_FUNCTION_ARGS)
{
	const char *plugin;
	char		xpos[MAXFNAMELEN];
	XLogReaderState *logical_reader;

	TupleDesc   tupdesc;
	HeapTuple   tuple;
	Datum       result;
	Datum       values[2];
	bool        nulls[2];

	if (slot_name)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 (errmsg("sorry, can't init logical replication twice"))));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Acquire a logical replication slot */
	plugin = text_to_cstring(PG_GETARG_TEXT_P(0));
	CheckLogicalReplicationRequirements();
	LogicalDecodingAcquireFreeSlot(plugin);

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

	/* Extract the values we want */
	MyLogicalDecodingSlot->confirmed_flush = logical_reader->EndRecPtr;
	slot_name = NameStr(MyLogicalDecodingSlot->name);
	snprintf(xpos, sizeof(xpos), "%X/%X",
			 (uint32) (MyLogicalDecodingSlot->confirmed_flush >> 32),
			 (uint32) MyLogicalDecodingSlot->confirmed_flush);

	/* Release the slot and return the values */
	LogicalDecodingReleaseSlot();

	values[0] = CStringGetTextDatum(slot_name);
	values[1] = CStringGetTextDatum(xpos);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	/* start_logical_replication should start from where we are now */
	startpoint = GetXLogWriteRecPtr();

	PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(start_logical_replication);

Datum
start_logical_replication(PG_FUNCTION_ARGS)
{
	FuncCallContext *ctx;

	XLogRecPtr now;
	XLogReaderState *logical_reader;
	ReaderApplyState *apply_state;
	ReorderBuffer *reorder;

	if (!slot_name)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("sorry, can't start logical replication outside of an init/stop pair"))));

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		ctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(ctx->multi_call_memory_ctx);

		/*
		 * XXX: It's impolite to ignore our argument and keep decoding
		 * until the current position.
		 */
		now = GetXLogWriteRecPtr();

		CheckLogicalReplicationRequirements();
		LogicalDecodingReAcquireSlot(slot_name);
		logical_reader = normal_snapshot_reader(MyLogicalDecodingSlot->last_required_checkpoint,
												MyLogicalDecodingSlot->xmin,
												NameStr(MyLogicalDecodingSlot->plugin),
												startpoint);

		/*
		 * We need to adjust the normal_snapshot_reader to use our
		 * page_read callback, and also make its reorder buffer use our
		 * callback wrappers that don't depend on walsender.
		 */

		logical_reader->read_page = test_read_page;
		apply_state = (ReaderApplyState *)logical_reader->private_data;
		reorder = apply_state->reorderbuffer;
		reorder->begin = begin_txn_wrapper;
		reorder->apply_change = change_wrapper;
		reorder->commit = commit_txn_wrapper;

		/*
		 * We're accumulating all the decoded WAL in memory until we
		 * reach the desired end point. That's not very nice, but it's
		 * simpler than trying to interleave the SRF_RETURN_NEXT() logic
		 * into the XLogReadRecord() loop.
		 */

		elog(DEBUG1, "Starting logical replication from %X/%X to %X/%x",
			 (uint32)(startpoint>>32), (uint32)startpoint,
			 (uint32)(now>>32), (uint32)now);

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

				DecodeRecordIntoReorderBuffer(logical_reader, apply_state, &buf);

				/* XXX: Is this the right condition? */
				rp = logical_reader->ReadRecPtr;
				if (rp >= now)
				{
					elog(DEBUG1, "Reached endpoint (wanted: %X/%X, got: %X/%X)",
						 (uint32)(now>>32), (uint32)now,
						 (uint32)(rp>>32), (uint32)rp);
					break;
				}
			}
		}

		/* Next time, start where we left off */
		startpoint = now;

		MemoryContextSwitchTo(oldcontext);
	}

	ctx = SRF_PERCALL_SETUP();

	while (output)
	{
		StringInfo si = linitial(output);
		output = list_delete_first(output);
		SRF_RETURN_NEXT(ctx, CStringGetTextDatum(si->data));
	}

	LogicalDecodingReleaseSlot();
	SRF_RETURN_DONE(ctx);
}

PG_FUNCTION_INFO_V1(stop_logical_replication);

Datum
stop_logical_replication(PG_FUNCTION_ARGS)
{
	int n = 0;

	if (slot_name)
	{
		CheckLogicalReplicationRequirements();
		LogicalDecodingFreeSlot(slot_name);
		slot_name = NULL;
		n = 1;
	}
	PG_RETURN_INT32(n);
}
