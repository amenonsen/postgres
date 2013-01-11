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

static const char *slot_name = NULL;
static Tuplestorestate *tupstore = NULL;
static TupleDesc tupdesc;

extern void XLogRead(char *buf, TimeLineID tli, XLogRecPtr startptr, Size count);

static int
test_read_page(XLogReaderState* state, XLogRecPtr targetPagePtr, int reqLen,
			   XLogRecPtr targetRecPtr, char* cur_page, TimeLineID *pageTLI)
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

static void
LogicalOutputPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid)
{
	resetStringInfo(ctx->out);
}

static void
LogicalOutputWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid)
{
	Datum values[3];
	bool nulls[3];
	char buf[60];

	sprintf(buf, "%X/%X", (uint32)(lsn >> 32), (uint32)lsn);

	memset(nulls, 0, sizeof(nulls));
	values[0] = CStringGetTextDatum(buf);
	values[1] = Int64GetDatum(xid);
	values[2] = CStringGetTextDatum(ctx->out->data);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

PG_FUNCTION_INFO_V1(init_logical_replication);

Datum
init_logical_replication(PG_FUNCTION_ARGS)
{
	const char *plugin;
	char		xpos[MAXFNAMELEN];

	TupleDesc   tupdesc;
	HeapTuple   tuple;
	Datum       result;
	Datum       values[2];
	bool        nulls[2];

	LogicalDecodingContext *ctx = NULL;

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
	/* FIXME */

	/* make sure we don't end up with an unreleased slot */
	PG_TRY();
	{
		XLogRecPtr startptr;

		/*
		 * Use the same initial_snapshot_reader, but with our own read_page
		 * callback that does not depend on walsender.
		 */
		MyLogicalDecodingSlot->last_required_checkpoint = GetRedoRecPtr();

		ctx = CreateLogicalDecodingContext(MyLogicalDecodingSlot, true,
				test_read_page, LogicalOutputPrepareWrite, LogicalOutputWrite);

		/* setup from where to read xlog */
		startptr = ctx->slot->last_required_checkpoint;
		/* Wait for a consistent starting point */
		for (;;)
		{
			XLogRecord *record;
			XLogRecordBuffer buf;
			char *err = NULL;

			/* the read_page callback waits for new WAL */
			record = XLogReadRecord(ctx->reader, startptr, &err);
			if (err)
				elog(ERROR, "%s", err);

			Assert(record);

			startptr = InvalidXLogRecPtr;

			buf.origptr = ctx->reader->ReadRecPtr;
			buf.record = *record;
			buf.record_data = XLogRecGetData(record);
			DecodeRecordIntoReorderBuffer(ctx, &buf);

			if (LogicalDecodingContextReady(ctx))
				break;
		}

		/* Extract the values we want */
		MyLogicalDecodingSlot->confirmed_flush = ctx->reader->EndRecPtr;
		slot_name = NameStr(MyLogicalDecodingSlot->name);
		snprintf(xpos, sizeof(xpos), "%X/%X",
				 (uint32) (MyLogicalDecodingSlot->confirmed_flush >> 32),
				 (uint32) MyLogicalDecodingSlot->confirmed_flush);
	}
	PG_CATCH();
	{
		LogicalDecodingReleaseSlot();
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Release the slot and return the values */
	LogicalDecodingReleaseSlot();

	values[0] = CStringGetTextDatum(slot_name);
	values[1] = CStringGetTextDatum(xpos);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(start_logical_replication);

Datum
start_logical_replication(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	XLogRecPtr now;
	XLogRecPtr startptr;
	XLogRecPtr rp;

	LogicalDecodingContext *ctx;

	ResourceOwner old_resowner = CurrentResourceOwner;

	if (!slot_name)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("sorry, can't start logical replication outside of an init/stop pair"))));

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
	LogicalDecodingReAcquireSlot(slot_name);

	ctx = CreateLogicalDecodingContext(MyLogicalDecodingSlot, false,
				test_read_page, LogicalOutputPrepareWrite, LogicalOutputWrite);

	ctx->snapshot_builder->transactions_after = MyLogicalDecodingSlot->confirmed_flush;
	startptr = MyLogicalDecodingSlot->last_required_checkpoint;

	elog(DEBUG1, "Starting logical replication from %X/%X to %X/%x",
		 (uint32)(MyLogicalDecodingSlot->last_required_checkpoint>>32),
		 (uint32)MyLogicalDecodingSlot->last_required_checkpoint,
		 (uint32)(now>>32), (uint32)now);

	CurrentResourceOwner = ResourceOwnerCreate(CurrentResourceOwner, "logical decoding");

	while ((startptr != InvalidXLogRecPtr && startptr < now) ||
		   (ctx->reader->EndRecPtr && ctx->reader->EndRecPtr < now))
	{
		XLogRecord *record;
		char *errm = NULL;

		record = XLogReadRecord(ctx->reader, startptr, &errm);
		if (errm)
			elog(ERROR, "%s", errm);

		startptr = InvalidXLogRecPtr;

		if (record != NULL)
		{
			XLogRecordBuffer buf;

			buf.origptr = ctx->reader->ReadRecPtr;
			buf.record = *record;
			buf.record_data = XLogRecGetData(record);

			/*
			 * The {begin_txn,change,commit_txn}_wrapper callbacks above
			 * will store the description into our tuplestore.
			 */
			DecodeRecordIntoReorderBuffer(ctx, &buf);

		}
	}

	rp = ctx->reader->EndRecPtr;
	if (rp >= now)
	{
		elog(DEBUG1, "Reached endpoint (wanted: %X/%X, got: %X/%X)",
			 (uint32)(now>>32), (uint32)now,
			 (uint32)(rp>>32), (uint32)rp);
	}

	tuplestore_donestoring(tupstore);

	CurrentResourceOwner = old_resowner;

	/* Next time, start where we left off */
	MyLogicalDecodingSlot->confirmed_flush = ctx->reader->EndRecPtr;

	LogicalDecodingReleaseSlot();

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(stop_logical_replication);

Datum
stop_logical_replication(PG_FUNCTION_ARGS)
{
	if (!slot_name)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 (errmsg("sorry, can't stop logical replication before init"))));

	CheckLogicalReplicationRequirements();
	LogicalDecodingFreeSlot(slot_name);
	slot_name = NULL;

	PG_RETURN_INT32(0);
}
