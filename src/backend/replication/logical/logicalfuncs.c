/*-------------------------------------------------------------------------
 *
 * logicalfuncs.c
 *
 *     Support functions for using xlog decoding
 *
 *
 * Portions Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logicalfuncs.c
 *
 */

#include "postgres.h"

#include "fmgr.h"

#include "replication/logicalfuncs.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "replication/reorderbuffer.h"
#include "replication/decode.h"
/*FIXME: XLogRead*/
#include "replication/walsender_private.h"
#include "replication/snapbuild.h"

#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


static int
replay_read_page(XLogReaderState* state, XLogRecPtr targetPagePtr, int reqLen,
				 char* cur_page, TimeLineID *pageTLI)
{
	XLogRecPtr flushptr;
	int count;

	flushptr = WalSndWaitForWal(targetPagePtr + reqLen);

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
	XLogRead(cur_page, targetPagePtr, XLOG_BLCKSZ);

	return count;
}

/*
 * Callbacks for ReorderBuffer which add in some more information and then call
 * output_plugin.h plugins.
 */
static void
begin_txn_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn)
{
	ReaderApplyState *state = cache->private_data;
	bool send;

	resetStringInfo(state->out);
	WalSndPrepareWrite(state->out, txn->lsn);

	send = state->begin_cb(state->user_private, state->out, txn);

	/* only send if the plugin decided to do so */
	if (send)
		WalSndWriteData(state->out);
}

static void
commit_txn_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
	ReaderApplyState *state = cache->private_data;
	bool send;

	resetStringInfo(state->out);
	WalSndPrepareWrite(state->out, commit_lsn);

	send = state->commit_cb(state->user_private, state->out, txn, commit_lsn);

	/* only send if the plugin decided to do so */
	if (send)
		WalSndWriteData(state->out);
}

static void
change_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn, ReorderBufferChange* change)
{
	ReaderApplyState *state = cache->private_data;
	bool send;
	HeapTuple table;
	Oid reloid;

	resetStringInfo(state->out);
	WalSndPrepareWrite(state->out, change->lsn);

	table = LookupTableByRelFileNode(&change->relnode);
	Assert(table);
	reloid = HeapTupleHeaderGetOid(table->t_data);
	ReleaseSysCache(table);


	send = state->change_cb(state->user_private, state->out, txn,
							reloid, change);

	/* only send if the plugin decided to do so */
	if (send)
		WalSndWriteData(state->out);
}

/*
 * Build a snapshot reader that doesn't ever outputs/decodes anything, but just
 * waits for the first point in the LSN stream where it reaches a consistent
 * state.
 */
XLogReaderState *
initial_snapshot_reader(XLogRecPtr startpoint, TransactionId xmin)
{
	XLogReaderState *xlogreader;
	ReaderApplyState *apply_state;
	ReorderBuffer *reorder;

	apply_state = (ReaderApplyState*)calloc(1, sizeof(ReaderApplyState));

	if (!apply_state)
		elog(ERROR, "Could not allocate the ReaderApplyState struct");

	xlogreader = XLogReaderAllocate(startpoint, replay_read_page,
									apply_state);

	reorder = ReorderBufferAllocate();
	/* don't decode yet, we're just searching for a consistent point */
	reorder->begin = NULL;
	reorder->apply_change = NULL;
	reorder->commit = NULL;
	reorder->private_data = apply_state;

	apply_state->reorderbuffer = reorder;
	apply_state->stop_after_consistent = true;
	apply_state->snapstate = AllocateSnapshotBuilder(reorder);
	apply_state->snapstate->initial_xmin_horizon = xmin;
	return xlogreader;
}

/*
 * Build a snapshot reader with callbacks found in the shared library "plugin"
 * under the symbol names found in output_plugin.h.
 * It wraps those callbacks so they send out their changes via an logical
 * walsender.
 */
XLogReaderState *
normal_snapshot_reader(XLogRecPtr startpoint, TransactionId xmin,
					   char *plugin, XLogRecPtr valid_after)
{
	/* to simplify things we reuse initial_snapshot_reader */
	XLogReaderState *xlogreader = initial_snapshot_reader(startpoint, xmin);
	ReaderApplyState *apply_state = (ReaderApplyState *)xlogreader->private_data;
	ReorderBuffer *reorder = apply_state->reorderbuffer;

	apply_state->stop_after_consistent = false;

	apply_state->snapstate->transactions_after = valid_after;

	reorder->begin = begin_txn_wrapper;
	reorder->apply_change = change_wrapper;
	reorder->commit = commit_txn_wrapper;

	/* lookup symbols in the shared libarary */

	/* optional */
	apply_state->init_cb = (LogicalDecodeInitCB)
		load_external_function(plugin, "pg_decode_init", false, NULL);

	/* required */
	apply_state->begin_cb = (LogicalDecodeBeginCB)
		load_external_function(plugin, "pg_decode_begin_txn", true, NULL);

	/* required */
	apply_state->change_cb = (LogicalDecodeChangeCB)
		load_external_function(plugin, "pg_decode_change", true, NULL);

	/* required */
	apply_state->commit_cb = (LogicalDecodeCommitCB)
		load_external_function(plugin, "pg_decode_commit_txn", true, NULL);

	/* optional */
	apply_state->cleanup_cb = (LogicalDecodeCleanupCB)
		load_external_function(plugin, "pg_decode_clean", false, NULL);

	reorder->private_data = xlogreader->private_data;

	apply_state->out = makeStringInfo();

	/* call output plugin initialization callback */
	if (apply_state->init_cb)
		apply_state->init_cb(&apply_state->user_private);

	return xlogreader;
}

/* has the initial snapshot found a consistent state? */
bool
initial_snapshot_ready(XLogReaderState *reader)
{
	ReaderApplyState* state = reader->private_data;
	return state->snapstate->state == SNAPBUILD_CONSISTENT;
}
