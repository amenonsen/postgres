/*-------------------------------------------------------------------------
 *
 * decode.c
 *		Decodes wal records from an xlogreader.h callback into an reorderbuffer
 *		while building an appropriate snapshots to decode those
 *
 * NOTE:
 * Its possible that the separation between decode.c and snapbuild.c is a
 * bit too strict, in the end they just about have the same switch.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/decode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/transam.h"
#include "access/xlog_internal.h"
#include "access/xact.h"
#include "access/xlogreader.h"

#include "catalog/pg_control.h"

#include "replication/reorderbuffer.h"
#include "replication/decode.h"
#include "replication/snapbuild.h"
#include "replication/logicalfuncs.h"

#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

static void DecodeXLogTuple(char *data, Size len, ReorderBufferTupleBuf *tuple);

static void DecodeInsert(ReorderBuffer *cache, XLogRecordBuffer *buf);

static void DecodeUpdate(ReorderBuffer *cache, XLogRecordBuffer *buf);

static void DecodeDelete(ReorderBuffer *cache, XLogRecordBuffer *buf);

static void DecodeMultiInsert(ReorderBuffer *cache, XLogRecordBuffer *buf);

static void DecodeCommit(ReaderApplyState *state, XLogRecordBuffer *buf, TransactionId xid,
						 TransactionId *sub_xids, int nsubxacts);

static void DecodeAbort(ReorderBuffer * cache, XLogRecPtr lsn, TransactionId xid,
						TransactionId *sub_xids, int nsubxacts);


void DecodeRecordIntoReorderBuffer(XLogReaderState *reader,
								   ReaderApplyState *state,
								   XLogRecordBuffer *buf)
{
	XLogRecord *r = &buf->record;
	uint8 info = r->xl_info & ~XLR_INFO_MASK;
	ReorderBuffer *reorder = state->reorderbuffer;
	SnapBuildAction action;

	/*
	 * FIXME: The existance of the snapshot builder is pretty obvious to the
	 * outside right now, that doesn't seem to be very good...
	 */
	if (!state->snapstate)
	{
		state->snapstate = AllocateSnapshotBuilder(reorder);
	}

	/*---------
	 * Call the snapshot builder. It needs to be called before we analyze
	 * tuples for two reasons:
	 *
	 * * Only in the snapshot building logic we know whether we have enough
	 *   information to decode a particular tuple
	 *
	 * * The Snapshot/CommandIds computed by the SnapshotBuilder need to be
	 *   added to the ReorderBuffer before we add tuples using them
	 *---------
	 */
	action = SnapBuildDecodeCallback(reorder, state->snapstate, buf);

	if (state->stop_after_consistent && state->snapstate->state == SNAPBUILD_CONSISTENT)
	{
		elog(WARNING, "reached consistent point, stopping!");
		return;
	}

	if (action == SNAPBUILD_SKIP)
		return;

	switch (r->xl_rmid)
	{
		case RM_HEAP_ID:
			{
				info &= XLOG_HEAP_OPMASK;
				switch (info)
				{
					case XLOG_HEAP_INSERT:
						DecodeInsert(reorder, buf);
						break;

						/*
						 * no guarantee that we get an HOT update again, so
						 * handle it as a normal update
						 */
					case XLOG_HEAP_HOT_UPDATE:
					case XLOG_HEAP_UPDATE:
						DecodeUpdate(reorder, buf);
						break;

					case XLOG_HEAP_NEWPAGE:
						/*
						 * XXX: There doesn't seem to be a usecase for decoding
						 * HEAP_NEWPAGE's. Its only used in various indexam's
						 * and CLUSTER, neither of which should be relevant for
						 * the logical changestream.
						 */
						break;

					case XLOG_HEAP_DELETE:
						DecodeDelete(reorder, buf);
						break;
					default:
						break;
				}
				break;
			}
		case RM_HEAP2_ID:
			{
				info &= XLOG_HEAP_OPMASK;
				switch (info)
				{
					case XLOG_HEAP2_MULTI_INSERT:
						DecodeMultiInsert(reorder, buf);
						break;
					default:
						/*
						 * everything else here is just physical stuff were not
						 * interested in
						 */
						break;
				}
				break;
			}

		case RM_XACT_ID:
			{
				switch (info)
				{
					case XLOG_XACT_COMMIT:
						{
							TransactionId *sub_xids;
							xl_xact_commit *xlrec =
								(xl_xact_commit *) buf->record_data;

							/*
							 * FIXME: theoretically computing this address is
							 * not really allowed if there are no
							 * subtransactions
							 */
							sub_xids = (TransactionId *) &(
								xlrec->xnodes[xlrec->nrels]);

							DecodeCommit(state, buf, r->xl_xid,
										 sub_xids, xlrec->nsubxacts);


							break;
						}
					case XLOG_XACT_COMMIT_PREPARED:
						{
							TransactionId *sub_xids;
							xl_xact_commit_prepared *xlrec =
								(xl_xact_commit_prepared*) buf->record_data;

							sub_xids = (TransactionId *) &(
								xlrec->crec.xnodes[xlrec->crec.nrels]);

							/* r->xl_xid is committed in a separate record */
							DecodeCommit(state, buf, xlrec->xid, sub_xids,
										 xlrec->crec.nsubxacts);

							break;
						}
					case XLOG_XACT_COMMIT_COMPACT:
						{
							xl_xact_commit_compact *xlrec =
								(xl_xact_commit_compact *) buf->record_data;
							DecodeCommit(state, buf, r->xl_xid,
										 xlrec->subxacts, xlrec->nsubxacts);
							break;
						}
					case XLOG_XACT_ABORT:
						{
							TransactionId *sub_xids;
							xl_xact_abort *xlrec =
								(xl_xact_abort *) buf->record_data;

							sub_xids = (TransactionId *) &(
								xlrec->xnodes[xlrec->nrels]);

							DecodeAbort(reorder, buf->origptr, r->xl_xid,
										sub_xids, xlrec->nsubxacts);
							break;
						}
					case XLOG_XACT_ABORT_PREPARED:
						{
							TransactionId *sub_xids;
							xl_xact_abort_prepared *xlrec =
								(xl_xact_abort_prepared *)buf->record_data;
							xl_xact_abort *arec = &xlrec->arec;

							sub_xids = (TransactionId *) &(
								arec->xnodes[arec->nrels]);
							/* r->xl_xid is committed in a separate record */
							DecodeAbort(reorder, buf->origptr, xlrec->xid,
										sub_xids, arec->nsubxacts);
							break;
						}

					case XLOG_XACT_ASSIGNMENT:
						/*
						 * XXX: We could reassign transactions to the parent
						 * here to save space and effort when merging
						 * transactions at commit.
						 */
						break;
					case XLOG_XACT_PREPARE:
						/*
						 * FXIME: we should replay the transaction and prepare
						 * it as well.
						 */
						break;
					default:
						break;
						;
				}
				break;
			}
		case RM_XLOG_ID:
			{
				switch (info)
				{
					/* this is also used in END_OF_RECOVERY checkpoints */
					case XLOG_CHECKPOINT_SHUTDOWN:
						/*
						 * abort all transactions that still are in progress,
						 * they aren't in progress anymore.  do not abort
						 * prepared transactions that have been prepared for
						 * commit.
						 *
						 * FIXME: implement.
						 */
						break;
				}
			}
		default:
			break;
	}
}

static void
DecodeCommit(ReaderApplyState *state, XLogRecordBuffer *buf, TransactionId xid,
			 TransactionId *sub_xids, int nsubxacts)
{
	int i;

	/* not interested in that part of the stream */
	if (XLByteLE(buf->origptr, state->snapstate->transactions_after))
	{
		DecodeAbort(state->reorderbuffer, buf->origptr, xid,
					sub_xids, nsubxacts);
		return;
	}

	for (i = 0; i < nsubxacts; i++)
	{
		ReorderBufferCommitChild(state->reorderbuffer, xid, *sub_xids,
								 buf->origptr);
		sub_xids++;
	}

	/* replay actions of all transaction + subtransactions in order */
	ReorderBufferCommit(state->reorderbuffer, xid, buf->origptr);
}

static void
DecodeAbort(ReorderBuffer *reorder, XLogRecPtr lsn, TransactionId xid,
			TransactionId *sub_xids, int nsubxacts)
{
	int i;

	elog(WARNING, "ABORT %u", xid);

	for (i = 0; i < nsubxacts; i++)
	{
		ReorderBufferAbort(reorder, *sub_xids, lsn);
		sub_xids++;
	}

	ReorderBufferAbort(reorder, xid, lsn);
}

static void
DecodeInsert(ReorderBuffer *reorder, XLogRecordBuffer *buf)
{
	XLogRecord *r = &buf->record;
	xl_heap_insert *xlrec = (xl_heap_insert *) buf->record_data;

	ReorderBufferChange *change;

	if (r->xl_info & XLR_BKP_BLOCK(0)
		&& r->xl_len < (SizeOfHeapUpdate + SizeOfHeapHeader))
	{
		elog(DEBUG2, "huh, no tuple data on wal_level = logical?");
		return;
	}

	change = ReorderBufferGetChange(reorder);
	change->action = REORDER_BUFFER_CHANGE_INSERT;

	memcpy(&change->relnode, &xlrec->target.node, sizeof(RelFileNode));

	change->newtuple = ReorderBufferGetTupleBuf(reorder);

	DecodeXLogTuple((char *) xlrec + SizeOfHeapInsert,
					r->xl_len - SizeOfHeapInsert,
					change->newtuple);

	ReorderBufferAddChange(reorder, r->xl_xid, buf->origptr, change);
}

static void
DecodeUpdate(ReorderBuffer *reorder, XLogRecordBuffer *buf)
{
	XLogRecord *r = &buf->record;
	xl_heap_update *xlrec = (xl_heap_update *) buf->record_data;


	ReorderBufferChange *change;

	if ((r->xl_info & XLR_BKP_BLOCK(0) || r->xl_info & XLR_BKP_BLOCK(1)) &&
		(r->xl_len < (SizeOfHeapUpdate + SizeOfHeapHeader)))
	{
		elog(DEBUG2, "huh, no tuple data on wal_level = logical?");
		return;
	}

	change = ReorderBufferGetChange(reorder);
	change->action = REORDER_BUFFER_CHANGE_UPDATE;

	memcpy(&change->relnode, &xlrec->target.node, sizeof(RelFileNode));

	/*
	 * FIXME: need to get/save the old tuple as well if we want primary key
	 * changes to work.
	 */
	change->newtuple = ReorderBufferGetTupleBuf(reorder);

	DecodeXLogTuple((char *) xlrec + SizeOfHeapUpdate,
					r->xl_len - SizeOfHeapUpdate,
					change->newtuple);

	ReorderBufferAddChange(reorder, r->xl_xid, buf->origptr, change);
}

static void
DecodeDelete(ReorderBuffer *reorder, XLogRecordBuffer *buf)
{
	XLogRecord *r = &buf->record;

	xl_heap_delete *xlrec = (xl_heap_delete *) buf->record_data;

	ReorderBufferChange *change;

	change = ReorderBufferGetChange(reorder);
	change->action = REORDER_BUFFER_CHANGE_DELETE;

	memcpy(&change->relnode, &xlrec->target.node, sizeof(RelFileNode));

	if (r->xl_len <= (SizeOfHeapDelete + SizeOfHeapHeader))
	{
		elog(DEBUG2, "huh, no primary key for a delete on wal_level = logical?");
		return;
	}

	change->oldtuple = ReorderBufferGetTupleBuf(reorder);

	DecodeXLogTuple((char *) xlrec + SizeOfHeapDelete,
					r->xl_len - SizeOfHeapDelete,
					change->oldtuple);

	ReorderBufferAddChange(reorder, r->xl_xid, buf->origptr, change);
}

/*
 * Decode xl_heap_multi_insert record into multiple changes.
 *
 * Due to slightly different layout we can't reuse DecodeXLogTuple without
 * making that even harder to understand than already is.
 */
static void
DecodeMultiInsert(ReorderBuffer *reorder, XLogRecordBuffer *buf)
{
	XLogRecord *r = &buf->record;
	xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *)buf->record_data;
	int i;
	char *data = buf->record_data;
	bool		isinit = (r->xl_info & XLOG_HEAP_INIT_PAGE) != 0;

	data += SizeOfHeapMultiInsert;

	/* OffsetNumber's are only stored if its not a HEAP_INIT_PAGE record */
	if (!isinit)
		data += sizeof(OffsetNumber) * xlrec->ntuples;

	for (i = 0; i < xlrec->ntuples; i++)
	{
		ReorderBufferChange *change;
		xl_multi_insert_tuple *xlhdr;
		int datalen;
		ReorderBufferTupleBuf *tuple;

		change = ReorderBufferGetChange(reorder);
		change->action = REORDER_BUFFER_CHANGE_INSERT;
		change->newtuple = ReorderBufferGetTupleBuf(reorder);
		memcpy(&change->relnode, &xlrec->node, sizeof(RelFileNode));

		tuple = change->newtuple;
		/* not a disk based tuple */
		ItemPointerSetInvalid(&tuple->tuple.t_self);

		xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(data);
		data = ((char *) xlhdr) + SizeOfMultiInsertTuple;
		datalen = xlhdr->datalen;

		/* we can only figure this out after reassembling the transactions */
		tuple->tuple.t_tableOid = InvalidOid;
		tuple->tuple.t_data = &tuple->header;
		tuple->tuple.t_len = datalen + offsetof(HeapTupleHeaderData, t_bits);

		memset(&tuple->header, 0, sizeof(HeapTupleHeaderData));

		memcpy((char *) &tuple->header + offsetof(HeapTupleHeaderData, t_bits),
			   (char *) data,
			   datalen);
		data += datalen;

		tuple->header.t_infomask = xlhdr->t_infomask;
		tuple->header.t_infomask2 = xlhdr->t_infomask2;
		tuple->header.t_hoff = xlhdr->t_hoff;

		ReorderBufferAddChange(reorder, r->xl_xid, buf->origptr, change);
	}
}


static void
DecodeXLogTuple(char *data, Size len, ReorderBufferTupleBuf *tuple)
{
	xl_heap_header xlhdr;
	int datalen = len - SizeOfHeapHeader;

	Assert(datalen >= 0);
	Assert(datalen <= MaxHeapTupleSize);

	tuple->tuple.t_len = datalen + offsetof(HeapTupleHeaderData, t_bits);

	/* not a disk based tuple */
	ItemPointerSetInvalid(&tuple->tuple.t_self);

	/* we can only figure this out after reassembling the transactions */
	tuple->tuple.t_tableOid = InvalidOid;
	tuple->tuple.t_data = &tuple->header;

	/* data is not stored aligned */
	memcpy((char *) &xlhdr,
		   data,
		   SizeOfHeapHeader);

	memset(&tuple->header, 0, sizeof(HeapTupleHeaderData));

	memcpy((char *) &tuple->header + offsetof(HeapTupleHeaderData, t_bits),
		   data + SizeOfHeapHeader,
		   datalen);

	tuple->header.t_infomask = xlhdr.t_infomask;
	tuple->header.t_infomask2 = xlhdr.t_infomask2;
	tuple->header.t_hoff = xlhdr.t_hoff;
}
