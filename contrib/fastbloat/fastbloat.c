/*
 * contrib/fastbloat/fastbloat.c
 *
 * Abhijit Menon-Sen <ams@2ndQuadrant.com>
 * Portions Copyright (c) 2001,2002	Tatsuo Ishii (from pg_stattuple)
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose, without fee, and without a
 * written agreement is hereby granted, provided that the above
 * copyright notice and this paragraph and the following two
 * paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
 * IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

#include "postgres.h"

#include "access/visibilitymap.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/multixact.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/tqual.h"
#include "commands/vacuum.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(fastbloat);
PG_FUNCTION_INFO_V1(fastbloatbyid);

extern Datum fastbloat(PG_FUNCTION_ARGS);
extern Datum fastbloatbyid(PG_FUNCTION_ARGS);

/*
 * tuple_percent, dead_tuple_percent and free_percent are computable,
 * so not defined here.
 */
typedef struct fastbloat_output_type
{
	uint64		table_len;
	uint64		tuple_count;
	uint64		tuple_len;
	uint64		dead_tuple_count;
	uint64		dead_tuple_len;
	uint64		free_space;
} fastbloat_output_type;

static Datum build_output_type(fastbloat_output_type *stat,
							   FunctionCallInfo fcinfo);
static Datum fbstat_relation(Relation rel, FunctionCallInfo fcinfo);
static Datum fbstat_heap(Relation rel, FunctionCallInfo fcinfo);
static HTSV_Result
HeapTupleSatisfiesVacuumNoHint(HeapTuple htup, TransactionId OldestXmin);

/*
 * build a fastbloat_output_type tuple
 */
static Datum
build_output_type(fastbloat_output_type *stat, FunctionCallInfo fcinfo)
{
#define NCOLUMNS	9
#define NCHARS		32

	HeapTuple	tuple;
	char	   *values[NCOLUMNS];
	char		values_buf[NCOLUMNS][NCHARS];
	int			i;
	double		tuple_percent;
	double		dead_tuple_percent;
	double		free_percent;	/* free/reusable space in % */
	TupleDesc	tupdesc;
	AttInMetadata *attinmeta;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/*
	 * Generate attribute metadata needed later to produce tuples from raw C
	 * strings
	 */
	attinmeta = TupleDescGetAttInMetadata(tupdesc);

	if (stat->table_len == 0)
	{
		tuple_percent = 0.0;
		dead_tuple_percent = 0.0;
		free_percent = 0.0;
	}
	else
	{
		tuple_percent = 100.0 * stat->tuple_len / stat->table_len;
		dead_tuple_percent = 100.0 * stat->dead_tuple_len / stat->table_len;
		free_percent = 100.0 * stat->free_space / stat->table_len;
	}

	for (i = 0; i < NCOLUMNS; i++)
		values[i] = values_buf[i];
	i = 0;
	snprintf(values[i++], NCHARS, INT64_FORMAT, stat->table_len);
	snprintf(values[i++], NCHARS, INT64_FORMAT, stat->tuple_count);
	snprintf(values[i++], NCHARS, INT64_FORMAT, stat->tuple_len);
	snprintf(values[i++], NCHARS, "%.2f", tuple_percent);
	snprintf(values[i++], NCHARS, INT64_FORMAT, stat->dead_tuple_count);
	snprintf(values[i++], NCHARS, INT64_FORMAT, stat->dead_tuple_len);
	snprintf(values[i++], NCHARS, "%.2f", dead_tuple_percent);
	snprintf(values[i++], NCHARS, INT64_FORMAT, stat->free_space);
	snprintf(values[i++], NCHARS, "%.2f", free_percent);

	tuple = BuildTupleFromCStrings(attinmeta, values);

	return HeapTupleGetDatum(tuple);
}

/* Returns a tuple with live/dead tuple statistics for the named table.
 */

Datum
fastbloat(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_P(0);
	RangeVar   *relrv;
	Relation	rel;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use fastbloat functions"))));

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	PG_RETURN_DATUM(fbstat_relation(rel, fcinfo));
}

/* As above, but takes a reloid instead of a relation name.
 */

Datum
fastbloatbyid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use fastbloat functions"))));

	rel = relation_open(relid, AccessShareLock);

	PG_RETURN_DATUM(fbstat_relation(rel, fcinfo));
}

/*
 * A helper function to reject unsupported relation types. We depend on
 * the visibility map to decide which pages we can skip, so we can't
 * support indexes, for example, which don't have a VM.
 */

static Datum
fbstat_relation(Relation rel, FunctionCallInfo fcinfo)
{
	const char *err;

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	switch (rel->rd_rel->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
			return fbstat_heap(rel, fcinfo);
		case RELKIND_TOASTVALUE:
			err = "toast value";
			break;
		case RELKIND_SEQUENCE:
			err = "sequence";
			break;
		case RELKIND_INDEX:
			err = "index";
			break;
		case RELKIND_VIEW:
			err = "view";
			break;
		case RELKIND_COMPOSITE_TYPE:
			err = "composite type";
			break;
		case RELKIND_FOREIGN_TABLE:
			err = "foreign table";
			break;
		default:
			err = "unknown";
			break;
	}

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("\"%s\" (%s) is not supported",
					RelationGetRelationName(rel), err)));
	return 0;
}

/*
 * This function takes an already open relation and scans its pages,
 * skipping those that have the corresponding visibility map bit set.
 * For pages we skip, we find the free space from the free space map
 * and approximate tuple_len on that basis. For the others, we count
 * the exact number of dead tuples etc.
 *
 * This scan is loosely based on vacuumlazy.c:lazy_scan_heap(), but
 * we do not try to avoid skipping single pages.
 */

static Datum
fbstat_heap(Relation rel, FunctionCallInfo fcinfo)
{
	BlockNumber scanned,
				nblocks,
				blkno;
	Buffer		vmbuffer = InvalidBuffer;
	HeapTupleData tuple;
	fastbloat_output_type stat = {0};
	BufferAccessStrategy bstrategy;
	TransactionId OldestXmin;

	OldestXmin = GetOldestXmin(rel, true);
	bstrategy = GetAccessStrategy(BAS_BULKREAD);

	scanned = 0;
	nblocks = RelationGetNumberOfBlocks(rel);

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offnum,
					maxoff;
		Size		freespace;

		/*
		 * If the page has only visible tuples, then we can find out the
		 * free space from the FSM and move on. The remainder of space
		 * on the page (including that used by line pointers)
		 */

		if (visibilitymap_test(rel, blkno, &vmbuffer))
		{
			freespace = GetRecordedFreeSpace(rel, blkno);
			stat.tuple_len += BLCKSZ - freespace;
			stat.free_space += freespace;
			continue;
		}

		scanned++;

		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, bstrategy);

		page = BufferGetPage(buf);

		if (PageIsNew(page))
		{
			ReleaseBuffer(buf);
			continue;
		}

		stat.free_space += PageGetHeapFreeSpace(page);

		/*
		 * Look at each tuple on the page and decide whether it's live
		 * or dead, then count it and its size. Unlike lazy_scan_heap,
		 * we can afford to ignore problems and special cases. We also
		 * do not need to hold a content lock on the buffer because of
		 * our non-hint-bit-setting copy of HeapTupleSatisfiesVacuum.
		 */

		maxoff = PageGetMaxOffsetNumber(page);

		for (offnum = FirstOffsetNumber;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid;

			itemid = PageGetItemId(page, offnum);

			if (!ItemIdIsUsed(itemid) || ItemIdIsRedirected(itemid) ||
				ItemIdIsDead(itemid))
			{
				continue;
			}

			Assert(ItemIdIsNormal(itemid));

			ItemPointerSet(&(tuple.t_self), blkno, offnum);

			tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(rel);

			switch (HeapTupleSatisfiesVacuumNoHint(&tuple, OldestXmin))
			{
				case HEAPTUPLE_DEAD:
				case HEAPTUPLE_RECENTLY_DEAD:
					stat.dead_tuple_len += tuple.t_len;
					stat.dead_tuple_count++;
					break;
				case HEAPTUPLE_LIVE:
					stat.tuple_len += tuple.t_len;
					stat.tuple_count++;
					break;
				case HEAPTUPLE_INSERT_IN_PROGRESS:
				case HEAPTUPLE_DELETE_IN_PROGRESS:
					break;
				default:
					elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
					break;
			}
		}

		ReleaseBuffer(buf);
	}

	stat.table_len = (uint64) nblocks * BLCKSZ;
	stat.tuple_count = vac_estimate_reltuples(rel, false, nblocks, scanned,
											  stat.tuple_count);

	if (BufferIsValid(vmbuffer))
	{
		ReleaseBuffer(vmbuffer);
		vmbuffer = InvalidBuffer;
	}

	relation_close(rel, AccessShareLock);

	return build_output_type(&stat, fcinfo);
}

/*
 * This is a copy of HeapTupleSatisfiesVacuum with all the hint-bit
 * setting code removed (and thus, some tests reversed). We use this
 * so that we don't need to hold any content locks on the buffer while
 * scanning for dead tuples. (We could use HeapTupleIsSurelyDead(), but
 * that's unnecessarily conservative.)
 *
 * See HTSV for comments.
 */

static HTSV_Result
HeapTupleSatisfiesVacuumNoHint(HeapTuple htup, TransactionId OldestXmin)
{
	HeapTupleHeader tuple = htup->t_data;
	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return HEAPTUPLE_DEAD;
		else if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			if (TransactionIdIsInProgress(xvac))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			if (TransactionIdDidCommit(xvac))
			{
				return HEAPTUPLE_DEAD;
			}
		}
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (TransactionIdIsInProgress(xvac))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (!TransactionIdDidCommit(xvac))
			{
				return HEAPTUPLE_DEAD;
			}
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) ||
				HeapTupleHeaderIsOnlyLocked(tuple))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		}
		else if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
		{
			return HEAPTUPLE_DEAD;
		}
	}

	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return HEAPTUPLE_LIVE;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
		{
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				if ((tuple->t_infomask & (HEAP_XMAX_EXCL_LOCK |
										  HEAP_XMAX_KEYSHR_LOCK)) &&
					MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple)))
					return HEAPTUPLE_LIVE;
			}
			else
			{
				if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
					return HEAPTUPLE_LIVE;
			}
		}

		return HEAPTUPLE_LIVE;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple)))
		{
			Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

			xmax = HeapTupleGetUpdateXid(tuple);

			Assert(TransactionIdIsValid(xmax));

			if (TransactionIdIsInProgress(xmax))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			else if (TransactionIdDidCommit(xmax))
				return HEAPTUPLE_RECENTLY_DEAD;
			return HEAPTUPLE_LIVE;
		}

		Assert(!(tuple->t_infomask & HEAP_XMAX_COMMITTED));

		xmax = HeapTupleGetUpdateXid(tuple);

		Assert(TransactionIdIsValid(xmax));

		Assert(!TransactionIdIsInProgress(xmax));
		if (TransactionIdDidCommit(xmax))
		{
			if (!TransactionIdPrecedes(xmax, OldestXmin))
				return HEAPTUPLE_RECENTLY_DEAD;
			else
				return HEAPTUPLE_DEAD;
		}

		return HEAPTUPLE_LIVE;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		else if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
		{
			return HEAPTUPLE_LIVE;
		}
	}

	if (!TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin))
		return HEAPTUPLE_RECENTLY_DEAD;

	return HEAPTUPLE_DEAD;
}
