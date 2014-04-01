/*
 * contrib/fastbloat/fastbloat.c
 *
 * Copyright (c) 2001,2002	Tatsuo Ishii
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
 * struct fastbloat_type
 *
 * tuple_percent, dead_tuple_percent and free_percent are computable,
 * so not defined here.
 */
typedef struct fastbloat_type
{
	uint64		table_len;
	uint64		tuple_count;
	uint64		tuple_len;
	uint64		dead_tuple_count;
	uint64		dead_tuple_len;
	uint64		free_space;		/* free/reusable space in bytes */
} fastbloat_type;

static Datum build_fastbloat_type(fastbloat_type *stat,
					   FunctionCallInfo fcinfo);
static Datum fbstat_relation(Relation rel, FunctionCallInfo fcinfo);
static Datum fbstat_heap(Relation rel, FunctionCallInfo fcinfo);
static HTSV_Result
HeapTupleSatisfiesUs(HeapTuple htup, TransactionId OldestXmin);

/*
 * build_fastbloat_type -- build a fastbloat_type tuple
 */
static Datum
build_fastbloat_type(fastbloat_type *stat, FunctionCallInfo fcinfo)
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

	/*
	 * Prepare a values array for constructing the tuple. This should be an
	 * array of C strings which will be processed later by the appropriate
	 * "in" functions.
	 */
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

	/* build a tuple */
	tuple = BuildTupleFromCStrings(attinmeta, values);

	/* make the tuple into a datum */
	return HeapTupleGetDatum(tuple);
}

/* ----------
 * fastbloat:
 * returns live/dead tuples info
 *
 * C FUNCTION definition
 * fastbloat(text) returns fastbloat_type
 * see fastbloat.sql for fastbloat_type
 * ----------
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

	/* open relation */
	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	PG_RETURN_DATUM(fbstat_relation(rel, fcinfo));
}

Datum
fastbloatbyid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use fastbloat functions"))));

	/* open relation */
	rel = relation_open(relid, AccessShareLock);

	PG_RETURN_DATUM(fbstat_relation(rel, fcinfo));
}

/*
 * fbstat_relation
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
		case RELKIND_TOASTVALUE:
		case RELKIND_SEQUENCE:
			return fbstat_heap(rel, fcinfo);
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
	return 0;					/* should not happen */
}

/*
 * fbstat_heap -- returns live/dead tuples info in a heap
 */
static Datum
fbstat_heap(Relation rel, FunctionCallInfo fcinfo)
{
	BlockNumber scanned,
				nblocks,
				blkno;
	Buffer		vmbuffer = InvalidBuffer;
	HeapTupleData tuple;
	fastbloat_type stat = {0};
	BufferAccessStrategy bstrategy;
	TransactionId OldestXmin;

	bstrategy = GetAccessStrategy(BAS_BULKREAD);
	OldestXmin = GetOldestXmin(rel, true);

	scanned = 0;
	nblocks = RelationGetNumberOfBlocks(rel);

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offnum,
					maxoff;
		Size		freespace;

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

		if (PageIsEmpty(page))
		{
			stat.free_space += PageGetHeapFreeSpace(page);
			ReleaseBuffer(buf);
			continue;
		}

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

			switch (HeapTupleSatisfiesUs(&tuple, OldestXmin))
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

		stat.free_space += PageGetHeapFreeSpace(page);

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

	return build_fastbloat_type(&stat, fcinfo);
}

/* This is a copy of HeapTupleSatisfiesVacuum with all the hint-bit
 * setting code removed. See HTSV for comments.
 */

static HTSV_Result
HeapTupleSatisfiesUs(HeapTuple htup, TransactionId OldestXmin)
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
