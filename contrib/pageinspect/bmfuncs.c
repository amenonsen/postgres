/*
 * bmfuncs.c
 *	  Functions to investigate bitmap index pages.
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pageinspect/bmfuncs.c
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/bitmap.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "catalog/pg_am.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"

extern Datum bm_metap(PG_FUNCTION_ARGS);
extern Datum bm_page_headers(PG_FUNCTION_ARGS);
extern Datum bm_lov_page_stats(PG_FUNCTION_ARGS);
extern Datum bm_bmv_page_stats(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bm_metap);
PG_FUNCTION_INFO_V1(bm_page_headers);
PG_FUNCTION_INFO_V1(bm_lov_page_stats);
PG_FUNCTION_INFO_V1(bm_bmv_page_stats);

#define IS_INDEX(r) ((r)->rd_rel->relkind == RELKIND_INDEX)
#define IS_BITMAP(r) ((r)->rd_rel->relam == BITMAP_AM_OID)

#define CHECK_PAGE_OFFSET_RANGE(pg, offnum) { \
		if ( !(FirstOffsetNumber <= (offnum) && \
						(offnum) <= PageGetMaxOffsetNumber(pg)) ) \
			 elog(ERROR, "page offset number out of range"); }

/* note: BlockNumber is unsigned, hence can't be negative */
#define CHECK_RELATION_BLOCK_RANGE(rel, blkno) { \
		if ( RelationGetNumberOfBlocks(rel) <= (BlockNumber) (blkno) ) \
			 elog(ERROR, "block number %d out of range", blkno); }

/*
 * cross-call data structure for page headers SRF
 */
struct bm_page_headers_args
{
    Relation rel;
    uint32 blockNum;
};

/*-------------------------------------------------------
 * bm_page_headers()
 *
 * Get the page headers of all the pages of the index
 *
 * Usage: SELECT * FROM bm_page_headers('bm_idx');
 *-------------------------------------------------------
 */

Datum
bm_page_headers(PG_FUNCTION_ARGS)
{
    text *relname = PG_GETARG_TEXT_P(0);
    FuncCallContext *fctx;
    struct bm_page_headers_args *uargs;
    char* values[7];
    Datum result = 0;

    if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				(errmsg("must be superuser to use pageinspect functions"))));

    if (SRF_IS_FIRSTCALL())
    {
		MemoryContext mctx;
		TupleDesc tupleDesc;
		RangeVar* relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
		Relation rel;

		fctx = SRF_FIRSTCALL_INIT();

		rel = relation_openrv(relrv, AccessShareLock);

		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		uargs = palloc(sizeof(struct bm_page_headers_args));

		uargs->rel = rel;
		uargs->blockNum = 0;

		if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		fctx->attinmeta = TupleDescGetAttInMetadata(tupleDesc);
		fctx->user_fctx = uargs;
		fctx->max_calls = RelationGetNumberOfBlocks(rel);

		MemoryContextSwitchTo(mctx);
    }

    fctx = SRF_PERCALL_SETUP();
    uargs = fctx->user_fctx;

    if (fctx->call_cntr < fctx->max_calls)
    {
		Buffer buffer;
		HeapTuple tuple;
		Page page;
		PageHeader phdr;
		uint32 page_size;
		uint32 free_size;
		int j = 0;

		if (!IS_INDEX(uargs->rel) || !IS_BITMAP(uargs->rel))
			elog(ERROR, "relation \"%s\" is not a bitmap index",
				 RelationGetRelationName(uargs->rel));

		CHECK_RELATION_BLOCK_RANGE(uargs->rel, uargs->blockNum);

		buffer = ReadBuffer(uargs->rel, uargs->blockNum);

		page = BufferGetPage(buffer);
		phdr = (PageHeader) page;
		page_size = PageGetPageSize(page);
		free_size = PageGetFreeSpace(page);

		values[j] = palloc(32);
		snprintf(values[j++], 32, "%d", uargs->blockNum);
		values[j] = palloc(32);

		if (uargs->blockNum == 0)
			snprintf(values[j++], 32, "META");
		else if (page_size == phdr->pd_special)
			snprintf(values[j++], 32, "LOV");
		else
			snprintf(values[j++], 32, "BMV");

		values[j] = palloc(32);
		snprintf(values[j++], 32, "%d", page_size);
		values[j] = palloc(32);
		snprintf(values[j++], 32, "%d", phdr->pd_lower);
		values[j] = palloc(32);
		snprintf(values[j++], 32, "%d", phdr->pd_upper);
		values[j] = palloc(32);
		snprintf(values[j++], 32, "%d", phdr->pd_special);
		values[j] = palloc(32);
		snprintf(values[j++], 32, "%d", free_size);

		++uargs->blockNum;

		ReleaseBuffer(buffer);

		tuple = BuildTupleFromCStrings(fctx->attinmeta, values);

		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(fctx, result);
    }
    else
    {
		relation_close(uargs->rel, AccessShareLock);
		pfree(uargs);
		SRF_RETURN_DONE(fctx);
    }
}

/* ------------------------------------------------
 * bm_metap()
 *
 * Get a bitmap index's meta-page information
 *
 * Usage: SELECT * FROM bm_metap('t1_bmkey')
 * ------------------------------------------------
 */

Datum
bm_metap(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_P(0);
	Datum result;
	Relation rel;
	RangeVar *relrv;
	BMMetaPage metad;
	TupleDesc tupleDesc;
	int j;
	char *values[3];
	Buffer buffer;
	Page page;
	HeapTuple tuple;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				(errmsg("must be superuser to use pageinspect functions"))));

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	if (!IS_INDEX(rel) || !IS_BITMAP(rel))
		elog(ERROR, "relation \"%s\" is not a bitmap index",
			 RelationGetRelationName(rel));

	buffer = ReadBuffer(rel, BM_METAPAGE);
	page = BufferGetPage(buffer);
	metad = (BMMetaPage) PageGetContents(page);

	j = 0;
	values[j] = palloc(32);
	snprintf(values[j++], 32, "%d", metad->bm_lov_heapId);
	values[j] = palloc(32);
	snprintf(values[j++], 32, "%d", metad->bm_lov_indexId);
	values[j] = palloc(32);
	snprintf(values[j++], 32, "%d", metad->bm_last_vmi_page);

	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc),
								   values);
	result = HeapTupleGetDatum(tuple);

	ReleaseBuffer(buffer);

	relation_close(rel, AccessShareLock);

	PG_RETURN_DATUM(result);
}

/*
 * structure for statistics regarding a single LOV page
 */

typedef struct BMLOVPageStat
{
    uint32 blkno;
    uint32 page_size;
    uint32 free_size;
    uint32 max_avail;

    uint32 live_items;
    uint32 dead_items;
    uint32 avg_item_size;
} BMLOVPageStat;

/* -----------------------------------------------
 * bm_lov_page_stats()
 *
 * Usage: SELECT * FROM bm_lov_page_stats('bm_idx');
 * -----------------------------------------------
 */

Datum
bm_lov_page_stats(PG_FUNCTION_ARGS)
{
    text *relname = PG_GETARG_TEXT_P(0);
    uint32 blkno = PG_GETARG_UINT32(1);
    Relation rel;
    RangeVar *relrv;
    Buffer buffer;
    Page page;
    PageHeader phdr;

    BMLOVPageStat stat;
    char *values[7];
    HeapTuple tuple;
    TupleDesc tupleDesc;
    int j = 0;
    Datum result;
    int item_size = 0;
    OffsetNumber maxoff = FirstOffsetNumber;
    OffsetNumber off = FirstOffsetNumber;

    if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				(errmsg("must be superuser to use pageinspect functions"))));

    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

    relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
    rel = relation_openrv(relrv, AccessShareLock);

    if (!IS_INDEX(rel) || !IS_BITMAP(rel))
		elog(ERROR, "relation \"%s\" is not a bitmap index",
			 RelationGetRelationName(rel));

    if (blkno == BM_METAPAGE)
		elog(ERROR, "block %d is a meta page", BM_METAPAGE);

    CHECK_RELATION_BLOCK_RANGE(rel, blkno);

    buffer = ReadBuffer(rel, blkno);
    page = BufferGetPage(buffer);
    phdr = (PageHeader) page;

    /* Initialise the data to be returned */
    stat.blkno = blkno;
    stat.page_size = PageGetPageSize(page);
    stat.free_size = PageGetFreeSpace(page);
    stat.max_avail = stat.live_items = stat.dead_items = stat.avg_item_size = 0;

    if (phdr->pd_special != stat.page_size)
		elog(ERROR, "block %d is a not a LOV page", blkno);

    maxoff = PageGetMaxOffsetNumber(page);

    /* count live and dead tuples, and free space */
    for (off = FirstOffsetNumber; off <= maxoff; ++off)
    {
		ItemId id = PageGetItemId(page, off);
		IndexTuple itup = (IndexTuple) PageGetItem(page, id);

		item_size += IndexTupleSize(itup);

		if (!ItemIdIsDead(id))
			stat.live_items++;
		else
			stat.dead_items++;
    }

    if ((stat.live_items + stat.dead_items) > 0)
		stat.avg_item_size = item_size / (stat.live_items + stat.dead_items);

    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.blkno);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.page_size);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.free_size);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.max_avail);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.live_items);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.dead_items);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.avg_item_size);

    tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc), values);

    result = HeapTupleGetDatum(tuple);

    ReleaseBuffer(buffer);

    relation_close(rel, AccessShareLock);

    PG_RETURN_DATUM(result);
}

/*
 * structure for statistics regarding a single bitmap page
 */

typedef struct BMBMVPageStat
{
    uint32 blkno;
    uint32 page_size;
    uint32 free_size;

    /* opaque data */
    uint16          bm_hrl_words_used;      /* the number of words used */
    BlockNumber     bm_bitmap_next;         /* the next page for this bitmap */
    uint64          bm_last_tid_location; /* the tid location for the last bit in this page */
    uint16          bm_page_id; /* bitmap index identifier */

} BMBMVPageStat;

/* -----------------------------------------------
 * bm_bmv_page_stats()
 *
 * Usage: SELECT * FROM bm_bmv_page_stats('bm_idx');
 * -----------------------------------------------
 */

Datum
bm_bmv_page_stats(PG_FUNCTION_ARGS)
{
    text *relname = PG_GETARG_TEXT_P(0);
    uint32 blkno = PG_GETARG_UINT32(1);
    Relation rel;
    RangeVar *relrv;
    Buffer buffer;
    Page page;
    PageHeader phdr;
    BMPageOpaque opaque = 0;

    BMBMVPageStat stat;
    char *values[7];
    HeapTuple tuple;
    TupleDesc tupleDesc;
    int j = 0;
    Datum result;

    if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				(errmsg("must be superuser to use pageinspect functions"))));

    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

    relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
    rel = relation_openrv(relrv, AccessShareLock);

    if (!IS_INDEX(rel) || !IS_BITMAP(rel))
		elog(ERROR, "relation \"%s\" is not a bitmap index",
			 RelationGetRelationName(rel));

    if (blkno == BM_METAPAGE)
		elog(ERROR, "block %d is a meta page", BM_METAPAGE);

    CHECK_RELATION_BLOCK_RANGE(rel, blkno);

    buffer = ReadBuffer(rel, blkno);
    page = BufferGetPage(buffer);
    phdr = (PageHeader) page;
    opaque = (BMPageOpaque) PageGetSpecialPointer(page);

    /* Initialise the data to be returned */
    stat.blkno = blkno;
    stat.page_size = PageGetPageSize(page);
    stat.free_size = PageGetFreeSpace(page);

    if (phdr->pd_special == stat.page_size)
		elog(ERROR, "block %d is a not a BMV page", blkno);

    stat.bm_hrl_words_used = opaque->bm_hrl_words_used;
    stat.bm_bitmap_next = opaque->bm_bitmap_next;
    stat.bm_last_tid_location = opaque->bm_last_tid_location;
    stat.bm_page_id = opaque->bm_page_id;

    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.blkno);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.page_size);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.free_size);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.bm_hrl_words_used);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.bm_bitmap_next);
    values[j] = palloc(32);
    snprintf(values[j++], 32, UINT64_FORMAT, stat.bm_last_tid_location);
    values[j] = palloc(32);
    snprintf(values[j++], 32, "%d", stat.bm_page_id);

    tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc), values);

    result = HeapTupleGetDatum(tuple);

    ReleaseBuffer(buffer);

    relation_close(rel, AccessShareLock);

    PG_RETURN_DATUM(result);
}
