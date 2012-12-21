/*-------------------------------------------------------------------------
 *
 * bitmappage.c
 *	  Bitmap index page management code for the bitmap index.
 *
 * Copyright (c) 2007, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/tupdesc.h"
#include "access/bitmap.h"
#include "parser/parse_oper.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "storage/bufmgr.h" /* for buffer manager functions */
#include "utils/tqual.h" /* for SnapshotAny */
#include "catalog/index.h"
#include "catalog/pg_collation.h"

/* 
 * Helper functions for hashing and matching build data. At this stage, the
 * hash API doesn't know about complex keys like those use during index
 * creation (the key is an array of key attributes). c.f. execGrouping.c.
 */
typedef struct BMBuildHashData
{
	int			natts;
	FmgrInfo   *hash_funcs;
	FmgrInfo   *eq_funcs;
	MemoryContext tmpcxt;
	MemoryContext hash_cxt;
} BMBuildHashData;

static BMBuildHashData *cur_bmbuild = NULL;

static void _bitmap_build_lovindex(BMBuildState *bmstate,
								   IndexInfo *indexInfo);
static uint32 build_hash_key(const void *key, Size keysize);
static int build_match_key(const void *key1, const void *key2, Size keysize);

/*
 * _bitmap_getbuf() -- return the buffer for the given block number and
 * 					   the access method.
 */
Buffer
_bitmap_getbuf(Relation rel, BlockNumber blkno, int access)
{
	Buffer buf;

	buf = ReadBuffer(rel, blkno);
	if (access != BM_NOLOCK)
		LockBuffer(buf, access);

	return buf;
}

/*
 * _bitmap_wrtbuf() -- write a buffer page to disk.
 *
 * Release the lock and the pin held on the buffer.
 */
void
_bitmap_wrtbuf(Buffer buf)
{
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
}

/*
 * _bitmap_wrtnorelbuf() -- write a buffer page to disk without still holding
 *		the pin on this page.
 */
void
_bitmap_wrtnorelbuf(Buffer buf)
{
	MarkBufferDirty(buf);
}

/*
 * _bitmap_relbuf() -- release the buffer without writing.
 */
void
_bitmap_relbuf(Buffer buf)
{
	UnlockReleaseBuffer(buf);
}

/*
 * _bitmap_init_vmipage -- initialize a new VMI page.
 */
void
_bitmap_init_vmipage(Buffer buf)
{
	Page page;

	page = (Page) BufferGetPage(buf);

	if (PageIsNew(page))
		PageInit(page, BufferGetPageSize(buf), 0);
}

/*
 * _bitmap_init_bitmappage() -- initialize a new page to store the bitmap.
 */
void
_bitmap_init_bitmappage(Buffer buf)
{

    Page page; /* temporary variable for identifying the buffer page */
    BMPageOpaque opaque; /* bitmap page */

    /* Get the buffer's page */
    page = (Page) BufferGetPage(buf);

    /* If the buffer page is new, we initialise the special space of the page
     * with the the "opaque" structure of the index page. The second argument of
     * PageInit is the sice of the page, the third one of the special area */
    if(PageIsNew(page))
	PageInit(page, BufferGetPageSize(buf), sizeof(BMPageOpaqueData));

    /* Reset all the values (even if the page is not new) */
    opaque = (BMPageOpaque) PageGetSpecialPointer(page);
    opaque->bm_hrl_words_used = 0;
    opaque->bm_bitmap_next = InvalidBlockNumber;
    opaque->bm_last_tid_location = 0;
    opaque->bm_page_id = BM_PAGE_ID;

}

/*
 * _bitmap_init_buildstate() -- initialize the build state before building
 *	a bitmap index.
 */
void
_bitmap_init_buildstate(Relation index, BMBuildState *bmstate,
						IndexInfo *indexInfo)
{
	/* BitMap Index Meta Page (first page of the index) */
	BMMetaPage	mp;

	/* Buffer and page management */
	Page		page;			/* temporary page variable */
	Buffer		metabuf;		/* META information buffer */

	int attno;

	/*
	 * Initialise the BMBuildState structure which will hold information
	 * about the state for the index build process
	 */
	bmstate->bm_tupDesc = RelationGetDescr(index); /* index tuples description */
	bmstate->ituples = 0;
	/* allocate the index build buffer and ... */
	bmstate->bm_tidLocsBuffer = (BMTidBuildBuf *)
		palloc(sizeof(BMTidBuildBuf));
	bmstate->bm_tidLocsBuffer->byte_size = 0; /* ... initialises it */
	bmstate->bm_tidLocsBuffer->vmi_blocks = NIL;
	bmstate->bm_tidLocsBuffer->max_vmi_block = InvalidBlockNumber;

	/* Get the meta page */
	metabuf = _bitmap_getbuf(index, BM_METAPAGE, BM_READ);
	page = BufferGetPage(metabuf);
	mp = (BMMetaPage) PageGetContents(page);

	/* Open the heap and the index in row exclusive mode */
	_bitmap_open_lov_heapandindex(mp, &(bmstate->bm_lov_heap),
								  &(bmstate->bm_lov_index),
								  RowExclusiveLock);

	/* release the buffer */
	_bitmap_relbuf(metabuf);

	/*
	 * Initialise the static variable cur_bmbuild with the helper functions for hashing
	 * and matching build data. One per index attribute.
	 */
	cur_bmbuild = (BMBuildHashData *) palloc(sizeof(BMBuildHashData));
	cur_bmbuild->hash_funcs = (FmgrInfo *)
		palloc(sizeof(FmgrInfo) * bmstate->bm_tupDesc->natts);
	cur_bmbuild->eq_funcs = (FmgrInfo *)
		palloc(sizeof(FmgrInfo) * bmstate->bm_tupDesc->natts);

	/* Iterate through the index attributes and initialise the helper functions */
	for (attno = 0; attno < bmstate->bm_tupDesc->natts; ++attno)
	{
		Oid		typid = bmstate->bm_tupDesc->attrs[attno]->atttypid;
		Oid		eq_opr;					/* equality operator */
		Oid		eq_function;			/* equality operator function */
		Oid		left_hash_function;		/* left hash function */
		Oid		right_hash_function;	/* right hash function */

		/* Get the equality operator OID */
		get_sort_group_operators(typid, false, true, false,
								 NULL, &eq_opr, NULL, NULL);

		/* Get the eq and hash operator functions */
		eq_function = get_opcode(eq_opr);
		if (!get_op_hash_functions(eq_opr, &left_hash_function,
								   &right_hash_function))
		{
			pfree(cur_bmbuild);
			cur_bmbuild = NULL;
			break;
		}

		fmgr_info(eq_function, &cur_bmbuild->eq_funcs[attno]);
		fmgr_info(right_hash_function, &cur_bmbuild->hash_funcs[attno]);
	}

	/* We found the hash functions for every attribute of the index */
	if (cur_bmbuild)
	{
		/* Hash management */
		HASHCTL hash_ctl;
		int		hash_flags;

		/* Allocate the temporary memory context */
		cur_bmbuild->natts = bmstate->bm_tupDesc->natts;
		cur_bmbuild->tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
													"Bitmap build temp space",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

		/* Setup the hash table and map it into the build state variable */
		MemSet(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(Datum) * cur_bmbuild->natts;
		hash_ctl.entrysize = hash_ctl.keysize + sizeof(BMVMIID) + 200;
		hash_ctl.hash = build_hash_key;
		hash_ctl.match = build_match_key;
		hash_ctl.hcxt = AllocSetContextCreate(CurrentMemoryContext,
											  "Bitmap build hash table",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);
		cur_bmbuild->hash_cxt = hash_ctl.hcxt;

		hash_flags = HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT;

		/* Create the hash table */
		bmstate->vmi_hash = hash_create("Bitmap index build lov item hash",
										100, &hash_ctl, hash_flags);
	}
	else
	{
		/*
		 * Contingency plan: no hash functions can be used and we have to
		 * search through the btree
		 */
		bmstate->vmi_hash = NULL;

		/* so build the LOV index now, so it can be filled with every tuple */
		_bitmap_build_lovindex(bmstate, indexInfo);

		bmstate->bm_lov_scanKeys =
			(ScanKey) palloc0(bmstate->bm_tupDesc->natts * sizeof(ScanKeyData));

		for (attno = 0; attno < bmstate->bm_tupDesc->natts; ++attno)
		{
			RegProcedure		opfuncid;
			Oid					eq_opr;	/* equality operator */
			/* Get the equality operator's function */
			Oid					atttypid =
				bmstate->bm_tupDesc->attrs[attno]->atttypid;

			get_sort_group_operators(atttypid, false, true, false,
									 NULL, &eq_opr, NULL, NULL);
			opfuncid = get_opcode(eq_opr);

			/* Initialise the scan key using a btree */
			ScanKeyEntryInitialize(&(bmstate->bm_lov_scanKeys[attno]), SK_ISNULL,
								   attno + 1, BTEqualStrategyNumber, InvalidOid,
								   DEFAULT_COLLATION_OID, opfuncid, 0);
		}

		bmstate->bm_lov_scanDesc = index_beginscan(bmstate->bm_lov_heap,
												   bmstate->bm_lov_index,
												   SnapshotAny,
												   bmstate->bm_tupDesc->natts,
												   0);
	}

	/*
	 * We need to log index creation in WAL iff WAL archiving is enabled
	 * AND it's not a temp index. Currently, since building an index
	 * writes page to the shared buffer, we can't disable WAL archiving.
	 * We will add this shortly.
	 */
	bmstate->use_wal = XLogArchivingActive() && RelationNeedsWAL(index);

	/* initialize HOT prebuffer data */
#ifdef DEBUG_BMI
	elog(NOTICE,"-[_bitmap_init_buildstate]--------- CP 0");
#endif
	bmstate->hot_prebuffer_block = InvalidBlockNumber;
#if 0
	MemSet(bmstate->hot_prebuffer_tdn, 0, BM_MAX_HTUP_PER_PAGE * sizeof(uint64));
#else
	{ /* misteriously, MemSet segfaults... :( */
		int i;
		for(i = 0; i < BM_MAX_HTUP_PER_PAGE; i++) {
			bmstate->hot_prebuffer_tdn[i] = (uint64) 0;
#ifdef DEBUG_BMI
			elog(NOTICE,"[_bitmap_init_buildstate]: i == %d",i);
#endif
		}
    }
#endif
	bmstate->hot_prebuffer_count = 0;
#ifdef DEBUG_BMI
	elog(NOTICE,"-[_bitmap_init_buildstate]--------- CP 99");
#endif
}

/*
 * _bitmap_cleanup_buildstate() -- clean up the build state after
 *	inserting all rows in the heap into the bitmap index.
 */
void
_bitmap_cleanup_buildstate(Relation index, BMBuildState *bmstate,
						   IndexInfo *indexInfo)
{
	/* write out remaining tids in bmstate->bm_tidLocsBuffer */
	BMTidBuildBuf *tidLocsBuffer = bmstate->bm_tidLocsBuffer;

#ifdef DEBUG_BMI
	elog(NOTICE,"-----[_bitmap_cleanup_buildstate]----- BEGIN");
#endif
#ifdef FIX_GC_3
	build_inserttuple_flush(index,bmstate);
#endif
#ifdef DEBUG_BMI
	elog(NOTICE,"-----[_bitmap_cleanup_buildstate]----- CP1");
#endif

	_bitmap_write_alltids(index, tidLocsBuffer, bmstate->use_wal);

	pfree(bmstate->bm_tidLocsBuffer);

	if (cur_bmbuild)
	{
		MemoryContextDelete(cur_bmbuild->tmpcxt);
		MemoryContextDelete(cur_bmbuild->hash_cxt);
		pfree(cur_bmbuild->hash_funcs);
		pfree(cur_bmbuild->eq_funcs);
		pfree(cur_bmbuild);
		cur_bmbuild = NULL;

		/* now fire the deferred index build for the list of values */
		_bitmap_build_lovindex(bmstate, indexInfo);
	}
	else
	{
		/*
		 * We might have build an index on a non-hashable data type, in which
		 * case we will have searched the btree manually. Free associated
		 * memory.
		 */
		index_endscan(bmstate->bm_lov_scanDesc);
		pfree(bmstate->bm_lov_scanKeys);
	}

	_bitmap_close_lov_heapandindex(bmstate->bm_lov_heap,bmstate->bm_lov_index,
								   RowExclusiveLock);
#ifdef DEBUG_BMI
	elog(NOTICE,"-----[_bitmap_cleanup_buildstate]----- END");
#endif
}

/*
 * _bitmap_init() -- initialize the bitmap index.
 *
 * Create the meta page, a new heap which stores the distinct values for
 * the attributes to be indexed, a btree index on this new heap for searching
 * those distinct values, and the first VMI page.
 */
void
_bitmap_init(Relation index, bool use_wal)
{
	/*
	 * BitMap Index Meta Page (first page of the index) and first VMI
	 */

	/* BitMap Index Meta Page (first page of the index) */
	BMMetaPage metapage;
	/* First item in the VMI page (set to be NULL) */
	BMVectorMetaItem vmi;

	/*
	 * Buffer and page management
	 */

	Page page; /* temporary page variable */
	Buffer metabuf; /* META information buffer */
	Buffer vmibuf; /* VMI buffer */
	OffsetNumber vmiOffset; /* First VMI page offset */
	OffsetNumber o; /* temporary offset */

	/* Sanity check (the index MUST be empty) */
	if (RelationGetNumberOfBlocks(index) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("cannot initialize non-empty bitmap index \"%s\"",
						RelationGetRelationName(index))));

	/*
	 * The first step is to create the META page for the BitMap index, which
	 * contains some meta-data information about the BM index. The META page
	 * MUST ALWAYS be the first page (or page 0) and it is identified by the
	 * macro BM_METAPAGE
	 */

	/* get a new buffer for the index (META buffer) */
	metabuf = _bitmap_getbuf(index, P_NEW, BM_WRITE);
	/* set the page associated with the META buffer */
	page = BufferGetPage(metabuf);
	/* check that the page is new */
	Assert(PageIsNew(page));

	START_CRIT_SECTION();

	/* marks the META buffer contents as dirty (uninitialised) */
	MarkBufferDirty(metabuf);

	/* Initialise the page by setting its opaque fields (am duty) */
	_bitmap_init_bitmappage(metabuf);

	/* Get the content of the page (first ItemPointer - see bufpage.h) */
	metapage = (BMMetaPage) PageGetContents(page);

	/* Initialise the META page elements (heap and index) */
	_bitmap_create_lov_heapandindex(index, &(metapage->bm_lov_heapId),
									&(metapage->bm_lov_indexId));

	/* Log the metapage in case of archiving */
	if (use_wal)
		_bitmap_log_metapage(index, page);

	/*
	 * The second step is to create the first VMI.  The very first value is the
	 * NULL value.
	 */

	/* get a new buffer for the VMI */
	vmibuf = _bitmap_getbuf(index, P_NEW, BM_WRITE);
	_bitmap_init_vmipage(vmibuf);

	/* mark the VMI buffer contents as dirty (uninitialised) */
	MarkBufferDirty(vmibuf);

	/* Get the page for the first VMI item */
	page = BufferGetPage(vmibuf);

	/* Set the first item to support NULL value */
	vmi = _bitmap_formitem(0);
	vmiOffset = OffsetNumberNext(PageGetMaxOffsetNumber(page));

	/*
	 * XXX: perhaps this could be a special page, with more efficient storage
	 * after all, we have fixed size data
	 */
	o = PageAddItem(page, (Item) vmi, sizeof(BMVectorMetaItemData),
					vmiOffset, false, false);

	if (o == InvalidOffsetNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to add vector meta item to \"%s\"",
						RelationGetRelationName(index))));

	/* Set the last page for the VMI */
	metapage->bm_last_vmi_page = BufferGetBlockNumber(vmibuf);

	/* Log that a new VMI has been added to a VMI page */
	if (use_wal)
		_bitmap_log_vmi(index, vmibuf, vmiOffset, vmi, metabuf, true);

	END_CRIT_SECTION();

	/* Write the two buffers to disk */
	_bitmap_wrtbuf(vmibuf);
	_bitmap_wrtbuf(metabuf);

	pfree(vmi); /* free the item from memory */
}

/*
 * _bitmap_build_lovindex() -- index the tuples of the LOV for the first time
 *
 * For performance reasons we defer indexing of the LOV tuples when building a
 * fresh bitmap index when possible.  This function allows to initiate the
 * indexing separately from the creation of the index and insertion of the
 * tuples.
 */
static void
_bitmap_build_lovindex(BMBuildState *bmstate, IndexInfo *indexInfo)
{
	index_build(bmstate->bm_lov_heap, bmstate->bm_lov_index, indexInfo,
				false, false);
}

/*
 * Build a hash of the key we're indexing.
 */

static uint32
build_hash_key(const void *key, Size keysize)
{
	Datum		*k		 = (Datum *) key;
	int			 i;
	uint32		 hashkey = 0;

	for(i = 0; i < cur_bmbuild->natts; i++)
	{
		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		hashkey ^= DatumGetUInt32(FunctionCall1(&cur_bmbuild->hash_funcs[i],
												k[i]));
	}
	return hashkey;
}

/*
 * Test whether key1 matches key2. Since the equality functions may leak,
 * reset the temporary context at each call and do all equality calculation
 * in that context.
 */
static int
build_match_key(const void *key1, const void *key2, Size keysize)
{
	int i;
	MemoryContext old;
	int result = 0;

	MemoryContextReset(cur_bmbuild->tmpcxt);
	old = MemoryContextSwitchTo(cur_bmbuild->tmpcxt);

	for(i = 0; i < cur_bmbuild->natts; i++)
	{
		Datum attr1 = ((Datum *)key1)[i];
		Datum attr2 = ((Datum *)key2)[i];
        if (!DatumGetBool(FunctionCall2(&cur_bmbuild->eq_funcs[i],
                                        attr1, attr2)))
        {
            result = 1;     /* they aren't equal */
            break;
        }
	}
	MemoryContextSwitchTo(old);
	return result;
}

