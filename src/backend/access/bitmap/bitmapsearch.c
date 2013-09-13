/*-------------------------------------------------------------------------
 *
 * bitmapsearch.c
 *	  Search routines for on-disk bitmap index access method.
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/bitmap/bitmapsearch.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/tupdesc.h"
#include "access/bitmap.h"
#include "storage/lmgr.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "storage/bufmgr.h" /* for buffer manager functions */
#include "utils/tqual.h" /* for SnapshotAny */

static void next_batch_words(IndexScanDesc scan);
static void read_words(Relation rel, Buffer vmiBuffer, OffsetNumber vmiOffset,
					   BlockNumber *nextBlockNoP, BM_WORD *headerWords,
					   BM_WORD *words, uint32 *numOfWordsP,
					   bool *readLastWords);
/*
 * _bitmap_first() -- find the first tuple that satisfies a given scan.
 */
bool
_bitmap_first(IndexScanDesc scan, ScanDirection dir)
{
	BMScanOpaque so;
	BMScanPosition scanpos;

	_bitmap_findbitmaps(scan, dir);
	so = (BMScanOpaque) scan->opaque;
	scanpos = (BMScanPosition) so->bm_currPos;
	if (scanpos->done)
		return false;

	return _bitmap_next(scan, dir);
}

/*
 * _bitmap_next() -- return the next tuple that satisfies a given scan.
 */
bool
_bitmap_next(IndexScanDesc scan, ScanDirection dir)
{
	BMScanOpaque	so;
	BMScanPosition	scanPos;
	uint64			nextTid;

	so = (BMScanOpaque) scan->opaque;
	scanPos = so->bm_currPos;

	if (scanPos->done)
		return false;

	for (;;)
	{
		/*
		 * If there are no more words left from the previous scan, we
		 * try to compute the next batch of words.
		 */
		if (scanPos->bm_batchWords->nwords == 0 &&
			scanPos->bm_result.nextTidLoc >= scanPos->bm_result.numOfTids)
		{
			_bitmap_reset_batchwords(scanPos->bm_batchWords);
			scanPos->bm_batchWords->firstTid = scanPos->bm_result.nextTid;

			next_batch_words(scan);

			_bitmap_begin_iterate(scanPos->bm_batchWords, &(scanPos->bm_result));
		}

		/* If we can not find more words, then this scan is over. */
		if (scanPos->bm_batchWords->nwords == 0 &&
			scanPos->bm_result.nextTidLoc >= scanPos->bm_result.numOfTids)
			return false;

		nextTid = _bitmap_findnexttid(scanPos->bm_batchWords,
									  &(scanPos->bm_result));
		if (nextTid == 0)
			continue;
		else
			break;
	}

	Assert((nextTid % BM_MAX_HTUP_PER_PAGE) + 1 > 0);

	ItemPointerSet(&(scan->xs_ctup.t_self), BM_INT_GET_BLOCKNO(nextTid),
				   BM_INT_GET_OFFSET(nextTid));
	so->cur_pos_valid = true;

	return true;
}

/*
 * _bitmap_firstbatchwords() -- find the first batch of bitmap words
 *  in a bitmap vector for a given scan.
 */
bool
_bitmap_firstbatchwords(IndexScanDesc scan,
						ScanDirection dir)
{
	_bitmap_findbitmaps(scan, dir);

	return _bitmap_nextbatchwords(scan, dir);
}

/*
 * _bitmap_nextbatchwords() -- find the next batch of bitmap words
 *  in a bitmap vector for a given scan.
 */
bool
_bitmap_nextbatchwords(IndexScanDesc scan,
					   ScanDirection dir)
{
	BMScanOpaque	so;

	so = (BMScanOpaque) scan->opaque;

	/* check if this scan if over */
	if (so->bm_currPos->done)
		return false;

	/*
	 * If there are some leftover words from the previous scan, simply
	 * return them.
	 */
	if (so->bm_currPos->bm_batchWords->nwords > 0)
		return true;

	next_batch_words(scan);

	return true;
}

/*
 * next_batch_words() -- compute the next batch of bitmap words
 *	from a given scan position.
 */
static void
next_batch_words(IndexScanDesc scan)
{
	BMScanPosition			scanPos;
	BMVector	bmScanPos;
	int						i;
	BMBatchWords		  **batches;
	int						numBatches;

	scanPos = ((BMScanOpaque) scan->opaque)->bm_currPos;
	bmScanPos = scanPos->posvecs;

	batches = (BMBatchWords **)
		palloc0(scanPos->nvec * sizeof(BMBatchWords *));

	numBatches = 0;
	/*
	 * Obtains the next batch of words for each bitmap vector.
	 * Ignores those bitmap vectors that contain no new words.
	 */
	for (i = 0; i < scanPos->nvec; i++)
	{
		BMBatchWords	*batchWords;
		batchWords = bmScanPos[i].bm_batchWords;

		/*
		 * If there are no words left from previous scan, read the next
		 * batch of words.
		 */
		if (bmScanPos[i].bm_batchWords->nwords == 0 &&
			!(bmScanPos[i].bm_readLastWords))
		{

			_bitmap_reset_batchwords(batchWords);
			read_words(scan->indexRelation,
					   bmScanPos[i].bm_vmiBuffer,
					   bmScanPos[i].bm_vmiOffset,
					   &(bmScanPos[i].bm_nextBlockNo),
					   batchWords->hwords,
					   batchWords->cwords,
					   &(batchWords->nwords),
					   &(bmScanPos[i].bm_readLastWords));
		}

		if (bmScanPos[i].bm_batchWords->nwords > 0)
		{
			batches[numBatches] = batchWords;
			numBatches++;
		}
	}

	/*
	 * We handle the case where only one bitmap vector contributes to
	 * the scan separately with other cases. This is because
	 * bmScanPos->bm_batchWords and scanPos->bm_batchWords
	 * are the same.
	 */
	if (scanPos->nvec == 1)
	{
		if (bmScanPos->bm_batchWords->nwords == 0)
			scanPos->done = true;
		pfree(batches);
		scanPos->bm_batchWords = scanPos->posvecs->bm_batchWords;

		return;
	}

	/*
	 * At least two bitmap vectors contribute to this scan, we
	 * ORed these bitmap vectors.
	 */
	if (numBatches == 0)
	{
		scanPos->done = true;
		pfree(batches);
		return;
	}

	_bitmap_union(batches, numBatches, scanPos->bm_batchWords);
	pfree(batches);
}

/*
 * read_words() -- read one-block of bitmap words from
 *	the bitmap page.
 *
 * If nextBlockNo is an invalid block number, then the two last words
 * are stored in vmi. Otherwise, read words from nextBlockNo.
 */
static void
read_words(Relation rel, Buffer vmiBuffer, OffsetNumber vmiOffset,
				  BlockNumber *nextBlockNoP, BM_WORD *headerWords,
				  BM_WORD *words, uint32 *numOfWordsP, bool *readLastWords)
{
	if (BlockNumberIsValid(*nextBlockNoP))
	{
		Buffer bitmapBuffer = _bitmap_getbuf(rel, *nextBlockNoP, BM_READ);

		Page			bitmapPage;
		BMBitmapVectorPage		bitmap;
		BMPageOpaque	bo;

		bitmapPage = BufferGetPage(bitmapBuffer);

		bitmap = (BMBitmapVectorPage) PageGetContents(bitmapPage);
		bo = (BMPageOpaque)PageGetSpecialPointer(bitmapPage);

		*numOfWordsP = bo->bm_hrl_words_used;
		memcpy(headerWords, bitmap->hwords,
				BM_NUM_OF_HEADER_WORDS * sizeof(BM_WORD));
		memcpy(words, bitmap->cwords, sizeof(BM_WORD) * *numOfWordsP);

		*nextBlockNoP = bo->bm_bitmap_next;

		_bitmap_relbuf(bitmapBuffer);

		*readLastWords = false;

		/*
		 * If this is the last bitmap page and the total number of words
		 * in this page is less than or equal to
		 * BM_NUM_OF_HRL_WORDS_PER_PAGE - 2, we read the last two words
		 * and append them into 'headerWords' and 'words'.
		 */

		if ((!BlockNumberIsValid(*nextBlockNoP)) &&
			(*numOfWordsP <= BM_NUM_OF_HRL_WORDS_PER_PAGE - 2))
		{
			BM_WORD	cwords[2];
			BM_WORD	hword;
			BM_WORD tmp;
			uint32		nwords;
			int			offs;

			read_words(rel, vmiBuffer, vmiOffset, nextBlockNoP, &hword,
					   cwords, &nwords, readLastWords);

			Assert(nwords > 0 && nwords <= 2);

			memcpy(words + *numOfWordsP, cwords, nwords * sizeof(BM_WORD));

			offs = *numOfWordsP / BM_WORD_SIZE;
			tmp = hword >> *numOfWordsP % BM_WORD_SIZE;
			headerWords[offs] |= tmp;

			if (*numOfWordsP % BM_WORD_SIZE == BM_WORD_SIZE - 1)
			{
				offs = (*numOfWordsP + 1)/BM_WORD_SIZE;
				headerWords[offs] |= hword << 1;
			}
			*numOfWordsP += nwords;
		}
	}
	else
	{
		BMVectorMetaItem vmi;
		Page		vmiPage;

		LockBuffer(vmiBuffer, BM_READ);

		vmiPage = BufferGetPage(vmiBuffer);
		vmi = (BMVectorMetaItem)
			PageGetItem(vmiPage, PageGetItemId(vmiPage, vmiOffset));

		if (vmi->bm_last_compword != LITERAL_ALL_ONE)
		{
			*numOfWordsP = 2;
			headerWords[0] = (((BM_WORD) vmi->vmi_words_header) <<
							  (BM_WORD_SIZE-2));
			words[0] = vmi->bm_last_compword;
			words[1] = vmi->bm_last_word;
		}
		else
		{
			*numOfWordsP = 1;
			headerWords[0] = (((BM_WORD) vmi->vmi_words_header) <<
							  (BM_WORD_SIZE-1));
			words[0] = vmi->bm_last_word;
		}

		LockBuffer(vmiBuffer, BUFFER_LOCK_UNLOCK);
		*readLastWords = true;
	}
}

/*
 * _bitmap_findbitmaps() -- find the bitmap vectors that satisfy the
 * index predicate.
 */
void
_bitmap_findbitmaps(IndexScanDesc scan, ScanDirection dir)
{
	BMScanOpaque			so;
	BMScanPosition			scanPos;
	Buffer					metabuf;
	BMMetaPage				metapage;
	BlockNumber				vmiBlock;
	OffsetNumber			vmiOffset;
	int						vectorNo, keyNo;

	so = (BMScanOpaque) scan->opaque;

	/* allocate space and initialize values for so->bm_currPos */
	if(so->bm_currPos == NULL)
		so->bm_currPos = (BMScanPosition) palloc0(sizeof(BMScanPositionData));

	scanPos = so->bm_currPos;
	scanPos->nvec = 0;
	scanPos->done = false;
	MemSet(&scanPos->bm_result, 0, sizeof(BMIterateResult));


	for (keyNo = 0; keyNo < scan->numberOfKeys; keyNo++)
	{
		if (scan->keyData[keyNo].sk_flags & SK_ISNULL)
		{
			scanPos->done = true;
			return;
		}
	}

	metabuf = _bitmap_getbuf(scan->indexRelation, BM_METAPAGE, BM_READ);
	metapage = (BMMetaPage) PageGetContents(BufferGetPage(metabuf));

	/*
	 * If the values for these keys are all NULL, the bitmap vector
	 * is accessed through the first VMI.
	 */
	if (0)
	{
		vmiBlock = BM_VMI_STARTPAGE;
		vmiOffset = 1;

		scanPos->posvecs = (BMVector) palloc0(sizeof(BMVectorData));

		_bitmap_initscanpos(scan, scanPos->posvecs, vmiBlock, vmiOffset);
		scanPos->nvec = 1;
	}
	else
	{
		Relation		lovHeap, lovIndex;
		TupleDesc		indexTupDesc;
		ScanKey			scanKeys;
		IndexScanDesc	scanDesc;
		BMVMIID			vmiid;
		List		   *vmiids = NIL;
		ListCell	   *cell;

		/*
		 * We haven't locked the metapage but that's okay... if these
		 * values change underneath us there's something much more
		 * fundamentally wrong. This could change when we have VACUUM
		 * support, of course.
		 */
		_bitmap_open_lov_heapandindex(metapage, &lovHeap, &lovIndex,
									  AccessShareLock);

		indexTupDesc = RelationGetDescr(lovIndex);

		scanKeys = palloc0(scan->numberOfKeys * sizeof(ScanKeyData));
		for (keyNo = 0; keyNo < scan->numberOfKeys; keyNo++)
		{
			ScanKey	scanKey = (ScanKey) (((char *)scanKeys) +
										 keyNo * sizeof(ScanKeyData));
			/* XXX (Daniel Bausch, 2012-09-05): isn't the previous line
			 * equivalent to 'scanKey = &scanKeys[keyNo];' ? */

			elog(NOTICE, "initialize scanKey for attno %d",
				 scan->keyData[keyNo].sk_attno);

			ScanKeyEntryInitialize(scanKey,
								   scan->keyData[keyNo].sk_flags,
								   scan->keyData[keyNo].sk_attno,
								   scan->keyData[keyNo].sk_strategy,
								   scan->keyData[keyNo].sk_subtype,
								   scan->keyData[keyNo].sk_collation,
								   scan->keyData[keyNo].sk_func.fn_oid,
								   scan->keyData[keyNo].sk_argument);
		}

		/* XXX: is SnapshotAny really the right choice? */
		scanDesc = index_beginscan(lovHeap, lovIndex, SnapshotAny,
								   scan->numberOfKeys, 0);
		index_rescan(scanDesc, scanKeys, scan->numberOfKeys, NULL, 0);

		/*
		 * finds all VMI IDs for this scan through lovHeap and lovIndex.
		 */
		while (_bitmap_findvalue(lovHeap, lovIndex, scanKeys, scanDesc,
								 &vmiid))
		{
			/*
			 * We find the VMI ID of one item. Append it into the list.
			 */
			BMVMIID *idCopy = (BMVMIID *) palloc0(sizeof(BMVMIID));

			*idCopy = vmiid;
			vmiids = lappend(vmiids, idCopy);

			scanPos->nvec++;
		}

		scanPos->posvecs =
			(BMVector)palloc0(sizeof(BMVectorData) * scanPos->nvec);
		vectorNo = 0;
		foreach(cell, vmiids)
		{
			BMVMIID	   *_vmiid = (BMVMIID *) lfirst(cell);
			BMVector	bmScanPos = &(scanPos->posvecs[vectorNo]);

			_bitmap_initscanpos(scan, bmScanPos, _vmiid->block,
								_vmiid->offset);

			vectorNo++;
		}

		list_free_deep(vmiids);

		index_endscan(scanDesc);
		_bitmap_close_lov_heapandindex(lovHeap, lovIndex, AccessShareLock);
		pfree(scanKeys);
	}

	_bitmap_relbuf(metabuf);

	if (scanPos->nvec == 0)
	{
		scanPos->done = true;
		return;
	}

	/*
	 * Since there is only one related bitmap vector, we have
	 * the scan position's batch words structure point directly to
	 * the vector's batch words.
	 */
	if (scanPos->nvec == 1)
		scanPos->bm_batchWords = scanPos->posvecs->bm_batchWords;
	else
	{
		scanPos->bm_batchWords = (BMBatchWords *) palloc0(sizeof(BMBatchWords));
		_bitmap_init_batchwords(scanPos->bm_batchWords,
								BM_NUM_OF_HRL_WORDS_PER_PAGE,
								CurrentMemoryContext);
	}
}

/*
 * _bitmap_initscanpos() -- initialize a BMScanPosition for a given
 *	bitmap vector.
 */
void
_bitmap_initscanpos(IndexScanDesc scan, BMVector bmScanPos,
					BlockNumber vmiBlock, OffsetNumber vmiOffset)
{
	Page				vmiPage;
	BMVectorMetaItem	vmi;

	bmScanPos->bm_vmiOffset = vmiOffset;
	bmScanPos->bm_vmiBuffer = _bitmap_getbuf(scan->indexRelation, vmiBlock,
											 BM_READ);

	vmiPage	= BufferGetPage(bmScanPos->bm_vmiBuffer);
	vmi = (BMVectorMetaItem)
		PageGetItem(vmiPage, PageGetItemId(vmiPage, bmScanPos->bm_vmiOffset));

	bmScanPos->bm_nextBlockNo = vmi->bm_bitmap_head;
	bmScanPos->bm_readLastWords = false;
	bmScanPos->bm_batchWords = (BMBatchWords *) palloc0(sizeof(BMBatchWords));
	_bitmap_init_batchwords(bmScanPos->bm_batchWords,
							BM_NUM_OF_HRL_WORDS_PER_PAGE,
							CurrentMemoryContext);

	LockBuffer(bmScanPos->bm_vmiBuffer, BUFFER_LOCK_UNLOCK);
}

/*
 * _bitmap_get_null_vmiid() -- return the vmiid of the all-nulls entry for the
 * given relation.
 */
void
_bitmap_get_null_vmiid(Relation index, BMVMIID *vmiid)
{
	vmiid->block = BM_VMI_STARTPAGE;
	vmiid->offset = 1;
}
