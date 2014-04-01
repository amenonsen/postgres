/*-------------------------------------------------------------------------
 *
 * bitmapxlog.c
 *	  WAL replay logic for the bitmap index.
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/bitmap/bitmapxlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/bitmap.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h" /* for buffer manager functions */
#include "utils/rel.h" /* for RelationGetDescr */
#include "utils/lsyscache.h"

static void forget_incomplete_insert_bitmapwords(RelFileNode node,
									 xl_bm_bitmapwords* newWords);
/*
 * We must keep track of expected insertion of bitmap words when these
 * bitmap words are inserted into multiple bitmap pages. We need to manually
 * insert these words if they are not seen in the WAL log during replay.
 * This makes it safe for page insertion to be a multiple-WAL-action process.
 */
typedef xl_bm_bitmapwords bm_incomplete_action;

static List *incomplete_actions;

static void
log_incomplete_insert_bitmapwords(RelFileNode node,
								  xl_bm_bitmapwords* newWords)
{
	int					lastTids_size;
	int					cwords_size;
	int					hwords_size;
	int					total_size;
	bm_incomplete_action *action;

	/* Delete the previous entry */
	forget_incomplete_insert_bitmapwords(node, newWords);

	lastTids_size = newWords->bm_num_cwords * sizeof(uint64);
	cwords_size = newWords->bm_num_cwords * sizeof(BM_WORD);
	hwords_size = (BM_CALC_H_WORDS(newWords->bm_num_cwords)) *
					sizeof(BM_WORD);
	total_size = sizeof(bm_incomplete_action) + lastTids_size +
				 cwords_size + hwords_size;

	action = palloc0(total_size);
	memcpy(action, newWords, total_size);

	/* Reset the following fields */
	action->bm_blkno = newWords->bm_next_blkno;
	action->bm_next_blkno = InvalidBlockNumber;
	action->bm_start_wordno =
		newWords->bm_start_wordno + newWords->bm_words_written;
	action->bm_words_written = 0;

	incomplete_actions = lappend(incomplete_actions, action);
}

static void
forget_incomplete_insert_bitmapwords(RelFileNode node,
									 xl_bm_bitmapwords* newWords)
{
	ListCell* l;

	foreach (l, incomplete_actions)
	{
		bm_incomplete_action *action = (bm_incomplete_action *) lfirst(l);

		if (RelFileNodeEquals(node, action->bm_node) &&
			(action->bm_vmi_blkno == newWords->bm_vmi_blkno &&
			 action->bm_vmi_offset == newWords->bm_vmi_offset &&
			 action->bm_last_setbit == newWords->bm_last_setbit) &&
			!action->bm_is_last)
		{
			Assert(action->bm_blkno == newWords->bm_blkno);

			incomplete_actions = list_delete_ptr(incomplete_actions, action);
			pfree(action);
			break;
		}
	}
}

/*
 * _bitmap_xlog_newpage() -- create a new page.
 */
static void
_bitmap_xlog_newpage(XLogRecPtr lsn, XLogRecord *record)
{
	xl_bm_newpage	*xlrec = (xl_bm_newpage *) XLogRecGetData(record);

	Page			page;
	uint8			info;
	Buffer		buffer;

	info = record->xl_info & ~XLR_INFO_MASK;

	buffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_new_blkno, true);
	if (!BufferIsValid(buffer))
		elog(PANIC, "_bitmap_xlog_newpage: block unfound: %d",
			 xlrec->bm_new_blkno);

	page = BufferGetPage(buffer);
	Assert(PageIsNew(page));

	if (PageGetLSN(page) < lsn)
	{
		switch (info)
		{
			case XLOG_BITMAP_INSERT_NEWVMIPAGE:
				_bitmap_init_vmipage(buffer);
				break;
			default:
				elog(PANIC, "_bitmap_xlog_newpage: unknown newpage op code %u",
					 info);
		}

		PageSetLSN(page, lsn);
		_bitmap_wrtbuf(buffer);
	}
	else
		_bitmap_relbuf(buffer);
}

/*
 * _bitmap_xlog_insert_vmi() -- insert a new vector meta item.
 */
static void
_bitmap_xlog_insert_vmi(XLogRecPtr lsn, XLogRecord *record)
{
	xl_bm_vmi	   *xlrec = (xl_bm_vmi *) XLogRecGetData(record);
	Buffer			vmiBuffer;
	Page			vmiPage;

	vmiBuffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_vmi_blkno, true);
	if (!BufferIsValid(vmiBuffer))
		elog(PANIC, "_bitmap_xlog_insert_vmi: block unfound: %d",
			 xlrec->bm_vmi_blkno);

	vmiPage = BufferGetPage(vmiBuffer);

	if (PageIsNew(vmiPage))
	{
		Assert(xlrec->bm_is_new_vmi_blkno);
		_bitmap_init_vmipage(vmiBuffer);
	}

	if (PageGetLSN(vmiPage) < lsn)
	{
		OffsetNumber	newOffset, itemSize;

		newOffset = OffsetNumberNext(PageGetMaxOffsetNumber(vmiPage));
		if (newOffset != xlrec->bm_vmi_offset)
			elog(PANIC, "_bitmap_xlog_insert_vmi: VMI is not inserted "
						"in pos %d(requested %d)",
				 newOffset, xlrec->bm_vmi_offset);

		itemSize = sizeof(BMVectorMetaItemData);
		if (itemSize > PageGetFreeSpace(vmiPage))
			elog(PANIC,
				 "_bitmap_xlog_insert_vmi: not enough space in VMI page %d",
				 xlrec->bm_vmi_blkno);

		if (PageAddItem(vmiPage, (Item) &(xlrec->bm_vmi), itemSize,
						newOffset, false, false) == InvalidOffsetNumber)
		{
			char *rel_name = get_rel_name(xlrec->bm_node.relNode);
			if (rel_name)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("_bitmap_xlog_insert_vmi: failed to add "
								"VMI to \"%s\"",
								rel_name)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("_bitmap_xlog_insert_vmi: failed to add "
								"VMI")));
		}

		if (xlrec->bm_is_new_vmi_blkno)
		{
			Buffer metabuf = XLogReadBuffer(xlrec->bm_node, BM_METAPAGE, false);
			BMMetaPage metapage;
			if (!BufferIsValid(metabuf))
				elog(PANIC, "_bitmap_xlog_insert_vmi: block unfound: %d",
					 BM_METAPAGE);

			metapage = (BMMetaPage)
				PageGetContents(BufferGetPage(metabuf));

			metapage->bm_last_vmi_page = xlrec->bm_vmi_blkno;

			PageSetLSN(BufferGetPage(metabuf), lsn);

			_bitmap_wrtbuf(metabuf);
		}

		PageSetLSN(vmiPage, lsn);

		_bitmap_wrtbuf(vmiBuffer);
	}
	else
		_bitmap_relbuf(vmiBuffer);
}

/*
 * _bitmap_xlog_insert_meta() -- update a metapage.
 */
static void
_bitmap_xlog_insert_meta(XLogRecPtr lsn, XLogRecord *record)
{
	xl_bm_metapage	*xlrec = (xl_bm_metapage *) XLogRecGetData(record);
	Buffer			metabuf;
	Page			mp;
	BMMetaPage		metapage;

	metabuf = XLogReadBuffer(xlrec->bm_node, BM_METAPAGE, true);

	mp = BufferGetPage(metabuf);
	if (PageIsNew(mp))
		PageInit(mp, BufferGetPageSize(metabuf), 0);

	if (PageGetLSN(mp) < lsn)
	{
		metapage = (BMMetaPage)PageGetContents(mp);

		metapage->bm_lov_heapId = xlrec->bm_lov_heapId;
		metapage->bm_lov_indexId = xlrec->bm_lov_indexId;
		metapage->bm_last_vmi_page = xlrec->bm_last_vmi_page;

		PageSetLSN(mp, lsn);
		_bitmap_wrtbuf(metabuf);
	}
	else
		_bitmap_relbuf(metabuf);
}

/*
 * _bitmap_xlog_insert_bitmap_lastwords() -- update the last two words
 * in a bitmap vector.
 */
static void
_bitmap_xlog_insert_bitmap_lastwords(XLogRecPtr lsn,
									 XLogRecord *record)
{
	xl_bm_bitmap_lastwords *xlrec;

	Buffer					vmiBuffer;
	Page					vmiPage;
	BMVectorMetaItem		vmi;

	xlrec = (xl_bm_bitmap_lastwords *) XLogRecGetData(record);

	vmiBuffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_vmi_blkno, false);
	if (!BufferIsValid(vmiBuffer))
		elog(PANIC, "_bitmap_xlog_insert_bitmap_lastwords: "
					" block not found: %d",
			 xlrec->bm_vmi_blkno);

	vmiPage = BufferGetPage(vmiBuffer);

	if (PageGetLSN(vmiPage) < lsn)
	{
		ItemId item = PageGetItemId(vmiPage, xlrec->bm_vmi_offset);

		if (!ItemIdIsUsed(item))
			elog(PANIC, "_bitmap_xlog_insert_bitmap_lastwords: "
						"offset not found: %d",
				 xlrec->bm_vmi_offset);

		vmi = (BMVectorMetaItem) PageGetItem(vmiPage, item);

		vmi->bm_last_compword = xlrec->bm_last_compword;
		vmi->bm_last_word = xlrec->bm_last_word;
		vmi->vmi_words_header = xlrec->vmi_words_header;

		PageSetLSN(vmiPage, lsn);
		_bitmap_wrtbuf(vmiBuffer);
	}
	else
		_bitmap_relbuf(vmiBuffer);
}

static void
_bitmap_xlog_insert_bitmapwords(XLogRecPtr lsn, XLogRecord *record)
{
	xl_bm_bitmapwords *xlrec;

	Buffer		bitmapBuffer;
	Page		bitmapPage;
	BMPageOpaque	bitmapPageOpaque;
	BMTIDBuffer newWords;
	uint64		words_written;

	int					lastTids_size;
	int					cwords_size;
	int					hwords_size;

	xlrec = (xl_bm_bitmapwords *) XLogRecGetData(record);

	bitmapBuffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_blkno, true);
	bitmapPage = BufferGetPage(bitmapBuffer);

	if (PageIsNew(bitmapPage))
		_bitmap_init_bitmappage(bitmapBuffer);

	bitmapPageOpaque =
		(BMPageOpaque)PageGetSpecialPointer(bitmapPage);

	if (PageGetLSN(bitmapPage) < lsn)
	{
		Buffer				vmiBuffer;
		Page				vmiPage;
		BMVectorMetaItem	vmi;
		uint64			   *last_tids;
		BM_WORD			   *cwords;
		BM_WORD			   *hwords;

		newWords.curword = xlrec->bm_num_cwords;
		newWords.start_wordno = xlrec->bm_start_wordno;

		lastTids_size = newWords.curword * sizeof(uint64);
		cwords_size = newWords.curword * sizeof(BM_WORD);
		hwords_size = (BM_CALC_H_WORDS(newWords.curword)) *
						sizeof(BM_WORD);

		newWords.last_tids = (uint64*)palloc0(lastTids_size);
		newWords.cwords = (BM_WORD*)palloc0(cwords_size);

		last_tids =
			(uint64*)(((char*)xlrec) + sizeof(xl_bm_bitmapwords));
		cwords =
			(BM_WORD*)(((char*)xlrec) +
						 sizeof(xl_bm_bitmapwords) + lastTids_size);
		hwords =
			(BM_WORD*)(((char*)xlrec) +
							 sizeof(xl_bm_bitmapwords) + lastTids_size +
							 cwords_size);
		memcpy(newWords.last_tids, last_tids, lastTids_size);
		memcpy(newWords.cwords, cwords, cwords_size);
		memcpy(newWords.hwords, hwords, hwords_size);

		/*
		 * If no words are written to this bitmap page, it means
		 * this bitmap page is full.
		 */
		if (xlrec->bm_words_written == 0)
		{
			Assert(BM_NUM_OF_HRL_WORDS_PER_PAGE -
				   bitmapPageOpaque->bm_hrl_words_used == 0);
			words_written = 0;
		}
		else
			words_written =
				_bitmap_write_bitmapwords(bitmapBuffer, &newWords);

		Assert(words_written == xlrec->bm_words_written);

		bitmapPageOpaque->bm_bitmap_next = xlrec->bm_next_blkno;
		Assert(bitmapPageOpaque->bm_last_tid_location == xlrec->bm_last_tid);

		vmiBuffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_vmi_blkno, false);
		if (!BufferIsValid(vmiBuffer))
			elog(PANIC,
				 "_bitmap_xlog_insert_last_bitmapwords: VMI block not found: %d",
				 xlrec->bm_vmi_blkno);
		vmiPage = BufferGetPage(vmiBuffer);

		vmi = (BMVectorMetaItem)
			PageGetItem(vmiPage, PageGetItemId(vmiPage, xlrec->bm_vmi_offset));

		if (xlrec->bm_is_last)
		{
			vmi->bm_last_compword = xlrec->bm_last_compword;
			vmi->bm_last_word = xlrec->bm_last_word;
			vmi->vmi_words_header = xlrec->vmi_words_header;
			vmi->bm_last_setbit = xlrec->bm_last_setbit;
			vmi->bm_last_tid_location = xlrec->bm_last_setbit -
				xlrec->bm_last_setbit % BM_WORD_SIZE;
			vmi->bm_bitmap_tail = BufferGetBlockNumber(bitmapBuffer);
			if (vmi->bm_bitmap_head == InvalidBlockNumber)
				vmi->bm_bitmap_head = vmi->bm_bitmap_tail;

			PageSetLSN(vmiPage, lsn);

			_bitmap_wrtbuf(vmiBuffer);

			forget_incomplete_insert_bitmapwords(xlrec->bm_node, xlrec);
		}
		else
		{

			Buffer	nextBuffer;
			Page	nextPage;

			/* create a new bitmap page */
			nextBuffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_next_blkno, true);
			nextPage = BufferGetPage(nextBuffer);

			Assert(PageIsNew(nextPage));

			_bitmap_init_bitmappage(nextBuffer);

			if (xlrec->bm_is_first)
			{
				vmi->bm_bitmap_head = BufferGetBlockNumber(bitmapBuffer);
				vmi->bm_bitmap_tail = vmi->bm_bitmap_head;

				PageSetLSN(vmiPage, lsn);

				_bitmap_wrtbuf(vmiBuffer);
			}
			else
				_bitmap_relbuf(vmiBuffer);

			PageSetLSN(nextPage, lsn);

			_bitmap_wrtbuf(nextBuffer);

			log_incomplete_insert_bitmapwords(xlrec->bm_node, xlrec);
		}

		PageSetLSN(bitmapPage, lsn);

		_bitmap_wrtbuf(bitmapBuffer);

		_bitmap_free_tidbuf(&newWords);
	}

	else {
		_bitmap_relbuf(bitmapBuffer);
	}
}

static void
_bitmap_xlog_updateword(XLogRecPtr lsn, XLogRecord *record)
{
	xl_bm_updateword *xlrec;

	Buffer			bitmapBuffer;
	Page			bitmapPage;
	BMPageOpaque	bitmapOpaque;
	BMBitmapVectorPage		bitmap;

	xlrec = (xl_bm_updateword *) XLogRecGetData(record);

	bitmapBuffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_blkno, false);
	if (!BufferIsValid(bitmapBuffer))
		elog(PANIC, "_bitmap_xlog_updateword block not found: %d",
			 xlrec->bm_blkno);

	bitmapPage = BufferGetPage(bitmapBuffer);
	bitmapOpaque =
		(BMPageOpaque)PageGetSpecialPointer(bitmapPage);
	bitmap = (BMBitmapVectorPage) PageGetContents(bitmapPage);

	if (PageGetLSN(bitmapPage) < lsn)
	{
		Assert(bitmapOpaque->bm_hrl_words_used > xlrec->bm_word_no);

		bitmap->cwords[xlrec->bm_word_no] = xlrec->bm_cword;
		bitmap->hwords[xlrec->bm_word_no/BM_WORD_SIZE] = xlrec->bm_hword;

		PageSetLSN(bitmapPage, lsn);
		_bitmap_wrtbuf(bitmapBuffer);
	}

	else
		_bitmap_relbuf(bitmapBuffer);
}

static void
_bitmap_xlog_updatewords(XLogRecPtr lsn, XLogRecord *record)
{
	xl_bm_updatewords *xlrec;
	Buffer			firstBuffer;
	Buffer			secondBuffer = InvalidBuffer;
	Page			firstPage;
	Page			secondPage = NULL;
	BMPageOpaque	firstOpaque;
	BMPageOpaque	secondOpaque = NULL;
	BMBitmapVectorPage		firstBitmap;
	BMBitmapVectorPage		secondBitmap = NULL;

	xlrec = (xl_bm_updatewords *) XLogRecGetData(record);

	firstBuffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_first_blkno, false);
	if (!BufferIsValid(firstBuffer))
		elog(PANIC, "_bitmap_xlog_updatewords first block not found: %d",
			 xlrec->bm_first_blkno);

	firstPage = BufferGetPage(firstBuffer);
	firstOpaque =
		(BMPageOpaque) PageGetSpecialPointer(firstPage);
	firstBitmap = (BMBitmapVectorPage) PageGetContents(firstPage);

	if (PageGetLSN(firstPage) < lsn)
	{
		if (xlrec->bm_two_pages)
		{
			secondBuffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_second_blkno, true);
			secondPage = BufferGetPage(secondBuffer);
			if (PageIsNew(secondPage))
				_bitmap_init_bitmappage(secondBuffer);

			secondOpaque =
				(BMPageOpaque) PageGetSpecialPointer(secondPage);
			secondBitmap = (BMBitmapVectorPage) PageGetContents(secondPage);
			Assert(PageGetLSN(secondPage) < lsn);
		}

		memcpy(firstBitmap->cwords, xlrec->bm_first_cwords,
			   BM_NUM_OF_HRL_WORDS_PER_PAGE * sizeof(BM_WORD));
		memcpy(firstBitmap->hwords, xlrec->bm_first_hwords,
			   BM_NUM_OF_HEADER_WORDS *	sizeof(BM_WORD));
		firstOpaque->bm_hrl_words_used = xlrec->bm_first_num_cwords;
		firstOpaque->bm_last_tid_location = xlrec->bm_first_last_tid;
		firstOpaque->bm_bitmap_next = xlrec->bm_second_blkno;

		if (xlrec->bm_two_pages)
		{
			memcpy(secondBitmap->cwords, xlrec->bm_second_cwords,
				   BM_NUM_OF_HRL_WORDS_PER_PAGE * sizeof(BM_WORD));
			memcpy(secondBitmap->hwords, xlrec->bm_second_hwords,
				   BM_NUM_OF_HEADER_WORDS *	sizeof(BM_WORD));
			secondOpaque->bm_hrl_words_used = xlrec->bm_second_num_cwords;
			secondOpaque->bm_last_tid_location = xlrec->bm_second_last_tid;
			secondOpaque->bm_bitmap_next = xlrec->bm_next_blkno;

			PageSetLSN(secondPage, lsn);
			_bitmap_wrtbuf(secondBuffer);
		}

		if (xlrec->bm_new_lastpage)
		{
			Buffer				vmiBuffer;
			Page				vmiPage;
			BMVectorMetaItem	vmi;

			vmiBuffer = XLogReadBuffer(xlrec->bm_node, xlrec->bm_vmi_blkno,
									   false);
			if (!BufferIsValid(vmiBuffer))
				elog(PANIC, "_bitmap_xlog_updatewords VMI block %d "
					 "does not exist", xlrec->bm_vmi_blkno);

			vmiPage = BufferGetPage(vmiBuffer);
			vmi = (BMVectorMetaItem)
				PageGetItem(vmiPage,
							PageGetItemId(vmiPage, xlrec->bm_vmi_offset));
			vmi->bm_bitmap_tail = BufferGetBlockNumber(secondBuffer);

			PageSetLSN(vmiPage, lsn);
			_bitmap_wrtbuf(vmiBuffer);
		}

		PageSetLSN(firstPage, lsn);
		_bitmap_wrtbuf(firstBuffer);
	}
	else
		_bitmap_relbuf(firstBuffer);
}


void
bitmap_redo(XLogRecPtr lsn, XLogRecord *record)
{
	uint8	info = record->xl_info & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_BITMAP_INSERT_NEWVMIPAGE:
			_bitmap_xlog_newpage(lsn, record);
			break;
		case XLOG_BITMAP_INSERT_VMI:
			_bitmap_xlog_insert_vmi(lsn, record);
			break;
		case XLOG_BITMAP_INSERT_META:
			_bitmap_xlog_insert_meta(lsn, record);
			break;
		case XLOG_BITMAP_INSERT_BITMAP_LASTWORDS:
			_bitmap_xlog_insert_bitmap_lastwords(lsn, record);
			break;
		case XLOG_BITMAP_INSERT_WORDS:
			_bitmap_xlog_insert_bitmapwords(lsn, record);
			break;
		case XLOG_BITMAP_UPDATEWORD:
			_bitmap_xlog_updateword(lsn, record);
			break;
		case XLOG_BITMAP_UPDATEWORDS:
			_bitmap_xlog_updatewords(lsn, record);
			break;
		default:
			elog(PANIC, "bitmap_redo: unknown op code %u", info);
	}
}

void
bitmap_xlog_startup(void)
{
	incomplete_actions = NIL;
	/* sleep(30); */
}

void
bitmap_xlog_cleanup(void)
{
	ListCell* l;
	foreach (l, incomplete_actions)
	{
		Relation		reln;
		Buffer			vmiBuffer;
		BMTIDBuffer		newWords;

		int				lastTids_size;
		int				cwords_size;
		int				hwords_size;
		BM_WORD        *hwords;

		bm_incomplete_action *action = (bm_incomplete_action *) lfirst(l);

		vmiBuffer = XLogReadBuffer(action->bm_node, action->bm_vmi_blkno, false);

		newWords.num_cwords = action->bm_num_cwords;
		newWords.start_wordno = action->bm_start_wordno;

		lastTids_size = newWords.num_cwords * sizeof(uint64);
		cwords_size = newWords.num_cwords * sizeof(BM_WORD);
		hwords_size = (BM_CALC_H_WORDS(newWords.num_cwords)) * sizeof(BM_WORD);

		newWords.last_tids = (uint64 *)
			(((char *) action) + sizeof(xl_bm_bitmapwords));
		newWords.cwords = (BM_WORD*)
			(((char *) action) + sizeof(xl_bm_bitmapwords) + lastTids_size);
		hwords = (BM_WORD*)
			(((char *) action) + sizeof(xl_bm_bitmapwords) + lastTids_size +
			 cwords_size);
		memcpy(newWords.hwords, hwords, hwords_size);

		newWords.last_compword = action->bm_last_compword;
		newWords.last_word = action->bm_last_word;
		newWords.is_last_compword_fill = (action->vmi_words_header == 2);
		newWords.last_tid = action->bm_last_setbit;

		/* Finish an incomplete insert
		 * XXX reln is not initialised here. Where should we get the
		 * value from?
		 */
		_bitmap_write_new_bitmapwords(reln, vmiBuffer, action->bm_vmi_offset,
									  &newWords, false);
	}
	incomplete_actions = NIL;
}
