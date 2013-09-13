/*-------------------------------------------------------------------------
 *
 * bitmapdesc.c
 *	  rmgr descriptor routines for access/bitmap/bitmap.c
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/bitmapdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bitmap.h"

static void
out_target(StringInfo buf, RelFileNode *node)
{
	appendStringInfo(buf, "rel %u/%u/%u",
			node->spcNode, node->dbNode, node->relNode);
}

void
bitmap_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	uint8		info = xl_info & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_BITMAP_INSERT_NEWVMIPAGE:
		{
			xl_bm_newpage *xlrec = (xl_bm_newpage *) rec;

			appendStringInfo(buf, "insert a new VMI page: ");
			out_target(buf, &(xlrec->bm_node));
			break;
		}
		case XLOG_BITMAP_INSERT_VMI:
		{
			xl_bm_vmi *xlrec = (xl_bm_vmi *) rec;

			appendStringInfo(buf, "insert a new vector meta item: ");
			out_target(buf, &(xlrec->bm_node));
			break;
		}
		case XLOG_BITMAP_INSERT_META:
		{
			xl_bm_metapage *xlrec = (xl_bm_metapage *) rec;

			appendStringInfo(buf, "update the metapage: ");
			out_target(buf, &(xlrec->bm_node));
			break;
		}
		case XLOG_BITMAP_INSERT_BITMAP_LASTWORDS:
		{
			xl_bm_bitmap_lastwords *xlrec = (xl_bm_bitmap_lastwords *) rec;

			appendStringInfo(buf, "update the last two words in a bitmap: ");
			out_target(buf, &(xlrec->bm_node));
			break;
		}

		case XLOG_BITMAP_INSERT_WORDS:
		{
			xl_bm_bitmapwords *xlrec = (xl_bm_bitmapwords *)rec;

			appendStringInfo(buf, "insert words in a not-last bitmap page: ");
			out_target(buf, &(xlrec->bm_node));
			break;
		}

		case XLOG_BITMAP_UPDATEWORD:
		{
			xl_bm_updateword *xlrec = (xl_bm_updateword *) rec;

			appendStringInfo(buf, "update a word in a bitmap page: ");
			out_target(buf, &(xlrec->bm_node));
			break;
		}
		case XLOG_BITMAP_UPDATEWORDS:
		{
			xl_bm_updatewords *xlrec = (xl_bm_updatewords*) rec;

			appendStringInfo(buf, "update words in bitmap pages: ");
			out_target(buf, &(xlrec->bm_node));
			break;
		}
		default:
			appendStringInfo(buf, "UNKNOWN");
			break;
	}
}
