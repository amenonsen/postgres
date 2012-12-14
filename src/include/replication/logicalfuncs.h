/*-------------------------------------------------------------------------
 * logicalfuns.h
 *     PostgreSQL WAL to logical transformation support functions
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALFUNCS_H
#define LOGICALFUNCS_H

#include "access/xlogreader.h"
#include "replication/reorderbuffer.h"
#include "replication/output_plugin.h"

typedef struct ReaderApplyState
{
	struct ReorderBuffer *reorderbuffer;
	bool stop_after_consistent;
	struct Snapstate *snapstate;

	LogicalDecodeInitCB init_cb;
	LogicalDecodeBeginCB begin_cb;
	LogicalDecodeChangeCB change_cb;
	LogicalDecodeCommitCB commit_cb;
	LogicalDecodeCleanupCB cleanup_cb;

	StringInfo out;
	void *user_private;


} ReaderApplyState;

XLogReaderState *
initial_snapshot_reader(XLogRecPtr startpoint, TransactionId xmin);

XLogReaderState *
normal_snapshot_reader(XLogRecPtr startpoint, TransactionId xmin,
					   char *plugin, XLogRecPtr valid_after);

bool
initial_snapshot_ready(XLogReaderState *);

#endif
