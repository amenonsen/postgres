/*-------------------------------------------------------------------------
 * logical.h
 *     PostgreSQL WAL to logical transformation support functions
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICAL_H
#define LOGICAL_H

#include "access/xlog.h"
#include "replication/syncrep.h"
#include "storage/shmem.h"
#include "storage/spin.h"

typedef struct
{
	TransactionId xmin;

	NameData      name;
	NameData      plugin;

	Oid           database;

	XLogRecPtr	  last_required_checkpoint;
	XLogRecPtr	  confirmed_flush;

	TransactionId candidate_xmin;
	XLogRecPtr	  candidate_xmin_after;

	/* is this slot defined */
	bool          in_use;
	/* is somebody streaming out changes for this slot */
	bool          active;
	slock_t		mutex;
} LogicalDecodingSlot;

/* There is one WalSndCtl struct for the whole database cluster */
typedef struct
{
	/*
	 * Xmin across all logical slots.
	 *
	 * Protected by ProcArrayLock.
	 */
	TransactionId xmin;

	LogicalDecodingSlot logical_slots[1];		/* VARIABLE LENGTH ARRAY */
} LogicalDecodingCtlData;

extern LogicalDecodingCtlData *LogicalDecodingCtl;

extern LogicalDecodingSlot *MyLogicalDecodingSlot;

extern int	max_logical_slots;

extern Size LogicalDecodingShmemSize(void);
extern void LogicalDecodingShmemInit(void);

extern void LogicalDecodingAcquireFreeSlot(void);
extern void LogicalDecodingReleaseSlot(void);
extern void LogicalDecodingReAcquireSlot(const char *name);

extern void ComputeLogicalXmin(void);

/* change logical xmin */
extern void IncreaseLogicalXminForSlot(XLogRecPtr lsn, TransactionId xmin);

extern void CheckLogicalReplicationRequirements(void);

#endif
