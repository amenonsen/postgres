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
	/* lock, on same cacheline as effective_xmin */
	slock_t		mutex;

	/* on-disk xmin, updated first */
	TransactionId xmin;

	/* in-memory xmin, updated after syncing to disk */
	TransactionId effective_xmin;

	XLogRecPtr	  last_required_checkpoint;

	/* is this slot defined */
	bool          in_use;

	/* is somebody streaming out changes for this slot */
	bool          active;

	/* have we been aborted while ->active */
	bool          aborted;

	/*
	 * If we shutdown, crash, whatever where do we have to restart decoding
	 * from to get
	 * a) a valid snapshot
	 * b) the complete content for all in-progress xacts
	 */
	XLogRecPtr	  restart_decoding;

	/*
	 * Last location we know the client has confirmed to have safely received
	 * data to. No earlier data can be decoded after a restart/crash.
	 */
	XLogRecPtr	  confirmed_flush;

	/*
	 * When the client has confirmed flushes >= candidate_xmin_after we can
	 * a) advance our xmin
	 * b) increase restart_decoding_from
	 *
	 */
	XLogRecPtr	  candidate_lsn;
	TransactionId candidate_xmin;
	XLogRecPtr	  candidate_restart_decoding;

	Oid           database;
	NameData      name;
	NameData      plugin;
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

#define LOGICAL_MAGIC	0x1051CA1		/* format identifier */

typedef struct
{
	uint32 magic;
	TransactionId Lsn;
	LogicalDecodingSlot slot;
} LogicalDecodingCheckpointData;

extern LogicalDecodingCtlData *LogicalDecodingCtl;

extern LogicalDecodingSlot *MyLogicalDecodingSlot;

extern int	max_logical_slots;

extern Size LogicalDecodingShmemSize(void);
extern void LogicalDecodingShmemInit(void);

extern void LogicalDecodingAcquireFreeSlot(const char *plugin);
extern void LogicalDecodingReleaseSlot(void);
extern void LogicalDecodingReAcquireSlot(const char *name);
extern void LogicalDecodingFreeSlot(const char *name);

extern void ComputeLogicalXmin(void);

/* change logical xmin */
extern void IncreaseLogicalXminForSlot(XLogRecPtr lsn, TransactionId xmin);
extern void LogicalConfirmReceivedLocation(XLogRecPtr lsn);

extern void CheckLogicalReplicationRequirements(void);

extern void CheckPointLogical(XLogRecPtr checkPointRedo);

extern void StartupLogical(XLogRecPtr checkPointRedo);

extern void logical_redo(XLogRecPtr lsn, XLogRecord *record);
extern void logical_desc(StringInfo buf, uint8 xl_info, char *rec);

typedef struct
{
	LogicalDecodingSlot slot;
} xl_logical_slot;

typedef struct
{
	TransactionId xmin;
} xl_logical_xmin_changed;


#endif
