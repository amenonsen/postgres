/*-------------------------------------------------------------------------
 *
 * snapbuild.h
 *	  Exports from replication/logical/snapbuild.c.
 *
 * Portions Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * src/include/replication/snapbuild.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SNAPBUILD_H
#define SNAPBUILD_H

#include "replication/reorderbuffer.h"

#include "utils/hsearch.h"
#include "utils/snapshot.h"
#include "access/htup.h"

typedef enum
{
	/*
	 * Initial state, we can't do much yet.
	 */
	SNAPBUILD_START,

	/*
	 * We have collected enough information to decode tuples in transactions
	 * that started after this.
	 *
	 * Once we reached this we start to collect changes. We cannot apply them
	 * yet because the might be based on transactions that were still running
	 * when we reached them yet.
	 */
	SNAPBUILD_FULL_SNAPSHOT,

	/*
	 * Found a point after hitting built_full_snapshot where all transactions
	 * that were running at that point finished. Till we reach that we hold
	 * off calling any commit callbacks.
	 */
	SNAPBUILD_CONSISTENT
}	SnapBuildState;

typedef enum
{
	SNAPBUILD_SKIP,
	SNAPBUILD_DECODE
}	SnapBuildAction;

typedef struct Snapstate
{
	SnapBuildState state;

	/* all transactions smaller than this have committed/aborted */
	TransactionId xmin;

	/* all transactions bigger than this are uncommitted */
	TransactionId xmax;

	/*
	 * All transactions in this window have to be checked via the running
	 * array. This will only be used initially till we are past xmax_running.
	 *
	 * Note that we initially assume treat already running transactions to
	 * have catalog modifications because we don't have enough information
	 * about them to properly judge that.
	 */
	TransactionId xmin_running;
	TransactionId xmax_running;

	/*
	 * array of running transactions.
	 *
	 * Kept in xidComparator order so it can be searched with bsearch().
	 */
	TransactionId *running;
	/* how many transactions are still running */
	size_t		nrrunning;

	/*
	 * we need to keep track of the amount of tracked transactions separately
	 * from nrrunning_space as nrunning_initial gives the range of valid xids
	 * in the array so bsearch() can work.
	 */
	size_t		nrrunning_initial;

	XLogRecPtr transactions_after;
	TransactionId initial_xmin_horizon;

	/*
	 * Transactions which could have catalog changes that committed between
	 * xmin and xmax
	 */
	size_t		nrcommitted;
	size_t		nrcommitted_space;
	/*
	 * Array of committed transactions that have modified the catalog.
	 *
	 * As this array is frequently modified we do *not* keep it in
	 * xidComparator order. Instead we sort the array when building &
	 * distributing a snapshot.
	 */
	TransactionId *committed;

	/*
	 * private memory context used to allocate memory for this module.
	 */
	MemoryContext context;

	/*
	 * Snapshot thats valid to see all committed transactions that see catalog
	 * modifications.
	 */
	Snapshot snapshot;
}	Snapstate;

extern Snapstate *AllocateSnapshotBuilder(ReorderBuffer *cache);

extern void	FreeSnapshotBuilder(Snapstate *cache);

struct XLogRecordBuffer;

extern SnapBuildAction SnapBuildDecodeCallback(ReorderBuffer *cache, Snapstate *snapstate, struct XLogRecordBuffer *buf);

extern HeapTuple LookupTableByRelFileNode(RelFileNode *r);

extern bool SnapBuildHasCatalogChanges(Snapstate *snapstate, TransactionId xid,
                                       RelFileNode *relfilenode);

extern void SnapBuildSnapDecRefcount(Snapshot snap);

extern const char *SnapBuildExportSnapshot(Snapstate *snapstate);
extern void SnapBuildClearExportedSnapshot(void);

#endif   /* SNAPBUILD_H */
