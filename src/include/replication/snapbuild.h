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
	/* how far are we along building our first full snapshot */
	SnapBuildState state;

	/* private memory context used to allocate memory for this module. */
	MemoryContext context;

	/* all transactions smaller than this have committed/aborted */
	TransactionId xmin;

	/* all transactions bigger than this are uncommitted */
	TransactionId xmax;

	/*
	 * Information about initially running transactions
	 *
	 * When we start building a snapshot there already may be transactions in
	 * progress. We don't have enough information about those to decode their
	 * contents, so until they are finished we cannot switch to a CONSISTENT
	 * state.
	 */
	struct
	{
		/*
		 * As long as running.xcnt all XIDs < running.xmin and > running.xmax
		 * have to be checked whether they still are running.
		 */
		TransactionId xmin;
		TransactionId xmax;

		/*
		 * how many transactions are still running. When this reaches zero we
		 * can switch to a consistent state.
		 */
		size_t		xcnt;

		/*
		 * we need to keep track of the amount of tracked transactions
		 * separately from nrrunning_space as nrunning_initial gives the range
		 * of valid xids in the array so a bsearch() can work.
		 */
		size_t		xcnt_space;

		/*
		 * xidComparator sorted array of running transactions.
		 */
		TransactionId *xip;
	} running;

	/*
	 * Array of transactions which could have catalog changes that committed
	 * between xmin and xmax
	 */
	struct
	{
		/* number of committed transactions */
		size_t		xcnt;
		/* available space for committed transactions */
		size_t		xcnt_space;
		/*
		 * Array of committed transactions that have modified the catalog.
		 *
		 * As this array is frequently modified we do *not* keep it in
		 * xidComparator order. Instead we sort the array when building &
		 * distributing a snapshot.
		 */
		TransactionId *xip;
	} committed;

	/*
	 * Don't replay commits from an LSN <= this LSN. This can be set externally
	 * but it will also be advanced (never reatreat) from within snapbuild.c.
	 */
	XLogRecPtr transactions_after;

	/*
	 * Don't start decoding WAL until the "xl_running_xacts" information
	 * indicates there are no running xids with a xid smaller than this.
	 */
	TransactionId initial_xmin_horizon;

	/*
	 * Snapshot thats valid to see all currently committed transactions that
	 * see catalog modifications.
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
