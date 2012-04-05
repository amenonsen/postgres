/*
 * reorderbuffer.h
 *
 * PostgreSQL logical replay "cache" management
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/replication/reorderbuffer.h
 */
#ifndef REORDERBUFFER_H
#define REORDERBUFFER_H

#include "access/htup_details.h"
#include "utils/hsearch.h"

#include "lib/ilist.h"

#include "storage/sinval.h"

#include "utils/snapshot.h"


typedef struct ReorderBuffer ReorderBuffer;

enum ReorderBufferChangeType
{
	REORDER_BUFFER_CHANGE_INSERT,
	REORDER_BUFFER_CHANGE_UPDATE,
	REORDER_BUFFER_CHANGE_DELETE
};

typedef struct ReorderBufferTupleBuf
{
	/* position in preallocated list */
	slist_node node;

	HeapTupleData tuple;
	HeapTupleHeaderData header;
	char		data[MaxHeapTupleSize];
}	ReorderBufferTupleBuf;

typedef struct ReorderBufferChange
{
	XLogRecPtr	lsn;

	union {
		enum ReorderBufferChangeType action;
		/* do not leak internal enum values to the outside */
		int action_internal;
	};

	RelFileNode relnode;

	union
	{
		ReorderBufferTupleBuf *newtuple;
		Snapshot	snapshot;
		CommandId	command_id;
		struct {
			RelFileNode node;
			ItemPointerData tid;
			CommandId	cmin;
			CommandId	cmax;
			CommandId	combocid;
		} tuplecid;
	};

	ReorderBufferTupleBuf *oldtuple;

	/*
	 * While in use this is how a change is linked into a transactions,
	 * otherwise its the preallocated list.
	 */
	dlist_node node;
} ReorderBufferChange;

typedef struct ReorderBufferTXN
{
	TransactionId xid;

	XLogRecPtr	lsn;

	/* did the TX have catalog changes */
	bool does_timetravel;

	bool has_base_snapshot;

	/*
	 * How many ReorderBufferChange's do we have in this txn.
	 *
	 * Subtransactions are *not* included.
	 */
	Size		nentries;

	/*
	 * How many of the above entries are stored in memory in contrast to being
	 * spilled to disk.
	 */
	Size		nentries_mem;

	/*
	 * List of actual changes, those include new Snapshots and new CommandIds
	 */
	dlist_head changes;

	/*
	 * List of cmin/cmax pairs for catalog tuples
	 */
	dlist_head tuplecids;

	/*
	 * Numer of stored cmin/cmax pairs. Used to create the tuplecid_hash with
	 * the correct size.
	 */
	Size       ntuplecids;

	/*
	 * On-demand built hash for looking up the above values.
	 */
	HTAB	   *tuplecid_hash;

	/*
	 * non-hierarchical list of subtransactions that are *not* aborted
	 */
	dlist_head subtxns;
	Size nsubtxns;

	/*
	 * our position in a list of subtransactions while the TXN is in use.
	 * Otherwise its the position in the list of preallocated transactions.
	 */
	dlist_node node;

	/*
	 * Number of stored cache invalidations.
	 */
	Size ninvalidations;

	/*
	 * Stored cache invalidations. This is not a linked list because we get all
	 * the invalidations at once.
	 */
	SharedInvalidationMessage *invalidations;

} ReorderBufferTXN;


/* XXX: were currently passing the originating subtxn. Not sure thats necessary */
typedef void (*ReorderBufferApplyChangeCB) (
	ReorderBuffer *cache,
	ReorderBufferTXN *txn,
	ReorderBufferChange *change);

typedef void (*ReorderBufferBeginCB) (
	ReorderBuffer *cache,
	ReorderBufferTXN *txn);

typedef void (*ReorderBufferCommitCB) (
	ReorderBuffer *cache,
	ReorderBufferTXN *txn,
	XLogRecPtr commit_lsn);

/*
 * max number of concurrent top-level transactions or transaction where we
 * don't know if they are top-level can be calculated by:
 * (max_connections + max_prepared_xactx + ?)  * PGPROC_MAX_CACHED_SUBXIDS
 */
struct ReorderBuffer
{
	/*
	 * Should snapshots for decoding be collected. If many catalog changes
	 * happen this can be considerably expensive.
	 */
	bool		build_snapshots;

	TransactionId last_txn;
	ReorderBufferTXN *last_txn_cache;
	HTAB	   *by_txn;

	ReorderBufferBeginCB begin;
	ReorderBufferApplyChangeCB apply_change;
	ReorderBufferCommitCB commit;

	void	   *private_data;

	MemoryContext context;

	/*
	 * we don't want to repeatedly (de-)allocated those structs, so cache them
	 * for reusage.
	 */
	dlist_head cached_transactions;
	Size		nr_cached_transactions;

	dlist_head cached_changes;
	Size		nr_cached_changes;

	slist_head cached_tuplebufs;
	Size		nr_cached_tuplebufs;
};


ReorderBuffer *ReorderBufferAllocate(void);

void ReorderBufferFree(ReorderBuffer *);

ReorderBufferTupleBuf *ReorderBufferGetTupleBuf(ReorderBuffer *);

void ReorderBufferReturnTupleBuf(ReorderBuffer *cache, ReorderBufferTupleBuf * tuple);

/*
 * Returns a (potentically preallocated) change struct. Its lifetime is managed
 * by the reorderbuffer module.
 *
 * If not added to a transaction with ReorderBufferAddChange it needs to be
 * returned via ReorderBufferReturnChange
 *
 * FIXME: better name
 */
ReorderBufferChange *
			ReorderBufferGetChange(ReorderBuffer *);

/*
 * Return an unused ReorderBufferChange struct
 */
void ReorderBufferReturnChange(ReorderBuffer *, ReorderBufferChange *);


/*
 * record the transaction as in-progress if not already done, add the current
 * change.
 *
 * We have a one-entry cache for lookin up the current ReorderBufferTXN so we
 * don't need to do a full hash-lookup if the same xid is used
 * sequentially. Them being used multiple times that way is rather frequent.
 */
void ReorderBufferAddChange(ReorderBuffer *, TransactionId, XLogRecPtr lsn, ReorderBufferChange *);

/*
 *
 */
void ReorderBufferCommit(ReorderBuffer *, TransactionId, XLogRecPtr lsn);

void ReorderBufferCommitChild(ReorderBuffer *, TransactionId, TransactionId, XLogRecPtr lsn);

void ReorderBufferAbort(ReorderBuffer *, TransactionId, XLogRecPtr lsn);

/*
 * if lsn == InvalidXLogRecPtr this is the first snap for the transaction
 *
 * most callers don't need snapshot.h, so we use struct SnapshotData instead
 */
void ReorderBufferAddBaseSnapshot(ReorderBuffer *, TransactionId, XLogRecPtr lsn, struct SnapshotData *snap);

/*
 * Will only be called for command ids > 1
 */
void ReorderBufferAddNewCommandId(ReorderBuffer *, TransactionId, XLogRecPtr lsn,
								  CommandId cid);

void ReorderBufferAddNewTupleCids(ReorderBuffer *, TransactionId, XLogRecPtr lsn,
								  RelFileNode node, ItemPointerData pt,
								  CommandId cmin, CommandId cmax, CommandId combocid);

void ReorderBufferAddInvalidations(ReorderBuffer *, TransactionId, XLogRecPtr lsn,
								   Size nmsgs, SharedInvalidationMessage* msgs);

bool ReorderBufferIsXidKnown(ReorderBuffer *cache, TransactionId xid);

/*
 * Announce that tx does timetravel. Relevant for the whole toplevel/subtxn
 * tree.
 */
void ReorderBufferXidSetTimetravel(ReorderBuffer *cache, TransactionId xid);

/*
 * Does the transaction indicated by 'xid' do timetravel?
 */
bool ReorderBufferXidDoesTimetravel(ReorderBuffer *cache, TransactionId xid);

bool ReorderBufferXidHasBaseSnapshot(ReorderBuffer *cache, TransactionId xid);

#endif
