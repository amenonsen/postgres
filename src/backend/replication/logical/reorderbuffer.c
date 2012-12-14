/*-------------------------------------------------------------------------
 *
 * reorderbuffer.c
 *
 * PostgreSQL logical replay "cache" management
 *
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/reorderbuffer.c
 *
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xact.h"

#include "catalog/pg_class.h"
#include "catalog/pg_control.h"

#include "lib/binaryheap.h"

#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"

#include "storage/sinval.h"
#include "storage/bufmgr.h"

#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/tqual.h"
#include "utils/syscache.h"



const Size max_memtries = 1 << 16;

const Size max_cached_changes = 1024;
const Size max_cached_tuplebufs = 1024; /* ~8MB */
const Size max_cached_transactions = 512;

/*
 * For efficiency and simplicity reasons we want to keep Snapshots, CommandIds
 * and ComboCids in the same list with the user visible INSERT/UPDATE/DELETE
 * changes. We don't want to leak those internal values to external users
 * though (they would just use switch()...default:) because that would make it
 * harder to add to new user visible values.
 *
 * This needs to be synchronized with ReorderBufferChangeType! Adjust the
 * StatisAssertExpr's in ReorderBufferAllocate if you add anything!
 */
enum ReorderBufferChangeTypeInternal
{
	REORDER_BUFFER_CHANGE_INTERNAL_INSERT,
	REORDER_BUFFER_CHANGE_INTERNAL_UPDATE,
	REORDER_BUFFER_CHANGE_INTERNAL_DELETE,
	REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT,
	REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID,
	REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID
};


/* entry for a hash table we use to map from xid to our transaction state */
typedef struct ReorderBufferTXNByIdEnt
{
	TransactionId xid;
	ReorderBufferTXN *txn;
}  ReorderBufferTXNByIdEnt;


/* data structures for (relfilenode, ctid) => (cmin, cmax) mapping */
typedef struct ReorderBufferTupleCidKey
{
	RelFileNode relnode;
	ItemPointerData tid;
} ReorderBufferTupleCidKey;

typedef struct ReorderBufferTupleCidEnt
{
	ReorderBufferTupleCidKey key;
	CommandId cmin;
	CommandId cmax;
	CommandId combocid; /* just for debugging */
} ReorderBufferTupleCidEnt;


/* k-way in-order change iteration support structures */
typedef struct ReorderBufferIterTXNEntry
{
	XLogRecPtr lsn;
	ReorderBufferChange *change;
	ReorderBufferTXN *txn;
} ReorderBufferIterTXNEntry;

typedef struct ReorderBufferIterTXNState
{
	binaryheap *heap;
	ReorderBufferIterTXNEntry entries[FLEXIBLE_ARRAY_MEMBER];
} ReorderBufferIterTXNState;


/* primary reorderbuffer support routines */
static ReorderBufferTXN *ReorderBufferGetTXN(ReorderBuffer *cache);
static void ReorderBufferReturnTXN(ReorderBuffer *cache, ReorderBufferTXN *txn);
static ReorderBufferTXN *ReorderBufferTXNByXid(ReorderBuffer *cache, TransactionId xid,
                                         bool create, bool *is_new);



/*
 * support functions for lsn-order iterating over the ->changes of a
 * transaction and its subtransactions
 *
 * used for iteration over the k-way heap merge of a transaction and its
 * subtransactions
 */

static ReorderBufferIterTXNState *
ReorderBufferIterTXNInit(ReorderBuffer *cache, ReorderBufferTXN *txn);
static ReorderBufferChange *
ReorderBufferIterTXNNext(ReorderBuffer *cache, ReorderBufferIterTXNState *state);
static void ReorderBufferIterTXNFinish(ReorderBuffer *cache,
									   ReorderBufferIterTXNState *state);
static void ReorderBufferExecuteInvalidations(ReorderBuffer *cache, ReorderBufferTXN *txn);

/*
 * Allocate a new ReorderBuffer
 */
ReorderBuffer *
ReorderBufferAllocate(void)
{
	ReorderBuffer *cache;
	HASHCTL hash_ctl;

	StaticAssertExpr((int)REORDER_BUFFER_CHANGE_INTERNAL_INSERT == (int)REORDER_BUFFER_CHANGE_INSERT, "out of sync enums");
	StaticAssertExpr((int)REORDER_BUFFER_CHANGE_INTERNAL_UPDATE == (int)REORDER_BUFFER_CHANGE_UPDATE, "out of sync enums");
	StaticAssertExpr((int)REORDER_BUFFER_CHANGE_INTERNAL_DELETE == (int)REORDER_BUFFER_CHANGE_DELETE, "out of sync enums");

	cache = (ReorderBuffer *) malloc(sizeof(ReorderBuffer));
	if (!cache)
		elog(ERROR, "Could not allocate the ReorderBuffer");

	memset(&hash_ctl, 0, sizeof(hash_ctl));

	cache->context = AllocSetContextCreate(TopMemoryContext,
	                                       "ReorderBuffer",
	                                       ALLOCSET_DEFAULT_MINSIZE,
	                                       ALLOCSET_DEFAULT_INITSIZE,
	                                       ALLOCSET_DEFAULT_MAXSIZE);

	hash_ctl.keysize = sizeof(TransactionId);
	hash_ctl.entrysize = sizeof(ReorderBufferTXNByIdEnt);
	hash_ctl.hash = tag_hash;
	hash_ctl.hcxt = cache->context;

	cache->by_txn = hash_create("ReorderBufferByXid", 1000, &hash_ctl,
	                            HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	cache->by_txn_last_xid = InvalidTransactionId;
	cache->by_txn_last_txn = NULL;

	cache->nr_cached_transactions = 0;
	cache->nr_cached_changes = 0;
	cache->nr_cached_tuplebufs = 0;

	dlist_init(&cache->cached_transactions);
	dlist_init(&cache->cached_changes);
	slist_init(&cache->cached_tuplebufs);

	return cache;
}

/*
 * Free a ReorderBuffer
 */
void
ReorderBufferFree(ReorderBuffer *cache)
{
	/* FIXME: check for in-progress transactions */
	/* FIXME: clean up cached transaction */
	/* FIXME: clean up cached changes */
	/* FIXME: clean up cached tuplebufs */
	hash_destroy(cache->by_txn);
	free(cache);
}

/*
 * Get a unused, possibly preallocated, ReorderBufferTXN.
 */
static ReorderBufferTXN *
ReorderBufferGetTXN(ReorderBuffer *cache)
{
	ReorderBufferTXN *txn;

	if (cache->nr_cached_transactions)
	{
		cache->nr_cached_transactions--;
		txn = dlist_container(ReorderBufferTXN, node,
		                      dlist_pop_head_node(&cache->cached_transactions));
	}
	else
	{
		txn = (ReorderBufferTXN *)
			malloc(sizeof(ReorderBufferTXN));

		if (!txn)
			elog(ERROR, "Could not allocate a ReorderBufferTXN struct");
	}

	memset(txn, 0, sizeof(ReorderBufferTXN));

	dlist_init(&txn->changes);
	dlist_init(&txn->tuplecids);
	dlist_init(&txn->subtxns);

	return txn;
}

/*
 * Free an ReorderBufferTXN. Deallocation might be delayed for efficiency
 * purposes.
 */
void
ReorderBufferReturnTXN(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
	/* clean the lookup cache if we were cached (quite likely) */
	if (cache->by_txn_last_xid == txn->xid)
	{
		cache->by_txn_last_xid = InvalidTransactionId;
		cache->by_txn_last_txn = NULL;
	}

	if (txn->tuplecid_hash != NULL)
	{
		hash_destroy(txn->tuplecid_hash);
		txn->tuplecid_hash = NULL;
	}

	if (txn->invalidations)
	{
		free(txn->invalidations);
		txn->invalidations = NULL;
	}


	if (cache->nr_cached_transactions < max_cached_transactions)
	{
		cache->nr_cached_transactions++;
		dlist_push_head(&cache->cached_transactions, &txn->node);
	}
	else
	{
		free(txn);
	}
}

/*
 * Get a unused, possibly preallocated, ReorderBufferChange.
 */
ReorderBufferChange *
ReorderBufferGetChange(ReorderBuffer *cache)
{
	ReorderBufferChange *change;

	if (cache->nr_cached_changes)
	{
		cache->nr_cached_changes--;
		change = dlist_container(ReorderBufferChange, node,
								 dlist_pop_head_node(&cache->cached_changes));
	}
	else
	{
		change = (ReorderBufferChange *) malloc(sizeof(ReorderBufferChange));

		if (!change)
			elog(ERROR, "Could not allocate a ReorderBufferChange struct");
	}


	memset(change, 0, sizeof(ReorderBufferChange));
	return change;
}

/*
 * Free an ReorderBufferChange. Deallocation might be delayed for efficiency
 * purposes.
 */
void
ReorderBufferReturnChange(ReorderBuffer *cache, ReorderBufferChange *change)
{
	switch ((enum ReorderBufferChangeTypeInternal)change->action_internal)
	{
		case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
		/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
		/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
			if (change->newtuple)
			{
				ReorderBufferReturnTupleBuf(cache, change->newtuple);
				change->newtuple = NULL;
			}

			if (change->oldtuple)
			{
				ReorderBufferReturnTupleBuf(cache, change->oldtuple);
				change->oldtuple = NULL;
			}
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
			if (change->snapshot)
			{
				SnapBuildSnapDecRefcount(change->snapshot);
				change->snapshot = NULL;
			}
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
			break;
	}

	if (cache->nr_cached_changes < max_cached_changes)
	{
		cache->nr_cached_changes++;
		dlist_push_head(&cache->cached_changes, &change->node);
	}
	else
	{
		free(change);
	}
}


/*
 * Get a unused, possibly preallocated, ReorderBufferTupleBuf
 */
ReorderBufferTupleBuf *
ReorderBufferGetTupleBuf(ReorderBuffer *cache)
{
	ReorderBufferTupleBuf *tuple;

	if (cache->nr_cached_tuplebufs)
	{
		cache->nr_cached_tuplebufs--;
		tuple = slist_container(ReorderBufferTupleBuf, node,
		                        slist_pop_head_node(&cache->cached_tuplebufs));
	}
	else
	{
		tuple =
			(ReorderBufferTupleBuf *) malloc(sizeof(ReorderBufferTupleBuf));

		if (!tuple)
			elog(ERROR, "Could not allocate a ReorderBufferTupleBuf struct");
	}

	return tuple;
}

/*
 * Free an ReorderBufferTupleBuf. Deallocation might be delayed for efficiency
 * purposes.
 */
void
ReorderBufferReturnTupleBuf(ReorderBuffer *cache, ReorderBufferTupleBuf *tuple)
{
	if (cache->nr_cached_tuplebufs < max_cached_tuplebufs)
	{
		cache->nr_cached_tuplebufs++;
		slist_push_head(&cache->cached_tuplebufs, &tuple->node);
	}
	else
	{
		free(tuple);
	}
}

/*
 * Access the transactions being processed via xid. Optionally create a new
 * entry.
 */
static ReorderBufferTXN *
ReorderBufferTXNByXid(ReorderBuffer *cache, TransactionId xid, bool create, bool *is_new)
{
	ReorderBufferTXN *txn = NULL;
	ReorderBufferTXNByIdEnt *ent = NULL;
	bool found = false;
	bool cached = false;

	/*
	 * check the one-entry lookup cache first
	 */
	if (TransactionIdIsValid(cache->by_txn_last_xid) &&
		cache->by_txn_last_xid == xid)
	{
		found = cache->by_txn_last_txn != NULL;
		txn = cache->by_txn_last_txn;

		cached = true;
	}

	/*
	 * If the cache wasn't hit or it yielded an "does-not-exist" and we want to
	 * create an entry.
	 */
	if (!cached || create)
	{
		/* search the lookup table */
		ent = (ReorderBufferTXNByIdEnt *)
			hash_search(cache->by_txn,
						(void *)&xid,
						(create ? HASH_ENTER : HASH_FIND),
						&found);

		if (found)
			txn = ent->txn;
	}

	/* don't do anything if were not creating new entries */
	if (!found && !create)
		;
	/* add new entry */
	else if (!found)
	{
		Assert(ent != NULL);

		ent->txn = ReorderBufferGetTXN(cache);
		ent->txn->xid = xid;
		txn = ent->txn;
	}

	/* update cache */
	cache->by_txn_last_xid = xid;
	cache->by_txn_last_txn = txn;

	if (is_new)
		*is_new = !found;

	Assert(!create || !!txn);
	return txn;
}

/*
 * Queue a change into a transaction so it can be replayed uppon commit.
 */
void
ReorderBufferAddChange(ReorderBuffer *cache, TransactionId xid, XLogRecPtr lsn,
                    ReorderBufferChange *change)
{
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, true, NULL);
	txn->lsn = lsn;
	dlist_push_tail(&txn->changes, &change->node);
}


/*
 * Associate a subtransaction with its toplevel transaction.
 */
void
ReorderBufferCommitChild(ReorderBuffer *cache, TransactionId xid,
						 TransactionId subxid, XLogRecPtr lsn)
{
	ReorderBufferTXN *txn;
	ReorderBufferTXN *subtxn;

	subtxn = ReorderBufferTXNByXid(cache, subxid, false, NULL);

	/*
	 * No need to do anything if that subtxn didn't contain any changes
	 */
	if (!subtxn)
		return;

	subtxn->lsn = lsn;

	txn = ReorderBufferTXNByXid(cache, xid, true, NULL);

	dlist_push_tail(&txn->subtxns, &subtxn->node);
	txn->nsubtxns++;
}


/*
 * Support for efficiently iterating over a transaction's and its
 * subtransactions' changes.
 *
 * We do by doing aa k-way merge between transactions/subtransactions. For that
 * we model the current heads of the different transactions as a binary heap so
 * we easily know which (sub-)transaction has the change with the smalles lsn
 * next.
 * Luckily the changes in individual transactions are already sorted by LSN.
 */

/*
 * Binary heap comparison function.
 */
static int
ReorderBufferIterCompare(Datum a, Datum b, void *arg)
{
	ReorderBufferChange *change_a =
		dlist_container(ReorderBufferChange, node,
						(dlist_node*)DatumGetPointer(a));

	ReorderBufferChange *change_b =
		dlist_container(ReorderBufferChange, node,
						(dlist_node*)DatumGetPointer(b));

	if (change_a->lsn < change_b->lsn)
		return -1;

	else if (change_a->lsn == change_b->lsn)
		return 0;

	return 1;
}

/*
 * Allocate & initialize an iterator which iterates in lsn order over a
 * transaction and all its subtransactions.
 */
static ReorderBufferIterTXNState *
ReorderBufferIterTXNInit(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
	Size nr_txns = 0;
	ReorderBufferIterTXNState *state;
	dlist_iter cur_txn_i;
	ReorderBufferTXN *cur_txn;
	ReorderBufferChange *cur_change;
	int32 off;

	if (!dlist_is_empty(&txn->changes))
		nr_txns++;

	/* count how large our heap must be */
	dlist_foreach(cur_txn_i, &txn->subtxns)
	{
		cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);

		if (!dlist_is_empty(&cur_txn->changes))
			nr_txns++;
	}

	/*
	 * XXX: Add fastpath for the rather common nr_txns=1 case, no need to
	 * allocate/build a heap in that case.
	 */

	/* allocate iteration state */
	state = calloc(1, sizeof(ReorderBufferIterTXNState)
				   + sizeof(ReorderBufferIterTXNEntry) * nr_txns);

	/* allocate heap */
	state->heap = binaryheap_allocate(nr_txns, ReorderBufferIterCompare, NULL);

	/*
	 * fill array with elements, heap condition not yet fullfilled. Properly
	 * building the heap afterwards is more efficient.
	 */

	off = 0;

	/* add toplevel transaction if it contains changes*/
	if (!dlist_is_empty(&txn->changes))
	{
		cur_change = dlist_head_element(ReorderBufferChange, node, &txn->changes);

		state->entries[off].lsn = cur_change->lsn;
		state->entries[off].change = cur_change;
		state->entries[off].txn = txn;

		binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
	}

	/* add subtransactions if they contain changes */
	dlist_foreach(cur_txn_i, &txn->subtxns)
	{
		cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);

		if (!dlist_is_empty(&cur_txn->changes))
		{
			cur_change = dlist_head_element(ReorderBufferChange, node,
											&cur_txn->changes);

			state->entries[off].lsn = cur_change->lsn;
			state->entries[off].change = cur_change;
			state->entries[off].txn = txn;

			binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
		}
	}

	/* make the array fullfill the heap property */
	binaryheap_build(state->heap);
	return state;
}

/*
 * Return the next change when iterating over a transaction and its
 * subtransaction.
 *
 * Returns NULL when no further changes exist.
 */
static ReorderBufferChange *
ReorderBufferIterTXNNext(ReorderBuffer *cache, ReorderBufferIterTXNState *state)
{
	ReorderBufferChange *change;
	ReorderBufferIterTXNEntry *entry;
	int32 off;

	/* nothing there anymore */
	if (state->heap->bh_size == 0)
		return NULL;

	off = DatumGetInt32(binaryheap_first(state->heap));
	entry = &state->entries[off];

	change = entry->change;

	if (!dlist_has_next(&entry->txn->changes, &entry->change->node))
	{
		binaryheap_remove_first(state->heap);
	}
	else
	{
		dlist_node *next = dlist_next_node(&entry->txn->changes, &change->node);
		ReorderBufferChange *next_change =
			dlist_container(ReorderBufferChange, node, next);

		/* txn stays the same */
		state->entries[off].lsn = next_change->lsn;
		state->entries[off].change = next_change;

		binaryheap_replace_first(state->heap, Int32GetDatum(off));
	}
	return change;
}

/*
 * Deallocate the iterator
 */
static void
ReorderBufferIterTXNFinish(ReorderBuffer *cache, ReorderBufferIterTXNState *state)
{
	binaryheap_free(state->heap);
	free(state);
}


/*
 * Cleanup the contents of a transaction, usually after the transaction
 * committed or aborted.
 */
static void
ReorderBufferCleanupTXN(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
	bool found;
	dlist_mutable_iter cur_change;
	dlist_mutable_iter cur_txn;

	/* cleanup subtransactions & their changes */
	dlist_foreach_modify(cur_txn, &txn->subtxns)
	{
		ReorderBufferTXN *subtxn = dlist_container(ReorderBufferTXN, node, cur_txn.cur);

		dlist_foreach_modify(cur_change, &subtxn->changes)
		{
			ReorderBufferChange *change =
				dlist_container(ReorderBufferChange, node, cur_change.cur);

			ReorderBufferReturnChange(cache, change);
		}
		ReorderBufferReturnTXN(cache, subtxn);
	}

	/* cleanup changes in the toplevel txn */
	dlist_foreach_modify(cur_change, &txn->changes)
	{
		ReorderBufferChange *change =
			dlist_container(ReorderBufferChange, node, cur_change.cur);

		ReorderBufferReturnChange(cache, change);
	}

	/*
	 * cleanup the tuplecids we stored timetravel access. They are always
	 * stored in the toplevel transaction.
	 */
	dlist_foreach_modify(cur_change, &txn->tuplecids)
	{
		ReorderBufferChange *change =
			dlist_container(ReorderBufferChange, node, cur_change.cur);
		Assert(change->action_internal == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);
		ReorderBufferReturnChange(cache, change);
	}

	/* now remove reference from cache */
	hash_search(cache->by_txn,
	            (void *)&txn->xid,
	            HASH_REMOVE,
	            &found);
	Assert(found);

	/* deallocate */
	ReorderBufferReturnTXN(cache, txn);
}

/*
 * Build a hash with a (relfilenode, ctid) -> (cmin, cmax) mapping for use by
 * tqual.c's HeapTupleSatisfiesMVCCDuringDecoding.
 */
static void
ReorderBufferBuildTupleCidHash(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
	dlist_iter cur_change;
	HASHCTL hash_ctl;

	if (!txn->does_timetravel || dlist_is_empty(&txn->tuplecids))
		return;

	memset(&hash_ctl, 0, sizeof(hash_ctl));

	hash_ctl.keysize = sizeof(ReorderBufferTupleCidKey);
	hash_ctl.entrysize = sizeof(ReorderBufferTupleCidEnt);
	hash_ctl.hash = tag_hash;
	hash_ctl.hcxt = cache->context;

	/*
	 * create the hash with the exact number of to-be-stored tuplecids from the
	 * start
	 */
	txn->tuplecid_hash =
		hash_create("ReorderBufferTupleCid", txn->ntuplecids, &hash_ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	dlist_foreach(cur_change, &txn->tuplecids)
	{
		ReorderBufferTupleCidKey key;
		ReorderBufferTupleCidEnt *ent;
		bool found;
		ReorderBufferChange *change =
			dlist_container(ReorderBufferChange, node, cur_change.cur);

		Assert(change->action_internal == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);

		/* be careful about padding */
		memset(&key, 0, sizeof(ReorderBufferTupleCidKey));

		key.relnode = change->tuplecid.node;

		ItemPointerCopy(&change->tuplecid.tid,
						&key.tid);

		ent = (ReorderBufferTupleCidEnt *)
			hash_search(txn->tuplecid_hash,
						(void *)&key,
						HASH_ENTER|HASH_FIND,
						&found);
		if (!found)
		{
			ent->cmin = change->tuplecid.cmin;
			ent->cmax = change->tuplecid.cmax;
			ent->combocid = change->tuplecid.combocid;
		}
		else
		{
			Assert(ent->cmin == change->tuplecid.cmin);
			Assert(ent->cmax == InvalidCommandId ||
				   ent->cmax == change->tuplecid.cmax);
			/*
			 * if the tuple got valid in this transaction and now got deleted
			 * we already have a valid cmin stored. The cmax will be
			 * InvalidCommandId though.
			 */
			ent->cmax = change->tuplecid.cmax;
		}
	}
}

/*
 * Copy a provided snapshot so we can modify it privately. This is needed so
 * that catalog modifying transactions can look into intermediate catalog
 * states.
 */
static Snapshot
ReorderBufferCopySnap(ReorderBuffer *cache, Snapshot orig_snap,
					  ReorderBufferTXN *txn, CommandId cid)
{
	Snapshot snap;
	dlist_iter sub_txn_i;
	ReorderBufferTXN *sub_txn;
	int i = 0;
	Size size = sizeof(SnapshotData) +
		sizeof(TransactionId) * orig_snap->xcnt +
		sizeof(TransactionId) * (txn->nsubtxns + 1)
		;

	elog(DEBUG1, "copying a non-transaction-specific snapshot into timetravel tx %u", txn->xid);

	/* we only want to start with snapshots as provided by snapbuild.c */
	Assert(!orig_snap->subxip);
	Assert(!orig_snap->copied);

	snap = calloc(1, size);
	memcpy(snap, orig_snap, sizeof(SnapshotData));

	snap->copied = true;
	snap->active_count = 0;
	snap->regd_count = 0;
	snap->xip = (TransactionId *) (snap + 1);

	memcpy(snap->xip, orig_snap->xip, sizeof(TransactionId) * snap->xcnt);

	/*
	 * ->subxip contains all txids that belong to our transaction which we need
	 * to check via cmin/cmax. Thats why we store the toplevel transaction in
	 * there as well.
	 */
	snap->subxip = snap->xip + snap->xcnt;
	snap->subxip[i++] = txn->xid;
	snap->subxcnt = txn->nsubtxns + 1;

	dlist_foreach(sub_txn_i, &txn->subtxns)
	{
		sub_txn = dlist_container(ReorderBufferTXN, node, sub_txn_i.cur);
		snap->subxip[i++] = sub_txn->xid;
	}

	/* bsearch()ability */
	qsort(snap->subxip, snap->subxcnt,
		  sizeof(TransactionId), xidComparator);

	/*
	 * store the specified current CommandId
	 */
	snap->curcid = cid;

	return snap;
}

/*
 * Free a previously ReorderBufferCopySnap'ed snapshot
 */
static void
ReorderBufferFreeSnap(ReorderBuffer *cache, Snapshot snap)
{
	Assert(snap->copied);
	free(snap);
}

/*
 * Commit a transaction and replay all actions that previously have been
 * ReorderBufferAddChange'd in the toplevel TX or any of the subtransactions
 * assigned via ReorderBufferCommitChild.
 */
void
ReorderBufferCommit(ReorderBuffer *cache, TransactionId xid, XLogRecPtr lsn)
{
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, false, NULL);
	ReorderBufferIterTXNState *iterstate = NULL;
	ReorderBufferChange *change;
	CommandId command_id = FirstCommandId;
	Snapshot snapshot_mvcc = NULL;
	Snapshot snapshot_now = NULL;
	bool snapshot_copied = false;

	/* empty transaction */
	if (!txn)
		return;

	txn->lsn = lsn;

	cache->begin(cache, txn);

	PG_TRY();
	{
		ReorderBufferBuildTupleCidHash(cache, txn);

		iterstate = ReorderBufferIterTXNInit(cache, txn);
		while ((change = ReorderBufferIterTXNNext(cache, iterstate)))
		{
			switch ((enum ReorderBufferChangeTypeInternal)change->action_internal)
			{
				case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
				case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
				case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
					Assert(snapshot_mvcc != NULL);
					if (!SnapBuildHasCatalogChanges(NULL, xid, &change->relnode))
						cache->apply_change(cache, txn, change);
					break;
				case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
					/* XXX: we could skip snapshots in non toplevel txns */
					if (snapshot_copied)
					{
						ReorderBufferFreeSnap(cache, snapshot_now);
						snapshot_now = ReorderBufferCopySnap(cache, change->snapshot,
															 txn, command_id);
					}
					else
					{
						snapshot_now = change->snapshot;
					}

					/*
					 * the first snapshot seen in a transaction is its mvcc
					 * snapshot
					 */
					if (!snapshot_mvcc)
						snapshot_mvcc = snapshot_now;
					else
						RevertFromDecodingSnapshots();

					SetupDecodingSnapshots(snapshot_now, txn->tuplecid_hash);
					break;

				case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
					if (!snapshot_copied && snapshot_now)
					{
						/* we don't use the global one anymore */
						snapshot_copied = true;
						snapshot_now = ReorderBufferCopySnap(cache, snapshot_now,
														  txn, command_id);
					}

					command_id = Max(command_id, change->command_id);

					if (snapshot_now && command_id != InvalidCommandId)
					{
						snapshot_now->curcid = command_id;

						RevertFromDecodingSnapshots();
						SetupDecodingSnapshots(snapshot_now, txn->tuplecid_hash);
					}

					/*
					 * everytime the CommandId is incremented, we could see new
					 * catalog contents
					 */
					ReorderBufferExecuteInvalidations(cache, txn);

					break;

				case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
					elog(ERROR, "tuplecid value in normal queue");
					break;
			}
		}

		ReorderBufferIterTXNFinish(cache, iterstate);

		/* call commit callback */
		cache->commit(cache, txn, lsn);

		/* cleanup */
		RevertFromDecodingSnapshots();
		ReorderBufferExecuteInvalidations(cache, txn);

		ReorderBufferCleanupTXN(cache, txn);

		if (snapshot_copied)
		{
			ReorderBufferFreeSnap(cache, snapshot_now);
		}
	}
	PG_CATCH();
	{
		if (iterstate)
			ReorderBufferIterTXNFinish(cache, iterstate);

		/*
		 * XXX: do we want to do this here?
		 * ReorderBufferCleanupTXN(cache, txn);
		 */

		RevertFromDecodingSnapshots();
		ReorderBufferExecuteInvalidations(cache, txn);

		if (snapshot_copied)
		{
			ReorderBufferFreeSnap(cache, snapshot_now);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Abort a transaction that possibly has previous changes. Needs to be done
 * independently for toplevel and subtransactions.
 */
void
ReorderBufferAbort(ReorderBuffer *cache, TransactionId xid, XLogRecPtr lsn)
{
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, false, NULL);

	/* no changes in this commit */
	if (!txn)
		return;

	ReorderBufferCleanupTXN(cache, txn);
}

/*
 * Check whether a transaction is already known in this module
 */
bool
ReorderBufferIsXidKnown(ReorderBuffer *cache, TransactionId xid)
{
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, false, NULL);

	return txn != NULL;
}

/*
 * Add a new snapshot to this transaction which is the "base" of snapshots we
 * modify if this is a catalog modifying transaction.
 *
 * FIXME: Split into ReorderBufferSetBaseSnapshot & ReorderBufferAddSnapshot
 */
void
ReorderBufferAddBaseSnapshot(ReorderBuffer *cache, TransactionId xid,
							 XLogRecPtr lsn, Snapshot snap)
{
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, true, NULL);
	ReorderBufferChange *change = ReorderBufferGetChange(cache);

	change->snapshot = snap;
	change->action_internal = REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT;

	if (lsn == InvalidXLogRecPtr)
		txn->has_base_snapshot = true;

	ReorderBufferAddChange(cache, xid, lsn, change);
}

/*
 * Access the catalog with this CommandId at this point in the changestream.
 *
 * May only be called for command ids > 1
 */
void
ReorderBufferAddNewCommandId(ReorderBuffer *cache, TransactionId xid,
							 XLogRecPtr lsn, CommandId cid)
{
	ReorderBufferChange *change = ReorderBufferGetChange(cache);

	change->command_id = cid;
	change->action_internal = REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID;

	ReorderBufferAddChange(cache, xid, lsn, change);
}


/*
 * Add new (relfilenode, tid) -> (cmin, cmax) mappings.
 */
void
ReorderBufferAddNewTupleCids(ReorderBuffer *cache, TransactionId xid, XLogRecPtr lsn,
							 RelFileNode node, ItemPointerData tid,
							 CommandId cmin, CommandId cmax, CommandId combocid)
{
	ReorderBufferChange *change = ReorderBufferGetChange(cache);
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, true, NULL);

	change->tuplecid.node = node;
	change->tuplecid.tid = tid;
	change->tuplecid.cmin = cmin;
	change->tuplecid.cmax = cmax;
	change->tuplecid.combocid = combocid;
	change->lsn = lsn;
	change->action_internal = REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID;

	dlist_push_tail(&txn->tuplecids, &change->node);
	txn->ntuplecids++;
}

/*
 * Setup the invalidation of the toplevel transaction.
 *
 * This needs to be done before ReorderBufferCommit is called!
 */
void
ReorderBufferAddInvalidations(ReorderBuffer *cache, TransactionId xid, XLogRecPtr lsn,
                                Size nmsgs, SharedInvalidationMessage* msgs)
{
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, true, NULL);

	if (txn->ninvalidations)
		elog(ERROR, "only ever add one set of invalidations");

	txn->invalidations = malloc(sizeof(SharedInvalidationMessage) * nmsgs);

	if (!txn->invalidations)
		elog(ERROR, "could not allocate memory for invalidations");

	memcpy(txn->invalidations, msgs, sizeof(SharedInvalidationMessage) * nmsgs);
	txn->ninvalidations = nmsgs;
}

/*
 * Apply all invalidations we know. Possibly we only need parts at this point
 * in the changestream but we don't know which those are.
 */
static void
ReorderBufferExecuteInvalidations(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
	int i;
	for (i = 0; i < txn->ninvalidations; i++)
	{
		LocalExecuteInvalidationMessage(&txn->invalidations[i]);
	}
}

/*
 * Mark a transaction as doing timetravel.
 */
void
ReorderBufferXidSetTimetravel(ReorderBuffer *cache, TransactionId xid)
{
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, true, NULL);
	txn->does_timetravel = true;
}

/*
 * Query whether a transaction is already *known* to be doing timetravel. This
 * can be wrong until directly before the commit!
 */
bool
ReorderBufferXidDoesTimetravel(ReorderBuffer *cache, TransactionId xid)
{
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, false, NULL);
	if (!txn)
		return false;
	return txn->does_timetravel;
}

/*
 * Have we already added the first snapshot?
 */
bool
ReorderBufferXidHasBaseSnapshot(ReorderBuffer *cache, TransactionId xid)
{
	ReorderBufferTXN *txn = ReorderBufferTXNByXid(cache, xid, false, NULL);
	if (!txn)
		return false;
	return txn->has_base_snapshot;
}

/*
 * Visibility support routines
 */

/*-------------------------------------------------------------------------
 * Lookup actual cmin/cmax values during timetravel access. We can't always
 * rely on stored cmin/cmax values because of two scenarios:
 *
 * * A tuple got changed multiple times during a single transaction and thus
 *   has got a combocid. Combocid's are only valid for the duration of a single
 *   transaction.
 * * A tuple with a cmin but no cmax (and thus no combocid) got deleted/updated
 *   in another transaction than the one which created it which we are looking
 *   at right now. As only one of cmin, cmax or combocid is actually stored in
 *   the heap we don't have access to the the value we need anymore.
 *
 * To resolve those problems we have a per-transaction hash of (cmin, cmax)
 * tuples keyed by (relfilenode, ctid) which contains the actual (cmin, cmax)
 * values. That also takes care of combocids by simply not caring about them at
 * all. As we have the real cmin/cmax values thats enough.
 *
 * As we only care about catalog tuples here the overhead of this hashtable
 * should be acceptable.
 * -------------------------------------------------------------------------
 */
extern bool
ResolveCminCmaxDuringDecoding(HTAB *tuplecid_data,
							  HeapTuple htup, Buffer buffer,
							  CommandId *cmin, CommandId *cmax)
{
	ReorderBufferTupleCidKey key;
	ReorderBufferTupleCidEnt* ent;
	ForkNumber forkno;
	BlockNumber blockno;

	/* be careful about padding */
	memset(&key, 0, sizeof(key));

	Assert(!BufferIsLocal(buffer));

	/*
	 * get relfilenode from the buffer, no convenient way to access it other
	 * than that.
	 */
	BufferGetTag(buffer, &key.relnode, &forkno, &blockno);

	/* tuples can only be in the main fork */
	Assert(forkno == MAIN_FORKNUM);
	Assert(blockno == ItemPointerGetBlockNumber(&htup->t_self));

	ItemPointerCopy(&htup->t_self,
					&key.tid);

	ent = (ReorderBufferTupleCidEnt *)
		hash_search(tuplecid_data,
					(void *)&key,
					HASH_FIND,
					NULL);

	if (ent == NULL)
		return false;

	if (cmin)
		*cmin = ent->cmin;
	if (cmax)
		*cmax = ent->cmax;
	return true;
}
