/*-------------------------------------------------------------------------
 *
 * logical.c
 *
 *     Logical decoding shared memory management
 *
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical.c
 *
 */

#include "postgres.h"

#include "replication/logical.h"

#include "miscadmin.h"
#include "access/transam.h"
#include "storage/ipc.h"
#include "storage/procarray.h"

/* Control array for logical decoding */
LogicalDecodingCtlData *LogicalDecodingCtl = NULL;

/* My slot for logical rep in the shared memory array */
LogicalDecodingSlot *MyLogicalDecodingSlot = NULL;

/* user settable parameters */
int			max_logical_slots = 0;	/* the maximum number of logical slots */

static void LogicalSlotKill(int code, Datum arg);

/* Report shared-memory space needed by LogicalDecodingShmemInit */
Size
LogicalDecodingShmemSize(void)
{
	Size		size = 0;

	size = offsetof(LogicalDecodingCtlData, logical_slots);
	size = add_size(size,
					mul_size(max_logical_slots, sizeof(LogicalDecodingSlot)));

	return size;
}

/* Allocate and initialize walsender-related shared memory */
void
LogicalDecodingShmemInit(void)
{
	bool		found;

	LogicalDecodingCtl = (LogicalDecodingCtlData *)
		ShmemInitStruct("Logical Decoding Ctl", LogicalDecodingShmemSize(),
						&found);

	if (!found)
	{
		int i;
		/* First time through, so initialize */
		MemSet(LogicalDecodingCtl, 0, LogicalDecodingShmemSize());

		LogicalDecodingCtl->xmin = InvalidTransactionId;

		if (max_logical_slots > 0)
		{
			for (i = 0; i < max_logical_slots; i++)
			{
				LogicalDecodingSlot *slot =
					&LogicalDecodingCtl->logical_slots[i];
				slot->xmin = InvalidTransactionId;
				SpinLockInit(&slot->mutex);
			}
		}
	}

	/* Arrange to clean up at exit */
	on_shmem_exit(LogicalSlotKill, 0);
}

static void
LogicalSlotKill(int code, Datum arg)
{
	/* LOCK? */
	if(MyLogicalDecodingSlot && MyLogicalDecodingSlot->active)
	{
		MyLogicalDecodingSlot->active = false;
	}
	MyLogicalDecodingSlot = NULL;
}

/*
 * Set the xmin required for catalog timetravel for the specific decoding slot.
 */
void
IncreaseLogicalXminForSlot(XLogRecPtr lsn, TransactionId xmin)
{
	Assert(MyLogicalDecodingSlot != NULL);

	SpinLockAcquire(&MyLogicalDecodingSlot->mutex);
	/*
	 * Only increase if the previous value has been applied...
	 */
	if (!TransactionIdIsValid(MyLogicalDecodingSlot->candidate_xmin))
	{
		MyLogicalDecodingSlot->candidate_xmin_after = lsn;
		MyLogicalDecodingSlot->candidate_xmin = xmin;
		elog(LOG, "got new xmin %u at %X/%X", xmin,
			 (uint32)(lsn >> 32), (uint32)lsn); /* FIXME: log level */
	}
	SpinLockRelease(&MyLogicalDecodingSlot->mutex);
}

/*
 * Compute the xmin between all of the decoding slots and store it in
 * WalSndCtlData.
 */
void
ComputeLogicalXmin(void)
{
	int i;
	TransactionId xmin = InvalidTransactionId;
	LogicalDecodingSlot *slot;

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	for (i = 0; i < max_logical_slots; i++)
	{
		slot = &LogicalDecodingCtl->logical_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->in_use &&
			TransactionIdIsValid(slot->xmin) && (
				!TransactionIdIsValid(xmin) ||
				TransactionIdPrecedes(slot->xmin, xmin))
			)
		{
			xmin = slot->xmin;
		}
		SpinLockRelease(&slot->mutex);
	}
	LogicalDecodingCtl->xmin = xmin;
	LWLockRelease(ProcArrayLock);

	elog(LOG, "computed new global xmin for decoding: %u", xmin);
}

void
CheckLogicalReplicationRequirements(void)
{
	if (wal_level < WAL_LEVEL_LOGICAL)
		ereport(ERROR, (errmsg("logical replication requires wal_level=logical")));

	if (MyDatabaseId == InvalidOid)
		ereport(ERROR, (errmsg("logical replication requires to be connected to a database")));

	if (max_logical_slots == 0)
		ereport(ERROR, (errmsg("logical replication requires needs max_logical_slots > 0")));
}

void LogicalDecodingAcquireFreeSlot()
{
	LogicalDecodingSlot *slot = NULL;
	int i;
	char	   *slot_name;

	Assert(!MyLogicalDecodingSlot);

	for (i = 0; i < max_logical_slots; i++)
	{
		slot = &LogicalDecodingCtl->logical_slots[i];
		SpinLockAcquire(&slot->mutex);
		if (!slot->in_use)
		{
			Assert(!slot->active);
			/* NOT releasing the lock yet */
			break;
		}
		SpinLockRelease(&slot->mutex);
		slot = NULL;
	}

	if (!slot)
	{
		elog(ERROR, "couldn't find free logical slot. free one or increase max_logical_slots");
	}

	MyLogicalDecodingSlot = slot;

	slot->last_required_checkpoint = GetRedoRecPtr();
	slot->in_use = true;
	slot->active = true;
	slot->database = MyDatabaseId;

	slot_name = NameStr(slot->name);
	sprintf(slot_name, "id-%d", i);

	/* release slot so it can be examined by others */
	SpinLockRelease(&slot->mutex);

	/*
	 * Acquire the current global xmin value and directly set the logical xmin
	 * before releasing the lock if necessary. We do this so wal decoding is
	 * guaranteed to have all catalog rows produced by xacts with an xid >
	 * walsnd->xmin available.
	 *
	 * We can't use ComputeLogicalXmin here as that acquires ProcArrayLock
	 * separately which would open a short window for the global xmin to
	 * advance above walsnd->xmin.
	 */
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	slot->xmin = GetOldestXminNoLock(true, true);

	if (!TransactionIdIsValid(LogicalDecodingCtl->xmin) ||
		NormalTransactionIdPrecedes(slot->xmin, LogicalDecodingCtl->xmin))
		LogicalDecodingCtl->xmin = slot->xmin;
	LWLockRelease(ProcArrayLock);
}

void LogicalDecodingReAcquireSlot(const char *name)
{
	LogicalDecodingSlot *slot;
	int i;

	Assert(!MyLogicalDecodingSlot);

	for (i = 0; i < max_logical_slots; i++)
	{
		slot = &LogicalDecodingCtl->logical_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->in_use && strcmp(name, NameStr(slot->name)) == 0)
		{
			MyLogicalDecodingSlot = slot;
			/* NOT releasing the lock yet */
			break;
		}
		SpinLockRelease(&slot->mutex);
	}

	if (!MyLogicalDecodingSlot)
		elog(ERROR, "couldn't find logical slot for \"%s\"",
		     name);

	slot = MyLogicalDecodingSlot;

	if (slot->active)
	{
		SpinLockRelease(&slot->mutex);
		elog(ERROR, "slot already active");
	}

	slot->active = true;
	/* now that we've marked it as active, we release our lock */
	SpinLockRelease(&slot->mutex);

	/* Don't let the user switch the database... */
	if (slot->database != MyDatabaseId)
	{
		SpinLockAcquire(&slot->mutex);
		slot->active = false;
		SpinLockRelease(&slot->mutex);

		ereport(ERROR, (errmsg("START_LOGICAL_REPLICATION needs to be run in the same database as INIT_LOGICAL_REPLICATION")));
	}
}

void
LogicalDecodingReleaseSlot(void)
{
	LogicalDecodingSlot *slot = MyLogicalDecodingSlot;
	Assert(slot != NULL);

	SpinLockAcquire(&slot->mutex);
	slot->active = false;
	SpinLockRelease(&slot->mutex);

	MyLogicalDecodingSlot = NULL;
}
