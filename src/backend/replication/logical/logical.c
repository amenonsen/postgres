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

#include <unistd.h>
#include <sys/stat.h>

#include "replication/logical.h"

#include "miscadmin.h"
#include "access/transam.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/fd.h"
#include "storage/copydir.h"

/* Control array for logical decoding */
LogicalDecodingCtlData *LogicalDecodingCtl = NULL;

/* My slot for logical rep in the shared memory array */
LogicalDecodingSlot *MyLogicalDecodingSlot = NULL;

/* user settable parameters */
int			max_logical_slots = 0;	/* the maximum number of logical slots */

static void LogicalSlotKill(int code, Datum arg);

/* persistency functions */
static void RestoreLogicalSlot(const char* name);
static void CreateLogicalSlot(LogicalDecodingSlot *slot);
static void SaveLogicalSlot(LogicalDecodingSlot *slot);
static void SaveLogicalSlotInternal(LogicalDecodingSlot *slot, const char *path);
static void DeleteLogicalSlot(LogicalDecodingSlot *slot);


/* Report shared-memory space needed by LogicalDecodingShmemInit */
Size
LogicalDecodingShmemSize(void)
{
	Size		size = 0;

	if (max_logical_slots == 0)
		return size;

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

	if (max_logical_slots == 0)
		return;

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

	Assert(LogicalDecodingCtl);

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

/*
 * Make sure the current settings & environment is capable of doing logical
 * replication.
 */
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

/*
 * Search for a free slot, mark it as used and acquire a valid xmin horizon
 * value.
 */
void LogicalDecodingAcquireFreeSlot(const char *plugin)
{
	LogicalDecodingSlot *slot = NULL;
	int i;
	char	   *slot_name;

	Assert(!MyLogicalDecodingSlot);

	CheckLogicalReplicationRequirements();

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
	/* XXX: do we want to use truncate identifier instead? */
	strncpy(NameStr(slot->plugin), plugin, NAMEDATALEN);
	NameStr(slot->plugin)[NAMEDATALEN-1] = '\0';

	/* Arrange to clean up at exit/error */
	on_shmem_exit(LogicalSlotKill, 0);

	slot_name = NameStr(slot->name);
	sprintf(slot_name, "id-%d-%d", MyDatabaseId, i);

	/* release slot so it can be examined by others */
	SpinLockRelease(&slot->mutex);

	/* verify that the specified plugin is valid */

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

	CreateLogicalSlot(slot);
}

/*
 * Find an previously initiated slot and mark it as used again.
 */
void LogicalDecodingReAcquireSlot(const char *name)
{
	LogicalDecodingSlot *slot;
	int i;

	CheckLogicalReplicationRequirements();

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

	/* Arrange to clean up at exit */
	on_shmem_exit(LogicalSlotKill, 0);

	SaveLogicalSlot(slot);
}

void
LogicalDecodingReleaseSlot(void)
{
	LogicalDecodingSlot *slot;

	CheckLogicalReplicationRequirements();

	slot = MyLogicalDecodingSlot;

	Assert(slot != NULL && slot->active);

	SpinLockAcquire(&slot->mutex);
	slot->active = false;
	SpinLockRelease(&slot->mutex);

	MyLogicalDecodingSlot = NULL;

	SaveLogicalSlot(slot);
}

void
LogicalDecodingFreeSlot(const char *name)
{
	LogicalDecodingSlot *slot = NULL;
	int i;

	CheckLogicalReplicationRequirements();

	for (i = 0; i < max_logical_slots; i++)
	{
		slot = &LogicalDecodingCtl->logical_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->in_use && strcmp(name, NameStr(slot->name)) == 0)
		{
			/* NOT releasing the lock yet */
			break;
		}
		SpinLockRelease(&slot->mutex);
		slot = NULL;
	}

	if (!slot)
		elog(ERROR, "couldn't find logical slot for \"%s\"", name);

	if (slot->active)
	{
		SpinLockRelease(&slot->mutex);

		elog(ERROR, "cannot free active logical slot \"%s\"", name);
	}

	/*
	 * Mark it as as active, so nobody can claim this slot while we are working
	 * on it. We don't want to hold the spinlock while doing stuff like
	 * fsyncing the state file to disk.
	 */
	slot->active = true;

	SpinLockRelease(&slot->mutex);

	/*
	 * Start critical section, we can't to be interrupted while on-disk/memory
	 * state aren't coherent.
	 */
	START_CRIT_SECTION();

	DeleteLogicalSlot(slot);

	/* ok, everything gone, after a crash we now wouldn't restore this slot */
	SpinLockAcquire(&slot->mutex);
	slot->active = false;
	slot->in_use = false;
	SpinLockRelease(&slot->mutex);

	END_CRIT_SECTION();

	/* slot is dead and doesn't nail the xmin anymore */
	ComputeLogicalXmin();
}

/* FIXME: better name */
void
CheckPointLogical(XLogRecPtr checkPointRedo)
{
	elog(LOG, "doing logical checkpoint to %X/%X",
		 (uint32)(checkPointRedo >> 32), (uint32)checkPointRedo);
}

void
StartupLogical(XLogRecPtr checkPointRedo)
{
	DIR		   *logical_dir;
	struct dirent *logical_de;

	elog(LOG, "doing logical startup from %X/%X",
		 (uint32)(checkPointRedo >> 32), (uint32)checkPointRedo);

	/* restore all slots */
	logical_dir = AllocateDir("pg_llog");
	while ((logical_de = ReadDir(logical_dir, "pg_llog")) != NULL)
	{
		if (strcmp(logical_de->d_name, ".") == 0 ||
			strcmp(logical_de->d_name, "..") == 0)
			continue;

		/* one of our own directories */
		if (strcmp(logical_de->d_name, "snapshots") == 0)
			continue;

		/* we crashed while a slot was being setup or deleted, clean up */
		if (strcmp(logical_de->d_name, "new") == 0 ||
			strcmp(logical_de->d_name, "old") == 0)
		{
			char path[MAXPGPATH];
			sprintf(path, "pg_llog/%s", logical_de->d_name);

			if (!rmtree(path, true))
			{
				FreeDir(logical_dir);
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not remove directory \"%s\": %m",
						        path)));
			}
			continue;
		}

		RestoreLogicalSlot(logical_de->d_name);
	}
	FreeDir(logical_dir);

	if (max_logical_slots <= 0)
		return;

	/* FIXME: should we up minRecoveryLSN? */

	/* Now that we have recovered all the data, compute logical xmin */
	ComputeLogicalXmin();
}

static void
CreateLogicalSlot(LogicalDecodingSlot *slot)
{
	char tmppath[MAXPGPATH];
	char path[MAXPGPATH];

	START_CRIT_SECTION();

	sprintf(tmppath, "pg_llog/new");
	sprintf(path, "pg_llog/%s", NameStr(slot->name));

	if (mkdir(tmppath, S_IRWXU) < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m",
						tmppath)));

	fsync_fname(tmppath, true);

	SaveLogicalSlotInternal(slot, tmppath);

	if (rename(tmppath, path) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename logical checkpoint from \"%s\" to \"%s\": %m",
						tmppath, path)));
	}

	fsync_fname(path, true);

	END_CRIT_SECTION();
}

static void
SaveLogicalSlot(LogicalDecodingSlot *slot)
{
	char path[MAXPGPATH];
	sprintf(path, "pg_llog/%s", NameStr(slot->name));
	SaveLogicalSlotInternal(slot, path);
}

static void
SaveLogicalSlotInternal(LogicalDecodingSlot *slot, const char *dir)
{
	char tmppath[MAXPGPATH];
	char path[MAXPGPATH];
	int fd;
	LogicalDecodingCheckpointData cp;

	sprintf(tmppath, "%s/state.tmp", dir);
	sprintf(path, "%s/state", dir);

	START_CRIT_SECTION();

	fd = OpenTransientFile(tmppath,
						   O_CREAT | O_EXCL | O_WRONLY | PG_BINARY,
						   S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not create logical checkpoint file \"%s\": %m",
						tmppath)));

	cp.magic = LOGICAL_MAGIC;

	SpinLockAcquire(&slot->mutex);

	cp.slot.xmin = slot->xmin;
	strcpy(NameStr(cp.slot.name), NameStr(slot->name));
	strcpy(NameStr(cp.slot.plugin), NameStr(slot->plugin));

	cp.slot.database = slot->database;
	cp.slot.last_required_checkpoint = slot->last_required_checkpoint;
	cp.slot.confirmed_flush = slot->confirmed_flush;
	cp.slot.candidate_xmin = InvalidTransactionId;
	cp.slot.candidate_xmin_after = InvalidXLogRecPtr;
	cp.slot.in_use = slot->in_use;
	cp.slot.active = false;

	SpinLockRelease(&slot->mutex);

	if ((write(fd, &cp, sizeof(cp))) != sizeof(cp))
	{
		CloseTransientFile(fd);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write logical checkpoint file \"%s\": %m",
						tmppath)));
	}

	/* fsync the file */
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not fsync logical checkpoint \"%s\": %m",
						tmppath)));
	}

	CloseTransientFile(fd);

	/* rename to permanent file, fsync file and directory */
	if (rename(tmppath, path) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename logical checkpoint from \"%s\" to \"%s\": %m",
						tmppath, path)));
	}

	fsync_fname(dir, true);
	fsync_fname(path, false);

	END_CRIT_SECTION();
}

static void
RestoreLogicalSlot(const char* name)
{
	LogicalDecodingCheckpointData cp;
	int i;
	char path[MAXPGPATH];
	int fd;
	bool restored = false;

	START_CRIT_SECTION();

	/* delete temp file if it exists */
	sprintf(path, "pg_llog/%s/state.tmp", name);
	if (unlink(name) < 0 && errno != ENOENT)
		ereport(PANIC, (errmsg("failed while unlinking %s",  path)));

	sprintf(path, "pg_llog/%s/state", name);

	elog(LOG, "restoring logical slot at %s", path);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);

	/*
	 * We do not need to handle this as we are rename()ing the directory into
	 * place only after we fsync()ed the state file.
	 */
	if (fd < 0)
		ereport(PANIC, (errmsg("could not open state file %s",  path)));

	if (read(fd, &cp, sizeof(cp)) != sizeof(cp))
	{
		CloseTransientFile(fd);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not read logical checkpoint file \"%s\": %m",
						path)));
	}

	CloseTransientFile(fd);

	if (cp.magic != LOGICAL_MAGIC)
		ereport(PANIC, (errmsg("Logical checkpoint has wrong magic %u instead of %u",
							   cp.magic, LOGICAL_MAGIC)));

	/* nothing can be active yet, don't lock anything */
	for (i = 0; i < max_logical_slots; i++)
	{
		LogicalDecodingSlot *slot;
		slot = &LogicalDecodingCtl->logical_slots[i];

		if (slot->in_use)
			continue;

		slot->xmin = cp.slot.xmin;
		strcpy(NameStr(slot->name), NameStr(cp.slot.name));
		strcpy(NameStr(slot->plugin), NameStr(cp.slot.plugin));
		slot->database = cp.slot.database;
		slot->last_required_checkpoint = cp.slot.last_required_checkpoint;
		slot->confirmed_flush = cp.slot.confirmed_flush;
		slot->candidate_xmin = InvalidTransactionId;
		slot->candidate_xmin_after = InvalidXLogRecPtr;
		slot->in_use = true;
		slot->active = false;
		restored = true;

		/*
		 * FIXME: Do some validation here.
		 */
		break;
	}

	if (!restored)
		ereport(PANIC,
				(errmsg("too many logical slots active before shutdown, increase max_logical_slots and try again")));

	END_CRIT_SECTION();
}

static void
DeleteLogicalSlot(LogicalDecodingSlot *slot)
{
	char path[MAXPGPATH];
	char tmppath[] = "pg_llog/old";

	START_CRIT_SECTION();

	sprintf(path, "pg_llog/%s", NameStr(slot->name));

	if (rename(path, tmppath) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename logical checkpoint from \"%s\" to \"%s\": %m",
						path, tmppath)));
	}

	/* make sure no partial state is visible after a crash */
	fsync_fname(tmppath, true);
	fsync_fname("pg_llog", true);

	if (!rmtree(tmppath, true))
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not remove directory \"%s\": %m",
						tmppath)));
	}

	END_CRIT_SECTION();
}
}
