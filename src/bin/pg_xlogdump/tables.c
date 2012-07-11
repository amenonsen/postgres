/*-------------------------------------------------------------------------
 *
 * tables.c
 *		Support data for xlogdump.c
 *
 * Portions Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/bin/pg_xlogdump/tables.c
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

/*
 * rmgr.c
 *
 * Resource managers definition
 *
 * src/backend/access/transam/rmgr.c
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/gin.h"
#include "access/gist_private.h"
#include "access/hash.h"
#include "access/heapam_xlog.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/spgist.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "commands/tablespace.h"
#include "storage/standby.h"
#include "utils/relmapper.h"
#include "catalog/catalog.h"

/*
 * Table of fork names.
 *
 * needs to be synced with src/backend/catalog/catalog.c
 */
const char *forkNames[] = {
	"main",						/* MAIN_FORKNUM */
	"fsm",						/* FSM_FORKNUM */
	"vm",						/* VISIBILITYMAP_FORKNUM */
	"init"						/* INIT_FORKNUM */
};

/*
 * RmgrTable linked only to functions available outside of the backend.
 *
 * needs to be synced with src/backend/access/transam/rmgr.c
 */
const RmgrData RmgrTable[RM_MAX_ID + 1] = {
	{"XLOG", NULL, xlog_desc, NULL, NULL, NULL},
	{"Transaction", NULL, xact_desc, NULL, NULL, NULL},
	{"Storage", NULL, smgr_desc, NULL, NULL, NULL},
	{"CLOG", NULL, clog_desc, NULL, NULL, NULL},
	{"Database", NULL, dbase_desc, NULL, NULL, NULL},
	{"Tablespace", NULL, tblspc_desc, NULL, NULL, NULL},
	{"MultiXact", NULL, multixact_desc, NULL, NULL, NULL},
	{"RelMap", NULL, relmap_desc, NULL, NULL, NULL},
	{"Standby", NULL, standby_desc, NULL, NULL, NULL},
	{"Heap2", NULL, heap2_desc, NULL, NULL, NULL},
	{"Heap", NULL, heap_desc, NULL, NULL, NULL},
	{"Btree", NULL, btree_desc, NULL, NULL, NULL},
	{"Hash", NULL, hash_desc, NULL, NULL, NULL},
	{"Gin", NULL, gin_desc, NULL, NULL, NULL},
	{"Gist", NULL, gist_desc, NULL, NULL, NULL},
	{"Sequence", NULL, seq_desc, NULL, NULL, NULL},
	{"SPGist", NULL, spg_desc, NULL, NULL, NULL}
};
