/*-------------------------------------------------------------------------
 *
 * logicalfuncs.c
 *
 *     Support functions for using xlog decoding
 *
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logicalfuncs.c
 *
 */

#include "postgres.h"


#include "fmgr.h"

#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/snapbuild.h"
