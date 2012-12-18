/*-------------------------------------------------------------------------
 *
 * compat.c
 *		Reimplementations of various backend functions.
 *
 * Portions Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pg_xlogdump/compat.c
 *
 * This file contains client-side implementations for various backend
 * functions that the rm_desc functions in *desc.c files rely on.
 *
 *-------------------------------------------------------------------------
 */

/* ugly hack, same as in e.g pg_controldata */
#define FRONTEND 1
#include "postgres.h"

#include "catalog/catalog.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "storage/relfilenode.h"
#include "utils/timestamp.h"

const char *
timestamptz_to_str(TimestampTz t)
{
	return "";
}

const char *
relpathbackend(RelFileNode rnode, BackendId backend, ForkNumber forknum)
{
	return "";
}

void
appendStringInfo(StringInfo str, const char *fmt, ...)
{
	va_list		args;

	va_start(args, fmt);
	vprintf(fmt, args);
	va_end(args);
}

void
appendStringInfoString(StringInfo str, const char *string)
{
	appendStringInfo(str, "%s", string);
}
