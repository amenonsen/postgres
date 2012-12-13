/*-------------------------------------------------------------------------
 *
 * compat.c
 *		Support functions for xlogdump.c
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

#ifdef USE_ASSERT_CHECKING
bool assert_enabled = false;
#endif

void
pfree(void *a)
{
}

const char *
timestamptz_to_str(TimestampTz t)
{
	return "";
}

char *
relpathbackend(RelFileNode rnode, BackendId backend, ForkNumber forknum)
{
	return NULL;
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

#ifdef USE_ASSERT_CHECKING
void
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName, int lineNumber)
{
	if (!PointerIsValid(conditionName)
		|| !PointerIsValid(fileName)
		|| !PointerIsValid(errorType))
		fprintf(stderr, "TRAP: ExceptionalCondition: bad arguments\n");
	else
	{
		fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
					 errorType, conditionName,
					 fileName, lineNumber);
	}

	/* Usually this shouldn't be needed, but make sure the msg went out */
	fflush(stderr);

#ifdef SLEEP_ON_ASSERT

	/*
	 * It would be nice to use pg_usleep() here, but only does 2000 sec or 33
	 * minutes, which seems too short.
	 */
	sleep(1000000);
#endif

	abort();
}
#endif	/* USE_ASSERT_CHECKING */
