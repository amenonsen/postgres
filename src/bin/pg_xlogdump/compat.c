/*-------------------------------------------------------------------------
 *
 * compat.c
 *		Support functions for xlogdump.c
 *
 * Portions Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/bin/pg_xlogdump/compat.c
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/timeline.h"
#include "access/xlogdefs.h"
#include "access/xlog_internal.h"
#include "catalog/catalog.h"
#include "datatype/timestamp.h"
#include "storage/relfilenode.h"
#include "utils/timestamp.h"

#include "pqexpbuffer.h"

bool assert_enabled = false;

/*
 * Returns true if 'expectedTLEs' contains a timeline with id 'tli'
 */
bool
tliInHistory(TimeLineID tli, List *expectedTLEs)
{
	ListCell *cell;

	foreach(cell, expectedTLEs)
	{
		if (((TimeLineHistoryEntry *) lfirst(cell))->tli == tli)
			return true;
	}

	return false;
}

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

/*
 * Write errors to stderr (or by equal means when stderr is
 * not available).
 */
void
write_stderr(const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);
#if !defined(WIN32) && !defined(__CYGWIN__)
	/* On Unix, we just fprintf to stderr */
	vfprintf(stderr, fmt, ap);
#else

	/*
	 * On Win32, we print to stderr if running on a console, or write to
	 * eventlog if running as a service
	 */
	if (!isatty(fileno(stderr)))	/* Running as a service */
	{
		char		errbuf[2048];		/* Arbitrary size? */

		vsnprintf(errbuf, sizeof(errbuf), fmt, ap);

		write_eventlog(EVENTLOG_ERROR_TYPE, errbuf);
	}
	else
		/* Not running as service, write to stderr */
		vfprintf(stderr, fmt, ap);
#endif
	va_end(ap);
}
