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
#include "storage/relfilenode.h"
#include "utils/timestamp.h"

bool assert_enabled = false;

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
