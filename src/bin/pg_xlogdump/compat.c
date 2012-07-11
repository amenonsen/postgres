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

typedef struct MyErrorData
{
	int elevel;
	char *filename;
	int lineno;
	char *funcname;
	char *domain;
	char *message;
	char *detail;
} MyErrorData;

static struct MyErrorData errdata;

bool
errstart(int elevel, const char *filename, int lineno,
		 const char *funcname, const char *domain)
{
	errdata.elevel = elevel;
	errdata.filename = strdup(filename);
	errdata.lineno = lineno;
	errdata.funcname = strdup(funcname);
	errdata.domain = domain ? strdup(domain) : NULL;
	errdata.message = NULL;
	errdata.detail = NULL;
	return true;
}

int
errmsg(const char *fmt,...)
{
	StringInfo s = makeStringInfo();
	va_list args;

	va_start(args, fmt);
	/* PQExpBuffer always succeeds or dies */
	appendStringInfoVA(s, fmt, args);
	va_end(args);

	/* FIXME, deallocate */
	errdata.message = ((PQExpBuffer)s)->data;
	return 0;
}

void
errfinish(int dummy,...)
{
	fprintf(stderr, "level: %d: MESSAGE: %s\n",
	        errdata.elevel, errdata.message);

	if (errdata.detail != NULL)
		fprintf(stderr, "DETAIL: %s\n",
		        errdata.detail);

	fprintf(stderr, "POS: file:%s func:%s() line:%d\n",
	        errdata.filename, errdata.funcname, errdata.lineno);

	if (errdata.elevel >= ERROR)
		abort();
}

int
errdetail(const char *fmt,...)
{
	StringInfo s = makeStringInfo();
	va_list args;

	va_start(args, fmt);
	/* PQExpBuffer always succeeds or dies */
	appendStringInfoVA(s, fmt, args);
	va_end(args);

	/* FIXME, deallocate */
	errdata.detail = ((PQExpBuffer)s)->data;
	return 0;
}

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
