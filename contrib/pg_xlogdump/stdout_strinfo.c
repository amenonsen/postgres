/*
 * A client-side StringInfo implementation that just prints everything to
 * stdout
 */
#include "postgres_fe.h"

#include "lib/stringinfo.h"

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
