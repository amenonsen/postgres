/*
 * A client-side StringInfo implementation
 *
 * This currently uses PQExpBuffer underneath.
 */
#include "postgres_fe.h"

#include "pqexpbuffer.h"
#include "lib/stringinfo.h"

StringInfo
makeStringInfo(void)
{
	PQExpBuffer	buffer;

	buffer = createPQExpBuffer();

	return (StringInfo) buffer;
}

void
appendStringInfo(StringInfo str, const char *fmt, ...)
{
	PQExpBuffer	buffer = (PQExpBuffer) str;
	va_list		args;

	if (PQExpBufferBroken(buffer))
	{
		fprintf(stderr, "out of memory\n");
		exit(1);
	}

	va_start(args, fmt);
	appendPQExpBufferVA(buffer, fmt, args);
	va_end(args);

	if (PQExpBufferBroken(buffer))
	{
		fprintf(stderr, "out of memory\n");
		exit(1);
	}
}

bool
appendStringInfoVA(StringInfo str, const char *fmt, va_list args)
{
	PQExpBuffer	buffer = (PQExpBuffer) str;

	if (PQExpBufferBroken(buffer))
	{
		fprintf(stderr, "out of memory\n");
		exit(1);
	}
	appendPQExpBufferVA(buffer, fmt, args);

	if (PQExpBufferBroken(buffer))
	{
		fprintf(stderr, "out of memory\n");
		exit(1);
	}
	return true;
}

void
appendStringInfoString(StringInfo str, const char *string)
{
	appendStringInfo(str, "%s", string);
}
