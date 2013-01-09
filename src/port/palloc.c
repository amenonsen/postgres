/*-------------------------------------------------------------------------
 *
 * palloc.c
 *	  memory management support for frontend code
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/palloc.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#error "This file is not expected to be compiled for backend code"
#endif

#include "postgres_fe.h"
#include "port/palloc.h"
#include "utils/palloc.h"

void *
pg_malloc(size_t size)
{
	void	   *tmp;

	/* Avoid unportable behavior of malloc(0) */
	if (size == 0)
		size = 1;
	tmp = malloc(size);
	if (!tmp)
	{
		fprintf(stderr, _("out of memory\n"));
		exit(EXIT_FAILURE);
	}
	return tmp;
}

void *
pg_malloc0(size_t size)
{
	void	   *tmp;

	tmp = pg_malloc(size);
	MemSet(tmp, 0, size);
	return tmp;
}

void *
pg_realloc(void *ptr, size_t size)
{
   void       *tmp;

   /* Avoid unportable behavior of realloc(NULL, 0) */
   if (ptr == NULL && size == 0)
       size = 1;
   tmp = realloc(ptr, size);
   if (!tmp)
   {
		fprintf(stderr, _("out of memory\n"));
		exit(EXIT_FAILURE);
   }
   return tmp;
}

/*
 * "Safe" wrapper around strdup().	Pulled from psql/common.c
 */
char *
pg_strdup(const char *string)
{
	char	   *tmp;

	if (!string)
	{
		fprintf(stderr, _("pg_strdup: cannot duplicate null pointer (internal error)\n"));
		exit(EXIT_FAILURE);
	}
	tmp = strdup(string);
	if (!tmp)
	{
		fprintf(stderr, _("out of memory\n"));
		exit(EXIT_FAILURE);
	}
	return tmp;
}

void
pg_free(void *ptr)
{
	if (ptr != NULL)
		free(ptr);
}

/*
 * Emulation of memory management functions commonly used in backend code that
 * are useful if code should be used by both front and backend code.
 */
void *
palloc(Size size)
{
	return pg_malloc(size);
}

void *
palloc0(Size size)
{
	return pg_malloc0(size);
}

void
pfree(void *pointer)
{
	pg_free(pointer);
}

char *
pstrdup(const char *string)
{
	return pg_strdup(string);
}

void *
repalloc(void *pointer, Size size)
{
	return pg_realloc(pointer, size);
}
