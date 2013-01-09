/*
 *	palloc.h
 *		memory management support for frontend code
 *
 *	Copyright (c) 2003-2013, PostgreSQL Global Development Group
 *
 *	src/include/port/palloc.h
 */

#ifndef PGPORT_PALLOC_H
#define PGPORT_PALLOC_H

extern char *pg_strdup(const char *string);
extern void *pg_malloc(size_t size);
extern void *pg_malloc0(size_t size);
extern void *pg_realloc(void *pointer, size_t size);
extern void  pg_free(void *pointer);

#endif   /* PGPORT_PALLOC_H */
