/*-------------------------------------------------------------------------
 *
 * copydir.h
 *	  Copy a directory.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/copydir.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYDIR_H
#define COPYDIR_H

extern void copydir(const char *fromdir, const char *todir, bool recurse);
extern void copy_file(const char *fromfile, const char *tofile);
extern void fsync_fname(const char *fname, bool isdir);

#endif   /* COPYDIR_H */
