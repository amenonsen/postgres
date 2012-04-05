/*-------------------------------------------------------------------------
 *
 * readxlog.h
 *
 *		Generic xlog reading facility.
 *
 * Portions Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/xlogreader.h
 *
 * NOTES
 *		Check the definition of the XLogReaderState struct for instructions on
 *		how to use the XLogReader infrastructure.
 *
 *		The basic idea is to allocate an XLogReaderState via
 *		XLogReaderAllocate, and call XLogReadRecord() until it returns NULL.
 *-------------------------------------------------------------------------
 */
#ifndef XLOGREADER_H
#define XLOGREADER_H

#include "access/xlog_internal.h"
#include "nodes/pg_list.h"

struct XLogReaderState;

/*
 * The callbacks are explained in more detail inside the XLogReaderState
 * struct.
 */

typedef int (*XLogPageReadCB) (struct XLogReaderState *state,
							   XLogRecPtr pageptr,
							   int reqLen,
							   int emode,
							   char *readBuf,
							   TimeLineID *pageTLI);

typedef int (*XLogEmodeCB) (struct XLogReaderState *state,
							int emode, XLogRecPtr RecPtr);


typedef struct XLogReaderState
{
	/* ----------------------------------------
	 * Public parameters
	 * ----------------------------------------
	 */

	/*
	 * Data input callback (mandatory).
	 *
	 * This callback shall read the the xlog page (of size XLOG_BLKSZ) in which
	 * RecPtr resides. All data <= RecPtr must be visible. The callback shall
	 * return the range of actually valid bytes returned or -1 upon
	 * failure. *pageTLI needs to be set to the TLI the page is assumed to be
	 * in.
	 */
	XLogPageReadCB read_page;


	/*
	 * Error handling callback (mandatory)
	 *
	 * This callback shall return how severe an error at a particular location
	 * is. ereport(level, ...) where level is the value returned by the
	 * callback will be raised. If the loglevel is < ERROR the respective
	 * XLogReadRecord will return NULL.
	 */
	XLogEmodeCB emode_for_ptr;

	/*
	 * System identifier of the xlog files were about to read.
	 *
	 * Set to zero (the default value) if unknown or unimportant.
	 */
	uint64		system_identifier;

	/*
	 * List of acceptable TLIs.
	 *
	 * Set to NIL (the default value) if this should not be checked.
	 */
	List	   *expectedTLEs;

	/*
	 * Opaque data for callbacks to use.  Not used by XLogReader.
	 */
	void	   *private_data;

	/*
	 * From where to where are we reading
	 */
	XLogRecPtr	ReadRecPtr;		/* start of last record read */
	XLogRecPtr	EndRecPtr;		/* end+1 of last record read */

	/* ----------------------------------------
	 * private/internal state
	 * ----------------------------------------
	 */



	/* Buffer for currently read page (XLOG_BLCKSZ bytes) */
	char	   *readBuf;

	/* last read segment, segment offset, read length, TLI */
	XLogSegNo   readSegNo;
	uint32      readOff;
	uint32      readLen;
	TimeLineID  readPageTLI;

	/* Highest TLI we have read so far  */
	TimeLineID latestReadTLI;
	XLogRecPtr latestReadPtr;

	/* Buffer for current ReadRecord result (expandable) */
	char	   *readRecordBuf;
	uint32		readRecordBufSize;

} XLogReaderState;


typedef struct XLogRecordBuffer
{
	XLogRecPtr origptr;
	XLogRecord record;
	char *record_data;
} XLogRecordBuffer;

/*
 * Get a new XLogReader
 *
 * At least the read_page callback, startptr and endptr have to be set before
 * the reader can be used.
 */
extern XLogReaderState *XLogReaderAllocate(XLogRecPtr startpoint,
					  XLogPageReadCB pagereadfunc, XLogEmodeCB emodecb,
										   void *private_data);

/*
 * Free an XLogReader
 */
extern void XLogReaderFree(XLogReaderState *state);

/*
 * Read the next record from xlog. Returns NULL on end-of-WAL or on failure.
 */
extern XLogRecord *XLogReadRecord(XLogReaderState *state, XLogRecPtr ptr,
			   int emode);

/*
 * Find the address of the next record with a lsn >= RecPtr.
 */
extern XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr,
									 int emode);

#endif   /* XLOGREADER_H */
