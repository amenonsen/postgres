/*-------------------------------------------------------------------------
 *
 * xlogreader.c
 *		Generic xlog reading facility
 *
 * Portions Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/access/transam/xlogreader.c
 *
 * NOTES
 *		Documentation about how do use this interface can be found in
 *		xlogreader.h, more specifically in the definition of the
 *		XLogReaderState struct where all parameters are documented.
 *
 * TODO:
 * * usable without backend code around
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/timeline.h"
#include "access/transam.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"

static bool allocate_recordbuf(XLogReaderState *state, uint32 reclength);

static bool ValidXLogPageHeader(XLogReaderState *state, XLogRecPtr recptr,
								XLogPageHeader hdr, int emode);
static bool ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr,
		XLogRecPtr PrevRecPtr, XLogRecord *record, int emode, bool randAccess);
static bool ValidXLogRecord(XLogReaderState *state, XLogRecord *record,
						    XLogRecPtr recptr, int emode);
static int ReadPageInternal(struct XLogReaderState *state, XLogRecPtr pageptr,
					int reqLen, int emode, char *readBuf, TimeLineID *pageTLI);


/*
 * Allocate and initialize a new xlog reader
 *
 * Returns NULL if the xlogreader couldn't be allocated.
 */
XLogReaderState *
XLogReaderAllocate(XLogRecPtr startpoint, XLogPageReadCB pagereadfunc,
				   XLogEmodeCB emodecb, void *private_data)
{
	XLogReaderState *state;

	state = (XLogReaderState *) malloc(sizeof(XLogReaderState));
	if (!state)
		return NULL;
	MemSet(state, 0, sizeof(XLogReaderState));

	/*
	 * Permanently allocate readBuf.  We do it this way, rather than just
	 * making a static array, for two reasons: (1) no need to waste the
	 * storage in most instantiations of the backend; (2) a static char array
	 * isn't guaranteed to have any particular alignment, whereas malloc()
	 * will provide MAXALIGN'd storage.
	 */
	state->readBuf = (char *) malloc(XLOG_BLCKSZ);
	if (!state->readBuf)
	{
		free(state);
		return NULL;
	}

	state->read_page = pagereadfunc;
	state->emode_for_ptr = emodecb;
	state->private_data = private_data;
	state->EndRecPtr = startpoint;
	state->readPageTLI = InvalidTimelineId;
	state->expectedTLEs = NIL;
	state->system_identifier = 0;

	/*
	 * Allocate an initial readRecordBuf of minimal size, which can later be
	 * enlarged if necessary.
	 */
	if (!allocate_recordbuf(state, 0))
	{
		free(state->readBuf);
		free(state);
		return NULL;
	}

	return state;
}

void
XLogReaderFree(XLogReaderState *state)
{
	if (state->readRecordBuf)
		free(state->readRecordBuf);
	free(state->readBuf);
	free(state);
}

/*
 * Allocate readRecordBuf to fit a record of at least the given length.
 * Returns true if successful, false if out of memory.
 *
 * readRecordBufSize is set to the new buffer size.
 *
 * To avoid useless small increases, round its size to a multiple of
 * XLOG_BLCKSZ, and make sure it's at least 5*Max(BLCKSZ, XLOG_BLCKSZ) to start
 * with.  (That is enough for all "normal" records, but very large commit or
 * abort records might need more space.)
 */
static bool
allocate_recordbuf(XLogReaderState *state, uint32 reclength)
{
	uint32		newSize = reclength;

	newSize += XLOG_BLCKSZ - (newSize % XLOG_BLCKSZ);
	newSize = Max(newSize, 5 * Max(BLCKSZ, XLOG_BLCKSZ));

	if (state->readRecordBuf)
		free(state->readRecordBuf);
	state->readRecordBuf = (char *) malloc(newSize);
	if (!state->readRecordBuf)
	{
		state->readRecordBufSize = 0;
		return false;
	}

	state->readRecordBufSize = newSize;
	return true;
}

/*
 * Attempt to read an XLOG record.
 *
 * If RecPtr is not NULL, try to read a record at that position.  Otherwise
 * try to read a record just after the last one previously read.
 *
 * If no valid record is available, returns NULL, or fails if emode is PANIC.
 * (emode must be either PANIC, LOG)
 *
 * The record is copied into readRecordBuf, so that on successful return,
 * the returned record pointer always points there.
 */
XLogRecord *
XLogReadRecord(XLogReaderState *state, XLogRecPtr RecPtr, int emode)
{
	XLogRecord *record;
	XLogRecPtr	tmpRecPtr = state->EndRecPtr;
	XLogRecPtr  targetPagePtr;
	bool		randAccess = false;
	uint32		len,
				total_len;
	uint32		targetRecOff;
	uint32		pageHeaderSize;
	bool		gotheader;
	int         readOff;

	if (RecPtr == InvalidXLogRecPtr)
	{
		RecPtr = tmpRecPtr;

		if (state->ReadRecPtr == InvalidXLogRecPtr)
			randAccess = true;

		/*
		 * RecPtr is pointing to end+1 of the previous WAL record.	If we're
		 * at a page boundary, no more records can fit on the current page. We
		 * must skip over the page header, but we can't do that until we've
		 * read in the page, since the header size is variable.
		 */
	}
	else
	{
		/*
		 * In this case, the passed-in record pointer should already be
		 * pointing to a valid record starting position.
		 */
		if (!XRecOffIsValid(RecPtr))
			ereport(PANIC,
					(errmsg("invalid record offset at %X/%X",
							(uint32) (RecPtr >> 32), (uint32) RecPtr)));
		randAccess = true;		/* allow readPageTLI to go backwards too */
	}

	targetPagePtr = RecPtr - (RecPtr % XLOG_BLCKSZ);

	/* Read the page containing the record */
	readOff = ReadPageInternal(state, targetPagePtr, SizeOfXLogRecord, emode,
							   state->readBuf, &state->readPageTLI);

	if (readOff < 0)
		return NULL;

	/* ReadPageInternal always returns at least the page header */
	pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
	targetRecOff = RecPtr % XLOG_BLCKSZ;
	if (targetRecOff == 0)
	{
		/*
		 * At page start, so skip over page header.
		 */
		RecPtr += pageHeaderSize;
		targetRecOff = pageHeaderSize;
	}
	else if (targetRecOff < pageHeaderSize)
	{
		ereport(state->emode_for_ptr(state, emode, RecPtr),
				(errmsg("invalid record offset at %X/%X",
						(uint32) (RecPtr >> 32), (uint32) RecPtr)));
		return NULL;
	}

	if ((((XLogPageHeader) state->readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD) &&
		targetRecOff == pageHeaderSize)
	{
		ereport(state->emode_for_ptr(state, emode, RecPtr),
				(errmsg("contrecord is requested by %X/%X",
						(uint32) (RecPtr >> 32), (uint32) RecPtr)));
		return NULL;
	}

	/* ReadPageInternal has verfied the page header */
	Assert(pageHeaderSize <= readOff);

	/*
	 * Ensure the whole record header or at least the part on this page is
	 * read.
	 */
	readOff = ReadPageInternal(state,
							   targetPagePtr,
							   Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ),
	                           emode, state->readBuf, &state->readPageTLI);
	if (readOff < 0)
		return NULL;

	/*
	 * Read the record length.
	 *
	 * NB: Even though we use an XLogRecord pointer here, the whole record
	 * header might not fit on this page. xl_tot_len is the first field of the
	 * struct, so it must be on this page (the records are MAXALIGNed), but we
	 * cannot access any other fields until we've verified that we got the
	 * whole header.
	 */
	record = (XLogRecord *) (state->readBuf + RecPtr % XLOG_BLCKSZ);
	total_len = record->xl_tot_len;

	/*
	 * If the whole record header is on this page, validate it immediately.
	 * Otherwise do just a basic sanity check on xl_tot_len, and validate the
	 * rest of the header after reading it from the next page.	The xl_tot_len
	 * check is necessary here to ensure that we enter the "Need to reassemble
	 * record" code path below; otherwise we might fail to apply
	 * ValidXLogRecordHeader at all.
	 */
	if (targetRecOff <= XLOG_BLCKSZ - SizeOfXLogRecord)
	{
		if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record,
								   emode, randAccess))
			return NULL;
		gotheader = true;
	}
	else
	{
		/* XXX: more validation should be done here */
		if (total_len < SizeOfXLogRecord)
		{
			ereport(state->emode_for_ptr(state, emode, RecPtr),
					(errmsg("invalid record length at %X/%X",
							(uint32) (RecPtr >> 32), (uint32) RecPtr)));
			return NULL;
		}
		gotheader = false;
	}

	/*
	 * Enlarge readRecordBuf as needed.
	 */
	if (total_len > state->readRecordBufSize &&
		!allocate_recordbuf(state, total_len))
	{
		/* We treat this as a "bogus data" condition */
		ereport(state->emode_for_ptr(state, emode, RecPtr),
				(errmsg("record length %u at %X/%X too long",
					  total_len, (uint32) (RecPtr >> 32), (uint32) RecPtr)));
		return NULL;
	}

	len = XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ;
	if (total_len > len)
	{
		/* Need to reassemble record */
		char	   *contdata;
		XLogPageHeader pageHeader;
		char	   *buffer;
		uint32		gotlen;

		/* Copy the first fragment of the record from the first page. */
		memcpy(state->readRecordBuf,
			   state->readBuf + RecPtr % XLOG_BLCKSZ, len);
		buffer = state->readRecordBuf + len;
		gotlen = len;

		do
		{
			/* Calculate pointer to beginning of next page */
			XLByteAdvance(targetPagePtr, XLOG_BLCKSZ);

			/* Wait for the next page to become available */
			readOff = ReadPageInternal(state, targetPagePtr, Min(len, XLOG_BLCKSZ),
									   emode, state->readBuf,
									   &state->readPageTLI);

			if (readOff < 0)
				goto err;

			Assert(SizeOfXLogShortPHD <= readOff);

			/* Check that the continuation on next page looks valid */
			pageHeader = (XLogPageHeader) state->readBuf;
			if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD))
			{
				ereport(state->emode_for_ptr(state, emode, RecPtr),
						(errmsg("there is no contrecord flag at %X/%X",
								(uint32) (RecPtr >> 32), (uint32) RecPtr)));
				goto err;
			}

			/*
			 * Cross-check that xlp_rem_len agrees with how much of the record
			 * we expect there to be left.
			 */
			if (pageHeader->xlp_rem_len == 0 ||
				total_len != (pageHeader->xlp_rem_len + gotlen))
			{
				ereport(state->emode_for_ptr(state, emode, RecPtr),
						(errmsg("invalid contrecord length %u at %X/%X",
								pageHeader->xlp_rem_len,
								(uint32) (RecPtr >> 32), (uint32) RecPtr)));
				goto err;
			}

			/* Append the continuation from this page to the buffer */
			pageHeaderSize = XLogPageHeaderSize(pageHeader);
			Assert(pageHeaderSize <= readOff);

			contdata = (char *) state->readBuf + pageHeaderSize;
			len = XLOG_BLCKSZ - pageHeaderSize;
			if (pageHeader->xlp_rem_len < len)
				len = pageHeader->xlp_rem_len;

			memcpy(buffer, (char *) contdata, len);
			buffer += len;
			gotlen += len;

			/* If we just reassembled the record header, validate it. */
			if (!gotheader)
			{
				record = (XLogRecord *) state->readRecordBuf;
				if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr,
										   record, emode, randAccess))
					goto err;
				gotheader = true;
			}
		} while (gotlen < total_len);

		Assert(gotheader);

		record = (XLogRecord *) state->readRecordBuf;
		if (!ValidXLogRecord(state, record, RecPtr, emode))
			goto err;

		pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
		state->ReadRecPtr = RecPtr;
		state->EndRecPtr = targetPagePtr + pageHeaderSize
			+ MAXALIGN(pageHeader->xlp_rem_len);
	}
	else
	{
		/* Wait for the record data to become available */
		readOff =
			ReadPageInternal(state, targetPagePtr,
							 Min(targetRecOff + total_len, XLOG_BLCKSZ),
			                 emode, state->readBuf, &state->readPageTLI);
		if (readOff < 0)
			goto err;

		/* Record does not cross a page boundary */
		if (!ValidXLogRecord(state, record, RecPtr, emode))
			goto err;

		state->EndRecPtr = RecPtr + MAXALIGN(total_len);

		state->ReadRecPtr = RecPtr;
		memcpy(state->readRecordBuf, record, total_len);
	}

	/*
	 * Special processing if it's an XLOG SWITCH record
	 */
	if (record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_SWITCH)
	{
		/* Pretend it extends to end of segment */
		state->EndRecPtr += XLogSegSize - 1;
		state->EndRecPtr -= state->EndRecPtr % XLogSegSize;
	}

	return record;

err:
	/*
	 * Invalidate the xlog page we've cached. We might read from a different
	 * source after failure.
	 */
	state->readSegNo = 0;
	state->readOff = 0;
	state->readLen = 0;

	return NULL;
}

/*
 * Find the first record with at an lsn >= RecPtr.
 *
 * Useful for checking wether RecPtr is a valid xlog address for reading and to
 * find the first valid address after some address when dumping records for
 * debugging purposes.
 */
XLogRecPtr
XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr, int emode)
{
	XLogReaderState saved_state = *state;
	XLogRecPtr	targetPagePtr;
	XLogRecPtr	tmpRecPtr;
	int	targetRecOff;
	XLogRecPtr found = InvalidXLogRecPtr;
	uint32		pageHeaderSize;
	XLogPageHeader header;
	XLogRecord *record;
	uint32 readLen;

	if (RecPtr == InvalidXLogRecPtr)
	{
		RecPtr = state->EndRecPtr;
	}

	targetRecOff = RecPtr % XLOG_BLCKSZ;

	/* scroll back to page boundary */
	targetPagePtr = RecPtr - targetRecOff;

	/* Read the page containing the record */
	readLen = ReadPageInternal(state, targetPagePtr, targetRecOff,
							   emode, state->readBuf, &state->readPageTLI);
	if (readLen < 0)
		goto err;

	header = (XLogPageHeader) state->readBuf;

	pageHeaderSize = XLogPageHeaderSize(header);

	/* make sure we have enough data for the page header */
	readLen = ReadPageInternal(state, targetPagePtr, pageHeaderSize,
							   emode, state->readBuf, &state->readPageTLI);
	if (readLen < 0)
		goto err;

	/* skip over potential continuation data */
	if (header->xlp_info & XLP_FIRST_IS_CONTRECORD)
	{
		/* record headers are MAXALIGN'ed */
		tmpRecPtr = targetPagePtr + pageHeaderSize
			+ MAXALIGN(header->xlp_rem_len);
	}
	else
	{
		tmpRecPtr = targetPagePtr + pageHeaderSize;
	}

	/*
	 * we know now that tmpRecPtr is an address pointing to a valid XLogRecord
	 * because either were at the first record after the beginning of a page or
	 * we just jumped over the remaining data of a continuation.
	 */
	while ((record = XLogReadRecord(state, tmpRecPtr, emode)))
	{
		/* continue after the record */
		tmpRecPtr = InvalidXLogRecPtr;

		/* past the record we've found, break out */
		if (XLByteLE(RecPtr, state->ReadRecPtr))
		{
			found = state->ReadRecPtr;
			goto out;
		}
	}

err:
out:
	/* Restore state to what we had before finding the record */
	saved_state.readRecordBuf = state->readRecordBuf;
	saved_state.readRecordBufSize = state->readRecordBufSize;
	*state = saved_state;
	return found;
}

/*
 * Read a single xlog page including at least [pagestart, RecPtr] of valid data
 * via the read_page() callback.
 *
 * Returns -1 if the required page cannot be read for some reason.
 *
 * We fetch the page from a reader-local cache if we know we have the required
 * data and if there hasn't been any error since caching the data.
 */
static int
ReadPageInternal(struct XLogReaderState *state, XLogRecPtr pageptr,
				 int reqLen, int emode, char *readBuf, TimeLineID *pageTLI)
{
	int readLen;
	uint32		targetPageOff;
	XLogSegNo	targetSegNo;
	XLogPageHeader hdr;

	Assert((pageptr % XLOG_BLCKSZ) == 0);

	XLByteToSeg(pageptr, targetSegNo);
	targetPageOff = (pageptr % XLogSegSize);

	/* check whether we have all the requested data already */
	if (targetSegNo == state->readSegNo && targetPageOff == state->readOff &&
		reqLen < state->readLen)
		return state->readLen;

	/*
	 * Data is not cached.
	 *
	 * Everytime we actually read the page, even if we looked at parts of it
	 * before, we need to do verification as the read_page callback might now
	 * be rereading data from a different source.
	 *
	 * Whenever switching to a new WAL segment, we read the first page of the
	 * file and validate its header, even if that's not where the target record
	 * is.  This is so that we can check the additional identification info
	 * that is present in the first page's "long" header.
	 */
	if (targetSegNo != state->readSegNo &&
		targetPageOff != 0)
	{
		XLogPageHeader hdr;
		XLogRecPtr targetSegmentPtr = pageptr - targetPageOff;

		readLen = state->read_page(state, targetSegmentPtr, XLOG_BLCKSZ,
								   emode, readBuf, pageTLI);

		if (readLen < 0)
			goto err;

		Assert(readLen <= XLOG_BLCKSZ);

		/* we can be sure to have enough WAL available, we scrolled back */
		Assert(readLen == XLOG_BLCKSZ);

		hdr = (XLogPageHeader) readBuf;

		if (!ValidXLogPageHeader(state, targetSegmentPtr, hdr, emode))
			goto err;
	}

	/* now read the target data */
	readLen = state->read_page(state, pageptr, Max(reqLen, SizeOfXLogShortPHD),
							   emode, readBuf, pageTLI);
	if (readLen < 0)
		goto err;

	Assert(readLen <= XLOG_BLCKSZ);

	/* check we have enough data to check for the actual length of a the page header */
	if (readLen <= SizeOfXLogShortPHD)
		goto err;

	Assert(readLen >= reqLen);

	hdr = (XLogPageHeader) readBuf;

	/* still not enough */
	if (readLen < XLogPageHeaderSize(hdr))
	{
		readLen = state->read_page(state, pageptr, XLogPageHeaderSize(hdr),
								   emode, readBuf, pageTLI);
		if (readLen < 0)
			goto err;
	}

	if (!ValidXLogPageHeader(state, pageptr, hdr, emode))
		goto err;

	/* update cache information */
	state->readSegNo = targetSegNo;
	state->readOff = targetPageOff;
	state->readLen = readLen;

	return readLen;
err:
	state->readSegNo = 0;
	state->readOff = 0;
	state->readLen = 0;
	return -1;
}

/*
 * Validate an XLOG record header.
 *
 * This is just a convenience subroutine to avoid duplicated code in
 * XLogReadRecord.	It's not intended for use from anywhere else.
 */
static bool
ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr,
	  XLogRecPtr PrevRecPtr, XLogRecord *record, int emode, bool randAccess)
{
	/*
	 * xl_len == 0 is bad data for everything except XLOG SWITCH, where it is
	 * required.
	 */
	if (record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_SWITCH)
	{
		if (record->xl_len != 0)
		{
			ereport(state->emode_for_ptr(state, emode, RecPtr),
					(errmsg("invalid xlog switch record at %X/%X",
							(uint32) (RecPtr >> 32), (uint32) RecPtr)));
			return false;
		}
	}
	else if (record->xl_len == 0)
	{
		ereport(state->emode_for_ptr(state, emode, RecPtr),
				(errmsg("record with zero length at %X/%X",
						(uint32) (RecPtr >> 32), (uint32) RecPtr)));
		return false;
	}
	if (record->xl_tot_len < SizeOfXLogRecord + record->xl_len ||
		record->xl_tot_len > SizeOfXLogRecord + record->xl_len +
		XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ))
	{
		ereport(state->emode_for_ptr(state, emode, RecPtr),
				(errmsg("invalid record length at %X/%X",
						(uint32) (RecPtr >> 32), (uint32) RecPtr)));
		return false;
	}
	if (record->xl_rmid > RM_MAX_ID)
	{
		ereport(state->emode_for_ptr(state, emode, RecPtr),
				(errmsg("invalid resource manager ID %u at %X/%X",
						record->xl_rmid, (uint32) (RecPtr >> 32),
						(uint32) RecPtr)));
		return false;
	}
	if (randAccess)
	{
		/*
		 * We can't exactly verify the prev-link, but surely it should be less
		 * than the record's own address.
		 */
		if (!XLByteLT(record->xl_prev, RecPtr))
		{
			ereport(state->emode_for_ptr(state, emode, RecPtr),
					(errmsg("record with incorrect prev-link %X/%X at %X/%X",
							(uint32) (record->xl_prev >> 32),
							(uint32) record->xl_prev,
							(uint32) (RecPtr >> 32), (uint32) RecPtr)));
			return false;
		}
	}
	else
	{
		/*
		 * Record's prev-link should exactly match our previous location. This
		 * check guards against torn WAL pages where a stale but valid-looking
		 * WAL record starts on a sector boundary.
		 */
		if (!XLByteEQ(record->xl_prev, PrevRecPtr))
		{
			ereport(state->emode_for_ptr(state, emode, RecPtr),
					(errmsg("record with incorrect prev-link %X/%X at %X/%X",
							(uint32) (record->xl_prev >> 32),
							(uint32) record->xl_prev,
							(uint32) (RecPtr >> 32), (uint32) RecPtr)));
			return false;
		}
	}

	return true;
}


/*
 * CRC-check an XLOG record.  We do not believe the contents of an XLOG
 * record (other than to the minimal extent of computing the amount of
 * data to read in) until we've checked the CRCs.
 *
 * We assume all of the record (that is, xl_tot_len bytes) has been read
 * into memory at *record.	Also, ValidXLogRecordHeader() has accepted the
 * record's header, which means in particular that xl_tot_len is at least
 * SizeOfXlogRecord, so it is safe to fetch xl_len.
 */
static bool
ValidXLogRecord(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr,
				int emode)
{
	pg_crc32	crc;
	int			i;
	uint32		len = record->xl_len;
	BkpBlock	bkpb;
	char	   *blk;
	size_t		remaining = record->xl_tot_len;

	/* First the rmgr data */
	if (remaining < SizeOfXLogRecord + len)
	{
		/* ValidXLogRecordHeader() should've caught this already... */
		ereport(state->emode_for_ptr(state, emode, recptr),
				(errmsg("invalid record length at %X/%X",
						(uint32) (recptr >> 32), (uint32) recptr)));
		return false;
	}
	remaining -= SizeOfXLogRecord + len;
	INIT_CRC32(crc);
	COMP_CRC32(crc, XLogRecGetData(record), len);

	/* Add in the backup blocks, if any */
	blk = (char *) XLogRecGetData(record) + len;
	for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
	{
		uint32		blen;

		if (!(record->xl_info & XLR_BKP_BLOCK(i)))
			continue;

		if (remaining < sizeof(BkpBlock))
		{
			ereport(state->emode_for_ptr(state, emode, recptr),
					(errmsg("invalid backup block size in record at %X/%X",
							(uint32) (recptr >> 32), (uint32) recptr)));
			return false;
		}
		memcpy(&bkpb, blk, sizeof(BkpBlock));

		if (bkpb.hole_offset + bkpb.hole_length > BLCKSZ)
		{
			ereport(state->emode_for_ptr(state, emode, recptr),
					(errmsg("incorrect hole size in record at %X/%X",
							(uint32) (recptr >> 32), (uint32) recptr)));
			return false;
		}
		blen = sizeof(BkpBlock) + BLCKSZ - bkpb.hole_length;

		if (remaining < blen)
		{
			ereport(state->emode_for_ptr(state, emode, recptr),
					(errmsg("invalid backup block size in record at %X/%X",
							(uint32) (recptr >> 32), (uint32) recptr)));
			return false;
		}
		remaining -= blen;
		COMP_CRC32(crc, blk, blen);
		blk += blen;
	}

	/* Check that xl_tot_len agrees with our calculation */
	if (remaining != 0)
	{
		ereport(state->emode_for_ptr(state, emode, recptr),
				(errmsg("incorrect total length in record at %X/%X",
						(uint32) (recptr >> 32), (uint32) recptr)));
		return false;
	}

	/* Finally include the record header */
	COMP_CRC32(crc, (char *) record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32(crc);

	if (!EQ_CRC32(record->xl_crc, crc))
	{
		ereport(state->emode_for_ptr(state, emode, recptr),
		(errmsg("incorrect resource manager data checksum in record at %X/%X",
				(uint32) (recptr >> 32), (uint32) recptr)));
		return false;
	}

	return true;
}

static bool
ValidXLogPageHeader(XLogReaderState *state, XLogRecPtr recptr,
					XLogPageHeader hdr, int emode)
{
	XLogRecPtr	recaddr;
	XLogSegNo segno;
	int32 offset;

	Assert((recptr % XLOG_BLCKSZ) == 0);

	XLByteToSeg(recptr, segno);
	offset = recptr % XLogSegSize;

	XLogSegNoOffsetToRecPtr(segno, offset, recaddr);

	if (hdr->xlp_magic != XLOG_PAGE_MAGIC)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		ereport(state->emode_for_ptr(state, emode, recaddr),
			(errmsg("invalid magic number %04X in log segment %s, offset %u",
					hdr->xlp_magic,
					fname,
					offset)));
		return false;
	}

	if ((hdr->xlp_info & ~XLP_ALL_FLAGS) != 0)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		ereport(state->emode_for_ptr(state, emode, recaddr),
				(errmsg("invalid info bits %04X in log segment %s, offset %u",
						hdr->xlp_info,
						fname,
						offset)));
		return false;
	}

	if (hdr->xlp_info & XLP_LONG_HEADER)
	{
		XLogLongPageHeader longhdr = (XLogLongPageHeader) hdr;

		if (state->system_identifier &&
		    longhdr->xlp_sysid != state->system_identifier)
		{
			char		fhdrident_str[32];
			char		sysident_str[32];

			/*
			 * Format sysids separately to keep platform-dependent format code
			 * out of the translatable message string.
			 */
			snprintf(fhdrident_str, sizeof(fhdrident_str), UINT64_FORMAT,
					 longhdr->xlp_sysid);
			snprintf(sysident_str, sizeof(sysident_str), UINT64_FORMAT,
					 state->system_identifier);
			ereport(state->emode_for_ptr(state, emode, recaddr),
					(errmsg("WAL file is from different database system"),
					 errdetail("WAL file database system identifier is %s, pg_control database system identifier is %s.",
							   fhdrident_str, sysident_str)));
			return false;
		}
		else if (longhdr->xlp_seg_size != XLogSegSize)
		{
			ereport(state->emode_for_ptr(state, emode, recaddr),
					(errmsg("WAL file is from different database system"),
					 errdetail("Incorrect XLOG_SEG_SIZE in page header.")));
			return false;
		}
		else if (longhdr->xlp_xlog_blcksz != XLOG_BLCKSZ)
		{
			ereport(state->emode_for_ptr(state, emode, recaddr),
					(errmsg("WAL file is from different database system"),
					 errdetail("Incorrect XLOG_BLCKSZ in page header.")));
			return false;
		}
	}
	else if (offset == 0)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		/* hmm, first page of file doesn't have a long header? */
		ereport(state->emode_for_ptr(state, emode, recaddr),
				(errmsg("invalid info bits %04X in log segment %s, offset %u",
						hdr->xlp_info,
						fname,
						offset)));
		return false;
	}

	if (!XLByteEQ(hdr->xlp_pageaddr, recaddr))
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		ereport(state->emode_for_ptr(state, emode, recaddr),
			(errmsg("unexpected pageaddr %X/%X in log segment %s, offset %u",
			  (uint32) (hdr->xlp_pageaddr >> 32), (uint32) hdr->xlp_pageaddr,
					fname,
					offset)));
		return false;
	}

	/*
	 * Check page TLI is one of the expected values.
	 */
	if (state->expectedTLEs != NIL &&
		!tliInHistory(hdr->xlp_tli, state->expectedTLEs))
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		ereport(state->emode_for_ptr(state, emode, recaddr),
			(errmsg("unexpected timeline ID %u in log segment %s, offset %u",
					hdr->xlp_tli,
					fname,
					offset)));
		return false;
	}

	/*
	 * Since child timelines are always assigned a TLI greater than their
	 * immediate parent's TLI, we should never see TLI go backwards across
	 * successive pages of a consistent WAL sequence.
	 *
	 * Of course this check should only be applied when advancing sequentially
	 * across pages; therefore ReadRecord resets lastPageTLI and lastSegmentTLI
	 * to zero when going to a random page. FIXME
	 *
	 * Sometimes we re-read a segment that's already been (partially) read. So
	 * we only verify TLIs for pages that are later than the last remembered
	 * LSN.
	 *
	 * XXX: This is slightly less precise than the check we did in earlier
	 * times. I don't see a problem with that though.
	 */
	if (state->latestReadPtr < recptr)
	{
		if (hdr->xlp_tli < state->latestReadTLI)
		{
			char		fname[MAXFNAMELEN];

			XLogFileName(fname, state->readPageTLI, segno);

			ereport(state->emode_for_ptr(state, emode, recaddr),
					(errmsg("out-of-sequence timeline ID %u (after %u) in log segment %s, offset %u",
							hdr->xlp_tli,
							state->latestReadTLI,
							fname,
							offset)));
			return false;
		}
		state->latestReadPtr = recptr;
		state->latestReadTLI = hdr->xlp_tli;
	}
	return true;
}
