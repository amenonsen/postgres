/*-------------------------------------------------------------------------
 *
 * pg_xlogdump.c - decode and display WAL
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/pg_xlogdump/pg_xlogdump.c
 *-------------------------------------------------------------------------
 */

/* ugly hack, same as in e.g pg_controldata */
#define FRONTEND 1
#include "postgres.h"

#include <unistd.h>

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/rmgr.h"
#include "access/transam.h"

#include "catalog/catalog.h"

#include "getopt_long.h"

static const char *progname;

typedef struct XLogDumpPrivateData
{
	TimeLineID	timeline;
	char	   *inpath;
	char	   *file;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;

	/* display options */
	bool		bkp_details;
	int			stop_after_records;
	int			already_displayed_records;

	/* filter options */
	int         filter_by_rmgr;
	TransactionId filter_by_xid;
} XLogDumpPrivateData;

static void fatal_error(const char *fmt, ...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

static void fatal_error(const char *fmt, ...)
{
	va_list		args;
	fflush(stdout);

	fprintf(stderr, "%s: fatal_error: ", progname);
	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);
	fputc('\n', stderr);
	exit(1);
}

/* this should probably be put in a general implementation */
static void
XLogDumpXLogRead(const char *directory, TimeLineID timeline_id,
				 XLogRecPtr startptr, char *buf, Size count)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	static int	sendFile = -1;
	static XLogSegNo sendSegNo = 0;
	static uint32 sendOff = 0;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

		startoff = recptr % XLogSegSize;

		if (sendFile < 0 || !XLByteInSeg(recptr, sendSegNo))
		{
			char		fname[MAXFNAMELEN];
			char		fpath[MAXPGPATH];

			/* Switch to another logfile segment */
			if (sendFile >= 0)
				close(sendFile);

			XLByteToSeg(recptr, sendSegNo);

			XLogFileName(fname, timeline_id, sendSegNo);

			/*
			 * Try to find the file in several places:
			 * if directory == NULL:
			 *   fname
			 *   XLOGDIR / fname
			 *   $DATADIR / XLOGDIR / fname
			 * else
			 *   directory / fname
			 *   directory / XLOGDIR / fname
			 */
			if (directory == NULL)
			{
				const char* datadir;

				/* fname */
				sendFile = open(fname, O_RDONLY, 0);
				if (sendFile < 0 && errno != ENOENT)
					goto file_not_found;
				else if (sendFile > 0)
					goto file_found;

				/* XLOGDIR / fname */
				snprintf(fpath, MAXPGPATH, "%s/%s",
				         XLOGDIR, fname);
				sendFile = open(fpath, O_RDONLY, 0);
				if (sendFile < 0 && errno != ENOENT)
					goto file_not_found;
				else if (sendFile > 0)
					goto file_found;

				datadir = getenv("DATADIR");
				/* $DATADIR / XLOGDIR / fname */
				if (datadir != NULL)
				{
					snprintf(fpath, MAXPGPATH, "%s/%s/%s",
					         datadir, XLOGDIR, fname);
					sendFile = open(fpath, O_RDONLY, 0);
					if (sendFile < 0 && errno != ENOENT)
						goto file_not_found;
					else if (sendFile > 0)
						goto file_found;
				}


			}
			else
			{
				/* directory / fname */
				snprintf(fpath, MAXPGPATH, "%s/%s",
				         directory, fname);
				sendFile = open(fpath, O_RDONLY, 0);
				if (sendFile < 0 && errno != ENOENT)
					goto file_not_found;
				else if (sendFile > 0)
					goto file_found;

				/* directory / XLOGDIR / fname */
				snprintf(fpath, MAXPGPATH, "%s/%s/%s",
				         directory, XLOGDIR, fname);
				sendFile = open(fpath, O_RDONLY, 0);
				if (sendFile < 0 && errno != ENOENT)
					goto file_not_found;
				else if (sendFile > 0)
					goto file_found;
			}

			if (sendFile < 0)
				goto file_not_found;
			else
				goto file_found;

		file_not_found:
			fatal_error("could not find file \"%s\": %s",
			            fname, strerror(errno));

		file_found:
			sendOff = 0;
		}

		/* Need to seek in the file? */
		if (sendOff != startoff)
		{
			if (lseek(sendFile, (off_t) startoff, SEEK_SET) < 0)
			{
				int		err = errno;
				char	fname[MAXPGPATH];
				XLogFileName(fname, timeline_id, sendSegNo);

				fatal_error("could not seek in log segment %s to offset %u: %s",
							fname, startoff, strerror(err));
			}
			sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (XLogSegSize - startoff))
			segbytes = XLogSegSize - startoff;
		else
			segbytes = nbytes;

		readbytes = read(sendFile, p, segbytes);
		if (readbytes <= 0)
		{
			int		err = errno;
			char	fname[MAXPGPATH];
			XLogFileName(fname, timeline_id, sendSegNo);

			fatal_error("could not read from log segment %s, offset %d, length %d: %s",
						fname, sendOff, segbytes, strerror(err));
		}

		/* Update state for read */
		recptr += readbytes;

		sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
}

static int
XLogDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				 char *readBuff, TimeLineID *curFileTLI)
{
	XLogDumpPrivateData *private = state->private_data;
	int			count = XLOG_BLCKSZ;

	if (private->endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
			return -1;
	}

	XLogDumpXLogRead(private->inpath, private->timeline, targetPagePtr,
					 readBuff, count);

	return count;
}


/*
 * Find the first record with at an lsn >= RecPtr.
 *
 * Useful for checking wether RecPtr is a valid xlog address for reading and to
 * find the first valid address after some address when dumping records for
 * debugging purposes.
 */
static XLogRecPtr
FindFirstRecord(XLogDumpPrivateData *private, XLogRecPtr RecPtr)
{
	XLogRecPtr	targetPagePtr;
	XLogRecPtr	tmpRecPtr;
	int			targetRecOff;
	uint32		pageHeaderSize;
	XLogPageHeader header;
	char	   *buffer;

	buffer = (char *) malloc(XLOG_BLCKSZ);
	if (buffer == NULL)
		fatal_error("out of memory");

	targetRecOff = RecPtr % XLOG_BLCKSZ;

	/* scroll back to page boundary */
	targetPagePtr = RecPtr - targetRecOff;

	/* Read the page containing the record */
	XLogDumpXLogRead(private->inpath, private->timeline,
					 targetPagePtr, buffer, XLOG_BLCKSZ);

	header = (XLogPageHeader) buffer;

	pageHeaderSize = XLogPageHeaderSize(header);

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
	 *
	 * FIXME: except if the continued record extends completely over this page,
	 * onto the next page.
	 */
	return tmpRecPtr;
}

static void
XLogDumpDisplayRecord(XLogReaderState *state, XLogRecord *record)
{
	XLogDumpPrivateData *config = (XLogDumpPrivateData *) state->private_data;
	const RmgrData *rmgr = &RmgrTable[record->xl_rmid];

	if (config->filter_by_rmgr != -1 &&
	    config->filter_by_rmgr != record->xl_rmid)
		return;

	if (TransactionIdIsValid(config->filter_by_xid) &&
	    config->filter_by_xid != record->xl_xid)
		return;

	config->already_displayed_records++;

	printf("xlog record: rmgr: %-11s, record_len: %6u, tot_len: %6u, tx: %10u, lsn: %X/%08X, prev %X/%08X, bkp: %u%u%u%u, desc:",
			rmgr->rm_name,
			record->xl_len, record->xl_tot_len,
			record->xl_xid,
			(uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr,
			(uint32) (record->xl_prev >> 32), (uint32) record->xl_prev,
			!!(XLR_BKP_BLOCK(0) & record->xl_info),
			!!(XLR_BKP_BLOCK(1) & record->xl_info),
			!!(XLR_BKP_BLOCK(2) & record->xl_info),
			!!(XLR_BKP_BLOCK(3) & record->xl_info));

	/* the desc routine will printf the description directly to stdout */
	rmgr->rm_desc(NULL, record->xl_info, XLogRecGetData(record));

	putchar('\n');

	if (config->bkp_details)
	{
		int		off;
		char   *blk = (char *) XLogRecGetData(record) + record->xl_len;

		for (off = 0; off < XLR_MAX_BKP_BLOCKS; off++)
		{
			BkpBlock	bkpb;

			if (!(XLR_BKP_BLOCK(off) & record->xl_info))
				continue;

			memcpy(&bkpb, blk, sizeof(BkpBlock));
			blk += sizeof(BkpBlock);
			blk += BLCKSZ - bkpb.hole_length;

			printf("\tbackup bkp #%u; rel %u/%u/%u; fork: %s; block: %u; hole: offset: %u, length: %u\n",
				   off, bkpb.node.spcNode, bkpb.node.dbNode, bkpb.node.relNode,
				   forkNames[bkpb.fork], bkpb.block, bkpb.hole_offset, bkpb.hole_length);
		}
	}
}

static void
usage(void)
{
	printf("%s: reads/writes postgres transaction logs for debugging.\n\n",
		   progname);
	printf("Usage:\n");
	printf("  %s [OPTION]...\n", progname);
	printf("\nOptions:\n");
	printf("  -b, --bkp-details      output detailed information about backup blocks\n");
	printf("  -e, --end RECPTR       read wal up to RECPTR\n");
	printf("  -f, --file FILE        wal file to parse, cannot be specified together with -p\n");
	printf("  -h, --help             show this help, then exit\n");
	printf("  -n, --limit RECORDS    only display n records, abort afterwards\n");
	printf("  -p, --path PATH        from where do we want to read? cwd/pg_xlog is the default\n");
	printf("  -r, --rmgr RMGR        only show records generated by the rmgr RMGR\n");
	printf("  -s, --start RECPTR     read wal in directory indicated by -p starting at RECPTR\n");
	printf("  -t, --timeline TLI     which timeline do we want to read, defaults to 1\n");
	printf("  -V, --version          output version information, then exit\n");
	printf("  -x, --xid XID          only show records with transactionid XID\n");
}

int
main(int argc, char **argv)
{
	uint32		xlogid;
	uint32		xrecoff;
	XLogReaderState *xlogreader_state;
	XLogDumpPrivateData private;
	XLogRecord *record;
	XLogRecPtr	first_record;
	char	   *errormsg;

	static struct option long_options[] = {
		{"bkp-details", no_argument, NULL, 'b'},
		{"end", required_argument, NULL, 'e'},
		{"file", required_argument, NULL, 'f'},
		{"help", no_argument, NULL, '?'},
		{"limit", required_argument, NULL, 'n'},
		{"path", required_argument, NULL, 'p'},
		{"rmgr", required_argument, NULL, 'r'},
		{"start", required_argument, NULL, 's'},
		{"timeline", required_argument, NULL, 't'},
		{"xid", required_argument, NULL, 'x'},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int			option;
	int			optindex = 0;

	progname = get_progname(argv[0]);

	memset(&private, 0, sizeof(XLogDumpPrivateData));

	private.timeline = 1;
	private.bkp_details = false;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.stop_after_records = -1;
	private.already_displayed_records = 0;
	private.filter_by_rmgr = -1;
	private.filter_by_xid = InvalidTransactionId;

	if (argc <= 1)
	{
		fprintf(stderr, "%s: no arguments specified\n", progname);
		goto bad_argument;
	}

	while ((option = getopt_long(argc, argv, "be:f:?n:p:r:s:t:Vx:",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'b':
				private.bkp_details = true;
				break;
			case 'e':
				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, "%s: could not parse parse --end %s\n",
							progname, optarg);
					goto bad_argument;
				}
				private.endptr = (uint64)xlogid << 32 | xrecoff;
				break;
			case 'f':
				private.file = strdup(optarg);
				break;
			case '?':
				usage();
				exit(0);
				break;
			case 'n':
				if (sscanf(optarg, "%d", &private.stop_after_records) != 1)
				{
					fprintf(stderr, "%s: could not parse parse --limit %s\n",
							progname, optarg);
					goto bad_argument;
				}
				break;
			case 'p':
				private.inpath = strdup(optarg);
				break;
			case 'r':
			{
				int i;
				for (i = 0; i < RM_MAX_ID; i++)
				{
					if (strcmp(optarg, RmgrTable[i].rm_name) == 0)
					{
						private.filter_by_rmgr = i;
						break;
					}
				}

				if (private.filter_by_rmgr == -1)
				{
					fprintf(stderr, "%s: --rmgr %s does not exist\n",
							progname, optarg);
					goto bad_argument;
				}
			}
			break;
			case 's':
				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, "%s: could not parse parse --end %s\n",
							progname, optarg);
					goto bad_argument;
				}
				else
					private.startptr = (uint64)xlogid << 32 | xrecoff;
				break;
			case 't':
				if (sscanf(optarg, "%d", &private.timeline) != 1)
				{
					fprintf(stderr, "%s: could not parse timeline --timeline %s\n",
							progname, optarg);
					goto bad_argument;
				}
				break;
			case 'V':
				puts("pg_xlogdump (PostgreSQL) " PG_VERSION);
				exit(0);
				break;
			case 'x':
				if (sscanf(optarg, "%u", &private.filter_by_xid) != 1)
				{
					fprintf(stderr, "%s: could not parse --xid %s as a valid xid\n",
							progname, optarg);
					goto bad_argument;
				}
				break;
			default:
				goto bad_argument;
		}
	}

	/* some parameter was badly specified, don't output further errors */
	if (optind < argc)
	{
		fprintf(stderr,
				"%s: too many command-line arguments (first is \"%s\")\n",
				progname, argv[optind]);
		goto bad_argument;
	}
	else if (private.inpath != NULL && private.file != NULL)
	{
		fprintf(stderr,
				"%s: only one of -p or -f can be specified\n",
				progname);
		goto bad_argument;
	}
	/* no file specified, but no range of of interesting data either */
	else if (private.file == NULL && XLogRecPtrIsInvalid(private.startptr))
	{
		fprintf(stderr, "%s: no -s given in range mode.\n", progname);
		goto bad_argument;
	}
	/* everything ok, do some more setup */
	else
	{
		if (private.file != NULL)
		{
			XLogSegNo segno;
			char *sep;

			/* split filepath into directory & filename */
			sep = strrchr(private.file, '/');
			/* directory path */
			if (sep != NULL)
			{
				/* windows doesn't have strndup */
				private.inpath = strdup(private.file);
				private.inpath[(sep - private.file) + 1] = '\0';
				private.file = strdup(sep + 1);
			}
			/* local directory */
			else
			{
				private.inpath = NULL;
				private.file = strdup(private.file);
			}

			/* FIXME: can we rely on basename? */
			XLogFromFileName(private.file, &private.timeline, &segno);

			if (XLogRecPtrIsInvalid(private.startptr))
				XLogSegNoOffsetToRecPtr(segno, 0, private.startptr);
			else if (!XLByteInSeg(private.startptr, segno))
			{
				fprintf(stderr,
						"%s: --start %X/%X is not inside --file \"%s\"\n",
						progname,
						(uint32)(private.startptr >> 32),
						(uint32)private.startptr,
						private.file);
				goto bad_argument;
			}

			if (XLogRecPtrIsInvalid(private.endptr))
				XLogSegNoOffsetToRecPtr(segno + 1, 0, private.endptr);
			else if (!XLByteInSeg(private.endptr, segno) &&
					 private.endptr != (segno + 1) * XLogSegSize)
			{
				fprintf(stderr,
						"%s: --end %X/%X is not inside --file \"%s\"\n",
						progname,
						(uint32)(private.endptr >> 32),
						(uint32)private.endptr,
						private.file);
				goto bad_argument;
			}
		}

		/* XXX: validate directory */
	}

	/* we have everything we need, start reading */
	xlogreader_state = XLogReaderAllocate(private.startptr,
										  XLogDumpReadPage,
										  &private);

	/* first find a valid recptr to start from */
	first_record = FindFirstRecord(&private, private.startptr);

	if (first_record == InvalidXLogRecPtr)
		fatal_error("could not find a valid record after %X/%X",
					(uint32) (private.startptr >> 32),
					(uint32) private.startptr);

	/*
	 * Display a message that were skipping data if `from` wasn't a pointer
	 * to the start of a record and also wasn't a pointer to the beginning
	 * of a segment (e.g. we were used in file mode).
	 */
	if (first_record != private.startptr && (private.startptr % XLogSegSize) != 0)
		printf("first record is after %X/%X, at %X/%X, skipping over %u bytes\n",
			   (uint32) (private.startptr >> 32), (uint32) private.startptr,
			   (uint32) (first_record >> 32), (uint32) first_record,
			   (uint32) (first_record - private.endptr));

	while ((record = XLogReadRecord(xlogreader_state, first_record, &errormsg)))
	{
		/* continue after the last record */
		first_record = InvalidXLogRecPtr;
		XLogDumpDisplayRecord(xlogreader_state, record);

		/* check whether we printed enough */
		if (private.stop_after_records > 0 &&
			private.already_displayed_records >= private.stop_after_records)
			break;
	}

	if (errormsg)
		fatal_error("error in WAL record at %X/%X: %s\n",
					(uint32)(xlogreader_state->ReadRecPtr >> 32),
					(uint32)xlogreader_state->ReadRecPtr,
					errormsg);

	XLogReaderFree(xlogreader_state);

	return 0;
bad_argument:
	fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
	exit(1);
}
