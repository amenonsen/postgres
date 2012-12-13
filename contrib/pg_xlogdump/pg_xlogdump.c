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
#include <libgen.h>

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/rmgr.h"
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

	bool		bkp_details;
} XLogDumpPrivateData;

static void fatal_error(const char *fmt, ...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

static void fatal_error(const char *fmt, ...)
{
	va_list		args;
	fflush(stdout);

	fprintf(stderr, "fatal_error: ");
	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);
	fprintf(stderr, "\n");
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

			snprintf(fpath, MAXPGPATH, "%s/%s",
					 (directory == NULL) ? XLOGDIR : directory, fname);

			sendFile = open(fpath, O_RDONLY, 0);
			if (sendFile < 0)
			{
				fatal_error("could not open file \"%s\": %s",
							fpath, strerror(errno));
			}
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
		XLByteAdvance(recptr, readbytes);

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
		if (targetPagePtr > private->endptr)
			return -1;

		if (targetPagePtr + reqLen > private->endptr)
			count = private->endptr - targetPagePtr;
	}

	XLogDumpXLogRead(private->inpath, private->timeline, targetPagePtr,
					 readBuff, count);

	return count;
}

static void
XLogDumpDisplayRecord(XLogReaderState *state, XLogRecord *record)
{
	XLogDumpPrivateData *config = (XLogDumpPrivateData *) state->private_data;
	const RmgrData *rmgr = &RmgrTable[record->xl_rmid];

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

	puts("\n");

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
	printf("  -p, --path PATH        from where do we want to read? cwd/pg_xlog is the default\n");
	printf("  -s, --start RECPTR     read wal in directory indicated by -p starting at RECPTR\n");
	printf("  -t, --timeline TLI     which timeline do we want to read, defaults to 1\n");
	printf("  -v, --version          output version information, then exit\n");
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
		{"path", required_argument, NULL, 'p'},
		{"start", required_argument, NULL, 's'},
		{"timeline", required_argument, NULL, 't'},
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

	if (argc <= 1)
	{
		fprintf(stderr, "%s: no arguments specified\n", progname);
		goto bad_argument;
	}

	while ((option = getopt_long(argc, argv, "be:f:hp:s:t:V",
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
					fprintf(stderr, "%s: couldn't parse -e %s\n",
							progname, optarg);
					goto bad_argument;
				}
				else
					private.endptr = (uint64)xlogid << 32 | xrecoff;
				break;
			case 'f':
				private.file = strdup(optarg);
				break;
			case '?':
				usage();
				exit(0);
				break;
			case 'p':
				private.inpath = strdup(optarg);
				break;
			case 's':
				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, "%s: couldn't parse -s %s\n",
							progname, optarg);
					goto bad_argument;
				}
				else
					private.startptr = (uint64)xlogid << 32 | xrecoff;
				break;
			case 't':
				if (sscanf(optarg, "%d", &private.timeline) != 1)
				{
					fprintf(stderr, "%s: couldn't parse timeline -t %s\n",
							progname, optarg);
					goto bad_argument;
				}
				break;
			case 'V':
				puts("pg_xlogdump (PostgreSQL) " PG_VERSION);
				exit(0);
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
	else if (private.file == NULL && XLByteEQ(private.startptr, InvalidXLogRecPtr))
	{
		fprintf(stderr, "%s: no -s given in range mode.\n", progname);
		goto bad_argument;
	}
	/* everything ok, do some more setup */
	else
	{
		/* default value */
		if (private.file == NULL && private.inpath == NULL)
			private.inpath = "pg_xlog";

		/* XXX: validate directory */

		/* default value */
		if (private.file != NULL)
		{
			XLogSegNo segno;

			/* FIXME: can we rely on basename? */
			XLogFromFileName(basename(private.file), &private.timeline, &segno);
			private.inpath = strdup(dirname(private.file));

			if (XLByteEQ(private.startptr, InvalidXLogRecPtr))
				XLogSegNoOffsetToRecPtr(segno, 0, private.startptr);
			else if (!XLByteInSeg(private.startptr, segno))
			{
				fprintf(stderr,
						"%s: -s does not lie inside file \"%s\"\n",
						progname,
						private.file);
				goto bad_argument;
			}

			if (XLByteEQ(private.endptr, InvalidXLogRecPtr))
				XLogSegNoOffsetToRecPtr(segno + 1, 0, private.endptr);
			else if (!XLByteInSeg(private.endptr, segno) &&
					 private.endptr != (segno + 1) * XLogSegSize)
			{
				fprintf(stderr,
						"%s: -e does not lie inside file \"%s\"\n",
						progname, private.file);
				goto bad_argument;
			}
		}
	}

	/* we have everything we need, start reading */
	xlogreader_state = XLogReaderAllocate(private.startptr,
										  XLogDumpReadPage,
										  &private);

	/* first find a valid recptr to start from */
	first_record = XLogFindNextRecord(xlogreader_state, private.startptr);

	if (first_record == InvalidXLogRecPtr)
		fatal_error("Could not find a valid record after %X/%X",
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
	}
	if (errormsg)
		fprintf(stderr, "error in WAL record: %s\n", errormsg);

	XLogReaderFree(xlogreader_state);

	return 0;
bad_argument:
	fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
	exit(1);
}
