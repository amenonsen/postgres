/*-------------------------------------------------------------------------
 *
 * pg_xlogdump.c - decode and display WAL
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_xlogdump/pg_xlogdump.c
 *-------------------------------------------------------------------------
 */

/* ugly hack, same as in e.g pg_controldata */
#define FRONTEND 1
#include "postgres.h"

#include <unistd.h>
#include <libgen.h>

/* we don't want to include postgres.h */
typedef uintptr_t Datum;

#include "access/xlogreader.h"
#include "access/rmgr.h"

#include "catalog/catalog.h"

#include "utils/elog.h"

#include "getopt_long.h"

#include "pg_config_manual.h"

static const char *progname;

typedef struct XLogDumpPrivateData {
	TimeLineID timeline;
	char* outpath;
	char* inpath;
	char* file;
	XLogRecPtr startptr;
	XLogRecPtr endptr;

	bool  bkp_details;
} XLogDumpPrivateData;

static void fatal_error(const char *fmt, ...)
__attribute__((format(printf, 1, 2)))
	;

static void fatal_error(const char *fmt, ...)
{
	va_list         args;
	fflush(stdout);

	fprintf(stderr, "fatal_error: ");
	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);
	fprintf(stderr, "\n");
	exit(1);
}

static void
XLogDumpXLogRead(const char *directory, TimeLineID timeline_id,
                 XLogRecPtr startptr, char *buf, Size count);

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
			         directory == NULL ? XLOGDIR : directory, fname);

			sendFile = open(fpath, O_RDONLY, 0);
			if (sendFile < 0)
			{
				/*
				 * If the file is not found, assume it's because the standby
				 * asked for a too old WAL segment that has already been
				 * removed or recycled.
				 */
				if (errno == ENOENT)
					fatal_error("requested WAL segment %s has already been removed",
								fname);
				else
					fatal_error("could not open file \"%s\": %u",
								fpath, errno);
			}
			sendOff = 0;
		}

		/* Need to seek in the file? */
		if (sendOff != startoff)
		{
			if (lseek(sendFile, (off_t) startoff, SEEK_SET) < 0){
				char fname[MAXPGPATH];
				XLogFileName(fname, timeline_id, sendSegNo);

				fatal_error("could not seek in log segment %s to offset %u: %d",
							fname,
							startoff,
							errno);
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
			char fname[MAXPGPATH];
			XLogFileName(fname, timeline_id, sendSegNo);

			fatal_error("could not read from log segment %s, offset %u, length %lu: %d",
						fname,
						sendOff, (unsigned long) segbytes, errno);
		}

		/* Update state for read */
		XLByteAdvance(recptr, readbytes);

		sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
}

static int
XLogDumpReadPage(XLogReaderState* state, XLogRecPtr targetPagePtr, int reqLen,
				 int emode, char *readBuff, TimeLineID *curFileTLI)
{
	XLogDumpPrivateData *private = state->private_data;
	int count = XLOG_BLCKSZ;

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
XLogDumpDisplayRecord(XLogReaderState* state, XLogRecord* record)
{
	XLogDumpPrivateData *config = (XLogDumpPrivateData *)state->private_data;
	const RmgrData *rmgr = &RmgrTable[record->xl_rmid];

	StringInfo str = makeStringInfo();
	initStringInfo(str);

	rmgr->rm_desc(str, record->xl_info, XLogRecGetData(record));

	fprintf(stdout, "xlog record: rmgr: %-11s, record_len: %6u, tot_len: %6u, tx: %10u, lsn: %X/%08X, prev %X/%08X, bkp: %u%u%u%u, desc: %s\n",
			rmgr->rm_name,
			record->xl_len, record->xl_tot_len,
			record->xl_xid,
			(uint32)(state->ReadRecPtr >> 32), (uint32)state->ReadRecPtr,
			(uint32)(record->xl_prev >> 32), (uint32)record->xl_prev,
			!!(XLR_BKP_BLOCK(0) & record->xl_info),
			!!(XLR_BKP_BLOCK(1) & record->xl_info),
			!!(XLR_BKP_BLOCK(2) & record->xl_info),
			!!(XLR_BKP_BLOCK(3) & record->xl_info),
			str->data);

	if (config->bkp_details)
	{
		int off;
		char *blk = (char *) XLogRecGetData(record) + record->xl_len;

		for (off = 0; off < XLR_MAX_BKP_BLOCKS; off++)
		{
			BkpBlock	bkpb;

			if (!(XLR_BKP_BLOCK(off) & record->xl_info))
				continue;

			memcpy(&bkpb, blk, sizeof(BkpBlock));
			blk += sizeof(BkpBlock);

			fprintf(stdout, "\tbackup bkp #%u; rel %u/%u/%u; fork: %s; block: %u; hole: offset: %u, length: %u\n",
					off, bkpb.node.spcNode, bkpb.node.dbNode, bkpb.node.relNode,
					forkNames[bkpb.fork], bkpb.block, bkpb.hole_offset, bkpb.hole_length);
		}
	}
}

static void
usage(const char *progname)
{
	printf(_("%s: reads/writes postgres transaction logs for debugging.\n\n"),
	       progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -b, --bkp-details      output detailed information about backup blocks\n"));
	printf(_("  -e, --end RECPTR       read wal up to RECPTR\n"));
	printf(_("  -f, --file FILE        wal file to parse, cannot be specified together with -p\n"));
	printf(_("  -h, --help             show this help, then exit\n"));
	printf(_("  -p, --path PATH        from where do we want to read? cwd/pg_xlog is the default\n"));
	printf(_("  -s, --start RECPTR     read wal in directory indicated by -p starting at RECPTR\n"));
	printf(_("  -t, --timeline TLI     which timeline do we want to read, defaults to 1\n"));
	printf(_("  -v, --version          output version information, then exit\n"));
}

static int
emode_for_corrupt_record(XLogReaderState *state, int emode, XLogRecPtr RecPtr)
{
	return emode;
}

int main(int argc, char **argv)
{
	uint32 xlogid;
	uint32 xrecoff;
	XLogReaderState *xlogreader_state;
	XLogDumpPrivateData private;
	XLogRecord *record;

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

	int			c;
	int			option_index;


	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_xlogdump"));

	progname = get_progname(argv[0]);


	memset(&private, 0, sizeof(XLogDumpPrivateData));

	private.timeline = 1;
	private.bkp_details = false;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;

	if (argc <= 1)
	{
		fprintf(stderr, _("%s: no arguments specified\n"), progname);
		goto bad_argument;
	}

	while ((c = getopt_long(argc, argv, "be:f:hp:s:t:V",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'b':
				private.bkp_details = true;
				break;
			case 'e':
				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, _("%s: couldn't parse -e %s\n"),
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
				usage(progname);
				exit(0);
				break;
			case 'p':
				private.inpath = strdup(optarg);
				break;
			case 's':
				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, _("%s: couldn't parse -s %s\n"),
							progname, optarg);
					goto bad_argument;
				}
				else
					private.startptr = (uint64)xlogid << 32 | xrecoff;
				break;
			case 't':
				if (sscanf(optarg, "%d", &private.timeline) != 1)
				{
					fprintf(stderr, _("%s: couldn't parse timeline -t %s\n"),
							progname, optarg);
					goto bad_argument;
				}
				break;
			case 'V':
				printf("%s: (PostgreSQL): %s\n", progname, PG_VERSION);
				exit(0);
				break;
			default:
				fprintf(stderr, _("%s: unknown argument -%c passed\n"),
						progname, c);
				goto bad_argument;
				break;
		}
	}

	/* some parameter was badly specified, don't output further errors */
	if (optind < argc)
	{
		fprintf(stderr,
				_("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		goto bad_argument;
	}
	else if (private.inpath != NULL && private.file != NULL)
	{
		fprintf(stderr,
				_("%s: only one of -p or -f can be specified\n"),
				progname);
		goto bad_argument;
	}
	/* no file specified, but no range of of interesting data either */
	else if (private.file == NULL && XLByteEQ(private.startptr, InvalidXLogRecPtr))
	{
		fprintf(stderr,
				_("%s: no -s given in range mode.\n"),
				progname);
		goto bad_argument;
	}
    /* everything ok, do some more setup */
	else
	{
		/* default value */
		if (private.file == NULL && private.inpath == NULL)
		{
			private.inpath = "pg_xlog";
		}

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
						_("%s: -s does not lie inside file \"%s\"\n"),
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
						_("%s: -e does not lie inside file \"%s\"\n"),
						progname, private.file);
				goto bad_argument;
			}
		}
	}

	/* we have everything we need, continue */
	{
		XLogRecPtr first_record;

		xlogreader_state = XLogReaderAllocate(private.startptr, XLogDumpReadPage,
											  emode_for_corrupt_record, &private);

		/* first find a valid recptr to start from */
		first_record = XLogFindNextRecord(xlogreader_state, private.startptr, ERROR);

		if (first_record == InvalidXLogRecPtr)
			fatal_error("Could not find a valid record after %X/%X",
						(uint32)(private.startptr >> 32), (uint32)private.startptr);

		/*
		 * Display a message that were skipping data if `from` wasn't a pointer
		 * to the start of a record and also wasn't a pointer to the beginning
		 * of a segment (e.g. we were used in file mode).
		 */
		if (first_record != private.startptr && (private.startptr % XLogSegSize) != 0)
			fprintf(stdout, "first record is after %X/%X, at %X/%X, skipping over %u bytes\n",
					(uint32)(private.startptr >> 32), (uint32)private.startptr,
					(uint32)(first_record >> 32), (uint32)first_record,
					(uint32)(first_record - private.endptr));

		while ((record = XLogReadRecord(xlogreader_state, first_record, ERROR)))
		{
			/* continue after the last record */
			first_record = InvalidXLogRecPtr;
			XLogDumpDisplayRecord(xlogreader_state, record);
		}

		XLogReaderFree(xlogreader_state);
	}

	return 0;
bad_argument:
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
	exit(1);
}
