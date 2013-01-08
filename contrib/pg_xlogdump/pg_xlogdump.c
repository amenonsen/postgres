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
#include <sys/types.h>
#include <dirent.h>

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
	exit(EXIT_FAILURE);
}

/*
 * Check whether directory exists and whether we can open it. Keep errno set
 * error reporting by the caller.
 */
static bool
verify_directory(const char *directory)
{
	DIR *dir = opendir(directory);
	if (dir == NULL)
		return false;
	closedir(dir);
	return true;
}

static void
split_path(const char *path, char **dir, char **fname)
{
	char *sep;

	/* split filepath into directory & filename */
	sep = strrchr(path, '/');

	/* directory path */
	if (sep != NULL)
	{
		/* windows doesn't have strndup */
		*dir = strdup(path);
		(*dir)[(sep - path) + 1] = '\0';
		*fname = strdup(sep + 1);
		}
	/* local directory */
	else
	{
		*dir = NULL;
		*fname = strdup(path);
	}
}

/*
 * Try to find the file in several places:
 * if directory == NULL:
 *   fname
 *   XLOGDIR / fname
 *   $PGDATA / XLOGDIR / fname
 * else
 *   directory / fname
 *   directory / XLOGDIR / fname
 *
 * return a read only fd
 */
static int
fuzzy_open_file(const char *directory, const char *fname)
{
	int fd = -1;
	char fpath[MAXPGPATH];

	if (directory == NULL)
	{
		const char* datadir;

		/* fname */
		fd = open(fname, O_RDONLY | PG_BINARY, 0);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd > 0)
			return fd;

		/* XLOGDIR / fname */
		snprintf(fpath, MAXPGPATH, "%s/%s",
				 XLOGDIR, fname);
		fd = open(fpath, O_RDONLY | PG_BINARY, 0);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd > 0)
			return fd;

		datadir = getenv("PGDATA");
		/* $PGDATA / XLOGDIR / fname */
		if (datadir != NULL)
		{
			snprintf(fpath, MAXPGPATH, "%s/%s/%s",
					 datadir, XLOGDIR, fname);
			fd = open(fpath, O_RDONLY | PG_BINARY, 0);
			if (fd < 0 && errno != ENOENT)
				return -1;
			else if (fd > 0)
				return fd;
		}
	}
	else
	{
		/* directory / fname */
		snprintf(fpath, MAXPGPATH, "%s/%s",
				 directory, fname);
		fd = open(fpath, O_RDONLY | PG_BINARY, 0);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd > 0)
			return fd;

		/* directory / XLOGDIR / fname */
		snprintf(fpath, MAXPGPATH, "%s/%s/%s",
				 directory, XLOGDIR, fname);
		fd = open(fpath, O_RDONLY | PG_BINARY, 0);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd > 0)
			return fd;
	}
	return -1;
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

			/* Switch to another logfile segment */
			if (sendFile >= 0)
				close(sendFile);

			XLByteToSeg(recptr, sendSegNo);

			XLogFileName(fname, timeline_id, sendSegNo);

			sendFile = fuzzy_open_file(directory, fname);

			if (sendFile < 0)
				fatal_error("could not find file \"%s\": %s",
							fname, strerror(errno));
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
	printf("  %s [OPTION] [STARTSEG [ENDSEG]] \n", progname);
	printf("\nOptions:\n");
	printf("  -b, --bkp-details      output detailed information about backup blocks\n");
	printf("  -e, --end RECPTR       read wal up to RECPTR\n");
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

	while ((option = getopt_long(argc, argv, "be:?n:p:r:s:t:Vx:",
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
			case '?':
				usage();
				exit(EXIT_SUCCESS);
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
				exit(EXIT_SUCCESS);
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

	if ((optind + 2) < argc)
	{
		fprintf(stderr,
				"%s: too many command-line arguments (first is \"%s\")\n",
				progname, argv[optind + 2]);
		goto bad_argument;
	}

	if (private.inpath != NULL)
	{
		/* validdate path points to directory */
		if (!verify_directory(private.inpath))
		{
			fprintf(stderr,
					"%s: --path %s is cannot be opened: %s\n",
					progname, private.inpath, strerror(errno));
			goto bad_argument;
		}
	}

	/* parse files as start/end boundaries, extract path if not specified */
	if (optind < argc)
	{
		char *directory = NULL;
		char *fname = NULL;
		int fd;
		XLogSegNo segno;

		split_path(argv[optind], &directory, &fname);

		if (private.inpath == NULL && directory != NULL)
		{
			private.inpath = directory;

			if (!verify_directory(private.inpath))
				fatal_error("cannot open directory %s: %s",
							private.inpath, strerror(errno));
		}

		fd = fuzzy_open_file(private.inpath, fname);
		if (fd < 0)
			fatal_error("could not open file %s", fname);
		close(fd);

		/* parse position from file */
		XLogFromFileName(fname, &private.timeline, &segno);

		if (XLogRecPtrIsInvalid(private.startptr))
			XLogSegNoOffsetToRecPtr(segno, 0, private.startptr);
		else if (!XLByteInSeg(private.startptr, segno))
		{
			fprintf(stderr,
					"%s: --end %X/%X is not inside file \"%s\"\n",
					progname,
					(uint32)(private.startptr >> 32),
					(uint32)private.startptr,
					fname);
			goto bad_argument;
		}

		/* no second file specified, set end position */
		if (!(optind + 1 < argc) && XLogRecPtrIsInvalid(private.endptr))
			XLogSegNoOffsetToRecPtr(segno + 1, 0, private.endptr);

		/* parse ENDSEG if passed */
		if (optind + 1 < argc)
		{
			XLogSegNo endsegno;

			/* ignore directory, already have that */
			split_path(argv[optind + 1], &directory, &fname);

			fd = fuzzy_open_file(private.inpath, fname);
			if (fd < 0)
				fatal_error("could not open file %s", fname);
			close(fd);

			/* parse position from file */
			XLogFromFileName(fname, &private.timeline, &endsegno);

			if (endsegno < segno)
				fatal_error("ENDSEG %s is before STARSEG %s",
							argv[optind + 1], argv[optind]);

			if (XLogRecPtrIsInvalid(private.endptr))
				XLogSegNoOffsetToRecPtr(endsegno + 1, 0, private.endptr);

			/* set segno to endsegno for check of --end */
			segno = endsegno;
		}


		if (!XLByteInSeg(private.endptr, segno) &&
			private.endptr != (segno + 1) * XLogSegSize)
		{
			fprintf(stderr,
					"%s: --end %X/%X is not inside file \"%s\"\n",
					progname,
					(uint32)(private.endptr >> 32),
					(uint32)private.endptr,
					argv[argc -1]);
			goto bad_argument;
		}
	}

	/* we don't know what to print */
	if (XLogRecPtrIsInvalid(private.startptr))
	{
		fprintf(stderr, "%s: no --start given in range mode.\n", progname);
		goto bad_argument;
	}

	/* done with argument parsing, do the actual work */

	/* we have everything we need, start reading */
	xlogreader_state = XLogReaderAllocate(private.startptr,
										  XLogDumpReadPage,
										  &private);

	/* first find a valid recptr to start from */
	first_record = XLogFindNextRecord(xlogreader_state, private.startptr);

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
			   (uint32) (first_record - private.startptr));

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

	return EXIT_SUCCESS;
bad_argument:
	fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
	return EXIT_FAILURE;
}
