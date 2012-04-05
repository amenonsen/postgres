/*-------------------------------------------------------------------------
 * output_plugin.h
 *     PostgreSQL Logical Decode Plugin Interface
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef OUTPUT_PLUGIN_H
#define OUTPUT_PLUGIN_H

#include "lib/stringinfo.h"

#include "replication/reorderbuffer.h"

/*
 * Callback that gets called in a user-defined plugin.
 * 'private' can be set to some private data.
 *
 * Gets looked up in the library symbol pg_decode_init.
 */
typedef void (*LogicalDecodeInitCB) (
	void **private
	);

/*
 * Gets called for every BEGIN of a successful transaction.
 *
 * Return "true" if the message in "out" should get sent, false otherwise.
 *
 * Gets looked up in the library symbol pg_decode_begin_txn.
 */
typedef bool (*LogicalDecodeBeginCB) (
	void *private,
	StringInfo out,
	ReorderBufferTXN *txn);

/*
 * Gets called for every change in a successful transaction.
 *
 * Return "true" if the message in "out" should get sent, false otherwise.
 *
 * Gets looked up in the library symbol pg_decode_change.
 */
typedef bool (*LogicalDecodeChangeCB) (
	void *private,
	StringInfo out,
	ReorderBufferTXN *txn,
	Oid tableoid,
	ReorderBufferChange *change
	);

/*
 * Gets called for every COMMIT of a successful transaction.
 *
 * Return "true" if the message in "out" should get sent, false otherwise.
 *
 * Gets looked up in the library symbol pg_decode_commit_txn.
 */
typedef bool (*LogicalDecodeCommitCB) (
	void *private,
	StringInfo out,
	ReorderBufferTXN *txn,
	XLogRecPtr commit_lsn);

/*
 * Gets called to cleanup the state of an output plugin
 *
 * Gets looked up in the library symbol pg_decode_cleanup.
 */
typedef void (*LogicalDecodeCleanupCB) (
	void *private
	);

#endif
