/*-------------------------------------------------------------------------
 *
 * test_deocding.c
 *		  example output plugin for the logical replication functionality
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/test_decoding/test_decoding.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"

#include "replication/output_plugin.h"
#include "replication/snapbuild.h"

#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


PG_MODULE_MAGIC;

void _PG_init(void);

void WalSndWriteData(XLogRecPtr lsn, const char *data, Size len);

extern void pg_decode_init(void **private_data);

extern bool pg_decode_begin_txn(void *private_data, StringInfo out, ReorderBufferTXN* txn);
extern bool pg_decode_commit_txn(void *private_data, StringInfo out, ReorderBufferTXN* txn, XLogRecPtr commit_lsn);
extern bool pg_decode_change(void *private_data, StringInfo out, ReorderBufferTXN* txn, Oid tableoid, ReorderBufferChange *change);


void
_PG_init(void)
{
}

void
pg_decode_init(void **private_data)
{
	AssertVariableIsOfType(&pg_decode_init, LogicalDecodeInitCB);
	*private_data = AllocSetContextCreate(TopMemoryContext,
									 "text conversion context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
}

bool
pg_decode_begin_txn(void *private_data, StringInfo out, ReorderBufferTXN* txn)
{
	AssertVariableIsOfType(&pg_decode_begin_txn, LogicalDecodeBeginCB);

	appendStringInfo(out, "BEGIN %d", txn->xid);
	return true;
}

bool
pg_decode_commit_txn(void *private_data, StringInfo out, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
	AssertVariableIsOfType(&pg_decode_commit_txn, LogicalDecodeCommitCB);

	appendStringInfo(out, "COMMIT %d", txn->xid);
	return true;
}

static void
tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
	int			i;
	HeapTuple	typeTuple;
	Form_pg_type pt;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Oid			typid, typoutput;
		bool		typisvarlena;
		Datum		origval, val;
		char        *outputstr;
		bool        isnull;
		if (tupdesc->attrs[i]->attisdropped)
			continue;
		if (tupdesc->attrs[i]->attnum < 0)
			continue;

		typid = tupdesc->attrs[i]->atttypid;

		typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(typeTuple))
			elog(ERROR, "cache lookup failed for type %u", typid);
		pt = (Form_pg_type) GETSTRUCT(typeTuple);

		appendStringInfoChar(s, ' ');
		appendStringInfoString(s, NameStr(tupdesc->attrs[i]->attname));
		appendStringInfoChar(s, '[');
		appendStringInfoString(s, NameStr(pt->typname));
		appendStringInfoChar(s, ']');

		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		ReleaseSysCache(typeTuple);

		origval = fastgetattr(tuple, i + 1, tupdesc, &isnull);

		if (typisvarlena && !isnull)
			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
		else
			val = origval;

		if (isnull)
			outputstr = "(null)";
		else
			outputstr = OidOutputFunctionCall(typoutput, val);

		appendStringInfoChar(s, ':');
		appendStringInfoString(s, outputstr);
	}
}

/* This is is just for demonstration, don't ever use this code for anything real! */
bool
pg_decode_change(void *private_data, StringInfo out, ReorderBufferTXN* txn,
				 Oid tableoid, ReorderBufferChange *change)
{
	Relation relation = RelationIdGetRelation(tableoid);
	Form_pg_class class_form = RelationGetForm(relation);
	TupleDesc	tupdesc = RelationGetDescr(relation);
	MemoryContext context = (MemoryContext)private_data;
	MemoryContext old = MemoryContextSwitchTo(context);

	AssertVariableIsOfType(&pg_decode_change, LogicalDecodeChangeCB);

	appendStringInfoString(out, "table \"");
	appendStringInfoString(out, NameStr(class_form->relname));
	appendStringInfoString(out, "\":");

	switch (change->action)
	{
	case REORDER_BUFFER_CHANGE_INSERT:
		appendStringInfoString(out, " INSERT:");
		tuple_to_stringinfo(out, tupdesc, &change->newtuple->tuple);
		break;
	case REORDER_BUFFER_CHANGE_UPDATE:
		appendStringInfoString(out, " UPDATE:");
		tuple_to_stringinfo(out, tupdesc, &change->newtuple->tuple);
		break;
	case REORDER_BUFFER_CHANGE_DELETE:
		{
			Oid indexoid = InvalidOid;
			Relation indexrel;
			TupleDesc	indexdesc;

			int16 pknratts;
			int16 pkattnum[INDEX_MAX_KEYS];
			Oid pktypoid[INDEX_MAX_KEYS];
			Oid pkopclass[INDEX_MAX_KEYS];

			MemSet(pkattnum, 0, sizeof(pkattnum));
			MemSet(pktypoid, 0, sizeof(pktypoid));
			MemSet(pkopclass, 0, sizeof(pkopclass));

			appendStringInfoString(out, " DELETE (pkey):");

			relationFindPrimaryKey(relation, &indexoid, &pknratts,
								   pkattnum, pktypoid, pkopclass);
			indexrel = RelationIdGetRelation(indexoid);

			indexdesc = RelationGetDescr(indexrel);

			tuple_to_stringinfo(out, indexdesc, &change->oldtuple->tuple);

			RelationClose(indexrel);
			break;
		}
	}
	RelationClose(relation);

	MemoryContextSwitchTo(old);
	MemoryContextReset(context);
	return true;
}
