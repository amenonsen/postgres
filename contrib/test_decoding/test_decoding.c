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

#include "access/sysattr.h"

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

extern void pg_decode_init(void **private_data);

extern bool pg_decode_begin_txn(void *private_data, StringInfo out, ReorderBufferTXN* txn);
extern bool pg_decode_commit_txn(void *private_data, StringInfo out, ReorderBufferTXN* txn, XLogRecPtr commit_lsn);
extern bool pg_decode_change(void *private_data, StringInfo out, ReorderBufferTXN* txn, Oid tableoid, ReorderBufferChange *change);

void
_PG_init(void)
{
}

/* initialize this plugin */
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

/* BEGIN callback */
bool
pg_decode_begin_txn(void *private_data, StringInfo out, ReorderBufferTXN* txn)
{
	AssertVariableIsOfType(&pg_decode_begin_txn, LogicalDecodeBeginCB);

	appendStringInfo(out, "BEGIN");
	return true;
}

/* COMMIT callback */
bool
pg_decode_commit_txn(void *private_data, StringInfo out, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
	AssertVariableIsOfType(&pg_decode_commit_txn, LogicalDecodeCommitCB);

	appendStringInfo(out, "COMMIT");
	return true;
}

/* print the tuple 'tuple' into the StringInfo s */
static void
tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
	int	natt;
	Oid oid;

	/* print oid of tuple, it's not included in the TupleDesc */
	if ((oid = HeapTupleHeaderGetOid(tuple->t_data)) != InvalidOid)
	{
		appendStringInfo(s, " oid[oid]:%u", oid);
	}

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid; /* type of current attribute */
		HeapTuple	type_tuple; /* information about a type */
		Form_pg_type type_form;
		Oid			typoutput; /* output function */
		bool		typisvarlena;
		Datum		origval; /* possibly toasted Datum */
		Datum		val; /* definitely detoasted Datum */
		char        *outputstr;
		bool        isnull; /* column is null? */

		attr = tupdesc->attrs[natt];
		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped)
			continue;

		/*
		 * Don't print system columns
		 */
		if (attr->attnum < 0)
			continue;

		typid = attr->atttypid;

		/* gather type name */
		type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(type_tuple))
			elog(ERROR, "cache lookup failed for type %u", typid);
		type_form = (Form_pg_type) GETSTRUCT(type_tuple);

		/* print attribute name */
		appendStringInfoChar(s, ' ');
		appendStringInfoString(s, NameStr(attr->attname));

		/* print attribute type */
		appendStringInfoChar(s, '[');
		appendStringInfoString(s, NameStr(type_form->typname));
		appendStringInfoChar(s, ']');

		/* query output function */
		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		ReleaseSysCache(type_tuple);

		/* get Datum from tuple */
		origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

		if (typisvarlena && !isnull)
			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
		else
			val = origval;

		/* print data */
		if (isnull)
			outputstr = "(null)";
		else
			outputstr = OidOutputFunctionCall(typoutput, val);

		appendStringInfoChar(s, ':');
		appendStringInfoString(s, outputstr);
	}
}

/*
 * callback for individual changed tuples
 */
bool
pg_decode_change(void *private_data, StringInfo out, ReorderBufferTXN* txn,
				 Oid tableoid, ReorderBufferChange *change)
{
	Relation relation = RelationIdGetRelation(tableoid);
	Form_pg_class class_form = RelationGetForm(relation);
	TupleDesc	tupdesc = RelationGetDescr(relation);
	MemoryContext context = (MemoryContext)private_data;
	/*
	 * switch to our own context we can reset after the tuple is printed,
	 * otherwise we will leak memory in via many of the output routines.
	 */
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
			Relation indexrel;
			TupleDesc	indexdesc;

			/*
			 * deletions only store the primary key part of the tuple, display
			 * that index.
			 */

			/* make sure rd_primary is set */
			RelationGetIndexList(relation);

			if (!OidIsValid(relation->rd_primary))
			{
				elog(LOG, "tuple in table with oid: %u without primary key", tableoid);
				break;
			}

			appendStringInfoString(out, " DELETE:");

			indexrel = RelationIdGetRelation(relation->rd_primary);

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
