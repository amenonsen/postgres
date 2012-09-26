/*-------------------------------------------------------------------------
 *
 * bitmapattutil.c
 *	Defines the routines to maintain all distinct attribute values
 *	which are indexed in the on-disk bitmap index.
 *
 * Copyright (c) 2007, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/tupdesc.h"
#include "access/bitmap.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "nodes/execnodes.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "access/heapam.h"
#include "optimizer/clauses.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"

static TupleDesc _bitmap_create_lov_heapTupleDesc(Relation rel);

/*
 * _bitmap_create_lov_heapandindex() -- create a new heap relation and
 *	a btree index for the list of values (LOV).
 */

void
_bitmap_create_lov_heapandindex(Relation rel, Oid *lovHeapId, Oid *lovIndexId)
{
	char		lovHeapName[NAMEDATALEN];
	char		lovIndexName[NAMEDATALEN];
	TupleDesc	tupDesc;
	IndexInfo  *indexInfo;
	ObjectAddress	objAddr, referenced;
	Oid		   *classObjectId;
    Oid		   *collationObjectId;
	Oid			heapid;
	Relation	heapRel;
	Oid			indid;
	int			indattrs;
	List	   *indexColNames;
	int			i;

	/* create the new names for the new lov heap and index */
	snprintf(lovHeapName, sizeof(lovHeapName), 
			 "pg_bm_%u", RelationGetRelid(rel)); 
	snprintf(lovIndexName, sizeof(lovIndexName), 
			 "pg_bm_%u_index", RelationGetRelid(rel)); 

	/*
	 * If this is happening during re-indexing, then such a heap should
	 * have existed already. Here, we delete this heap and its btree
	 * index first.
	 */
	heapid = get_relname_relid(lovHeapName, PG_BITMAPINDEX_NAMESPACE);
	if (OidIsValid(heapid))
	{
		ObjectAddress object;
		indid = get_relname_relid(lovIndexName, PG_BITMAPINDEX_NAMESPACE);

		Assert(OidIsValid(indid));

		/*
		 * Remove the dependency between the LOV heap relation, 
		 * the LOV index, and the parent bitmap index before 
		 * we drop the lov heap and index.
		 */
		deleteDependencyRecordsFor(RelationRelationId, heapid, false);
		deleteDependencyRecordsFor(RelationRelationId, indid, false);
		CommandCounterIncrement();

		object.classId = RelationRelationId;
		object.objectId = indid;
		object.objectSubId = 0;
		performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

		object.objectId = heapid;
		performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
	}

	/*
	 * create a new empty heap to store all attribute values with their
	 * corresponding block number and offset in LOV.
	 */
	tupDesc = _bitmap_create_lov_heapTupleDesc(rel);

	*lovHeapId = heap_create_with_catalog(lovHeapName,					/* relname */
										  PG_BITMAPINDEX_NAMESPACE,		/* relnamespace */
										  rel->rd_rel->reltablespace,	/* reltablespace */
										  InvalidOid,					/* relid */
										  InvalidOid,					/* reltypeid */
										  InvalidOid,					/* reloftypeid */
										  rel->rd_rel->relowner,		/* ownerid */
										  tupDesc,						/* tupdesc */
										  NIL,							/* cooked_constraints */
										  RELKIND_RELATION,				/* relkind */
										  RELPERSISTENCE_PERMANENT,		/* relpersistence */
										  rel->rd_rel->relisshared,		/* shared_relation */
										  RelationIsMapped(rel),		/* mapped_relation */
										  false,						/* oidislocal */
										  0,							/* oidinhcount */
										  ONCOMMIT_NOOP,				/* oncommit */
										  (Datum)0,						/* reloptions */
										  false,						/* use_user_acl */
										  true);						/* allow_system_table_mods */

	/*
	 * We must bump the command counter to make the newly-created relation
	 * tuple visible for opening.
	 */
	CommandCounterIncrement();

	objAddr.classId = RelationRelationId;
	objAddr.objectId = *lovHeapId;
	objAddr.objectSubId = 0 ;

	referenced.classId = RelationRelationId;
	referenced.objectId = RelationGetRelid(rel);
	referenced.objectSubId = 0;

	recordDependencyOn(&objAddr, &referenced, DEPENDENCY_INTERNAL);

	heapRel = RelationIdGetRelation(*lovHeapId); /* open relation */

	/*
	 * create a btree index on the newly-created heap. 
	 * The key includes all attributes to be indexed in this bitmap index.
	 */
	indattrs = tupDesc->natts - 2;
	indexInfo = makeNode(IndexInfo);
	indexInfo->ii_NumIndexAttrs = indattrs;
	indexInfo->ii_Expressions = NIL;
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = make_ands_implicit(NULL);
	indexInfo->ii_PredicateState = NIL;
	indexInfo->ii_Unique = true;

	indexColNames = NIL;
	classObjectId = (Oid *) palloc(indattrs * sizeof(Oid));
	collationObjectId = (Oid *) palloc(indattrs * sizeof(Oid));
	for (i = 0; i < indattrs; i++)
	{
		indexInfo->ii_KeyAttrNumbers[i] = i + 1;

		/*
		 * Append just a pointer to the name character data in the descriptor
		 * to the column name list.  This is safe because it will be
		 * immediately copied by index_create.
		 */
		indexColNames = lappend(indexColNames,
								NameStr(tupDesc->attrs[i]->attname)); 

		/* Use the default class respective to the attribute type. */
		classObjectId[i] = GetDefaultOpClass(tupDesc->attrs[i]->atttypid,
											 BTREE_AM_OID);

		/* Copy the collation objects from the index. */
		collationObjectId[i] = tupDesc->attrs[i]->attcollation;
	}

	*lovIndexId = index_create(heapRel,						/* heapRelation */
						   /* *lovHeapId, */				/* heapRelationId */			/* removed */
							   lovIndexName,				/* indexRelationName */
							   InvalidOid,					/* indexRelationId */
							   InvalidOid,					/* relFileNode */
						 	   indexInfo,					/* indexInfo */
							   indexColNames,				/* indexColNames */
							   BTREE_AM_OID,				/* accessMethodObjectId */
							   rel->rd_rel->reltablespace,	/* tableSpaceId */
							   collationObjectId,			/* collationObjectId */
							   classObjectId,				/* classObjectId */
							   NULL,						/* coloptions */
							   0,							/* reloptions */
							   false,						/* isprimary */
							   false,						/* isconstant */
							   false,						/* deferrable */
							   false,						/* initdeferred */
							   true,						/* allow_system_table_mods */
							   false,						/* skip_build */
							   false);						/* concurrent */
		
	list_free(indexColNames);
	indexColNames = NIL;

	objAddr.classId = RelationRelationId;
	objAddr.objectId = *lovIndexId;
	objAddr.objectSubId = 0 ;

	recordDependencyOn(&objAddr, &referenced, DEPENDENCY_INTERNAL);

	RelationClose(heapRel);
}

/*
 * _bitmap_create_lov_heapTupleDesc() -- create the new heap tuple descriptor.
 */

TupleDesc
_bitmap_create_lov_heapTupleDesc(Relation rel)
{
	TupleDesc	tupDesc;
	TupleDesc	oldTupDesc;
	AttrNumber	attno;
	int			natts;

	oldTupDesc = RelationGetDescr(rel);
	natts = oldTupDesc->natts + 2;

	tupDesc = CreateTemplateTupleDesc(natts, false);

	for (attno = 1; attno <= oldTupDesc->natts; attno++)
	{
		/* copy the attribute to be indexed. */
		memcpy(tupDesc->attrs[attno - 1], oldTupDesc->attrs[attno - 1], 
			   ATTRIBUTE_FIXED_PART_SIZE);
		tupDesc->attrs[attno - 1]->attnum = attno;
		tupDesc->attrs[attno - 1]->attnotnull = false;
		tupDesc->attrs[attno - 1]->atthasdef = false;
	}

	/* the block number */
	TupleDescInitEntry(tupDesc, attno, "blockNumber", INT4OID, -1, 0);
	attno++;

	/* the offset number */
	TupleDescInitEntry(tupDesc, attno, "offsetNumber", INT4OID, -1, 0);

	return tupDesc;
}

/*
 * _bitmap_open_lov_heapandindex() -- open the heap relation and the btree
 *		index for LOV.
 */

void
_bitmap_open_lov_heapandindex(BMMetaPage metapage,
				  Relation *lovHeapP, Relation *lovIndexP,
				  LOCKMODE lockMode)
{
	*lovHeapP = heap_open(metapage->bm_lov_heapId, lockMode);
	*lovIndexP = index_open(metapage->bm_lov_indexId, lockMode);
}

/*
 * _bitmap_insert_lov() -- insert a new data into the given heap and index.
 */
void
_bitmap_insert_lov(Relation lovHeap, Relation lovIndex, Datum *datum, 
				   bool *nulls, bool use_wal)
{
	TupleDesc	tupDesc;
	HeapTuple	tuple;
	bool		result;
	Datum	   *indexDatum;
	bool	   *indexNulls;

	tupDesc = RelationGetDescr(lovHeap);

	/* insert this tuple into the heap */
	tuple = heap_form_tuple(tupDesc, datum, nulls);
	heap_insert(lovHeap, tuple, GetCurrentCommandId(true), 0, NULL);

	/* insert a new tuple into the index */
	indexDatum = palloc0((tupDesc->natts - 2) * sizeof(Datum));
	indexNulls = palloc0((tupDesc->natts - 2) * sizeof(bool));
	memcpy(indexDatum, datum, (tupDesc->natts - 2) * sizeof(Datum));
	memcpy(indexNulls, nulls, (tupDesc->natts - 2) * sizeof(bool));
	result = index_insert(lovIndex, indexDatum, indexNulls, 
					 	  &(tuple->t_self), lovHeap, true);

	pfree(indexDatum);
	pfree(indexNulls);
	Assert(result);

	heap_freetuple(tuple);
}


/*
 * _bitmap_close_lov_heapandindex() -- close the heap and the index.
 */
void
_bitmap_close_lov_heapandindex(Relation lovHeap, Relation lovIndex, 
							   LOCKMODE lockMode)
{
	heap_close(lovHeap, lockMode);
	index_close(lovIndex, lockMode);
}

/*
 * _bitmap_findvalue() -- find a row in a given heap using
 *  a given index that satisfies the given scan key.
 * 
 * If this value exists, this function returns true. Otherwise,
 * returns false.
 *
 * If this value exists in the heap, this function also returns
 * the block number and the offset number that are stored in the same
 * row with this value. This block number and the offset number
 * are for the LOV item that points the bitmap vector for this value.
 */
bool
_bitmap_findvalue(Relation lovHeap, Relation lovIndex,
				  ScanKey scanKey, IndexScanDesc scanDesc,
				  BlockNumber *lovBlock, bool *blockNull,
				  OffsetNumber *lovOffset, bool *offsetNull)
{
	TupleDesc		tupDesc;
	HeapTuple		tuple;
	bool			found = false;

	tupDesc = RelationGetDescr(lovIndex);

	tuple = index_getnext(scanDesc, ForwardScanDirection);

	if (tuple != NULL)
	{
		TupleDesc 	heapTupDesc;
		Datum 		d;

		found = true;
		heapTupDesc = RelationGetDescr(lovHeap);

		d = heap_getattr(tuple, tupDesc->natts + 1, heapTupDesc, blockNull);
		*lovBlock =	DatumGetInt32(d);
		d = heap_getattr(tuple, tupDesc->natts + 2, heapTupDesc, offsetNull);
		*lovOffset = DatumGetInt16(d);
	}
	return found;
}

