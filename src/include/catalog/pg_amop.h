/*-------------------------------------------------------------------------
 *
 * pg_amop.h
 *	  definition of the system "amop" relation (pg_amop)
 *	  along with the relation's initial contents.
 *
 * The amop table identifies the operators associated with each index operator
 * family and operator class (classes are subsets of families).  An associated
 * operator can be either a search operator or an ordering operator, as
 * identified by amoppurpose.
 *
 * The primary key for this table is <amopfamily, amoplefttype, amoprighttype,
 * amopstrategy>.  amoplefttype and amoprighttype are just copies of the
 * operator's oprleft/oprright, ie its declared input data types.  The
 * "default" operators for a particular opclass within the family are those
 * with amoplefttype = amoprighttype = opclass's opcintype.  An opfamily may
 * also contain other operators, typically cross-data-type operators.  All the
 * operators within a family are supposed to be compatible, in a way that is
 * defined by each individual index AM.
 *
 * We also keep a unique index on <amopopr, amoppurpose, amopfamily>, so that
 * we can use a syscache to quickly answer questions of the form "is this
 * operator in this opfamily, and if so what are its semantics with respect to
 * the family?"  This implies that the same operator cannot be listed for
 * multiple strategy numbers within a single opfamily, with the exception that
 * it's possible to list it for both search and ordering purposes (with
 * different strategy numbers for the two purposes).
 *
 * amopmethod is a copy of the owning opfamily's opfmethod field.  This is an
 * intentional denormalization of the catalogs to buy lookup speed.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_amop.h
 *
 * NOTES
 *	 the genbki.pl script reads this file and generates .bki
 *	 information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AMOP_H
#define PG_AMOP_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_amop definition.  cpp turns this into
 *		typedef struct FormData_pg_amop
 * ----------------
 */
#define AccessMethodOperatorRelationId	2602

CATALOG(pg_amop,2602)
{
	Oid			amopfamily;		/* the index opfamily this entry is for */
	Oid			amoplefttype;	/* operator's left input data type */
	Oid			amoprighttype;	/* operator's right input data type */
	int16		amopstrategy;	/* operator strategy number */
	char		amoppurpose;	/* is operator for 's'earch or 'o'rdering? */
	Oid			amopopr;		/* the operator's pg_operator OID */
	Oid			amopmethod;		/* the index access method this entry is for */
	Oid			amopsortfamily; /* ordering opfamily OID, or 0 if search op */
} FormData_pg_amop;

/* allowed values of amoppurpose: */
#define AMOP_SEARCH		's'		/* operator is for search */
#define AMOP_ORDER		'o'		/* operator is for ordering */

/* ----------------
 *		Form_pg_amop corresponds to a pointer to a tuple with
 *		the format of pg_amop relation.
 * ----------------
 */
typedef FormData_pg_amop *Form_pg_amop;

/* ----------------
 *		compiler constants for pg_amop
 * ----------------
 */
#define Natts_pg_amop					8
#define Anum_pg_amop_amopfamily			1
#define Anum_pg_amop_amoplefttype		2
#define Anum_pg_amop_amoprighttype		3
#define Anum_pg_amop_amopstrategy		4
#define Anum_pg_amop_amoppurpose		5
#define Anum_pg_amop_amopopr			6
#define Anum_pg_amop_amopmethod			7
#define Anum_pg_amop_amopsortfamily		8

/* ----------------
 *		initial contents of pg_amop
 * ----------------
 */

/*
 *	btree integer_ops
 */

/* default operators int2 */
DATA(insert (	1976   21 21 1 s	95	403 0 ));
DATA(insert (	1976   21 21 2 s	522 403 0 ));
DATA(insert (	1976   21 21 3 s	94	403 0 ));
DATA(insert (	1976   21 21 4 s	524 403 0 ));
DATA(insert (	1976   21 21 5 s	520 403 0 ));
/* crosstype operators int24 */
DATA(insert (	1976   21 23 1 s	534 403 0 ));
DATA(insert (	1976   21 23 2 s	540 403 0 ));
DATA(insert (	1976   21 23 3 s	532 403 0 ));
DATA(insert (	1976   21 23 4 s	542 403 0 ));
DATA(insert (	1976   21 23 5 s	536 403 0 ));
/* crosstype operators int28 */
DATA(insert (	1976   21 20 1 s	1864	403 0 ));
DATA(insert (	1976   21 20 2 s	1866	403 0 ));
DATA(insert (	1976   21 20 3 s	1862	403 0 ));
DATA(insert (	1976   21 20 4 s	1867	403 0 ));
DATA(insert (	1976   21 20 5 s	1865	403 0 ));
/* default operators int4 */
DATA(insert (	1976   23 23 1 s	97	403 0 ));
DATA(insert (	1976   23 23 2 s	523 403 0 ));
DATA(insert (	1976   23 23 3 s	96	403 0 ));
DATA(insert (	1976   23 23 4 s	525 403 0 ));
DATA(insert (	1976   23 23 5 s	521 403 0 ));
/* crosstype operators int42 */
DATA(insert (	1976   23 21 1 s	535 403 0 ));
DATA(insert (	1976   23 21 2 s	541 403 0 ));
DATA(insert (	1976   23 21 3 s	533 403 0 ));
DATA(insert (	1976   23 21 4 s	543 403 0 ));
DATA(insert (	1976   23 21 5 s	537 403 0 ));
/* crosstype operators int48 */
DATA(insert (	1976   23 20 1 s	37	403 0 ));
DATA(insert (	1976   23 20 2 s	80	403 0 ));
DATA(insert (	1976   23 20 3 s	15	403 0 ));
DATA(insert (	1976   23 20 4 s	82	403 0 ));
DATA(insert (	1976   23 20 5 s	76	403 0 ));
/* default operators int8 */
DATA(insert (	1976   20 20 1 s	412 403 0 ));
DATA(insert (	1976   20 20 2 s	414 403 0 ));
DATA(insert (	1976   20 20 3 s	410 403 0 ));
DATA(insert (	1976   20 20 4 s	415 403 0 ));
DATA(insert (	1976   20 20 5 s	413 403 0 ));
/* crosstype operators int82 */
DATA(insert (	1976   20 21 1 s	1870	403 0 ));
DATA(insert (	1976   20 21 2 s	1872	403 0 ));
DATA(insert (	1976   20 21 3 s	1868	403 0 ));
DATA(insert (	1976   20 21 4 s	1873	403 0 ));
DATA(insert (	1976   20 21 5 s	1871	403 0 ));
/* crosstype operators int84 */
DATA(insert (	1976   20 23 1 s	418 403 0 ));
DATA(insert (	1976   20 23 2 s	420 403 0 ));
DATA(insert (	1976   20 23 3 s	416 403 0 ));
DATA(insert (	1976   20 23 4 s	430 403 0 ));
DATA(insert (	1976   20 23 5 s	419 403 0 ));

/*
 *	btree oid_ops
 */

DATA(insert (	1989   26 26 1 s	609 403 0 ));
DATA(insert (	1989   26 26 2 s	611 403 0 ));
DATA(insert (	1989   26 26 3 s	607 403 0 ));
DATA(insert (	1989   26 26 4 s	612 403 0 ));
DATA(insert (	1989   26 26 5 s	610 403 0 ));

/*
 * btree tid_ops
 */

DATA(insert (	2789   27 27 1 s 2799 403 0 ));
DATA(insert (	2789   27 27 2 s 2801 403 0 ));
DATA(insert (	2789   27 27 3 s 387  403 0 ));
DATA(insert (	2789   27 27 4 s 2802 403 0 ));
DATA(insert (	2789   27 27 5 s 2800 403 0 ));

/*
 *	btree oidvector_ops
 */

DATA(insert (	1991   30 30 1 s	645 403 0 ));
DATA(insert (	1991   30 30 2 s	647 403 0 ));
DATA(insert (	1991   30 30 3 s	649 403 0 ));
DATA(insert (	1991   30 30 4 s	648 403 0 ));
DATA(insert (	1991   30 30 5 s	646 403 0 ));

/*
 *	btree float_ops
 */

/* default operators float4 */
DATA(insert (	1970   700 700 1 s	622 403 0 ));
DATA(insert (	1970   700 700 2 s	624 403 0 ));
DATA(insert (	1970   700 700 3 s	620 403 0 ));
DATA(insert (	1970   700 700 4 s	625 403 0 ));
DATA(insert (	1970   700 700 5 s	623 403 0 ));
/* crosstype operators float48 */
DATA(insert (	1970   700 701 1 s	1122 403 0 ));
DATA(insert (	1970   700 701 2 s	1124 403 0 ));
DATA(insert (	1970   700 701 3 s	1120 403 0 ));
DATA(insert (	1970   700 701 4 s	1125 403 0 ));
DATA(insert (	1970   700 701 5 s	1123 403 0 ));
/* default operators float8 */
DATA(insert (	1970   701 701 1 s	672 403 0 ));
DATA(insert (	1970   701 701 2 s	673 403 0 ));
DATA(insert (	1970   701 701 3 s	670 403 0 ));
DATA(insert (	1970   701 701 4 s	675 403 0 ));
DATA(insert (	1970   701 701 5 s	674 403 0 ));
/* crosstype operators float84 */
DATA(insert (	1970   701 700 1 s	1132 403 0 ));
DATA(insert (	1970   701 700 2 s	1134 403 0 ));
DATA(insert (	1970   701 700 3 s	1130 403 0 ));
DATA(insert (	1970   701 700 4 s	1135 403 0 ));
DATA(insert (	1970   701 700 5 s	1133 403 0 ));

/*
 *	btree char_ops
 */

DATA(insert (	429   18 18 1 s  631	403 0 ));
DATA(insert (	429   18 18 2 s  632	403 0 ));
DATA(insert (	429   18 18 3 s   92	403 0 ));
DATA(insert (	429   18 18 4 s  634	403 0 ));
DATA(insert (	429   18 18 5 s  633	403 0 ));

/*
 *	btree name_ops
 */

DATA(insert (	1986   19 19 1 s	660 403 0 ));
DATA(insert (	1986   19 19 2 s	661 403 0 ));
DATA(insert (	1986   19 19 3 s	93	403 0 ));
DATA(insert (	1986   19 19 4 s	663 403 0 ));
DATA(insert (	1986   19 19 5 s	662 403 0 ));

/*
 *	btree text_ops
 */

DATA(insert (	1994   25 25 1 s	664 403 0 ));
DATA(insert (	1994   25 25 2 s	665 403 0 ));
DATA(insert (	1994   25 25 3 s	98	403 0 ));
DATA(insert (	1994   25 25 4 s	667 403 0 ));
DATA(insert (	1994   25 25 5 s	666 403 0 ));

/*
 *	btree bpchar_ops
 */

DATA(insert (	426   1042 1042 1 s 1058	403 0 ));
DATA(insert (	426   1042 1042 2 s 1059	403 0 ));
DATA(insert (	426   1042 1042 3 s 1054	403 0 ));
DATA(insert (	426   1042 1042 4 s 1061	403 0 ));
DATA(insert (	426   1042 1042 5 s 1060	403 0 ));

/*
 *	btree bytea_ops
 */

DATA(insert (	428   17 17 1 s 1957	403 0 ));
DATA(insert (	428   17 17 2 s 1958	403 0 ));
DATA(insert (	428   17 17 3 s 1955	403 0 ));
DATA(insert (	428   17 17 4 s 1960	403 0 ));
DATA(insert (	428   17 17 5 s 1959	403 0 ));

/*
 *	btree abstime_ops
 */

DATA(insert (	421   702 702 1 s  562	403 0 ));
DATA(insert (	421   702 702 2 s  564	403 0 ));
DATA(insert (	421   702 702 3 s  560	403 0 ));
DATA(insert (	421   702 702 4 s  565	403 0 ));
DATA(insert (	421   702 702 5 s  563	403 0 ));

/*
 *	btree datetime_ops
 */

/* default operators date */
DATA(insert (	434   1082 1082 1 s 1095	403 0 ));
DATA(insert (	434   1082 1082 2 s 1096	403 0 ));
DATA(insert (	434   1082 1082 3 s 1093	403 0 ));
DATA(insert (	434   1082 1082 4 s 1098	403 0 ));
DATA(insert (	434   1082 1082 5 s 1097	403 0 ));
/* crosstype operators vs timestamp */
DATA(insert (	434   1082 1114 1 s 2345	403 0 ));
DATA(insert (	434   1082 1114 2 s 2346	403 0 ));
DATA(insert (	434   1082 1114 3 s 2347	403 0 ));
DATA(insert (	434   1082 1114 4 s 2348	403 0 ));
DATA(insert (	434   1082 1114 5 s 2349	403 0 ));
/* crosstype operators vs timestamptz */
DATA(insert (	434   1082 1184 1 s 2358	403 0 ));
DATA(insert (	434   1082 1184 2 s 2359	403 0 ));
DATA(insert (	434   1082 1184 3 s 2360	403 0 ));
DATA(insert (	434   1082 1184 4 s 2361	403 0 ));
DATA(insert (	434   1082 1184 5 s 2362	403 0 ));
/* default operators timestamp */
DATA(insert (	434   1114 1114 1 s 2062	403 0 ));
DATA(insert (	434   1114 1114 2 s 2063	403 0 ));
DATA(insert (	434   1114 1114 3 s 2060	403 0 ));
DATA(insert (	434   1114 1114 4 s 2065	403 0 ));
DATA(insert (	434   1114 1114 5 s 2064	403 0 ));
/* crosstype operators vs date */
DATA(insert (	434   1114 1082 1 s 2371	403 0 ));
DATA(insert (	434   1114 1082 2 s 2372	403 0 ));
DATA(insert (	434   1114 1082 3 s 2373	403 0 ));
DATA(insert (	434   1114 1082 4 s 2374	403 0 ));
DATA(insert (	434   1114 1082 5 s 2375	403 0 ));
/* crosstype operators vs timestamptz */
DATA(insert (	434   1114 1184 1 s 2534	403 0 ));
DATA(insert (	434   1114 1184 2 s 2535	403 0 ));
DATA(insert (	434   1114 1184 3 s 2536	403 0 ));
DATA(insert (	434   1114 1184 4 s 2537	403 0 ));
DATA(insert (	434   1114 1184 5 s 2538	403 0 ));
/* default operators timestamptz */
DATA(insert (	434   1184 1184 1 s 1322	403 0 ));
DATA(insert (	434   1184 1184 2 s 1323	403 0 ));
DATA(insert (	434   1184 1184 3 s 1320	403 0 ));
DATA(insert (	434   1184 1184 4 s 1325	403 0 ));
DATA(insert (	434   1184 1184 5 s 1324	403 0 ));
/* crosstype operators vs date */
DATA(insert (	434   1184 1082 1 s 2384	403 0 ));
DATA(insert (	434   1184 1082 2 s 2385	403 0 ));
DATA(insert (	434   1184 1082 3 s 2386	403 0 ));
DATA(insert (	434   1184 1082 4 s 2387	403 0 ));
DATA(insert (	434   1184 1082 5 s 2388	403 0 ));
/* crosstype operators vs timestamp */
DATA(insert (	434   1184 1114 1 s 2540	403 0 ));
DATA(insert (	434   1184 1114 2 s 2541	403 0 ));
DATA(insert (	434   1184 1114 3 s 2542	403 0 ));
DATA(insert (	434   1184 1114 4 s 2543	403 0 ));
DATA(insert (	434   1184 1114 5 s 2544	403 0 ));

/*
 *	btree time_ops
 */

DATA(insert (	1996   1083 1083 1 s 1110 403 0 ));
DATA(insert (	1996   1083 1083 2 s 1111 403 0 ));
DATA(insert (	1996   1083 1083 3 s 1108 403 0 ));
DATA(insert (	1996   1083 1083 4 s 1113 403 0 ));
DATA(insert (	1996   1083 1083 5 s 1112 403 0 ));

/*
 *	btree timetz_ops
 */

DATA(insert (	2000   1266 1266 1 s 1552 403 0 ));
DATA(insert (	2000   1266 1266 2 s 1553 403 0 ));
DATA(insert (	2000   1266 1266 3 s 1550 403 0 ));
DATA(insert (	2000   1266 1266 4 s 1555 403 0 ));
DATA(insert (	2000   1266 1266 5 s 1554 403 0 ));

/*
 *	btree interval_ops
 */

DATA(insert (	1982   1186 1186 1 s 1332 403 0 ));
DATA(insert (	1982   1186 1186 2 s 1333 403 0 ));
DATA(insert (	1982   1186 1186 3 s 1330 403 0 ));
DATA(insert (	1982   1186 1186 4 s 1335 403 0 ));
DATA(insert (	1982   1186 1186 5 s 1334 403 0 ));

/*
 *	btree macaddr
 */

DATA(insert (	1984   829 829 1 s 1222 403 0 ));
DATA(insert (	1984   829 829 2 s 1223 403 0 ));
DATA(insert (	1984   829 829 3 s 1220 403 0 ));
DATA(insert (	1984   829 829 4 s 1225 403 0 ));
DATA(insert (	1984   829 829 5 s 1224 403 0 ));

/*
 *	btree network
 */

DATA(insert (	1974   869 869 1 s 1203 403 0 ));
DATA(insert (	1974   869 869 2 s 1204 403 0 ));
DATA(insert (	1974   869 869 3 s 1201 403 0 ));
DATA(insert (	1974   869 869 4 s 1206 403 0 ));
DATA(insert (	1974   869 869 5 s 1205 403 0 ));

/*
 *	btree numeric
 */

DATA(insert (	1988   1700 1700 1 s 1754 403 0 ));
DATA(insert (	1988   1700 1700 2 s 1755 403 0 ));
DATA(insert (	1988   1700 1700 3 s 1752 403 0 ));
DATA(insert (	1988   1700 1700 4 s 1757 403 0 ));
DATA(insert (	1988   1700 1700 5 s 1756 403 0 ));

/*
 *	btree bool
 */

DATA(insert (	424   16 16 1 s 58		403 0 ));
DATA(insert (	424   16 16 2 s 1694	403 0 ));
DATA(insert (	424   16 16 3 s 91		403 0 ));
DATA(insert (	424   16 16 4 s 1695	403 0 ));
DATA(insert (	424   16 16 5 s 59		403 0 ));

/*
 *	btree bit
 */

DATA(insert (	423   1560 1560 1 s 1786	403 0 ));
DATA(insert (	423   1560 1560 2 s 1788	403 0 ));
DATA(insert (	423   1560 1560 3 s 1784	403 0 ));
DATA(insert (	423   1560 1560 4 s 1789	403 0 ));
DATA(insert (	423   1560 1560 5 s 1787	403 0 ));

/*
 *	btree varbit
 */

DATA(insert (	2002   1562 1562 1 s 1806 403 0 ));
DATA(insert (	2002   1562 1562 2 s 1808 403 0 ));
DATA(insert (	2002   1562 1562 3 s 1804 403 0 ));
DATA(insert (	2002   1562 1562 4 s 1809 403 0 ));
DATA(insert (	2002   1562 1562 5 s 1807 403 0 ));

/*
 *	btree text pattern
 */

DATA(insert (	2095   25 25 1 s 2314 403 0 ));
DATA(insert (	2095   25 25 2 s 2315 403 0 ));
DATA(insert (	2095   25 25 3 s 98   403 0 ));
DATA(insert (	2095   25 25 4 s 2317 403 0 ));
DATA(insert (	2095   25 25 5 s 2318 403 0 ));

/*
 *	btree bpchar pattern
 */

DATA(insert (	2097   1042 1042 1 s 2326 403 0 ));
DATA(insert (	2097   1042 1042 2 s 2327 403 0 ));
DATA(insert (	2097   1042 1042 3 s 1054 403 0 ));
DATA(insert (	2097   1042 1042 4 s 2329 403 0 ));
DATA(insert (	2097   1042 1042 5 s 2330 403 0 ));

/*
 *	btree money_ops
 */

DATA(insert (	2099   790 790 1 s	902 403 0 ));
DATA(insert (	2099   790 790 2 s	904 403 0 ));
DATA(insert (	2099   790 790 3 s	900 403 0 ));
DATA(insert (	2099   790 790 4 s	905 403 0 ));
DATA(insert (	2099   790 790 5 s	903 403 0 ));

/*
 *	btree reltime_ops
 */

DATA(insert (	2233   703 703 1 s	568 403 0 ));
DATA(insert (	2233   703 703 2 s	570 403 0 ));
DATA(insert (	2233   703 703 3 s	566 403 0 ));
DATA(insert (	2233   703 703 4 s	571 403 0 ));
DATA(insert (	2233   703 703 5 s	569 403 0 ));

/*
 *	btree tinterval_ops
 */

DATA(insert (	2234   704 704 1 s	813 403 0 ));
DATA(insert (	2234   704 704 2 s	815 403 0 ));
DATA(insert (	2234   704 704 3 s	811 403 0 ));
DATA(insert (	2234   704 704 4 s	816 403 0 ));
DATA(insert (	2234   704 704 5 s	814 403 0 ));

/*
 *	btree array_ops
 */

DATA(insert (	397   2277 2277 1 s 1072	403 0 ));
DATA(insert (	397   2277 2277 2 s 1074	403 0 ));
DATA(insert (	397   2277 2277 3 s 1070	403 0 ));
DATA(insert (	397   2277 2277 4 s 1075	403 0 ));
DATA(insert (	397   2277 2277 5 s 1073	403 0 ));

/*
 *	btree record_ops
 */

DATA(insert (	2994  2249 2249 1 s 2990	403 0 ));
DATA(insert (	2994  2249 2249 2 s 2992	403 0 ));
DATA(insert (	2994  2249 2249 3 s 2988	403 0 ));
DATA(insert (	2994  2249 2249 4 s 2993	403 0 ));
DATA(insert (	2994  2249 2249 5 s 2991	403 0 ));

/*
 *	btree record_image_ops
 */

DATA(insert (	3194  2249 2249 1 s 3190	403 0 ));
DATA(insert (	3194  2249 2249 2 s 3192	403 0 ));
DATA(insert (	3194  2249 2249 3 s 3188	403 0 ));
DATA(insert (	3194  2249 2249 4 s 3193	403 0 ));
DATA(insert (	3194  2249 2249 5 s 3191	403 0 ));

/*
 * btree uuid_ops
 */

DATA(insert (	2968  2950 2950 1 s 2974	403 0 ));
DATA(insert (	2968  2950 2950 2 s 2976	403 0 ));
DATA(insert (	2968  2950 2950 3 s 2972	403 0 ));
DATA(insert (	2968  2950 2950 4 s 2977	403 0 ));
DATA(insert (	2968  2950 2950 5 s 2975	403 0 ));

/*
 *	hash index _ops
 */

/* bpchar_ops */
DATA(insert (	427   1042 1042 1 s 1054	405 0 ));
/* char_ops */
DATA(insert (	431   18 18 1 s 92	405 0 ));
/* date_ops */
DATA(insert (	435   1082 1082 1 s 1093	405 0 ));
/* float_ops */
DATA(insert (	1971   700 700 1 s	620 405 0 ));
DATA(insert (	1971   701 701 1 s	670 405 0 ));
DATA(insert (	1971   700 701 1 s 1120 405 0 ));
DATA(insert (	1971   701 700 1 s 1130 405 0 ));
/* network_ops */
DATA(insert (	1975   869 869 1 s 1201 405 0 ));
/* integer_ops */
DATA(insert (	1977   21 21 1 s	94	 405 0 ));
DATA(insert (	1977   23 23 1 s	96	 405 0 ));
DATA(insert (	1977   20 20 1 s	410  405 0 ));
DATA(insert (	1977   21 23 1 s	532  405 0 ));
DATA(insert (	1977   21 20 1 s	1862 405 0 ));
DATA(insert (	1977   23 21 1 s	533  405 0 ));
DATA(insert (	1977   23 20 1 s	15	 405 0 ));
DATA(insert (	1977   20 21 1 s	1868 405 0 ));
DATA(insert (	1977   20 23 1 s	416  405 0 ));
/* interval_ops */
DATA(insert (	1983   1186 1186 1 s 1330 405 0 ));
/* macaddr_ops */
DATA(insert (	1985   829 829 1 s 1220 405 0 ));
/* name_ops */
DATA(insert (	1987   19 19 1 s	93	405 0 ));
/* oid_ops */
DATA(insert (	1990   26 26 1 s	607 405 0 ));
/* oidvector_ops */
DATA(insert (	1992   30 30 1 s	649 405 0 ));
/* text_ops */
DATA(insert (	1995   25 25 1 s	98	405 0 ));
/* time_ops */
DATA(insert (	1997   1083 1083 1 s 1108 405 0 ));
/* timestamptz_ops */
DATA(insert (	1999   1184 1184 1 s 1320 405 0 ));
/* timetz_ops */
DATA(insert (	2001   1266 1266 1 s 1550 405 0 ));
/* timestamp_ops */
DATA(insert (	2040   1114 1114 1 s 2060 405 0 ));
/* bool_ops */
DATA(insert (	2222   16 16 1 s   91 405 0 ));
/* bytea_ops */
DATA(insert (	2223   17 17 1 s 1955 405 0 ));
/* int2vector_ops */
DATA(insert (	2224   22 22 1 s	386 405 0 ));
/* xid_ops */
DATA(insert (	2225   28 28 1 s	352 405 0 ));
/* cid_ops */
DATA(insert (	2226   29 29 1 s	385 405 0 ));
/* abstime_ops */
DATA(insert (	2227   702 702 1 s	560 405 0 ));
/* reltime_ops */
DATA(insert (	2228   703 703 1 s	566 405 0 ));
/* text_pattern_ops */
DATA(insert (	2229   25 25 1 s	98	405 0 ));
/* bpchar_pattern_ops */
DATA(insert (	2231   1042 1042 1 s 1054 405 0 ));
/* aclitem_ops */
DATA(insert (	2235   1033 1033 1 s  974 405 0 ));
/* uuid_ops */
DATA(insert (	2969   2950 2950 1 s 2972 405 0 ));
/* numeric_ops */
DATA(insert (	1998   1700 1700 1 s 1752 405 0 ));
/* array_ops */
DATA(insert (	627    2277 2277 1 s 1070 405 0 ));


/*
 *	gist box_ops
 */

DATA(insert (	2593   603 603 1 s	493 783 0 ));
DATA(insert (	2593   603 603 2 s	494 783 0 ));
DATA(insert (	2593   603 603 3 s	500 783 0 ));
DATA(insert (	2593   603 603 4 s	495 783 0 ));
DATA(insert (	2593   603 603 5 s	496 783 0 ));
DATA(insert (	2593   603 603 6 s	499 783 0 ));
DATA(insert (	2593   603 603 7 s	498 783 0 ));
DATA(insert (	2593   603 603 8 s	497 783 0 ));
DATA(insert (	2593   603 603 9 s	2571 783 0 ));
DATA(insert (	2593   603 603 10 s 2570 783 0 ));
DATA(insert (	2593   603 603 11 s 2573 783 0 ));
DATA(insert (	2593   603 603 12 s 2572 783 0 ));
DATA(insert (	2593   603 603 13 s 2863 783 0 ));
DATA(insert (	2593   603 603 14 s 2862 783 0 ));

/*
 * gist point_ops
 */
DATA(insert (	1029   600 600 11 s 506 783 0 ));
DATA(insert (	1029   600 600 1 s	507 783 0 ));
DATA(insert (	1029   600 600 5 s	508 783 0 ));
DATA(insert (	1029   600 600 10 s 509 783 0 ));
DATA(insert (	1029   600 600 6 s	510 783 0 ));
DATA(insert (	1029   600 600 15 o 517 783 1970 ));
DATA(insert (	1029   600 603 28 s 511 783 0 ));
DATA(insert (	1029   600 604 48 s 756 783 0 ));
DATA(insert (	1029   600 718 68 s 758 783 0 ));


/*
 *	gist poly_ops (supports polygons)
 */

DATA(insert (	2594   604 604 1 s	485 783 0 ));
DATA(insert (	2594   604 604 2 s	486 783 0 ));
DATA(insert (	2594   604 604 3 s	492 783 0 ));
DATA(insert (	2594   604 604 4 s	487 783 0 ));
DATA(insert (	2594   604 604 5 s	488 783 0 ));
DATA(insert (	2594   604 604 6 s	491 783 0 ));
DATA(insert (	2594   604 604 7 s	490 783 0 ));
DATA(insert (	2594   604 604 8 s	489 783 0 ));
DATA(insert (	2594   604 604 9 s	2575 783 0 ));
DATA(insert (	2594   604 604 10 s 2574 783 0 ));
DATA(insert (	2594   604 604 11 s 2577 783 0 ));
DATA(insert (	2594   604 604 12 s 2576 783 0 ));
DATA(insert (	2594   604 604 13 s 2861 783 0 ));
DATA(insert (	2594   604 604 14 s 2860 783 0 ));

/*
 *	gist circle_ops
 */

DATA(insert (	2595   718 718 1 s	1506 783 0 ));
DATA(insert (	2595   718 718 2 s	1507 783 0 ));
DATA(insert (	2595   718 718 3 s	1513 783 0 ));
DATA(insert (	2595   718 718 4 s	1508 783 0 ));
DATA(insert (	2595   718 718 5 s	1509 783 0 ));
DATA(insert (	2595   718 718 6 s	1512 783 0 ));
DATA(insert (	2595   718 718 7 s	1511 783 0 ));
DATA(insert (	2595   718 718 8 s	1510 783 0 ));
DATA(insert (	2595   718 718 9 s	2589 783 0 ));
DATA(insert (	2595   718 718 10 s 1515 783 0 ));
DATA(insert (	2595   718 718 11 s 1514 783 0 ));
DATA(insert (	2595   718 718 12 s 2590 783 0 ));
DATA(insert (	2595   718 718 13 s 2865 783 0 ));
DATA(insert (	2595   718 718 14 s 2864 783 0 ));

/*
 * gin array_ops (these anyarray operators are used with all the opclasses
 * of the family)
 */
DATA(insert (	2745   2277 2277 1 s 2750 2742 0 ));
DATA(insert (	2745   2277 2277 2 s 2751 2742 0 ));
DATA(insert (	2745   2277 2277 3 s 2752 2742 0 ));
DATA(insert (	2745   2277 2277 4 s 1070 2742 0 ));

/*
 * btree enum_ops
 */
DATA(insert (	3522   3500 3500 1 s 3518 403 0 ));
DATA(insert (	3522   3500 3500 2 s 3520 403 0 ));
DATA(insert (	3522   3500 3500 3 s 3516 403 0 ));
DATA(insert (	3522   3500 3500 4 s 3521 403 0 ));
DATA(insert (	3522   3500 3500 5 s 3519 403 0 ));

/*
 * hash enum_ops
 */
DATA(insert (	3523   3500 3500 1 s 3516 405 0 ));

/*
 * btree tsvector_ops
 */
DATA(insert (	3626   3614 3614 1 s	3627 403 0 ));
DATA(insert (	3626   3614 3614 2 s	3628 403 0 ));
DATA(insert (	3626   3614 3614 3 s	3629 403 0 ));
DATA(insert (	3626   3614 3614 4 s	3631 403 0 ));
DATA(insert (	3626   3614 3614 5 s	3632 403 0 ));

/*
 * GiST tsvector_ops
 */
DATA(insert (	3655   3614 3615 1 s	3636 783 0 ));

/*
 * GIN tsvector_ops
 */
DATA(insert (	3659   3614 3615 1 s	3636 2742 0 ));
DATA(insert (	3659   3614 3615 2 s	3660 2742 0 ));

/*
 * btree tsquery_ops
 */
DATA(insert (	3683   3615 3615 1 s	3674 403 0 ));
DATA(insert (	3683   3615 3615 2 s	3675 403 0 ));
DATA(insert (	3683   3615 3615 3 s	3676 403 0 ));
DATA(insert (	3683   3615 3615 4 s	3678 403 0 ));
DATA(insert (	3683   3615 3615 5 s	3679 403 0 ));

/*
 * GiST tsquery_ops
 */
DATA(insert (	3702   3615 3615 7 s	3693 783 0 ));
DATA(insert (	3702   3615 3615 8 s	3694 783 0 ));

/*
 * btree range_ops
 */
DATA(insert (	3901   3831 3831 1 s	3884 403 0 ));
DATA(insert (	3901   3831 3831 2 s	3885 403 0 ));
DATA(insert (	3901   3831 3831 3 s	3882 403 0 ));
DATA(insert (	3901   3831 3831 4 s	3886 403 0 ));
DATA(insert (	3901   3831 3831 5 s	3887 403 0 ));

/*
 * hash range_ops
 */
DATA(insert (	3903   3831 3831 1 s	3882 405 0 ));

/*
 * GiST range_ops
 */
DATA(insert (	3919   3831 3831 1 s	3893 783 0 ));
DATA(insert (	3919   3831 3831 2 s	3895 783 0 ));
DATA(insert (	3919   3831 3831 3 s	3888 783 0 ));
DATA(insert (	3919   3831 3831 4 s	3896 783 0 ));
DATA(insert (	3919   3831 3831 5 s	3894 783 0 ));
DATA(insert (	3919   3831 3831 6 s	3897 783 0 ));
DATA(insert (	3919   3831 3831 7 s	3890 783 0 ));
DATA(insert (	3919   3831 3831 8 s	3892 783 0 ));
DATA(insert (	3919   3831 2283 16 s	3889 783 0 ));
DATA(insert (	3919   3831 3831 18 s	3882 783 0 ));

/*
 * SP-GiST quad_point_ops
 */
DATA(insert (	4015   600 600 11 s 506 4000 0 ));
DATA(insert (	4015   600 600 1 s	507 4000 0 ));
DATA(insert (	4015   600 600 5 s	508 4000 0 ));
DATA(insert (	4015   600 600 10 s 509 4000 0 ));
DATA(insert (	4015   600 600 6 s	510 4000 0 ));
DATA(insert (	4015   600 603 8 s	511 4000 0 ));

/*
 * SP-GiST kd_point_ops
 */
DATA(insert (	4016   600 600 11 s 506 4000 0 ));
DATA(insert (	4016   600 600 1 s	507 4000 0 ));
DATA(insert (	4016   600 600 5 s	508 4000 0 ));
DATA(insert (	4016   600 600 10 s 509 4000 0 ));
DATA(insert (	4016   600 600 6 s	510 4000 0 ));
DATA(insert (	4016   600 603 8 s	511 4000 0 ));

/*
 * SP-GiST text_ops
 */
DATA(insert (	4017   25 25 1 s	2314 4000 0 ));
DATA(insert (	4017   25 25 2 s	2315 4000 0 ));
DATA(insert (	4017   25 25 3 s	98	4000 0 ));
DATA(insert (	4017   25 25 4 s	2317 4000 0 ));
DATA(insert (	4017   25 25 5 s	2318 4000 0 ));
DATA(insert (	4017   25 25 11 s	664 4000 0 ));
DATA(insert (	4017   25 25 12 s	665 4000 0 ));
DATA(insert (	4017   25 25 14 s	667 4000 0 ));
DATA(insert (	4017   25 25 15 s	666 4000 0 ));

/*
 * btree jsonb_ops
 */
DATA(insert (	4033   3802 3802 1 s	3242 403 0 ));
DATA(insert (	4033   3802 3802 2 s	3244 403 0 ));
DATA(insert (	4033   3802 3802 3 s	3240 403 0 ));
DATA(insert (	4033   3802 3802 4 s	3245 403 0 ));
DATA(insert (	4033   3802 3802 5 s	3243 403 0 ));

/*
 * hash jsonb ops
 */
DATA(insert (	4034   3802 3802 1 s 3240 405 0 ));

/*
 * GIN jsonb ops
 */
DATA(insert (	4036   3802 3802 7 s 3246 2742 0 ));
DATA(insert (	4036   3802 25 9 s 3247 2742 0 ));
DATA(insert (	4036   3802 1009 10 s 3248 2742 0 ));
DATA(insert (	4036   3802 1009 11 s 3249 2742 0 ));

/*
 * GIN jsonb hash ops
 */
DATA(insert (	4037   3802 3802 7 s 3246 2742 0 ));

/*
 * SP-GiST range_ops
 */
DATA(insert (	3474   3831 3831 1 s	3893 4000 0 ));
DATA(insert (	3474   3831 3831 2 s	3895 4000 0 ));
DATA(insert (	3474   3831 3831 3 s	3888 4000 0 ));
DATA(insert (	3474   3831 3831 4 s	3896 4000 0 ));
DATA(insert (	3474   3831 3831 5 s	3894 4000 0 ));
DATA(insert (	3474   3831 3831 6 s	3897 4000 0 ));
DATA(insert (	3474   3831 3831 7 s	3890 4000 0 ));
DATA(insert (	3474   3831 3831 8 s	3892 4000 0 ));
DATA(insert (	3474   3831 2283 16 s	3889 4000 0 ));
DATA(insert (	3474   3831 3831 18 s	3882 4000 0 ));

/*
 * on-disk bitmap index operators
 */

/*
 *	bitmap integer_ops
 */

/* default operators int2 */
DATA(insert (	3026   21 21 1	95	3013 ));
DATA(insert (	3026   21 21 2	522	3013 ));
DATA(insert (	3026   21 21 3	94	3013 ));
DATA(insert (	3026   21 21 4	524	3013 ));
DATA(insert (	3026   21 21 5	520	3013 ));
/* crosstype operators int24 */
DATA(insert (	3026   21 23 1	534	3013 ));
DATA(insert (	3026   21 23 2	540	3013 ));
DATA(insert (	3026   21 23 3	532	3013 ));
DATA(insert (	3026   21 23 4	542	3013 ));
DATA(insert (	3026   21 23 5	536	3013 ));
/* crosstype operators int28 */
DATA(insert (	3026   21 20 1	1864	3013 ));
DATA(insert (	3026   21 20 2	1866	3013 ));
DATA(insert (	3026   21 20 3	1862	3013 ));
DATA(insert (	3026   21 20 4	1867	3013 ));
DATA(insert (	3026   21 20 5	1865	3013 ));
/* default operators int4 */
DATA(insert (	3026   23 23 1	97	3013 ));
DATA(insert (	3026   23 23 2	523	3013 ));
DATA(insert (	3026   23 23 3	96	3013 ));
DATA(insert (	3026   23 23 4	525	3013 ));
DATA(insert (	3026   23 23 5	521	3013 ));
/* crosstype operators int42 */
DATA(insert (	3026   23 21 1	535	3013 ));
DATA(insert (	3026   23 21 2	541	3013 ));
DATA(insert (	3026   23 21 3	533	3013 ));
DATA(insert (	3026   23 21 4	543	3013 ));
DATA(insert (	3026   23 21 5	537	3013 ));
/* crosstype operators int48 */
DATA(insert (	3026   23 20 1	37	3013 ));
DATA(insert (	3026   23 20 2	80	3013 ));
DATA(insert (	3026   23 20 3	15	3013 ));
DATA(insert (	3026   23 20 4	82	3013 ));
DATA(insert (	3026   23 20 5	76	3013 ));
/* default operators int8 */
DATA(insert (	3026   20 20 1	412	3013 ));
DATA(insert (	3026   20 20 2	414	3013 ));
DATA(insert (	3026   20 20 3	410	3013 ));
DATA(insert (	3026   20 20 4	415	3013 ));
DATA(insert (	3026   20 20 5	413	3013 ));
/* crosstype operators int82 */
DATA(insert (	3026   20 21 1	1870	3013 ));
DATA(insert (	3026   20 21 2	1872	3013 ));
DATA(insert (	3026   20 21 3	1868	3013 ));
DATA(insert (	3026   20 21 4	1873	3013 ));
DATA(insert (	3026   20 21 5	1871	3013 ));
/* crosstype operators int84 */
DATA(insert (	3026   20 23 1	418	3013 ));
DATA(insert (	3026   20 23 2	420	3013 ));
DATA(insert (	3026   20 23 3	416	3013 ));
DATA(insert (	3026   20 23 4	430	3013 ));
DATA(insert (	3026   20 23 5	419	3013 ));

/*
 *	bitmap oid_ops
 */

DATA(insert (	3033   26 26 1	609	3013 ));
DATA(insert (	3033   26 26 2	611	3013 ));
DATA(insert (	3033   26 26 3	607	3013 ));
DATA(insert (	3033   26 26 4	612	3013 ));
DATA(insert (	3033   26 26 5	610	3013 ));

/*
 *	bitmap oidvector_ops
 */

DATA(insert (	3034   30 30 1	645	3013 ));
DATA(insert (	3034   30 30 2	647	3013 ));
DATA(insert (	3034   30 30 3	649	3013 ));
DATA(insert (	3034   30 30 4	648	3013 ));
DATA(insert (	3034   30 30 5	646	3013 ));

/*
 *	bitmap float_ops
 */

/* default operators float4 */
DATA(insert (	3023   700 700 1	622 3013 ));
DATA(insert (	3023   700 700 2	624 3013 ));
DATA(insert (	3023   700 700 3	620 3013 ));
DATA(insert (	3023   700 700 4	625 3013 ));
DATA(insert (	3023   700 700 5	623 3013 ));
/* crosstype operators float48 */
DATA(insert (	3023   700 701 1	1122 3013 ));
DATA(insert (	3023   700 701 2	1124 3013 ));
DATA(insert (	3023   700 701 3	1120 3013 ));
DATA(insert (	3023   700 701 4	1125 3013 ));
DATA(insert (	3023   700 701 5	1123 3013 ));
/* default operators float8 */
DATA(insert (	3023   701 701 1	672 3013 ));
DATA(insert (	3023   701 701 2	673 3013 ));
DATA(insert (	3023   701 701 3	670 3013 ));
DATA(insert (	3023   701 701 4	675 3013 ));
DATA(insert (	3023   701 701 5	674 3013 ));
/* crosstype operators float84 */
DATA(insert (	3023   701 700 1	1132 3013 ));
DATA(insert (	3023   701 700 2	1134 3013 ));
DATA(insert (	3023   701 700 3	1130 3013 ));
DATA(insert (	3023   701 700 4	1135 3013 ));
DATA(insert (	3023   701 700 5	1133 3013 ));

/*
 *	bitmap char_ops
 */

DATA(insert (	3020   18 18 1	631	3013 ));
DATA(insert (	3020   18 18 2	632	3013 ));
DATA(insert (	3020   18 18 3	92	3013 ));
DATA(insert (	3020   18 18 4	634	3013 ));
DATA(insert (	3020   18 18 5	633	3013 ));

/*
 *	bitmap name_ops
 */

DATA(insert (	3031   19 19 1	660	3013 ));
DATA(insert (	3031   19 19 2	661	3013 ));
DATA(insert (	3031   19 19 3	93	3013 ));
DATA(insert (	3031   19 19 4	663	3013 ));
DATA(insert (	3031   19 19 5	662	3013 ));

/*
 *	bitmap text_ops
 */

DATA(insert (	3035   25 25 1	664	3013 ));
DATA(insert (	3035   25 25 2	665	3013 ));
DATA(insert (	3035   25 25 3	98	3013 ));
DATA(insert (	3035   25 25 4	667	3013 ));
DATA(insert (	3035   25 25 5	666	3013 ));

/*
 *	bitmap bpchar_ops
 */

DATA(insert (	3018   1042 1042 1	1058	3013 ));
DATA(insert (	3018   1042 1042 2	1059	3013 ));
DATA(insert (	3018   1042 1042 3	1054	3013 ));
DATA(insert (	3018   1042 1042 4	1061	3013 ));
DATA(insert (	3018   1042 1042 5	1060	3013 ));

/*
 *	bitmap bytea_ops
 */

DATA(insert (	3019   17 17 1	1957	3013 ));
DATA(insert (	3019   17 17 2	1958	3013 ));
DATA(insert (	3019   17 17 3	1955	3013 ));
DATA(insert (	3019   17 17 4	1960	3013 ));
DATA(insert (	3019   17 17 5	1959	3013 ));

/*
 *	bitmap abstime_ops
 */

DATA(insert (	3014   702 702 1	562	3013 ));
DATA(insert (	3014   702 702 2	564	3013 ));
DATA(insert (	3014   702 702 3	560	3013 ));
DATA(insert (	3014   702 702 4	565	3013 ));
DATA(insert (	3014   702 702 5	563	3013 ));

/*
 *	bitmap datetime_ops
 */

/* default operators date */
DATA(insert (	3037   1082 1082 1	1095	3013 ));
DATA(insert (	3037   1082 1082 2	1096	3013 ));
DATA(insert (	3037   1082 1082 3	1093	3013 ));
DATA(insert (	3037   1082 1082 4	1098	3013 ));
DATA(insert (	3037   1082 1082 5	1097	3013 ));
/* crosstype operators vs timestamp */
DATA(insert (	3037   1082 1114 1	2345	3013 ));
DATA(insert (	3037   1082 1114 2	2346	3013 ));
DATA(insert (	3037   1082 1114 3	2347	3013 ));
DATA(insert (	3037   1082 1114 4	2348	3013 ));
DATA(insert (	3037   1082 1114 5	2349	3013 ));
/* crosstype operators vs timestamptz */
DATA(insert (	3037   1082 1184 1	2358	3013 ));
DATA(insert (	3037   1082 1184 2	2359	3013 ));
DATA(insert (	3037   1082 1184 3	2360	3013 ));
DATA(insert (	3037   1082 1184 4	2361	3013 ));
DATA(insert (	3037   1082 1184 5	2362	3013 ));
/* default operators timestamp */
DATA(insert (	3037   1114 1114 1	2062	3013 ));
DATA(insert (	3037   1114 1114 2	2063	3013 ));
DATA(insert (	3037   1114 1114 3	2060	3013 ));
DATA(insert (	3037   1114 1114 4	2065	3013 ));
DATA(insert (	3037   1114 1114 5	2064	3013 ));
/* crosstype operators vs date */
DATA(insert (	3037   1114 1082 1	2371	3013 ));
DATA(insert (	3037   1114 1082 2	2372	3013 ));
DATA(insert (	3037   1114 1082 3	2373	3013 ));
DATA(insert (	3037   1114 1082 4	2374	3013 ));
DATA(insert (	3037   1114 1082 5	2375	3013 ));
/* crosstype operators vs timestamptz */
DATA(insert (	3037   1114 1184 1	2534	3013 ));
DATA(insert (	3037   1114 1184 2	2535	3013 ));
DATA(insert (	3037   1114 1184 3	2536	3013 ));
DATA(insert (	3037   1114 1184 4	2537	3013 ));
DATA(insert (	3037   1114 1184 5	2538	3013 ));
/* default operators timestamptz */
DATA(insert (	3037   1184 1184 1	1322	3013 ));
DATA(insert (	3037   1184 1184 2	1323	3013 ));
DATA(insert (	3037   1184 1184 3	1320	3013 ));
DATA(insert (	3037   1184 1184 4	1325	3013 ));
DATA(insert (	3037   1184 1184 5	1324	3013 ));
/* crosstype operators vs date */
DATA(insert (	3037   1184 1082 1	2384	3013 ));
DATA(insert (	3037   1184 1082 2	2385	3013 ));
DATA(insert (	3037   1184 1082 3	2386	3013 ));
DATA(insert (	3037   1184 1082 4	2387	3013 ));
DATA(insert (	3037   1184 1082 5	2388	3013 ));
/* crosstype operators vs timestamp */
DATA(insert (	3037   1184 1114 1	2540	3013 ));
DATA(insert (	3037   1184 1114 2	2541	3013 ));
DATA(insert (	3037   1184 1114 3	2542	3013 ));
DATA(insert (	3037   1184 1114 4	2543	3013 ));
DATA(insert (	3037   1184 1114 5	2544	3013 ));

/*
 *	bitmap time_ops
 */

DATA(insert (	3036   1083 1083 1	1110	3013 ));
DATA(insert (	3036   1083 1083 2	1111	3013 ));
DATA(insert (	3036   1083 1083 3	1108	3013 ));
DATA(insert (	3036   1083 1083 4	1113	3013 ));
DATA(insert (	3036   1083 1083 5	1112	3013 ));

/*
 *	bitmap timetz_ops
 */

DATA(insert (	3038   1266 1266 1	1552	3013 ));
DATA(insert (	3038   1266 1266 2	1553	3013 ));
DATA(insert (	3038   1266 1266 3	1550	3013 ));
DATA(insert (	3038   1266 1266 4	1555	3013 ));
DATA(insert (	3038   1266 1266 5	1554	3013 ));

/*
 *	bitmap interval_ops
 */

DATA(insert (	3029   1186 1186 1	1332	3013 ));
DATA(insert (	3029   1186 1186 2	1333	3013 ));
DATA(insert (	3029   1186 1186 3	1330	3013 ));
DATA(insert (	3029   1186 1186 4	1335	3013 ));
DATA(insert (	3029   1186 1186 5	1334	3013 ));

/*
 *	bitmap macaddr
 */

DATA(insert (	3030   829 829 1	1222	3013 ));
DATA(insert (	3030   829 829 2	1223	3013 ));
DATA(insert (	3030   829 829 3	1220	3013 ));
DATA(insert (	3030   829 829 4	1225	3013 ));
DATA(insert (	3030   829 829 5	1224	3013 ));

/*
 *	bitmap network
 */

DATA(insert (	3021   869 869 1	1203	3013 ));
DATA(insert (	3021   869 869 2	1204	3013 ));
DATA(insert (	3021   869 869 3	1201	3013 ));
DATA(insert (	3021   869 869 4	1206	3013 ));
DATA(insert (	3021   869 869 5	1205	3013 ));

/*
 *	bitmap numeric
 */

DATA(insert (	3032   1700 1700 1	1754	3013 ));
DATA(insert (	3032   1700 1700 2	1755	3013 ));
DATA(insert (	3032   1700 1700 3	1752	3013 ));
DATA(insert (	3032   1700 1700 4	1757	3013 ));
DATA(insert (	3032   1700 1700 5	1756	3013 ));

/*
 *	bitmap bool
 */

DATA(insert (	3017   16 16 1	58	3013 ));
DATA(insert (	3017   16 16 2	1694	3013 ));
DATA(insert (	3017   16 16 3	91	3013 ));
DATA(insert (	3017   16 16 4	1695	3013 ));
DATA(insert (	3017   16 16 5	59	3013 ));

/*
 *	bitmap bit
 */

DATA(insert (	3016   1560 1560 1	1786	3013 ));
DATA(insert (	3016   1560 1560 2	1788	3013 ));
DATA(insert (	3016   1560 1560 3	1784	3013 ));
DATA(insert (	3016   1560 1560 4	1789	3013 ));
DATA(insert (	3016   1560 1560 5	1787	3013 ));

/*
 *	bitmap varbit
 */

DATA(insert (	3039   1562 1562 1	1806	3013 ));
DATA(insert (	3039   1562 1562 2	1808	3013 ));
DATA(insert (	3039   1562 1562 3	1804	3013 ));
DATA(insert (	3039   1562 1562 4	1809	3013 ));
DATA(insert (	3039   1562 1562 5	1807	3013 ));

/*
 *	bitmap text pattern
 */

DATA(insert (	3042   25 25 1	2314	3013 ));
DATA(insert (	3042   25 25 2	2315	3013 ));
DATA(insert (	3042   25 25 3	2317	3013 ));
DATA(insert (	3042   25 25 4	2318	3013 ));

/*
 *	bitmap bpchar pattern
 */

DATA(insert (	3044   1042 1042 1	2326	3013 ));
DATA(insert (	3044   1042 1042 2	2327	3013 ));
DATA(insert (	3044   1042 1042 3	2329	3013 ));
DATA(insert (	3044   1042 1042 4	2330	3013 ));

/*
 *	bitmap money_ops
 */

DATA(insert (	3046   790 790 1	902	3013 ));
DATA(insert (	3046   790 790 2	904	3013 ));
DATA(insert (	3046   790 790 3	900	3013 ));
DATA(insert (	3046   790 790 4	905	3013 ));
DATA(insert (	3046   790 790 5	903	3013 ));

/*
 *	bitmap reltime_ops
 */

DATA(insert (	3047   703 703 1	568	3013 ));
DATA(insert (	3047   703 703 2	570	3013 ));
DATA(insert (	3047   703 703 3	566	3013 ));
DATA(insert (	3047   703 703 4	571	3013 ));
DATA(insert (	3047   703 703 5	569	3013 ));

/*
 *	bitmap tinterval_ops
 */

DATA(insert (	3048   704 704 1	813	3013 ));
DATA(insert (	3048   704 704 2	815	3013 ));
DATA(insert (	3048   704 704 3	811	3013 ));
DATA(insert (	3048   704 704 4	816	3013 ));
DATA(insert (	3048   704 704 5	814	3013 ));

/*
 *	bitmap array_ops
 */

DATA(insert (	3015   2277 2277 1	1072	3013 ));
DATA(insert (	3015   2277 2277 2	1074	3013 ));
DATA(insert (	3015   2277 2277 3	1070	3013 ));
DATA(insert (	3015   2277 2277 4	1075	3013 ));
DATA(insert (	3015   2277 2277 5	1073	3013 ));

/* 
 * bitmap uuid_ops 
 */
 
DATA(insert (	3024  2950 2950 1	2974	3013 ));
DATA(insert (	3024  2950 2950 2	2976	3013 ));
DATA(insert (	3024  2950 2950 3	2972	3013 ));
DATA(insert (	3024  2950 2950 4	2977	3013 ));
DATA(insert (	3024  2950 2950 5	2975	3013 ));

#endif   /* PG_AMOP_H */
