/* contrib/fastbloat/fastbloat--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION fastbloat" to load this file. \quit

CREATE FUNCTION fastbloat(IN relname text,
    OUT table_len BIGINT,		-- physical table length in bytes
    OUT approx_tuple_count BIGINT,		-- estimated number of live tuples
    OUT approx_tuple_len BIGINT,		-- estimated total length in bytes of live tuples
    OUT tuple_percent FLOAT8,		-- live tuples in % (based on estimate)
    OUT dead_tuple_count BIGINT,	-- exact number of dead tuples
    OUT dead_tuple_len BIGINT,		-- exact total length in bytes of dead tuples
    OUT dead_tuple_percent FLOAT8,	-- dead tuples in % (based on estimate)
    OUT free_space BIGINT,		-- exact free space in bytes
    OUT free_percent FLOAT8)		-- free space in %
AS 'MODULE_PATHNAME', 'fastbloat'
LANGUAGE C STRICT;

CREATE FUNCTION fastbloat(IN reloid regclass,
    OUT table_len BIGINT,		-- physical table length in bytes
    OUT approx_tuple_count BIGINT,		-- estimated number of live tuples
    OUT approx_tuple_len BIGINT,		-- estimated total length in bytes of live tuples
    OUT tuple_percent FLOAT8,		-- live tuples in % (based on estimate)
    OUT dead_tuple_count BIGINT,	-- exact number of dead tuples
    OUT dead_tuple_len BIGINT,		-- exact total length in bytes of dead tuples
    OUT dead_tuple_percent FLOAT8,	-- dead tuples in % (based on estimate)
    OUT free_space BIGINT,		-- exact free space in bytes
    OUT free_percent FLOAT8)		-- free space in %
AS 'MODULE_PATHNAME', 'fastbloatbyid'
LANGUAGE C STRICT;
