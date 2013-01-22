-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_logical_decoding" to load this file. \quit

CREATE FUNCTION init_logical_replication (plugin text, OUT slot_name text, OUT xlog_position text)
AS 'MODULE_PATHNAME', 'init_logical_replication'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION start_logical_replication (pos text, OUT location text, OUT xid bigint, OUT data text) RETURNS SETOF record
AS 'MODULE_PATHNAME', 'start_logical_replication'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION stop_logical_replication () RETURNS int
AS 'MODULE_PATHNAME', 'stop_logical_replication'
LANGUAGE C IMMUTABLE STRICT;
