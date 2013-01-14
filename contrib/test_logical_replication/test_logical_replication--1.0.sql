-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_logical_replication" to load this file. \quit

CREATE FUNCTION init_logical_replication (text, OUT slot_name text, OUT xlog_position text)
AS 'MODULE_PATHNAME', 'init_logical_replication'
LANGUAGE C STRICT;

CREATE FUNCTION start_logical_replication (text) RETURNS SETOF TEXT
AS 'MODULE_PATHNAME', 'start_logical_replication'
LANGUAGE C STRICT;

CREATE FUNCTION stop_logical_replication () RETURNS int
AS 'MODULE_PATHNAME', 'stop_logical_replication'
LANGUAGE C STRICT;
