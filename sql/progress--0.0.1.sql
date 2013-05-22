-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION progress" to load this file. \quit

CREATE OR REPLACE FUNCTION pg_progress_update(int)
RETURNS bool
AS '$libdir/progress', 'pg_progress_update'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_progress()
RETURNS double precision
AS '$libdir/progress', 'pg_progress'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_progress_dot()
RETURNS text
AS '$libdir/progress', 'pg_progress_dot'
LANGUAGE C STRICT;
