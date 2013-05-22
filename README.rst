Query progress estimation for PostgreSQL
========================================

A PostgreSQL extension to estimate query progress. Based on ideas from a
research paper "Estimating progress of SQL queries" by Surajit Chaudhuri and
Vivek Narasayya from Microsoft Research and Ravi Ramamurthy from the University
of Wisconsin.

Needs a slightly modified version of Postgres from
https://github.com/wulczer/postgres/tree/progress-rebasing

Usage
-----

To use the extension, compile it, install, adjust the Postgres configuration
file and use the driver script to launch a monitored query::

  cd pg-progress
  sudo make install
  psql -c 'create extension progress'
  $EDITOR $DATADIR/postgresql.conf
  # add progress.so to shared_preload_libraries
  sudo service postgresql restart
  ./show-progress.py -c 'select * from tab' 'dbname=mydb user=admin'

Presentation
------------

The `pgcon2013` catalog contains a presentation about this extension. To build
it, enter that catalog and use `make`.

License
-------

Distributed unde the PostgreSQL license.
