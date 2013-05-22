EXTENSION    = progress
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
DATA         = $(wildcard sql/*.sql)
MODULE_big   = progress
OBJS         = src/progress.o src/progress_util.o src/progress_pipeline.o
PG_CONFIG    = pg_config


src/progress.o: src/progress.h
src/progress_util.o: src/progress_util.h
src/progress_pipeline.o: src/progress_pipeline.h

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
