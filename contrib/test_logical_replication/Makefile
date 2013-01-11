MODULE_big = test_logical_replication
OBJS = test_logical_replication.o

EXTENSION = test_logical_replication
DATA = test_logical_replication--1.0.sql

REGRESS = ddl

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/test_logical_replication
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

test_logical_replication.o: test_logical_replication.c
