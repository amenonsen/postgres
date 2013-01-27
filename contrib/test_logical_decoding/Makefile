MODULE_big = test_logical_decoding
OBJS = test_logical_decoding.o

EXTENSION = test_logical_decoding
DATA = test_logical_decoding--1.0.sql

REGRESS = ddl

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/test_logical_decoding
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

test_logical_decoding.o: test_logical_decoding.c

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).

installcheck:;

check: all
	$(pg_regress_check) --temp-config logical.conf --temp-install=./tmp_check \
	    --extra-install=contrib/test_decoding \
	    --extra-install=contrib/test_logical_decoding \
	    ddl
