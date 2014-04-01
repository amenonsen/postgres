# contrib/fastbloat/Makefile

MODULE_big	= fastbloat
OBJS		= fastbloat.o

EXTENSION = fastbloat
DATA = fastbloat--1.0.sql

REGRESS = fastbloat

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/fastbloat
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
