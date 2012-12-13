# contrib/pg_xlogdump/Makefile

PGFILEDESC = "pg_xlogdump"
PGAPPICON=win32

PROGRAM = pg_xlogdump
OBJS = clogdesc.o dbasedesc.o gindesc.o gistdesc.o hashdesc.o heapdesc.o \
	mxactdesc.o nbtdesc.o relmapdesc.o seqdesc.o smgrdesc.o spgdesc.o \
	standbydesc.o tblspcdesc.o xactdesc.o xlogdesc.o assert.o \
	$(WIN32RES) \
	pg_xlogdump.o stdout_strinfo.o compat.o tables.o xlogreader.o
EXTRA_CLEAN = $(rmgrdescfiles) xlogreader.c assert.c

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_xlogdump
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

assert.c: % : $(top_srcdir)/src/backend/utils/error/%
	rm -f $@ && $(LN_S) $< .

rmgrdescfiles = clogdesc.c dbasedesc.c gindesc.c gistdesc.c hashdesc.c \
	heapdesc.c mxactdesc.c nbtdesc.c relmapdesc.c seqdesc.c smgrdesc.c \
	spgdesc.c standbydesc.c tblspcdesc.c xactdesc.c xlogdesc.c

$(rmgrdescfiles): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .
