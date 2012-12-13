# contrib/pg_xlogdump/Makefile

PGFILEDESC = "pg_xlogdump"
PGAPPICON=win32

PROGRAM = pg_xlogdump
OBJS =  pg_xlogdump.o stdout_strinfo.o compat.o tables.o \
	xlogreader.o \
	$(WIN32RES) $(RMGRDESCOBJS)

# XXX: Perhaps this should be done by a wildcard rule so that you don't need
# to remember to add new rmgrdesc files to this list.
RMGRDESCSOURCES = clogdesc.c dbasedesc.c gindesc.c gistdesc.c hashdesc.c \
	heapdesc.c mxactdesc.c nbtdesc.c relmapdesc.c seqdesc.c smgrdesc.c \
	spgdesc.c standbydesc.c tblspcdesc.c xactdesc.c xlogdesc.c

RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))

EXTRA_CLEAN = $(RMGRDESCSOURCES) xlogreader.c

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

$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .
