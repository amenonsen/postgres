#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_xlogdump
#
# Copyright (c) 1998-2012, PostgreSQL Global Development Group
#
# src/bin/pg_xlogdump/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_xlogdump"
PGAPPICON=win32

subdir = src/bin/pg_xlogdump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) $(CPPFLAGS)

all: pg_xlogdump

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

assert.c: % : $(top_srcdir)/src/backend/utils/error/%
	rm -f $@ && $(LN_S) $< .

rmgrdescfiles = clogdesc.c dbasedesc.c gindesc.c gistdesc.c hashdesc.c \
	heapdesc.c mxactdesc.c nbtdesc.c relmapdesc.c seqdesc.c smgrdesc.c \
	spgdesc.c standbydesc.c tblspcdesc.c xactdesc.c xlogdesc.c

$(rmgrdescfiles): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

OBJS = \
	clogdesc.o dbasedesc.o gindesc.o gistdesc.o hashdesc.o heapdesc.o \
	mxactdesc.o nbtdesc.o relmapdesc.o seqdesc.o smgrdesc.o spgdesc.o \
	standbydesc.o tblspcdesc.o xactdesc.o xlogdesc.o assert.o \
	$(WIN32RES) \
	pg_xlogdump.o pqexpbuf_strinfo.o compat.o tables.o xlogreader.o \

pg_xlogdump: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX)  $(LIBS) $(libpq_pgport) -o $@$(X)

clean distclean maintainer-clean:
	rm -f $(OBJS) pg_xlogdump xlogreader.c assert.c $(rmgrdescfiles)
