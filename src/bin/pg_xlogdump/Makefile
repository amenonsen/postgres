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

clogdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

dbasedesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

gindesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

gistdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

hashdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

heapdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

mxactdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

nbtdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

relmapdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

seqdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

smgrdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

spgdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

standbydesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

tblspcdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

xactdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

xlogdesc.c: % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

assert.c: % : $(top_srcdir)/src/backend/utils/error/%
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
	rm -f $(OBJS) pg_xlogdump
