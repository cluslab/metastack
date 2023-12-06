# Makefile for gres/dcu plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = gres_dcu.la

# Gres DCU plugin.
gres_dcu_la_SOURCES = gres_dcu.c
gres_dcu_la_LDFLAGS = $(PLUGIN_FLAGS)
gres_dcu_la_LIBADD = ../common/libgres_common.la

force:
$(gres_dcu_la_LIBADD) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
