# Makefile for gres/mlu plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = gres_mlu.la

# Gres MLU plugin.
gres_mlu_la_SOURCES = gres_mlu.c
gres_mlu_la_LDFLAGS = $(PLUGIN_FLAGS)
gres_mlu_la_LIBADD = ../common/libgres_common.la

force:
$(gres_mlu_la_LIBADD) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
