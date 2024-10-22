# Makefile for gres/npu plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = gres_npu.la

# Gres GPU plugin.
gres_npu_la_SOURCES = gres_npu.c
gres_npu_la_LDFLAGS = $(PLUGIN_FLAGS)
gres_npu_la_LIBADD = ../common/libgres_common.la

force:
$(gres_npu_la_LIBADD) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
