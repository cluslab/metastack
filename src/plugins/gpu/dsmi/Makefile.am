# Makefile for gpu/dsmi plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(top_srcdir) -I$(top_srcdir)/src/common $(DSMI_CPPFLAGS) $(DSMI_LIBS)

DSMI_SOURCES = gpu_dsmi.c

pkglib_LTLIBRARIES = gpu_dsmi.la

# GPU DSMI plugin.
gpu_dsmi_la_SOURCES = $(DSMI_SOURCES)
gpu_dsmi_la_LDFLAGS = $(PLUGIN_FLAGS) $(DSMI_LDFLAGS)
gpu_dsmi_la_LIBADD = $(DSMI_LIBS)

# force:
# $(gpu_dsmi_la_LIBADD) : force
# 	@cd `dirname $@` && $(MAKE) `basename $@`
