

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(top_srcdir) -I$(top_srcdir)/src/common -I$(top_srcdir)/src/plugins/burst_buffer/common

pkglib_LTLIBRARIES = burst_buffer_parastorbb.la

# Trackable RESources selection plugin.
burst_buffer_parastorbb_la_SOURCES = \
	bb_api.c bb_api.h \
	bb_curl_wrapper.c bb_curl_wrapper.h \
	burst_buffer_parastorbb.c 
burst_buffer_parastorbb_la_LDFLAGS = $(PLUGIN_FLAGS) 
burst_buffer_parastorbb_la_LIBADD = ../common/libburst_buffer_common.la \
			     -lcurl \
				 -ljansson 