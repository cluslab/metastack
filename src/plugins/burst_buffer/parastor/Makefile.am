

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(top_srcdir) -I$(top_srcdir)/src/common -I$(top_srcdir)/src/plugins/burst_buffer/common

# Create libbb_api.la as a shared library that can be loaded as a plugin
# and also linked by other libraries
pkglib_LTLIBRARIES = libbb_api.la burst_buffer_parastorbb.la

libbb_api_la_SOURCES = \
	bb_api.c bb_api.h \
	bb_curl_wrapper.c bb_curl_wrapper.h

libbb_api_la_LDFLAGS = $(PLUGIN_FLAGS)
libbb_api_la_LIBADD = ../common/libburst_buffer_common.la \
		       -lcurl \
		       -ljansson

# Trackable RESources selection plugin.
burst_buffer_parastorbb_la_SOURCES = \
	burst_buffer_parastorbb.c 
burst_buffer_parastorbb_la_LDFLAGS = $(PLUGIN_FLAGS) 
burst_buffer_parastorbb_la_LIBADD = libbb_api.la \
				    ../common/libburst_buffer_common.la \
			            -lcurl \
				    -ljansson 