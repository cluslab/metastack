# Makefile for accounting_storage/ plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(top_srcdir) -I$(top_srcdir)/src/common

AS_KINGBASE_SOURCES = accounting_storage_kingbase.c accounting_storage_kingbase.h \
                as_kingbase_acct.c as_kingbase_acct.h \
                as_kingbase_tres.c as_kingbase_tres.h \
                as_kingbase_archive.c as_kingbase_archive.h \
                as_kingbase_assoc.c as_kingbase_assoc.h \
                as_kingbase_cluster.c as_kingbase_cluster.h \
                as_kingbase_convert.c as_kingbase_convert.h \
                as_kingbase_federation.c as_kingbase_federation.h \
                as_kingbase_fix_runaway_jobs.c as_kingbase_fix_runaway_jobs.h \
                as_kingbase_job.c as_kingbase_job.h \
                as_kingbase_jobacct_process.c as_kingbase_jobacct_process.h \
                as_kingbase_problems.c as_kingbase_problems.h \
                as_kingbase_qos.c as_kingbase_qos.h \
                as_kingbase_resource.c as_kingbase_resource.h \
                as_kingbase_resv.c as_kingbase_resv.h \
                as_kingbase_rollup.c as_kingbase_rollup.h \
                as_kingbase_txn.c as_kingbase_txn.h \
                as_kingbase_usage.c as_kingbase_usage.h \
                as_kingbase_user.c as_kingbase_user.h \
                as_kingbase_wckey.c as_kingbase_wckey.h

if WITH_KINGBASE
pkglib_LTLIBRARIES = accounting_storage_kingbase.la

#  storage plugin.
accounting_storage_kingbase_la_SOURCES = $(AS_KINGBASE_SOURCES)
accounting_storage_kingbase_la_LDFLAGS = $(PLUGIN_FLAGS)
accounting_storage_kingbase_la_CFLAGS =$(KINGBASE_CFLAGS)
accounting_storage_kingbase_la_LIBADD = \
        $(top_builddir)/src/database/libslurm_kingbase.la $(KINGBASE_LIBS) \
        ../common/libaccounting_storage_common.la

force:
$(accounting_storage_kingbase_la_LIBADD) : force
        @cd `dirname $@` && $(MAKE) `basename $@`
else
EXTRA_accounting_storage_kingbase_la_SOURCES = $(AS_KINGBASE_SOURCES)
endif
