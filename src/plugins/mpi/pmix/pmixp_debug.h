/*****************************************************************************\
 **  pmix_debug.h - PMIx debug primitives
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015      Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com, artemp@mellanox.com>.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
 \*****************************************************************************/
#ifndef PMIXP_DEBUG_H
#define PMIXP_DEBUG_H

#include "pmixp_common.h"
#include "pmixp_info.h"
#include <time.h>

#define PMIXP_DEBUG_GET_TS(ts) {                                \
	struct timeval tv;                                      \
	gettimeofday(&tv, NULL);                                \
	ts = tv.tv_sec + 1E-6 * tv.tv_usec;                     \
}

#define PMIXP_DEBUG_TS(ts, format, args...) {                   \
	debug("%s [%d]: [%.6lf] %s:%d: " format "",             \
	      pmixp_info_hostname(), pmixp_info_nodeid(),       \
	      ts, THIS_FILE, __LINE__, ## args);                \
}

#define PMIXP_DEBUG(format, args...) {                          \
	double ts;                                              \
	PMIXP_DEBUG_GET_TS(ts);                                 \
	PMIXP_DEBUG_TS(ts, format, ## args);                    \
}

#define PMIXP_DEBUG_BASE(format, args...) {                     \
	char file[] = __FILE__;                                 \
	struct timeval tv;                                      \
	double ts;                                              \
	gettimeofday(&tv, NULL);                                \
	ts = tv.tv_sec + 1E-6 * tv.tv_usec;                     \
	char *file_base = strrchr(file, '/');                   \
	if (file_base == NULL) {                                \
		file_base = file;                               \
	} else {                                                \
		file_base++;                                    \
	}                                                       \
	debug("[%s:%d] [%.6lf] [%s:%d:%s] mpi/pmix:  " format,  \
	      pmixp_info_hostname(), pmixp_info_nodeid(), ts,   \
	      file_base, __LINE__, __func__, ## args);          \
}


#define PMIXP_ERROR_STD(format, args...) {			\
	error(" %s: %s: %s [%d]: %s:%d: " format ": %s (%d)",	\
	      plugin_type, __func__, pmixp_info_hostname(),	\
	      pmixp_info_nodeid(), THIS_FILE, __LINE__,		\
	      ## args, strerror(errno), errno);			\
}

#define PMIXP_ERROR(format, args...) {				\
	error(" %s: %s: %s [%d]: %s:%d: " format,		\
	      plugin_type, __func__, pmixp_info_hostname(),	\
	      pmixp_info_nodeid(), THIS_FILE, __LINE__,		\
	      ## args);						\
}

#define PMIXP_ABORT(format, args...) {				\
	PMIXP_ERROR(format, ##args);				\
	slurm_kill_job_step(pmixp_info_jobid(),			\
			    pmixp_info_stepid(), SIGKILL);	\
}

#define PMIXP_ERROR_NO(err, format, args...) {			\
	error(" %s: %s: %s [%d]: %s:%d: " format ": %s (%d)",	\
	      plugin_type, __func__, pmixp_info_hostname(),	\
	      pmixp_info_nodeid(), THIS_FILE, __LINE__,		\
	      ## args, strerror(err), err);			\
}

#define PMIXP_PROF_SERIALIZE(bptr, bsize, offset, val){         \
	xassert( (offset + sizeof(val)) <= bsize);              \
	memcpy(bptr + offset, &val, sizeof(val));               \
	offset += sizeof(val);                                  \
}

#define PMIXP_PROF_DESERIALIZE(bptr, bsize, offset, val) {      \
	xassert( (offset + sizeof(val)) <= bsize);              \
	memcpy(&val, bptr + offset, sizeof(val));               \
	offset += sizeof(val);                                  \
}


#ifdef NDEBUG
#define pmixp_debug_hang(x)
#define PMIXP_DELAYED_PROF_MAX 0
#define PMIXP_PROF_INIT(max_threads)
#define PMIXP_PROF_FINI()
#else

/* Lightweight profiling interface */

typedef struct {
	void *priv;
	void (*output)(void *priv, double ts, size_t size, char data[]);
} pmixp_profile_t;

void pmixp_profile_in(pmixp_profile_t *prof, char *buf, size_t size);
void pmixp_profile_init(int max_threads, size_t thread_buf_size);
void pmixp_profile_fini();

/* Use the limit of 100M for the buffer */
#define PMIXP_DELAYED_PROF_MAX (100*1024*1024)
#define PMIXP_PROFILE_DELAY() (pmixp_info_prof_delayed())

#define PMIXP_PROFILE_DEFINE(prefix, var, ctx, outfunc)         \
prefix pmixp_profile_t var = { .priv = ctx, .output = outfunc };

#define PMIXP_PROF_INIT(max_threads) {                          \
	if( pmixp_info_prof_delayed()) {                        \
		pmixp_profile_init(max_threads,                 \
				   pmixp_info_prof_bufsize());  \
	}                                                       \
}

#define PMIXP_PROF_FINI() {                                     \
	if( pmixp_info_prof_delayed()) {                        \
		pmixp_profile_fini();                           \
	}                                                       \
}

#define PMIXP_PROFILE(prof, buf, buf_size) {                    \
	pmixp_profile_in(prof, buf, buf_size);                  \
}

static inline void _pmixp_debug_hang(int delay)
{
	while (delay) {
		sleep(1);
	}
}

#define pmixp_debug_hang(x) _pmixp_debug_hang(x)

#endif
#endif /* PMIXP_DEBUG_H */
