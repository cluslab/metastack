/*****************************************************************************\
 *  state_save.c - Keep saved slurmctld state current
 *****************************************************************************
 *  Copyright (C) 2004-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette1@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
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

#include "config.h"

#if HAVE_SYS_PRCTL_H
#  include <sys/prctl.h>
#endif

#include <pthread.h>
#include "src/common/macros.h"
#include "src/slurmctld/front_end.h"
#include "src/slurmctld/reservation.h"
#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/trigger_mgr.h"

#ifdef __METASTACK_OPT_CACHE_QUERY
#include "src/slurmctld/locks.h"
#endif

/* Maximum delay for pending state save to be processed, in seconds */
#ifndef SAVE_MAX_WAIT
#define SAVE_MAX_WAIT	5
#endif

#ifdef __METASTACK_OPT_CACHE_QUERY
static pthread_mutex_t state_copy_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  state_copy_cond = PTHREAD_COND_INITIALIZER;
static int copy_all_data = 0;
static int copy_jobs = 0;
static int copy_parts = 0;
static int copy_nodes = 0;
//static int copy_front_end = 0, copy_triggers = 0, copy_resv = 0, copy_nodes = 0, copy_parts = 0;
static bool run_copy_thread = true;
#endif

static pthread_mutex_t state_save_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  state_save_cond = PTHREAD_COND_INITIALIZER;
static int save_jobs = 0, save_nodes = 0, save_parts = 0;
static int save_front_end = 0, save_triggers = 0, save_resv = 0;
static bool run_save_thread = true;

#ifdef __METASTACK_OPT_CACHE_QUERY
#if 0
/* Copy the node state pointer */
static void copy_node_update(void)
{
	slurm_mutex_lock(&state_copy_lock);
	copy_nodes++;
	slurm_cond_broadcast(&state_copy_cond);
	slurm_mutex_unlock(&state_copy_lock);
}

/* Copy the part state pointer */
static void copy_part_update(void)
{
	slurm_mutex_lock(&state_copy_lock);
	copy_parts++;
	slurm_cond_broadcast(&state_copy_cond);
	slurm_mutex_unlock(&state_copy_lock);
}

/* Copy the job state pointer */
static void copy_job_update(void)
{
	slurm_mutex_lock(&state_copy_lock);
	copy_jobs++;
	slurm_cond_broadcast(&state_copy_cond);
	slurm_mutex_unlock(&state_copy_lock);
}
#endif

/* Perform a status copy immediately */
extern void real_time_state_copy(void)
{
	slurm_mutex_lock(&state_copy_lock);
	copy_all_data++;
	slurm_cond_broadcast(&state_copy_cond);
	slurm_mutex_unlock(&state_copy_lock);
}

/* shutdown the slurmctld_state_save thread */
extern void shutdown_state_copy(void)
{
	slurm_mutex_lock(&state_copy_lock);
	run_copy_thread = false;
	slurm_cond_broadcast(&state_copy_cond);
	slurm_mutex_unlock(&state_copy_lock);
}
#endif

/* Queue saving of front_end state information */
extern void schedule_front_end_save(void)
{
	slurm_mutex_lock(&state_save_lock);
	save_front_end++;
	slurm_cond_broadcast(&state_save_cond);
	slurm_mutex_unlock(&state_save_lock);
}

/* Queue saving of job state information */
extern void schedule_job_save(void)
{
	slurm_mutex_lock(&state_save_lock);
	save_jobs++;
	slurm_cond_broadcast(&state_save_cond);
	slurm_mutex_unlock(&state_save_lock);
//#ifdef __METASTACK_OPT_CACHE_QUERY
//	copy_job_update();
//#endif
}

/* Queue saving of node state information */
extern void schedule_node_save(void)
{
	slurm_mutex_lock(&state_save_lock);
	save_nodes++;
	slurm_cond_broadcast(&state_save_cond);
	slurm_mutex_unlock(&state_save_lock);
//#ifdef __METASTACK_OPT_CACHE_QUERY
//	copy_node_update();
//#endif
}

/* Queue saving of partition state information */
extern void schedule_part_save(void)
{
	slurm_mutex_lock(&state_save_lock);
	save_parts++;
	slurm_cond_broadcast(&state_save_cond);
	slurm_mutex_unlock(&state_save_lock);
//#ifdef __METASTACK_OPT_CACHE_QUERY
//	copy_part_update();
//#endif
}

/* Queue saving of reservation state information */
extern void schedule_resv_save(void)
{
	slurm_mutex_lock(&state_save_lock);
	save_resv++;
	slurm_cond_broadcast(&state_save_cond);
	slurm_mutex_unlock(&state_save_lock);
}

/* Queue saving of trigger state information */
extern void schedule_trigger_save(void)
{
	slurm_mutex_lock(&state_save_lock);
	save_triggers++;
	slurm_cond_broadcast(&state_save_cond);
	slurm_mutex_unlock(&state_save_lock);
}

/* shutdown the slurmctld_state_save thread */
extern void shutdown_state_save(void)
{
	slurm_mutex_lock(&state_save_lock);
	run_save_thread = false;
	slurm_cond_broadcast(&state_save_cond);
	slurm_mutex_unlock(&state_save_lock);
#ifdef __METASTACK_OPT_CACHE_QUERY
	shutdown_state_copy();
#endif
}

/*
 * Run as pthread to keep saving slurmctld state information as needed,
 * Use schedule_job_save(),  schedule_node_save(), and schedule_part_save()
 * to queue state save of each data structure
 * no_data IN - unused
 * RET - NULL
 */
extern void *slurmctld_state_save(void *no_data)
{
	time_t last_save = 0, now;
	double save_delay;
	bool run_save;
	int save_count;

#if HAVE_SYS_PRCTL_H
	if (prctl(PR_SET_NAME, "sstate", NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m", __func__, "sstate");
	}
#endif

	if (test_config)	/* Should be redundant, but just to be safe */
		return NULL;

	while (1) {
		/* wait for work to perform */
		slurm_mutex_lock(&state_save_lock);
		while (1) {
			save_count = save_jobs + save_nodes + save_parts +
				     save_front_end + save_resv +
				     save_triggers;
			now = time(NULL);
			save_delay = difftime(now, last_save);
			if (save_count &&
			    (!run_save_thread ||
			     (save_delay >= SAVE_MAX_WAIT))) {
				last_save = now;
				break;		/* do the work */
			} else if (!run_save_thread) {
				run_save_thread = true;
				slurm_mutex_unlock(&state_save_lock);
				return NULL;	/* shutdown */
			} else if (save_count) { /* wait for a timeout */
				struct timespec ts = {0, 0};
				ts.tv_sec = now + 1;
				slurm_cond_timedwait(&state_save_cond,
					  	     &state_save_lock, &ts);
			} else {		/* wait for more work */
				slurm_cond_wait(&state_save_cond,
					  	&state_save_lock);
			}
		}

		/* save front_end node info if necessary */
		run_save = false;
		/* slurm_mutex_lock(&state_save_lock); done above */
		if (save_front_end) {
			run_save = true;
			save_front_end = 0;
		}
		slurm_mutex_unlock(&state_save_lock);
		if (run_save)
			(void)dump_all_front_end_state();

		/* save job info if necessary */
		run_save = false;
		slurm_mutex_lock(&state_save_lock);
		if (save_jobs) {
			run_save = true;
			save_jobs = 0;
		}
		slurm_mutex_unlock(&state_save_lock);
		if (run_save)
			(void)dump_all_job_state();

		/* save node info if necessary */
		run_save = false;
		slurm_mutex_lock(&state_save_lock);
		if (save_nodes) {
			run_save = true;
			save_nodes = 0;
		}
		slurm_mutex_unlock(&state_save_lock);
		if (run_save)
			(void)dump_all_node_state();

		/* save partition info if necessary */
		run_save = false;
		slurm_mutex_lock(&state_save_lock);
		if (save_parts) {
			run_save = true;
			save_parts = 0;
		}
		slurm_mutex_unlock(&state_save_lock);
		if (run_save)
			(void)dump_all_part_state();

		/* save reservation info if necessary */
		run_save = false;
		slurm_mutex_lock(&state_save_lock);
		if (save_resv) {
			run_save = true;
			save_resv = 0;
		}
		slurm_mutex_unlock(&state_save_lock);
		if (run_save)
			(void)dump_all_resv_state();

		/* save trigger info if necessary */
		run_save = false;
		slurm_mutex_lock(&state_save_lock);
		if (save_triggers) {
			run_save = true;
			save_triggers = 0;
		}
		slurm_mutex_unlock(&state_save_lock);
		if (run_save)
			(void)trigger_state_save();
	}
}

#ifdef __METASTACK_OPT_CACHE_QUERY

/*
 * Run as pthread to copy slurmctld state information as needed,
 * no_data IN - unused
 * RET - NULL
 */
extern void *slurmctld_state_copy(void *no_data)
{
	time_t last_save = 0, now;
	double save_delay;
	int save_count;
	slurmctld_lock_t copy_write_lock = {
			NO_LOCK, WRITE_LOCK, WRITE_LOCK, WRITE_LOCK, NO_LOCK };
	slurmctld_lock_t config_read_lock = {
			READ_LOCK, NO_LOCK, NO_LOCK, NO_LOCK, NO_LOCK };
#if HAVE_SYS_PRCTL_H
	if (prctl(PR_SET_NAME, "cstate", NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m", __func__, "cstate");
	}
#endif
	
	if (test_config)	/* Should be redundant, but just to be safe */
		return NULL;
	lock_slurmctld(config_read_lock);
	if(!slurm_conf.cache_query){
		unlock_slurmctld(config_read_lock);
		return NULL;
	}
	unlock_slurmctld(config_read_lock);
	DEF_TIMERS;
	last_save = time(NULL);
	while (1) {
		/* wait for work to perform */
		slurm_mutex_lock(&state_copy_lock);
		while (1) {
			//save_count = copy_jobs + copy_parts + copy_nodes;
            save_count = 1;
			now = time(NULL);
			save_delay = difftime(now, last_save);
			 if (!run_copy_thread) {
				run_copy_thread = true;
				slurm_mutex_unlock(&state_copy_lock);
				return NULL;	/* shutdown */
			} else if (copy_all_data){
				last_save = now;
				break;
			} else if (save_count && (save_delay >= slurm_conf.cachedup_interval)){
				last_save = now;
				break;		/* do the work */
			} else if (save_count) { /* wait for a timeout */
				struct timespec ts = {0, 0};
				ts.tv_sec = now + 1;
				slurm_cond_timedwait(&state_copy_cond, &state_copy_lock, &ts);
//			} else {		/* wait for more work */
//				slurm_cond_wait(&state_copy_cond,
//					  	&state_copy_lock);
			}
		}
		copy_all_data = 0;
		copy_parts = 0;
		copy_nodes = 0;
		copy_jobs = 0;
		slurm_mutex_unlock(&state_copy_lock);
		START_TIMER;
		copy_all_part_state();
		copy_all_node_state();
		copy_all_job_state();
		END_TIMER2(__func__);
		debug2("%s: CACHE_QUERY copy_all_state, %s", __func__, TIME_STR);
		lock_cache_query(copy_write_lock);
		update_part_cache_data();
		update_node_cache_data();
		update_job_cache_data();
		unlock_cache_query(copy_write_lock);
		END_TIMER2(__func__);
		debug2("%s:CACHE_QUERY update_cache_data, %s", __func__, TIME_STR);
	}
}
#endif