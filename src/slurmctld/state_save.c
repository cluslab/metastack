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
#include "src/common/list.h"
#endif

/* Maximum delay for pending state save to be processed, in seconds */
#ifndef SAVE_MAX_WAIT
#define SAVE_MAX_WAIT	5
#endif

#ifdef __METASTACK_OPT_CACHE_QUERY
slurmctld_cache_t *cache_queue = NULL;
#endif

static pthread_mutex_t state_save_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  state_save_cond = PTHREAD_COND_INITIALIZER;
static int save_jobs = 0, save_nodes = 0, save_parts = 0;
static int save_front_end = 0, save_triggers = 0, save_resv = 0;
static bool run_save_thread = true;

#ifdef __METASTACK_OPT_CACHE_QUERY

static void _list_delete_cache_record(void *entry);

/*cache_queue_init:Initialize the cache_queue variable.*/
extern void cache_queue_init()
{
	if(!cache_queue){
		cache_queue = xmalloc(sizeof(*cache_queue));
		cache_queue->work = list_create(_list_delete_cache_record);
		slurm_cond_init(&cache_queue->cond, NULL);
		slurm_mutex_init(&cache_queue->mutex);
		cache_queue->shutdown = false;
		cache_queue->copy_all_data = false;
	}
}

extern void cache_queue_fini()
{
	if(cache_queue){
		list_destroy(cache_queue->work);
		xfree(cache_queue);
	}
}

/*cache_queue_shutdown: Shut down the cache query thread.*/
extern void cache_queue_shutdown()
{
	if(cache_queue){
		slurm_mutex_lock(&cache_queue->mutex);
		cache_queue->shutdown = true;
		slurm_cond_signal(&cache_queue->cond);
		slurm_mutex_unlock(&cache_queue->mutex);
	}
}
/*cache_enqueue: Add the status message to the list.*/
extern bool cache_enqueue(slurm_cache_date_t *msg)
{
	if(cache_queue){
		list_enqueue(cache_queue->work, msg);
		slurm_mutex_lock(&cache_queue->mutex);
		slurm_cond_signal(&cache_queue->cond);
		slurm_mutex_unlock(&cache_queue->mutex);
	}else{
		return false;
	}
	return true;
}

/* Perform a status copy immediately */
extern void update_all_cache_state(void)
{	
	if(cache_queue){
		slurm_mutex_lock(&cache_queue->mutex);
		cache_queue->copy_all_data = true;
		slurm_cond_signal(&cache_queue->cond);
		slurm_mutex_unlock(&cache_queue->mutex);
	}
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
}

/* Queue saving of node state information */
extern void schedule_node_save(void)
{
	slurm_mutex_lock(&state_save_lock);
	save_nodes++;
	slurm_cond_broadcast(&state_save_cond);
	slurm_mutex_unlock(&state_save_lock);
}

/* Queue saving of partition state information */
extern void schedule_part_save(void)
{
	slurm_mutex_lock(&state_save_lock);
	save_parts++;
	slurm_cond_broadcast(&state_save_cond);
	slurm_mutex_unlock(&state_save_lock);
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
	cache_queue_shutdown();
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

/*_list_delete_cache_record: Clear the corresponding message 
 *memory for different message types.*/
static void _list_delete_cache_record(void *entry)
{
	slurm_cache_date_t *msg_ptr = (slurm_cache_date_t *) entry;
	int i;
	switch (msg_ptr->msg_type) {
		case CREATE_CACHE_JOB_RECORD:
			_delete_copy_job_state(msg_ptr->job_ptr);
			break;
		case CREATE_CACHE_NODE_RECORD:
			_list_delete_copy_config(msg_ptr->node_ptr->config_ptr);
			if(msg_ptr->node_ptr->part_pptr){
				char **part_pptr_t = (char **)msg_ptr->node_ptr->part_pptr;
				for (i = 0; i < msg_ptr->node_ptr->part_cnt; i++) {
					xfree(part_pptr_t[i]);
				}
			}
			purge_cache_node_rec(msg_ptr->node_ptr);
			break;
		case CREATE_CACHE_PART_RECORD:
            xfree(msg_ptr->default_part_name);
			_list_delete_cache_part(msg_ptr->part_ptr);
			break;
		case UPDATE_CACHE_JOB_RECORD:
			del_cache_job_state_record(msg_ptr->job_state_ptr);
			break;
		case UPDATE_CACHE_NODE_RECORD:
			del_cache_node_state_record(msg_ptr->node_state_ptr);
			break;
		case UPDATE_CACHE_NODE_INFO:
			del_cache_node_info_record(msg_ptr->select_nodeinfo,msg_ptr->node_record_count);
			break;
        case UPDATE_CACHE_NODE_STATE:
            FREE_NULL_BITMAP(msg_ptr->node_state_bitmap);
            FREE_NULL_BITMAP(msg_ptr->para_sched_node_bitmap);
			break;
		case UPDATE_CACHE_PART_RECORD:
            xfree(msg_ptr->default_part_name);
			del_cache_part_state_record(msg_ptr->part_state_ptr);
			break;
		case DELETE_CACHE_NODE_RECORD:
			xfree(msg_ptr->node_name);
			break;
		case DELETE_CACHE_PART_RECORD:
            xfree(msg_ptr->default_part_name);
			xfree(msg_ptr->part_name);
			break;
	}
	xfree(msg_ptr);
}

/*_update_cache_record: Perform a full update of all data.*/
static void _update_cache_record()
{
	slurmctld_lock_t copy_write_lock = {
			NO_LOCK, WRITE_LOCK, WRITE_LOCK, WRITE_LOCK, NO_LOCK };
	DEF_TIMERS;
	START_TIMER;
	copy_all_part_state();
	copy_all_node_state();
	copy_all_job_state();
	END_TIMER2(__func__);
	debug2("%s: CACHE_QUERY copy_all_state, %s", __func__, TIME_STR);
	lock_cache_query(copy_write_lock);
	purge_cache_part_data();
	purge_cache_node_data();
	purge_cache_job_data();
	replace_cache_part_data();
	replace_cache_node_data();
	replace_cache_job_data();
	unlock_cache_query(copy_write_lock);
	END_TIMER2(__func__);
	debug2("%s:CACHE_QUERY replace_cache_data, %s", __func__, TIME_STR);
}

/*
 * Run as pthread to copy slurmctld state information as needed,
 * no_data IN - unused
 * RET - NULL
 */
extern void *slurmctld_state_copy(void *no_data)
{
	slurm_cache_date_t *msg = NULL;
	time_t now;
	slurmctld_lock_t config_read_lock = {
			READ_LOCK, NO_LOCK, NO_LOCK, NO_LOCK, NO_LOCK };
	slurmctld_lock_t cache_job_write_lock = {
			NO_LOCK, WRITE_LOCK, NO_LOCK, READ_LOCK, NO_LOCK };
	slurmctld_lock_t cache_node_write_lock = {
			NO_LOCK, NO_LOCK, WRITE_LOCK, WRITE_LOCK, NO_LOCK };
	slurmctld_lock_t cache_part_write_lock = {
			NO_LOCK, WRITE_LOCK, WRITE_LOCK, WRITE_LOCK, NO_LOCK };
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
		while (true) {
			slurm_mutex_lock(&cache_queue->mutex);
		 	if (cache_queue->shutdown) {
				slurm_mutex_unlock(&cache_queue->mutex);
				debug2("%s CACHE_QUERY thread shutdown", __func__);
				return NULL;	/* shutdown */
			}
			slurm_cond_wait(&cache_queue->cond, &cache_queue->mutex);
			slurm_mutex_unlock(&cache_queue->mutex);
		}
	}
	unlock_slurmctld(config_read_lock);
	
	while (true) {
		slurm_mutex_lock(&cache_queue->mutex);
	 	if (cache_queue->shutdown) {
			cache_queue->shutdown = false;
			slurm_mutex_unlock(&cache_queue->mutex);
			debug2("%s CACHE_QUERY shutdown", __func__);
			return NULL;	/* shutdown */
		} else if (cache_queue->copy_all_data){
			cache_queue->copy_all_data = false;
			slurm_mutex_unlock(&cache_queue->mutex);
			if (list_count(cache_queue->work)){
				list_flush(cache_queue->work);
			}
			_update_cache_record();
			continue;
		}
		slurm_mutex_unlock(&cache_queue->mutex);
		now = time(NULL);
		if(difftime(now, last_node_info) >= 3){
            debug2("%s CACHE_QUERY cache_queue list count %d ", __func__, list_count(cache_queue->work));
			_update_node_info();
			now = time(NULL);
			last_node_info = now;
		}
		/*Pop from the list and perform different data update
		 *operations based on the message type. */
		msg = list_dequeue(cache_queue->work);
		if (!msg) {	
			usleep(500);
			slurm_mutex_lock(&cache_queue->mutex);
			if (!list_count(cache_queue->work)){		/* wait for more work */
				struct timespec ts = {0, 0};
				ts.tv_sec = now + 1;
				slurm_cond_timedwait(&cache_queue->cond, &cache_queue->mutex, &ts);
			}
			slurm_mutex_unlock(&cache_queue->mutex);
		} else {
			switch (msg->msg_type) {
				case CREATE_CACHE_JOB_RECORD:
					debug4("%s CACHE_QUERY CREATE_CACHE_JOB_RECORD", __func__);
					lock_cache_query(cache_job_write_lock);
					_add_queue_job_to_cache(msg->job_ptr);
					unlock_cache_query(cache_job_write_lock);
					xfree(msg);
					break;
				case CREATE_CACHE_NODE_RECORD:
					debug4("%s CACHE_QUERY CREATE_CACHE_NODE_RECORD", __func__);
					lock_cache_query(cache_node_write_lock);
					reset_cache_node_record_table_ptr(msg->node_record_count);
					_add_queue_node_to_cache(msg->node_ptr);
					unlock_cache_query(cache_node_write_lock);
					xfree(msg);
					break;
				case CREATE_CACHE_PART_RECORD:
					debug4("%s CACHE_QUERY CREATE_CACHE_PART_RECORD", __func__);
					lock_cache_query(cache_part_write_lock);
					xfree(default_cache_part_name);
					default_cache_part_name = msg->default_part_name;
					msg->default_part_name = NULL;
					_add_queue_part_to_cache(msg->part_ptr);
					unlock_cache_query(cache_part_write_lock);
					xfree(msg);
					break;
				case UPDATE_CACHE_JOB_RECORD:
					debug4("%s CACHE_QUERY UPDATE_CACHE_JOB_RECORD", __func__);
					lock_cache_query(cache_job_write_lock);
					update_cache_job_record(msg->job_state_ptr);
					unlock_cache_query(cache_job_write_lock);
					xfree(msg);
					break;
				case UPDATE_CACHE_NODE_RECORD:
					debug4("%s CACHE_QUERY UPDATE_CACHE_NODE_RECORD", __func__);
					lock_cache_query(cache_node_write_lock);
					update_cache_node_record(msg->node_state_ptr);
					unlock_cache_query(cache_node_write_lock);
					xfree(msg);
					break;
				case UPDATE_CACHE_NODE_INFO:
					debug4("%s CACHE_QUERY UPDATE_CACHE_NODE_INFO", __func__);
					lock_cache_query(cache_node_write_lock);
					update_cache_node_info(msg->select_nodeinfo,msg->node_record_count);
					unlock_cache_query(cache_node_write_lock);
					xfree(msg);
					break;
				case UPDATE_CACHE_NODE_STATE:
					debug4("%s CACHE_QUERY UPDATE_CACHE_NODE_STATE", __func__);
					lock_cache_query(cache_node_write_lock);
					update_cache_node_state(msg->node_state_bitmap,msg->para_sched_node_bitmap);
					unlock_cache_query(cache_node_write_lock);
					xfree(msg);
					break;
				case UPDATE_CACHE_PART_RECORD:
					debug4("%s CACHE_QUERY UPDATE_CACHE_PART_RECORD", __func__);
					lock_cache_query(cache_part_write_lock);
					xfree(default_cache_part_name);
					default_cache_part_name = msg->default_part_name;
					msg->default_part_name = NULL;
					update_cache_part_record(msg->part_state_ptr);
					unlock_cache_query(cache_part_write_lock);
					xfree(msg);
					break;
				case DELETE_CACHE_JOB_RECORD:
					debug4("%s CACHE_QUERY DELETE_CACHE_JOB_RECORD", __func__);
					lock_cache_query(cache_job_write_lock);
					_del_cache_job(msg->job_id);
					unlock_cache_query(cache_job_write_lock);
					xfree(msg);
					break;
				case DELETE_CACHE_NODE_RECORD:
					debug4("%s CACHE_QUERY DELETE_CACHE_NODE_RECORD", __func__);
					lock_cache_query(cache_node_write_lock);
					_del_cache_node(msg->node_name);
					_del_hash_cache_node(msg->node_name);
					unlock_cache_query(cache_node_write_lock);
					xfree(msg->node_name);
					xfree(msg);
					break;
				case DELETE_CACHE_PART_RECORD:
					debug4("%s CACHE_QUERY DELETE_CACHE_PART_RECORD", __func__);
					lock_cache_query(cache_part_write_lock);
					xfree(default_cache_part_name);
					default_cache_part_name = msg->default_part_name;
					msg->default_part_name = NULL;
					_del_cache_part(msg->part_name);
					unlock_cache_query(cache_part_write_lock);
					xfree(msg->part_name);
					xfree(msg);
					break;
				default:
					error("%s: Unknown message type.", __func__);
					break;
			}
			
		}
	}
}
#endif