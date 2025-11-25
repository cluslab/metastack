/*****************************************************************************\
 *  slurm_jobacct_gather.c - implementation-independent job accounting logging
 *  functions
 *****************************************************************************
 *  Copyright (C) 2003-2007/ The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Jay Windley <jwindley@lnxi.com>, Morris Jette <jette1@llnl.com>
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  Copyright (C) 2005 Hewlett-Packard Development Company, L.P.
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

/*****************************************************************************\
 *  Modification history
 *
 *  19 Jan 2005 by Andy Riebs <andy.riebs@hp.com>
 *  	 This file is derived from the file slurm_jobcomp.c, written by
 *  	 Morris Jette, et al.
\*****************************************************************************/

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

#if HAVE_SYS_PRCTL_H
#  include <sys/prctl.h>
#endif

#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
#include "slurm/slurm.h"
#include "src/common/slurm_acct_gather.h"
#endif
#include "src/common/assoc_mgr.h"
#include "src/common/macros.h"
#include "src/common/pack.h"
#include "src/common/plugin.h"
#include "src/common/plugrack.h"
#include "src/common/read_config.h"
#include "src/common/slurm_acct_gather_profile.h"
#include "src/common/slurm_jobacct_gather.h"
#include "src/common/slurm_protocol_pack.h"
#include "src/common/slurmdbd_defs.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/slurmd/slurmstepd/slurmstepd_job.h"
#include "src/slurmdbd/read_config.h"
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
#include "src/common/run_command.h"
#include "src/common/fd.h"
// #include <sys/syscall.h>
// #include "src/slurmd/common/proctrack.h"
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
#include <sys/stat.h>
#endif
#define KB_ADJ 1024
#define MB_ADJ 1048576

/*
** Define slurm-specific aliases for use by plugins, see slurm_xlator.h
** for details.
 */
strong_alias(jobacctinfo_pack, slurm_jobacctinfo_pack);
strong_alias(jobacctinfo_unpack, slurm_jobacctinfo_unpack);
strong_alias(jobacctinfo_create, slurm_jobacctinfo_create);
strong_alias(jobacctinfo_destroy, slurm_jobacctinfo_destroy);

#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
static buf_t *apptype_properties_apptype_buf = NULL;
static pthread_mutex_t conf_mutex2 = PTHREAD_MUTEX_INITIALIZER;
static bool inited = 0;
typedef struct s_p_hashtbl_info {
	int full_option_cnt;
	s_p_options_t full_option;
} s_p_hashtbl_info_t;
static uint64_t total_cpu_time = 0;		/* Record the maximum machine consumption of the process */
static char *max_cputime_proc = NULL;	/* Record the process Command at the time of maximum machine consumption */
static pthread_t apptype_recognition_thread_id = 0;
static acct_gather_profile_timer_t *profile_apptype =
	&acct_gather_profile_timer[PROFILE_APPTYPE];
#endif

#ifdef __METASTACK_LOAD_ABNORMAL
step_gather_t step_gather = {
	PTHREAD_COND_INITIALIZER,
	PTHREAD_MUTEX_INITIALIZER,
	0,
	-1,
	-1,
	-1,
	-1,
	-1,
	0,/*number node of child count now*/
	0.0,
	0.0,
	0,
	0,
	0,
	0,
	0,
	{},
	(bitstr_t *)NULL,
	true,
	0
};

typedef struct slurm_jobacct_gather_ops {
	void (*poll_data) (List task_list, uint64_t cont_id, bool profile, collection_t *collect, write_t *data);
	int (*endpoll)    ();
	int (*add_task)   (pid_t pid, jobacct_id_t *jobacct_id);
} slurm_jobacct_gather_ops_t;

#endif
/*
 * These strings must be in the same order as the fields declared
 * for slurm_jobacct_gather_ops_t.
 */
static const char *syms[] = {
	"jobacct_gather_p_poll_data",
	"jobacct_gather_p_endpoll",
	"jobacct_gather_p_add_task",
};

static slurm_jobacct_gather_ops_t ops;
static plugin_context_t *g_context = NULL;
static pthread_mutex_t g_context_lock = PTHREAD_MUTEX_INITIALIZER;
static bool init_run = false;
static pthread_mutex_t init_run_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t watch_tasks_thread_id = 0;

#ifdef __METASTACK_LOAD_ABNORMAL
static pthread_t watch_stepd_thread_id = 0;
static acct_gather_profile_timer_t *profile_stepd =
	&acct_gather_profile_timer[PROFILE_STEPD];
collection_t share_data = {
	.lock = PTHREAD_MUTEX_INITIALIZER,
	.load_flag = 0,
	.update = false,
	.step = false,
	.cpu_step_real = 0.0,
	.cpu_step_ave = 0.0,
	.mem_step = 0,
	.vmem_step = 0,
	.step_pages = 0,
	.start = 0,
};
#endif

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
bool update_watch_dog = false;
bool update_node_watch_dog = false;
static pthread_t watch_dog_thread_id = 0;
static acct_gather_profile_timer_t *profile_watch_dog_timer =
	&acct_gather_profile_timer_watch_dog;
static write_t watch_dog_collect;
static write_t watch_dog_node_step_collect;
static char **_build_watch_dog_env(acct_gather_rank_t *watch_dog);
//pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_mutex_t watch_dog_env_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t watch_dog_all_env_lock = PTHREAD_MUTEX_INITIALIZER;
#endif

static int freq = 0;
static List task_list = NULL;
static uint64_t cont_id = NO_VAL64;
static pthread_mutex_t task_list_lock = PTHREAD_MUTEX_INITIALIZER;

static bool jobacct_shutdown = true;
static pthread_mutex_t jobacct_shutdown_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool plugin_polling = true;

static slurm_step_id_t jobacct_step_id = {
	.job_id = 0,
	.step_het_comp = NO_VAL,
	.step_id = 0,
};
static uint64_t jobacct_mem_limit  = 0;
static uint64_t jobacct_vmem_limit = 0;
static acct_gather_profile_timer_t *profile_timer =
	&acct_gather_profile_timer[PROFILE_TASK];

static void _init_tres_usage(struct jobacctinfo *jobacct,
			     jobacct_id_t *jobacct_id,
			     uint32_t tres_cnt)
{
	int alloc_size, i;

	jobacct->tres_count = tres_cnt;

	jobacct->tres_ids = xcalloc(tres_cnt, sizeof(uint32_t));

	alloc_size = tres_cnt * sizeof(uint64_t);

	jobacct->tres_usage_in_max = xmalloc(alloc_size);
	jobacct->tres_usage_in_max_nodeid = xmalloc(alloc_size);
	jobacct->tres_usage_in_max_taskid = xmalloc(alloc_size);
	jobacct->tres_usage_in_min = xmalloc(alloc_size);
	jobacct->tres_usage_in_min_nodeid = xmalloc(alloc_size);
	jobacct->tres_usage_in_min_taskid = xmalloc(alloc_size);
	jobacct->tres_usage_in_tot = xmalloc(alloc_size);
	jobacct->tres_usage_out_max = xmalloc(alloc_size);
	jobacct->tres_usage_out_max_nodeid = xmalloc(alloc_size);
	jobacct->tres_usage_out_max_taskid = xmalloc(alloc_size);
	jobacct->tres_usage_out_min = xmalloc(alloc_size);
	jobacct->tres_usage_out_min_nodeid = xmalloc(alloc_size);
	jobacct->tres_usage_out_min_taskid = xmalloc(alloc_size);
	jobacct->tres_usage_out_tot = xmalloc(alloc_size);
#ifdef __METASTACK_LOAD_ABNORMAL
	int alloc_start_end_size = JOBACCTINFO_START_END_ARRAY_SIZE * sizeof(time_t);
	jobacct->cpu_start = xmalloc(alloc_start_end_size);
	jobacct->cpu_end = xmalloc(alloc_start_end_size);
	jobacct->pid_start = xmalloc(alloc_start_end_size);
	jobacct->pid_end = xmalloc(alloc_start_end_size);
	jobacct->node_start = xmalloc(alloc_start_end_size);
	jobacct->node_end = xmalloc(alloc_start_end_size);
#endif
	for (i = 0; i < jobacct->tres_count; i++) {
		jobacct->tres_ids[i] =
			assoc_mgr_tres_array ? assoc_mgr_tres_array[i]->id : i;

		jobacct->tres_usage_in_min[i] = INFINITE64;
		jobacct->tres_usage_in_max[i] = INFINITE64;
		jobacct->tres_usage_in_tot[i] = INFINITE64;
		jobacct->tres_usage_out_max[i] = INFINITE64;
		jobacct->tres_usage_out_min[i] = INFINITE64;
		jobacct->tres_usage_out_tot[i] = INFINITE64;

		if (jobacct_id && jobacct_id->taskid != NO_VAL) {
			jobacct->tres_usage_in_max_taskid[i] =
				(uint64_t) jobacct_id->taskid;
			jobacct->tres_usage_in_min_taskid[i] =
				(uint64_t) jobacct_id->taskid;
			jobacct->tres_usage_out_max_taskid[i] =
				(uint64_t) jobacct_id->taskid;
			jobacct->tres_usage_out_min_taskid[i] =
				(uint64_t) jobacct_id->taskid;
		} else {
			jobacct->tres_usage_in_max_taskid[i] = INFINITE64;
			jobacct->tres_usage_in_min_taskid[i] = INFINITE64;
			jobacct->tres_usage_out_max_taskid[i] = INFINITE64;
			jobacct->tres_usage_out_min_taskid[i] = INFINITE64;
		}

		if (jobacct_id && jobacct_id->nodeid != NO_VAL) {
			jobacct->tres_usage_in_max_nodeid[i] =
				(uint64_t) jobacct_id->nodeid;
			jobacct->tres_usage_in_min_nodeid[i] =
				(uint64_t) jobacct_id->nodeid;
			jobacct->tres_usage_out_max_nodeid[i] =
				(uint64_t) jobacct_id->nodeid;
			jobacct->tres_usage_out_min_nodeid[i] =
				(uint64_t) jobacct_id->nodeid;
		} else {
			jobacct->tres_usage_in_max_nodeid[i] = INFINITE64;
			jobacct->tres_usage_in_min_nodeid[i] = INFINITE64;
			jobacct->tres_usage_out_max_nodeid[i] = INFINITE64;
			jobacct->tres_usage_out_min_nodeid[i] = INFINITE64;
		}
	}
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
	jobacct->pjobs = NULL;
#endif
}

static void _free_tres_usage(struct jobacctinfo *jobacct)
{

	if (jobacct) {
		xfree(jobacct->tres_ids);

		if (jobacct->tres_list &&
		    (jobacct->tres_list != assoc_mgr_tres_list))
			FREE_NULL_LIST(jobacct->tres_list);
#ifdef __METASTACK_LOAD_ABNORMAL		
		xfree(jobacct->cpu_start);
		xfree(jobacct->cpu_end);
		xfree(jobacct->pid_start);
		xfree(jobacct->pid_end);
		xfree(jobacct->node_start);
		xfree(jobacct->node_end);
#endif
		xfree(jobacct->tres_usage_in_max);
		xfree(jobacct->tres_usage_in_max_nodeid);
		xfree(jobacct->tres_usage_in_max_taskid);
		xfree(jobacct->tres_usage_in_min);
		xfree(jobacct->tres_usage_in_min_nodeid);
		xfree(jobacct->tres_usage_in_min_taskid);
		xfree(jobacct->tres_usage_in_tot);
		xfree(jobacct->tres_usage_out_max);
		xfree(jobacct->tres_usage_out_max_nodeid);
		xfree(jobacct->tres_usage_out_max_taskid);
		xfree(jobacct->tres_usage_out_min);
		xfree(jobacct->tres_usage_out_min_nodeid);
		xfree(jobacct->tres_usage_out_min_taskid);
		xfree(jobacct->tres_usage_out_tot);
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
        if(jobacct->pjobs) 
			FREE_NULL_LIST(jobacct->pjobs);
#endif
	}
}

static void _copy_tres_usage(jobacctinfo_t **dest_jobacct,
			     jobacctinfo_t *source_jobacct)
{
	uint32_t i=0;

	xassert(dest_jobacct);

	if (!*dest_jobacct)
		*dest_jobacct = xmalloc(sizeof(jobacctinfo_t));
	else
		_free_tres_usage(*dest_jobacct);

	memcpy(*dest_jobacct, source_jobacct, sizeof(jobacctinfo_t));

	_init_tres_usage(*dest_jobacct, NULL, source_jobacct->tres_count);
#ifdef __METASTACK_LOAD_ABNORMAL
	for (i = 0 ; i < JOBACCTINFO_START_END_ARRAY_SIZE ; i++) {
		(*dest_jobacct)->cpu_start[i] = source_jobacct->cpu_start[i];
		(*dest_jobacct)->cpu_end[i] = source_jobacct->cpu_end[i];
		(*dest_jobacct)->pid_start[i] = source_jobacct->pid_start[i];
		(*dest_jobacct)->pid_end[i] = source_jobacct->pid_end[i];
		(*dest_jobacct)->node_start[i] = source_jobacct->node_start[i];
		(*dest_jobacct)->node_end[i] = source_jobacct->node_end[i];
	}	
#endif
	for (i = 0; i < source_jobacct->tres_count; i++) {
		(*dest_jobacct)->tres_usage_in_max[i] =
			source_jobacct->tres_usage_in_max[i];
		(*dest_jobacct)->tres_usage_in_max_nodeid[i] =
			source_jobacct->tres_usage_in_max_nodeid[i];
		(*dest_jobacct)->tres_usage_in_max_taskid[i] =
			source_jobacct->tres_usage_in_max_taskid[i];
		(*dest_jobacct)->tres_usage_in_min[i] =
			source_jobacct->tres_usage_in_min[i];
		(*dest_jobacct)->tres_usage_in_min_nodeid[i] =
			source_jobacct->tres_usage_in_min_nodeid[i];
		(*dest_jobacct)->tres_usage_in_min_taskid[i] =
			source_jobacct->tres_usage_in_min_taskid[i];
		(*dest_jobacct)->tres_usage_in_tot[i] =
			source_jobacct->tres_usage_in_tot[i];
		(*dest_jobacct)->tres_usage_out_max[i] =
			source_jobacct->tres_usage_out_max[i];
		(*dest_jobacct)->tres_usage_out_max_nodeid[i] =
			source_jobacct->tres_usage_out_max_nodeid[i];
		(*dest_jobacct)->tres_usage_out_max_taskid[i] =
			source_jobacct->tres_usage_out_max_taskid[i];
		(*dest_jobacct)->tres_usage_out_min[i] =
			source_jobacct->tres_usage_out_min[i];
		(*dest_jobacct)->tres_usage_out_min_nodeid[i] =
			source_jobacct->tres_usage_out_min_nodeid[i];
		(*dest_jobacct)->tres_usage_out_min_taskid[i] =
			source_jobacct->tres_usage_out_min_taskid[i];
		(*dest_jobacct)->tres_usage_out_tot[i] =
			source_jobacct->tres_usage_out_tot[i];
	}

	return;
}

/* _acct_kill_step() issue RPC to kill a slurm job step */
static void _acct_kill_step(void)
{
	slurm_msg_t msg;
	job_step_kill_msg_t req;
	job_notify_msg_t notify_req;

	slurm_msg_t_init(&msg);
	memcpy(&notify_req.step_id, &jobacct_step_id,
	       sizeof(notify_req.step_id));
	notify_req.message     = "Exceeded job memory limit";
	msg.msg_type    = REQUEST_JOB_NOTIFY;
	msg.data        = &notify_req;
	slurm_send_only_controller_msg(&msg, working_cluster_rec);

	/*
	 * Request message:
	 */
	memset(&req, 0, sizeof(job_step_kill_msg_t));
	memcpy(&req.step_id, &jobacct_step_id, sizeof(req.step_id));
	req.signal      = SIGKILL;
	req.flags       = 0;
	msg.msg_type    = REQUEST_CANCEL_JOB_STEP;
	msg.data        = &req;

	slurm_send_only_controller_msg(&msg, working_cluster_rec);
}

static bool _jobacct_shutdown_test(void)
{
	bool rc;
	slurm_mutex_lock(&jobacct_shutdown_mutex);
	rc = jobacct_shutdown;
	slurm_mutex_unlock(&jobacct_shutdown_mutex);
	return rc;
}

#ifdef __METASTACK_LOAD_ABNORMAL
static void _poll_data(bool profile, collection_t *collect, write_t *data)
{
	/* Update the data */
	slurm_mutex_lock(&task_list_lock);

	if (task_list)
		(*(ops.poll_data))(task_list, cont_id, profile, collect, data);
	slurm_mutex_unlock(&task_list_lock);
}
#endif

static bool _init_run_test(void)
{
	bool rc;
	slurm_mutex_lock(&init_run_mutex);
	rc = init_run;
	slurm_mutex_unlock(&init_run_mutex);
	return rc;
}
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
static void *apptype_recognition(void *args){
	bool have_recogn = false, have_recogn_weak = false;
	apptype_recogn_thread_args_t *arg = (apptype_recogn_thread_args_t *)args;
	char *apptype_cli = arg->apptype_cli;
	uint32_t profile = arg->profile;
	char *apptype_step = NULL;
	s_p_hashtbl_t *tbl = NULL;
	char *temp = NULL, *temp_weak = NULL, *weak_apptype = NULL;
	collection_t *collect = NULL;
	write_t *write_data = NULL;
	/* Prepare exit criteria */
	if (acct_gather_profile_timer[PROFILE_APPTYPE].freq <= 0) {
		xfree(arg->apptype_cli);
		xfree(arg);
		apptype_recongn_count = 0;
		return NULL;
	}
#if HAVE_SYS_PRCTL_H
	if (prctl(PR_SET_NAME, "acctg_apptype", NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m", __func__, "acctg_apptype");
	}
#endif
	tbl = s_p_unpack_hashtbl(apptype_properties_apptype_buf);
	/*
		Resolve the weak application type of the setting
		When the match is complete, the weak application type will be returned only 
		if the strong application type is not matched.
	*/
	s_p_get_string_2(&weak_apptype, "WEAK_APPTYPE", tbl);
	while (_init_run_test() && !_jobacct_shutdown_test() &&
	       acct_gather_profile_test()) {
		slurm_mutex_lock(&profile_apptype->notify_mutex);
		slurm_cond_wait(&profile_apptype->notify,
				&profile_apptype->notify_mutex);
		slurm_mutex_unlock(&profile_apptype->notify_mutex);

		if (apptype_recongn_count <= 0) 
			break;

		collect = xmalloc(sizeof(collection_t));
		collect->collect_flag = JOBACCT_GATHER_PROFILE_APPTYPE;
		collect->step = true;
		collect->cmdlines = xmalloc(JOBACCTINFO_START_END_ARRAY_SIZE * sizeof(const char *));
		collect->commands = xmalloc(JOBACCTINFO_START_END_ARRAY_SIZE * sizeof(const char *));
		collect->max_cpu_time = total_cpu_time;
		
		_poll_data(0, collect, NULL);
		/*
			According to the collected process information, word segmentation mapping is performed
			 to judge the application type
		*/
		int limit = (collect->app_rec_cnt < JOBACCTINFO_START_END_ARRAY_SIZE 
				? collect->app_rec_cnt 
				: JOBACCTINFO_START_END_ARRAY_SIZE);
		for (int i = 0; i < limit && !have_recogn; i++) {
			int idx = (collect->app_rec_cnt < JOBACCTINFO_START_END_ARRAY_SIZE) 
						? i 
						: (collect->app_rec_cnt + i) % JOBACCTINFO_START_END_ARRAY_SIZE;
			/*
				max_cputime_proc holds the process that currently consumes the most cputime during apptype 
				thread collection. If max_cputime_proc has a value and is consistent with the segmented 
				process name, it means that the process has replaced the application type with cputime 
				because it cannot map the specific application type, so there is no need to judge 
				again. Just skip it
			*/
			if (max_cputime_proc && (xstrcasecmp(collect->commands[idx], max_cputime_proc) == 0))
				break;
			char *input_copy = xstrdup(collect->cmdlines[idx]);
			if (!input_copy) 
				continue;
			char *saveptr1 = NULL, *saveptr2 = NULL;
    		char *token1 = NULL, *token2 = NULL;
			// Press ' /' to segment words
			for (token1 = strtok_r(input_copy, " /", &saveptr1); token1 && !have_recogn; token1 = strtok_r(NULL, " /", &saveptr1)) {
				xfree(temp);
				if (s_p_get_string_2(&temp, token1, tbl) && temp) {
					if (xstrcasestr(weak_apptype, temp)) {
						have_recogn_weak = true;
						xfree(temp_weak);
						temp_weak = xstrdup(temp);
					} else {
						have_recogn = true;
					}
				}
				// Press '.-=_+' to segment words
				for (token2 = strtok_r(token1, ".-=_+", &saveptr2); token2 && !have_recogn; token2 = strtok_r(NULL, ".-=_+", &saveptr2)) {
					xfree(temp);
					if (s_p_get_string_2(&temp, token2, tbl) && temp) {
						if (xstrcasestr(weak_apptype, temp)) {
							have_recogn_weak = true;
							xfree(temp_weak);
							temp_weak = xstrdup(temp);
						} else {
							have_recogn = true;
						}
					}
				}
			}
			xfree(input_copy);
		}

		/* send data */
		if (!have_recogn && collect->max_cpu_time >= total_cpu_time) {
			total_cpu_time = collect->max_cpu_time;
			if (collect->max_cputime_comm) {
				xfree(max_cputime_proc);
				/* Copy max_cputime_proc to apptype_rp for last_resend */
				max_cputime_proc = xstrdup(collect->max_cputime_comm);
			}
		}

		/* free collect memory */
		for (int i = 0 ; i < limit ; i++) {
			xfree(collect->cmdlines[i]);
			xfree(collect->commands[i]);
		}
		xfree(collect->cmdlines);
		xfree(collect->commands);
		xfree(collect->max_cputime_comm);
		xfree(collect);

		if (have_recogn || have_recogn_weak) {
			if (profile & ACCT_GATHER_PROFILE_APPTYPE) {
				/* send_data */
				write_data = xmalloc(sizeof(write_t));
				write_data->send_flag2 = JOBACCT_GATHER_PROFILE_APPTYPE;
				write_data->apptype_step = have_recogn ? xstrdup(temp) : xstrdup(temp_weak);
				write_data->apptype_cli = xstrdup(apptype_cli);
				write_data->have_recogn = true;
				/*
					If the application type is identified, cputime is recorded as the maximum value.
					If the application type is not identified, the process with the largest cputime 
						is selected as the identification result, and the real value of cputime is emitted together.
					Setting cputime to its maximum value also makes a difference.
				*/
				write_data->cputime = UINT64_MAX;
				/* send job apptype data to influxdb */
				_poll_data(1, NULL, write_data);

				xfree(write_data->apptype_step);
				xfree(write_data->apptype_cli);
				xfree(write_data);
				/* The thread is exited when the application type is successfully mapped */
			}
			if (apptype_step) 
				xfree(apptype_step);
			apptype_step = have_recogn ? xstrdup(temp) : xstrdup(temp_weak);
			break;
		} else {
			apptype_recongn_count--;
			if (profile & ACCT_GATHER_PROFILE_APPTYPE) {
				write_data = xmalloc(sizeof(write_t));
				write_data->send_flag2 = JOBACCT_GATHER_PROFILE_APPTYPE;
				write_data->apptype_step = xstrdup(max_cputime_proc);
				write_data->apptype_cli = xstrdup(apptype_cli);
				write_data->cputime = total_cpu_time;
				if (apptype_recongn_count <= 0) {
					/* send job apptype data to influxdb */
					write_data->have_recogn = true;
					_poll_data(1, NULL, write_data);
					
				} else {
					/* send job apptype data to influxdb plugin but no send data to influxdb, just cache */
					write_data->have_recogn = false;
					_poll_data(0, NULL, write_data);
				}
				xfree(write_data->apptype_step);
				xfree(write_data->apptype_cli);
				xfree(write_data);
			}
			if (apptype_step)
				xfree(apptype_step);
			apptype_step = xstrdup(max_cputime_proc);
		}

		if (apptype_recongn_count <= 0) 
			break;

		/* shutting down, woken by jobacct_gather_fini() */
		if (!_init_run_test())
			break;
	}
	if (apptype_step)
		debug3("apptype recognition: apptype_step is %s", apptype_step);
	else
		debug3("apptype recognition: apptype_step not recognized");
	/* free memory */
	xfree(apptype_step);
	xfree(arg->apptype_cli);
	xfree(arg);
	xfree(weak_apptype);
	xfree(temp_weak);
	xfree(temp);
	xfree(max_cputime_proc);
	s_p_hashtbl_destroy(tbl);
	/* 
		apptype_recongn_count is set to 0 to mark the exit of the thread and to prevent further 
		access to the process cmdline information in _poll_data 
	*/
	apptype_recongn_count = 0;
	debug3("acctg_apptype thread exit");
	return NULL;
}
#endif

#ifdef __METASTACK_LOAD_ABNORMAL
/* _acct_send_step() issue RPC to aggregation job step data*/
static void _acct_send_data_step(acct_gather_rank_t *job_send, step_gather_msg_t msg, int timeout)
{
	slurm_msg_t req;
	//step_gather_msg_t msg_send;
	int rc = -1;
	int retcode = 0;

	//memset(&msg_send, 0, sizeof(msg_send));
	memcpy(&msg.step_id, &job_send->step_id, sizeof(msg.step_id));
 
	slurm_msg_t_init(&req);
	slurm_msg_set_r_uid(&req, slurm_conf.slurmd_user_id);
	req.protocol_version = SLURM_PROTOCOL_VERSION;
	req.msg_type = REQUEST_JOB_STEP_DATA;
	req.data = &msg;
	req.address = step_gather.parent_addr_gather;
    
	/* Do NOT change this check to "step_gather.rank != 0", because
	 * there are odd situations where SlurmUser or root could
	 * craft a launch without a valid credential, and no tree information
	 * can be built with out the hostlist from the credential.
	 */
	if (step_gather.parent_rank_gather != -1) {
		/* On error, pause then try sending to parent again.
		 * The parent slurmstepd may just not have started yet, because
		 * of the way that the launch message forwarding works.
		 */
		retcode = slurm_send_recv_rc_msg_only_one(&req, &rc, timeout);
		debug("Rank %d sending data to rank %d ip parent = %pA is sucessed ",
				 step_gather.rank_gather, step_gather.parent_rank_gather, &req.address);
		if ((retcode != 0) || (rc != 0))
			debug("Rank %d sending data to rank %d error", step_gather.rank_gather, step_gather.parent_rank_gather);
	} 
	
}

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION

static char **_build_watch_dog_env(acct_gather_rank_t *watch_dog)
{
	if(watch_dog == NULL)
		return NULL;
	char **my_env;
	my_env = xmalloc(sizeof(char *));
	my_env[0] = NULL;
    
	setenvf(&my_env, "SLURM_JOB_ID", "%u", watch_dog->job_id);
	setenvf(&my_env, "SLURM_STEP_ID", "%d", watch_dog->step_id.step_id);	
	setenvf(&my_env, "SLURM_JOB_UID", "%u", watch_dog->uid);
	setenvf(&my_env, "SLURM_JOB_GID", "%u", watch_dog->gid);
	setenvf(&my_env, "SLURM_STEP_WATCH_DOG", "%s", watch_dog->watch_dog);
	if(watch_dog->job_stdout != NULL)
		setenvf(&my_env, "SLURM_JOB_STDOUT", "%s", watch_dog->job_stdout);
	if(watch_dog->job_stderr != NULL)
		setenvf(&my_env, "SLURM_JOB_STDERR", "%s", watch_dog->job_stderr);

	slurm_mutex_lock(&watch_dog_env_lock);
	if((watch_dog_node_step_collect.update == true) || (update_watch_dog == true)) {
		setenvf(&my_env, "SLURM_JOB_NODE_AVE_CPU", "%.2f",watch_dog_node_step_collect.cpu_step_ave);
		setenvf(&my_env, "SLURM_JOB_NODE_NOW_CPU", "%.2f", watch_dog_node_step_collect.cpu_step_real);
		setenvf(&my_env, "SLURM_JOB_NODE_MEM", "%ld", watch_dog_node_step_collect.mem_step);
		setenvf(&my_env, "SLURM_JOB_NODE_VMEM", "%ld", watch_dog_node_step_collect.vmem_step);
		setenvf(&my_env, "SLURM_JOB_NODE_PAGES", "%ld", watch_dog_node_step_collect.step_pages);
		watch_dog_node_step_collect.update = false;
		if(update_watch_dog == false)
			update_watch_dog = true;
	}

	if(watch_dog_node_step_collect.pids) {
		char *pid_str = NULL;
		if (watch_dog_node_step_collect.npids > 0) {
			for (int i = 0; i < watch_dog_node_step_collect.npids; i++) {
				if(i == 0)
					xstrfmtcat(pid_str, "%d",watch_dog_node_step_collect.pids[i]);
				else
					xstrfmtcat(pid_str, ",%d",watch_dog_node_step_collect.pids[i]);
			}
			setenvf(&my_env, "SLURM_STEP_NODE_PIDS", "%s", pid_str);
			xfree(pid_str);
			xfree(watch_dog_node_step_collect.pids);
		}			
	}	

	slurm_mutex_unlock(&watch_dog_env_lock);
	if(watch_dog->head_node) {
		slurm_mutex_lock(&watch_dog_all_env_lock);
		/*set environment variables for the head node.*/
		if(watch_dog_collect.update || update_node_watch_dog) {
			setenvf(&my_env, "SLURM_JOB_AVE_CPU", "%.2f", watch_dog_collect.cpu_step_ave);
			setenvf(&my_env, "SLURM_JOB_NOW_CPU", "%.2f", watch_dog_collect.cpu_step_real);
			setenvf(&my_env, "SLURM_JOB_MEM", "%ld", watch_dog_collect.mem_step);
			setenvf(&my_env, "SLURM_JOB_VMEM", "%ld", watch_dog_collect.vmem_step);
			setenvf(&my_env, "SLURM_JOB_PAGES", "%ld", watch_dog_collect.step_pages);
			watch_dog_collect.update = false;
			if(update_node_watch_dog == false)
				update_node_watch_dog = true;
		}
		slurm_mutex_unlock(&watch_dog_all_env_lock);
	}

	return my_env;
}

static void *step_watch_dog(void *args)
{
	acct_gather_rank_t *watch_dog_tran =(acct_gather_rank_t *) args;
	const char watch_dog[32] = "watch dog";
	pid_t cpid = -1 ;
	bool  start = false;
	time_t diff_time = time(NULL);
 #if HAVE_SYS_PRCTL_H
	if (prctl(PR_SET_NAME, "watch_dog_acct", NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m", __func__, "acctg_watch_dog_step");
	}
#endif
	while (_init_run_test() && !_jobacct_shutdown_test() &&
	       acct_gather_profile_test()) {
		/* Do this until shutdown is requested */
		struct timeval now;
		struct timespec timeout = {0, 0};
		gettimeofday(&now, NULL);
		timeout.tv_sec = now.tv_sec;
		timeout.tv_nsec = now.tv_usec*1000;

		timeout.tv_nsec = timeout.tv_nsec  + 100000000;  // add 100ms，100000000ns
		if (timeout.tv_nsec >= 1000000000) {
			timeout.tv_sec += 1;
			timeout.tv_nsec -= 1000000000;
		}

		slurm_mutex_lock(&profile_watch_dog_timer->notify_mutex);
		int ret = pthread_cond_timedwait(&profile_watch_dog_timer->notify,
				&profile_watch_dog_timer->notify_mutex, &timeout);	
		slurm_mutex_unlock(&profile_watch_dog_timer->notify_mutex);
        
		/* shutting down, woken by jobacct_gather_fini() */
		if (!_init_run_test())
			break;
		if (watch_dog_tran->watch_dog_script == NULL || watch_dog_tran->watch_dog_script[0] == '\0')
			continue;
		/* Prevent job steps from running as soon as they are started */
		if(!start) {
			if(difftime(time(NULL), diff_time) >= 1)
				start = true;
		}
		if(!start)
			continue;
		if(ret == 0) {
			char **my_env;
			my_env = _build_watch_dog_env(watch_dog_tran);
			run_command_args_t run_script_msg;
			char *cmd_argv[2] = {0};
			memset(&run_script_msg, 0, sizeof(run_script_msg));
			run_script_msg.job_id = watch_dog_tran->job_id;
			run_script_msg.script_type = watch_dog;
			run_script_msg.env = my_env;
			run_script_msg.max_wait = -1;
			run_script_msg.script_path = watch_dog_tran->watch_dog_script;
			run_script_msg.script_argv = cmd_argv;
			debug3("attempting to run  [%s]", run_script_msg.script_path);
			if(run_watch_dog_wait(&cpid, false))
				run_watch_dog_command(&run_script_msg, &cpid, watch_dog_tran->gid, watch_dog_tran->uid, 0);
			if(my_env) {
				for (int i = 0; my_env[i]; i++)
					xfree(my_env[i]);

				xfree(my_env);
			}
		} else {
			if((cpid != -1) && (cpid != 0))
				run_watch_dog_wait(&cpid, false);
		}
	}

	if((cpid != -1) && (cpid != 0))
		run_watch_dog_wait(&cpid, true);

	if(watch_dog_tran) {
		xfree(watch_dog_tran->watch_dog);
		xfree(watch_dog_tran->watch_dog_script);
		if(watch_dog_tran->job_stdout)
			xfree(watch_dog_tran->job_stdout);
		if(watch_dog_tran->job_stderr)
			xfree(watch_dog_tran->job_stderr);
		xfree(watch_dog_tran);
	}
	return NULL;
}
#endif

static void *step_collect(void *args)
{
	acct_gather_rank_t *job_info = (acct_gather_rank_t *) args;
	uint32_t rank = -1;
	bool head = false;
	bool update = false;
	write_t *write_data = NULL;
    write_data = xmalloc(sizeof(write_t));
	time_t record_time = 0;
	uint64_t threshold = 0;
	uint64_t minutes_synch = 0;
	uint64_t minutes_count = 0;

	bool  start = false;
	time_t diff_time = time(NULL);
	
	/* write_data data initialization*/
	write_data->cpu_step_ave = 0.0;
	write_data->cpu_step_real = 0.0;
	write_data->mem_step = 0;
	write_data->vmem_step = 0;
	write_data->step_pages = 0;
	write_data->send_flag = false;
	write_data->load_flag = 0;
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	write_data->send_flag2 = 0;
#endif
#if HAVE_SYS_PRCTL_H
	if (prctl(PR_SET_NAME, "acctg_step", NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m", __func__, "acctg_step");
	}
#endif

	minutes_synch = job_info->timer / job_info->frequency;
	step_gather_msg_t msg;
	memset(&msg, 0, sizeof(msg));
	while (_init_run_test() && !_jobacct_shutdown_test() &&
	       acct_gather_profile_test()) {
		 /* Do this until shutdown is requested */
		slurm_mutex_lock(&profile_stepd->notify_mutex);
		slurm_cond_wait(&profile_stepd->notify,
				&profile_stepd->notify_mutex);
		slurm_mutex_unlock(&profile_stepd->notify_mutex);

		/* shutting down, woken by jobacct_gather_fini() */
		if (!_init_run_test())
			break;


		if(job_info->frequency <=0) {
			continue;
		}

	  if(!start) {
			if(difftime(time(NULL), diff_time) >= (job_info->frequency))
				start = true;
		} else
			minutes_count++;

		slurm_mutex_lock(&share_data.lock);

		if(minutes_count >= minutes_synch ) {
			msg.rank = rank;
			msg.cpu_util = share_data.cpu_step_real;
			msg.cpu_ave =  share_data.cpu_step_ave;
			msg.mem_real = share_data.mem_step;
			msg.vmem_real = share_data.vmem_step;
			msg.load_flag = share_data.load_flag;
			msg.page_fault  = share_data.step_pages;
			msg.node_alloc_cpu =(uint64_t)job_info->node_alloc_cpu;
			share_data.start = time(NULL);
			update = true;
		}

		slurm_mutex_unlock(&share_data.lock);
	    record_time = time(NULL);
		double diff = difftime(record_time, share_data.start);
	    
		/*batch step*/	

		if((job_info->step_id.step_id == SLURM_BATCH_SCRIPT) && update) {

			update = false;
			minutes_count = 0;

			write_data->cpu_step_ave = msg.cpu_ave;
			write_data->cpu_step_real = msg.cpu_util;
			write_data->mem_step = msg.mem_real;
			write_data->vmem_step = msg.vmem_real;
			write_data->step_pages = msg.page_fault;
			write_data->send_flag = false;
			write_data->load_flag = msg.load_flag;
			write_data->cpu_threshold = job_info->cpu_min_load;
			write_data->node_alloc_cpu = job_info->node_alloc_cpu;
			write_data->timer = job_info->timer;

			/*abnormal event determination of cpu*/
			threshold = job_info->cpu_min_load * write_data->node_alloc_cpu;
			write_data->cpu_threshold = threshold;
			if(threshold > write_data->cpu_step_real) {
				write_data->cpu_start = record_time - job_info->timer;
				write_data->cpu_end = record_time;
				write_data->load_flag=write_data->load_flag|LOAD_LOW;
			}

			/*abnormal event determination of process status*/
			if(write_data->load_flag & PROC_AB) {
				write_data->pid_start = record_time - job_info->timer;
				write_data->pid_end = record_time;
//				write_data->load_flag = write_data->load_flag | PROC_AB;
			}

		    /*abnormal event determination of node status*/
			if(write_data->load_flag & JNODE_STAT) {
				write_data->node_start = record_time - job_info->timer;
				write_data->node_end = record_time;
//				write_data->load_flag = write_data->load_flag | JNODE_STAT;
			}
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
			slurm_mutex_lock(&watch_dog_all_env_lock);
			watch_dog_collect.cpu_step_ave  = write_data->cpu_step_ave ;
			watch_dog_collect.cpu_step_real = write_data->cpu_step_real ;
			watch_dog_collect.mem_step 	    = write_data->mem_step ;
			watch_dog_collect.vmem_step     = write_data->vmem_step ;
			watch_dog_collect.step_pages    = write_data->step_pages ;
			watch_dog_collect.update		= true;
			slurm_mutex_unlock(&watch_dog_all_env_lock);
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
			write_data->send_flag2 |= JOBACCT_GATHER_PROFILE_ABNORMAL;
#endif
			_poll_data(1, NULL, write_data);

			memset(&msg, 0, sizeof(msg));
		}

       	if((job_info->step_id.step_id != SLURM_BATCH_SCRIPT) && update)  {

			/* digital work steps */
			slurm_mutex_lock(&step_gather.lock);
			/* It is necessary to keep the time points of the job step collection of each node consistent.*/
			
			if((step_gather.children_gather <= 0) && (step_gather.parent_rank_gather >= 0) ) {

				int millisecond_delay = 0;
				minutes_count = 0;
				update = false;
				/*convert to milliseconds*/
				if(step_gather.max_depth_gather > 0)
					millisecond_delay = (job_info->timer*1000)/step_gather.max_depth_gather;
				else
					millisecond_delay = job_info->timer*1000;

				debug3("Rank %d sending data to rank %d ip parent = %pA,jobid is %ps  msg.cpu_util=%.2f millisecond_delay=%d",
						rank, step_gather.parent_rank_gather, &step_gather.parent_addr_gather, &job_info->step_id, msg.cpu_util, millisecond_delay);
				//_acct_send_data_step (jobinfo, msg, time_delay * 1000);
				_acct_send_data_step (job_info, msg, millisecond_delay);
				memset(&msg, 0, sizeof(msg));

			} else if((step_gather.children_gather > 0) && (step_gather.parent_rank_gather >=0) ) {

				if(minutes_count >= minutes_synch)
					minutes_count = 0;

				rank = step_gather.rank_gather;
				double time_delay = 0;
				time_delay = job_info->timer - job_info->frequency;

				if((step_gather.wait_child_count == step_gather.children_gather) || (diff >= time_delay)) {
					debug("Waiting for child nodes number is %d, diff is %f  time delay %f ", (step_gather.children_gather - step_gather.wait_child_count), diff, time_delay);
					if(step_gather.wait_child_count == step_gather.children_gather) {
						msg.cpu_ave += step_gather.step_cpu_ave;
						msg.cpu_util += step_gather.step_cpu;
						msg.mem_real += step_gather.step_mem;
						msg.vmem_real += step_gather.step_vmem;
						msg.page_fault += step_gather.page_fault;
						msg.node_alloc_cpu +=step_gather.node_alloc_cpu;

						msg.load_flag  = msg.load_flag | step_gather.load_status; 
						//msg.depth_child = step_gather.children_gather+1;

					} else if(diff >= time_delay) {
						msg.cpu_ave += step_gather.step_cpu_ave;
						msg.cpu_util += step_gather.step_cpu;
						msg.mem_real += step_gather.step_mem;
						msg.vmem_real += step_gather.step_vmem;
						msg.page_fault += step_gather.page_fault;
						msg.node_alloc_cpu +=step_gather.node_alloc_cpu;

						msg.load_flag  = msg.load_flag | step_gather.load_status; 
						msg.load_flag  = msg.load_flag | JNODE_STAT;
					}

					int millisecond_delay = 0;
					if(step_gather.depth_gather > 0)
						millisecond_delay = (job_info->timer - diff) * 1000 /step_gather.depth_gather;
					else
						millisecond_delay = 10*1000;

					_acct_send_data_step(job_info, msg, millisecond_delay);
					update = false;
			
			        memset(&msg, 0, sizeof(msg));
					step_gather.wait_child_count = 0;
					step_gather.step_cpu_ave = 0.0;
					step_gather.step_cpu = 0.0;
					step_gather.step_mem = 0;
					step_gather.step_vmem = 0;
					step_gather.page_fault = 0;
					step_gather.load_status = 0 ;
					step_gather.node_alloc_cpu = 0;
				}
		
			} else if((step_gather.parent_rank_gather < 0)) {
				
				if(minutes_count >= minutes_synch)
					minutes_count = 0;
				
				int time_delay = 0;
	
				time_delay = job_info->timer - job_info->frequency;
				if((step_gather.children_gather == -1 ) || (step_gather.wait_child_count == step_gather.children_gather) || (diff >= time_delay)) {
					/*it is necessary to wait until all node data is aggregated to ensure data consistency.*/
					if(step_gather.wait_child_count == step_gather.children_gather) {
						msg.cpu_ave += step_gather.step_cpu_ave;
						msg.cpu_util += step_gather.step_cpu;
						msg.mem_real += step_gather.step_mem;
						msg.vmem_real += step_gather.step_vmem;
						msg.page_fault += step_gather.page_fault;
						msg.node_alloc_cpu +=step_gather.node_alloc_cpu;
						msg.load_flag  = msg.load_flag | step_gather.load_status; /*if have node not respond set load_status*/
					} else if(diff >= time_delay) {
						msg.cpu_ave += step_gather.step_cpu_ave;
						msg.cpu_util += step_gather.step_cpu;
						msg.mem_real += step_gather.step_mem;
						msg.vmem_real += step_gather.step_vmem;
						msg.page_fault += step_gather.page_fault;
						msg.node_alloc_cpu += step_gather.node_alloc_cpu;

						msg.load_flag  = msg.load_flag | step_gather.load_status; /*if have node not respond set load_status*/
						msg.load_flag  = msg.load_flag | JNODE_STAT;
					}

					write_data->cpu_step_ave = msg.cpu_ave;
					write_data->cpu_step_real = msg.cpu_util;
					write_data->mem_step = msg.mem_real;
					write_data->vmem_step = msg.vmem_real;
					write_data->step_pages = msg.page_fault;
					write_data->send_flag = true;
					write_data->load_flag = msg.load_flag;
					write_data->node_alloc_cpu = msg.node_alloc_cpu;
					write_data->timer = job_info->timer;
                    head = true;
					update = false;

					memset(&msg, 0, sizeof(msg));
					step_gather.wait_child_count = 0;
					step_gather.step_cpu_ave = 0.0;
					step_gather.step_cpu = 0;
					step_gather.step_mem = 0;
					step_gather.step_vmem = 0;
					step_gather.page_fault = 0;
					step_gather.load_status = 0 ;
					step_gather.node_alloc_cpu = 0;
				}

			} 
			slurm_mutex_unlock(&step_gather.lock);
		}

		if(head) {
			head = false;
			//minutes_count = 0;
			if(write_data->send_flag) {
				write_data->send_flag = false;
				//write_data->cpu_threshold = job_info->cpu_min_load;	
				
				/*need to multiply by allocated alloc cpu*/
				threshold = job_info->cpu_min_load * write_data->node_alloc_cpu;
				write_data->cpu_threshold = threshold;
				if(threshold > write_data->cpu_step_real) {
					write_data->cpu_start = record_time - job_info->timer;
					write_data->cpu_end = record_time;
					write_data->load_flag=write_data->load_flag| LOAD_LOW;
				}
				if(write_data->load_flag & PROC_AB){
					write_data->pid_start = record_time - job_info->timer;
					write_data->pid_end = record_time;
//					write_data->load_flag = write_data->load_flag | PROC_AB;
				}
				if(write_data->load_flag & JNODE_STAT){
					write_data->node_start = record_time - job_info->timer;
					write_data->node_end = record_time;
//					write_data->load_flag = write_data->load_flag | JNODE_STAT;
				}
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
				slurm_mutex_lock(&watch_dog_all_env_lock);
				watch_dog_collect.cpu_step_ave  = write_data->cpu_step_ave ;
				watch_dog_collect.cpu_step_real = write_data->cpu_step_real ;
				watch_dog_collect.mem_step 	    = write_data->mem_step ;
				watch_dog_collect.vmem_step     = write_data->vmem_step ;
				watch_dog_collect.step_pages    = write_data->step_pages ;
				watch_dog_collect.update		= true;
				slurm_mutex_unlock(&watch_dog_all_env_lock);
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
				write_data->send_flag2 |= JOBACCT_GATHER_PROFILE_ABNORMAL;
#endif
				_poll_data(1, NULL, write_data);
			}

		}
	}

	xfree(write_data);
	if(job_info)
		xfree(job_info);
	return NULL;
}
#endif

/* _watch_tasks() -- monitor slurm jobs and track their memory usage
 */

static void *_watch_tasks(void *arg)
{
#ifdef __METASTACK_LOAD_ABNORMAL
	time_t diff = 0;
	acct_gather_rank_t *job_message = (acct_gather_rank_t *) arg;
	int count = 0,  update_share = 0;
	collection_t *collect = NULL;
	List fifo = NULL;
	bool reset = false;
	bool start = false;
    /*Resource consumption variable*/
    double total_step_cpuutil = 0.0;
    double  tmp_cpuutil = 0.0;
	collect = xmalloc(sizeof(collection_t));
    diff = time(NULL);
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	int max_watch_dog_pid = 2000;
	collect->npids = 0;
	collect->pids = xmalloc(sizeof(pid_t)* max_watch_dog_pid);
#endif
	if(job_message->timer > 0)
		count = (job_message->timer) / job_message->frequency; 
	
	/*Set time limit*/
	if(count > MAX_SIZE) 
		count = MAX_SIZE;

	collect->step = job_message->switch_step;
	if((count > 0) && (collect->step)) {
		fifo = list_create(xfree_ptr);
	}
#endif 
#if HAVE_SYS_PRCTL_H
	if (prctl(PR_SET_NAME, "acctg", NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m", __func__, "acctg");
	}
#endif

	while (_init_run_test() && !_jobacct_shutdown_test() &&
	       acct_gather_profile_test()) {
		/* Do this until shutdown is requested */
		slurm_mutex_lock(&profile_timer->notify_mutex);
		slurm_cond_wait(&profile_timer->notify,
				&profile_timer->notify_mutex);
		slurm_mutex_unlock(&profile_timer->notify_mutex);

		/* shutting down, woken by jobacct_gather_fini() */
		if (!_init_run_test())
			break;

		slurm_mutex_lock(&g_context_lock);
#ifdef __METASTACK_LOAD_ABNORMAL
		if(!start) {
			if(difftime(time(NULL), diff) >= (job_message->frequency))
				start = true;
		}
		if((collect->step) && (count > 0))
			_poll_data(1, collect, NULL);	
		else
			_poll_data(1, NULL, NULL);
#endif
		slurm_mutex_unlock(&g_context_lock);
#ifdef __METASTACK_LOAD_ABNORMAL
		double *item = NULL;
		if((start) && (collect->step) && (count > 0)) {
			/*******************
			 *calculate threshold
			 *******************/
			if(list_count(fifo) < count) {
                /*Reached queue length*/
                double *cpu_step_real = xmalloc(sizeof(double));
                *cpu_step_real = collect->cpu_step_real;
                list_enqueue(fifo, cpu_step_real);
                total_step_cpuutil += collect->cpu_step_real;
            }

            if(list_count(fifo) >= count) {
                /*dequeue*/
                item = (double*)list_dequeue(fifo);
                if(*item >= 0) {
                    tmp_cpuutil = total_step_cpuutil / (list_count(fifo) + 1);
                    total_step_cpuutil = total_step_cpuutil - *item;
                } else {
                    total_step_cpuutil = 0;
                    reset = true;
                }  
			}

			if(reset) {
				list_flush(fifo);
                reset = false;
			}
			update_share++;

			if(update_share >= count ) {
				update_share  = 0; 
				slurm_mutex_lock(&share_data.lock);
				share_data.cpu_step_real = tmp_cpuutil;

				share_data.cpu_step_ave = collect->cpu_step_ave;
				share_data.load_flag =  collect->load_flag;
				share_data.mem_step = collect->mem_step;
				share_data.vmem_step = collect->vmem_step;
				share_data.step_pages = collect->step_pages;
				//share_data.update = true;

				slurm_mutex_unlock(&share_data.lock);
			} 
		}
		if(item)
			xfree(item);
#endif
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
		if(collect && collect->pids) {
			slurm_mutex_lock(&watch_dog_env_lock);
			watch_dog_node_step_collect.cpu_step_real = collect->cpu_step_real;
			watch_dog_node_step_collect.cpu_step_ave  = collect->cpu_step_ave;				
			watch_dog_node_step_collect.mem_step	  = collect->mem_step;
			watch_dog_node_step_collect.vmem_step	  = collect->vmem_step;
			watch_dog_node_step_collect.step_pages	  = collect->step_pages;
			watch_dog_node_step_collect.npids		  = collect->npids;
			watch_dog_node_step_collect.update		  = true;
			if(watch_dog_node_step_collect.pids)
				xfree(watch_dog_node_step_collect.pids);
			
			if((collect->npids <= max_watch_dog_pid) && (collect->npids > 0)) {
				watch_dog_node_step_collect.pids = xmalloc(sizeof(pid_t) * (watch_dog_node_step_collect.npids)+ 1);
				memcpy(watch_dog_node_step_collect.pids, collect->pids, (sizeof(pid_t) * (collect->npids)));
			} else if(collect->npids > max_watch_dog_pid) {
				watch_dog_node_step_collect.pids = xmalloc(sizeof(pid_t) * (max_watch_dog_pid) + 1);
				memcpy(watch_dog_node_step_collect.pids, collect->pids, (sizeof(pid_t) * max_watch_dog_pid));
			}
			slurm_mutex_unlock(&watch_dog_env_lock);
		}
#endif
	}
#ifdef __METASTACK_LOAD_ABNORMAL
	if((collect->step) && (count > 0)) {
		FREE_NULL_LIST(fifo);
	}
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	if(watch_dog_node_step_collect.pids)
		xfree(watch_dog_node_step_collect.pids);
	if(collect) 
		xfree(collect->pids);
#endif
	xfree(collect);
	if(job_message)
		xfree(job_message);
#endif
	return NULL;
}

static void _jobacctinfo_create_tres_usage(jobacct_id_t *jobacct_id,
					   struct jobacctinfo *jobacct)
{
	assoc_mgr_lock_t locks = { .tres = READ_LOCK };

	assoc_mgr_lock(&locks);
	_init_tres_usage(jobacct, jobacct_id, g_tres_count);
	assoc_mgr_unlock(&locks);
}

static void _jobacctinfo_aggregate_tres_usage(jobacctinfo_t *dest,
					      jobacctinfo_t *from)
{
	uint32_t i = 0;

	xassert(dest->tres_count == from->tres_count);

	for (i = 0; i < dest->tres_count; i++) {
		if (from->tres_usage_in_max[i] != INFINITE64) {
			if ((dest->tres_usage_in_max[i] == INFINITE64) ||
			    (dest->tres_usage_in_max[i] <
			     from->tres_usage_in_max[i])) {
				dest->tres_usage_in_max[i] =
					from->tres_usage_in_max[i];
				/*
				 * At the time of writing Energy was only on a
				 * per node basis.
				 */
				if (i != TRES_ARRAY_ENERGY)
					dest->tres_usage_in_max_taskid[i] =
						from->
						tres_usage_in_max_taskid[i];
				dest->tres_usage_in_max_nodeid[i] =
					from->tres_usage_in_max_nodeid[i];
			}
		}

		if (from->tres_usage_in_min[i] != INFINITE64) {
			if ((dest->tres_usage_in_min[i] == INFINITE64) ||
			    (dest->tres_usage_in_min[i] >
			     from->tres_usage_in_min[i])) {
				dest->tres_usage_in_min[i] =
					from->tres_usage_in_min[i];
				/*
				 * At the time of writing Energy was only on a
				 * per node basis.
				 */
				if (i != TRES_ARRAY_ENERGY)
					dest->tres_usage_in_min_taskid[i] =
						from->
						tres_usage_in_min_taskid[i];
				dest->tres_usage_in_min_nodeid[i] =
					from->tres_usage_in_min_nodeid[i];
			}
		}

		if (from->tres_usage_in_tot[i] != INFINITE64) {
			if (dest->tres_usage_in_tot[i] == INFINITE64)
				dest->tres_usage_in_tot[i] =
					from->tres_usage_in_tot[i];
			else
				dest->tres_usage_in_tot[i] +=
					from->tres_usage_in_tot[i];
		}

		if (from->tres_usage_out_max[i] != INFINITE64) {
			if ((dest->tres_usage_out_max[i] == INFINITE64) ||
			    (dest->tres_usage_out_max[i] <
			     from->tres_usage_out_max[i])) {
				dest->tres_usage_out_max[i] =
					from->tres_usage_out_max[i];
				/*
				 * At the time of writing Energy was only on a
				 * per node basis.
				 */
				if (i != TRES_ARRAY_ENERGY)
					dest->tres_usage_out_max_taskid[i] =
						from->
						tres_usage_out_max_taskid[i];
				dest->tres_usage_out_max_nodeid[i] =
					from->tres_usage_out_max_nodeid[i];
			}
		}

		if (from->tres_usage_out_min[i] != INFINITE64) {
			if ((dest->tres_usage_out_min[i] == INFINITE64) ||
			    (dest->tres_usage_out_min[i] >
			     from->tres_usage_out_min[i])) {
				dest->tres_usage_out_min[i] =
					from->tres_usage_out_min[i];
				/*
				 * At the time of writing Energy was only on a
				 * per node basis.
				 */
				if (i != TRES_ARRAY_ENERGY)
					dest->tres_usage_out_min_taskid[i] =
						from->
						tres_usage_out_min_taskid[i];
				dest->tres_usage_out_min_nodeid[i] =
					from->tres_usage_out_min_nodeid[i];
			}
		}

		if (from->tres_usage_out_tot[i] != INFINITE64) {
			if (dest->tres_usage_out_tot[i] == INFINITE64)
				dest->tres_usage_out_tot[i] =
					from->tres_usage_out_tot[i];
			else
				dest->tres_usage_out_tot[i] +=
					from->tres_usage_out_tot[i];
		}
	}
}

static void _jobacctinfo_2_stats_tres_usage(slurmdb_stats_t *stats,
					    jobacctinfo_t *jobacct)
{
	assoc_mgr_lock_t locks = { .tres = READ_LOCK };
	uint32_t flags = TRES_STR_FLAG_ALLOW_REAL | TRES_STR_FLAG_SIMPLE;
	assoc_mgr_lock(&locks);

	stats->tres_usage_in_ave = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_in_tot, flags, true);
	stats->tres_usage_in_tot = xstrdup(stats->tres_usage_in_ave);
	stats->tres_usage_in_max = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_in_max, flags, true);
	stats->tres_usage_in_max_nodeid = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_in_max_nodeid, flags, true);
	stats->tres_usage_in_max_taskid = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_in_max_taskid, flags, true);
	stats->tres_usage_in_min = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_in_min, flags, true);
	stats->tres_usage_in_min_nodeid = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_in_min_nodeid, flags, true);
	stats->tres_usage_in_min_taskid = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_in_min_taskid, flags, true);
	stats->tres_usage_out_ave = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_out_tot, flags, true);
	stats->tres_usage_out_tot = xstrdup(stats->tres_usage_out_ave);
	stats->tres_usage_out_max = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_out_max, flags, true);
	stats->tres_usage_out_max_taskid = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_out_max_taskid, flags, true);
	stats->tres_usage_out_max_nodeid = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_out_max_nodeid, flags, true);
	stats->tres_usage_out_min = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_out_min, flags, true);
	stats->tres_usage_out_min_nodeid = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_out_min_nodeid, flags, true);
	stats->tres_usage_out_min_taskid = assoc_mgr_make_tres_str_from_array(
		jobacct->tres_usage_out_min_taskid, flags, true);
	assoc_mgr_unlock(&locks);
}

extern int jobacct_gather_init(void)
{
	char    *plugin_type = "jobacct_gather";
	int	retval=SLURM_SUCCESS;

	if (slurmdbd_conf || (_init_run_test() && g_context))
		return retval;

	slurm_mutex_lock(&g_context_lock);
	if (g_context)
		goto done;

	g_context = plugin_context_create(plugin_type,
					  slurm_conf.job_acct_gather_type,
					  (void **) &ops, syms, sizeof(syms));

	if (!g_context) {
		error("cannot create %s context for %s",
		      plugin_type, slurm_conf.job_acct_gather_type);
		retval = SLURM_ERROR;
		goto done;
	}

	if (!xstrcasecmp(slurm_conf.job_acct_gather_type,
			 "jobacct_gather/none")) {
		plugin_polling = false;
		goto done;
	}

	slurm_mutex_lock(&init_run_mutex);
	init_run = true;
	slurm_mutex_unlock(&init_run_mutex);

	/* only print the WARNING messages if in the slurmctld */
	if (!running_in_slurmctld())
		goto done;

	if (!xstrcasecmp(slurm_conf.proctrack_type, "proctrack/pgid"))
		info("WARNING: We will use a much slower algorithm with proctrack/pgid, use Proctracktype=proctrack/linuxproc or some other proctrack when using %s",
		     slurm_conf.job_acct_gather_type);

	if (!xstrcasecmp(slurm_conf.accounting_storage_type,
	                 ACCOUNTING_STORAGE_TYPE_NONE)) {
		error("WARNING: Even though we are collecting accounting "
		      "information you have asked for it not to be stored "
		      "(%s) if this is not what you have in mind you will "
		      "need to change it.", ACCOUNTING_STORAGE_TYPE_NONE);
	}

done:
	slurm_mutex_unlock(&g_context_lock);
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	/*
		Do not execute apptype_properties_conf_init conditionally. Otherwise, it may cause 
		the slurmd service to crash under multi-threading conditions.
	*/
	if (retval == SLURM_SUCCESS) {
		retval = apptype_properties_conf_init();
	}
#endif
	if (retval != SLURM_SUCCESS)
		fatal("can not open the %s plugin",
		      slurm_conf.job_acct_gather_type);

	return(retval);
}
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
static int _get_option_from_file(s_p_options_t **full_options, int *full_options_cnt)
{
	char *conf_path = NULL;
	struct stat buf;
	int size = 0, capacity = 10;
	s_p_options_t *options = NULL;

	conf_path = get_extra_conf_path("apptype.properties");
	if ((conf_path == NULL) || (stat(conf_path, &buf) == -1)) {
		debug2("No apptype.properties file (%s)", conf_path);
		xfree(conf_path);
		return SLURM_ERROR;
	} 

	debug2("Build option struct array from apptype.properties file %s", conf_path);
	/* Build an array of option structs by reading apptype.properties */
	FILE *file = fopen(conf_path, "r");
	if (!file) {
		debug2("Failed to open file: %s to Build s_p_options_t", conf_path);
		xfree(conf_path);
		return SLURM_ERROR;
	}
	options = xmalloc(capacity * sizeof(s_p_options_t));
	if(!options){
		fclose(file);
		xfree(conf_path);
		return SLURM_ERROR;
	}
	char line[256] = {0};
	while (fgets(line, sizeof(line), file)) {
		/* Remove blank lines and comment lines */
		if (line[0] == '\n' || line[0] == '#') {
			continue;
		}
		/* Remove end-of-line newlines */
		line[strcspn(line, "\n")] = '\0';

		/* Press '=' to split the key and value and extract the key */
		char *saveptr = NULL;
		char *key = strtok_r(line, "=", &saveptr);
		if (!key) continue;

		if (size >= capacity) {
			capacity *= 2;
			options = xrealloc(options, capacity * sizeof(s_p_options_t));
			if (!options) {
				fclose(file);
				return -1;
			}
		}

		/* Save the key and fix the type to S_P_STRING */
		options[size].key = xstrdup(key);
		options[size].type = S_P_STRING;
		size++;
	}
	fclose(file);

	if (size < capacity) {
		options[size].key = NULL;
	}

	transfer_s_p_options(full_options, options, full_options_cnt);
	for (int i = 0; i < size; i++) { 
		xfree(options[i].key);
	}
	xfree(options);
	xfree(conf_path);
	return SLURM_SUCCESS;
}
extern int apptype_properties_conf_init(void)
{
	char *conf_path = NULL;
	struct stat buf;
	int rc = SLURM_SUCCESS;
	s_p_options_t *full_options = NULL;
	int full_options_cnt = 0, i;
	s_p_hashtbl_t *tbl = NULL;
	
	/* Determine if the apptype is set and load the appType.properties file */
	if (inited)
		return SLURM_SUCCESS;
	inited = 1;

	/* slurmctld daemon is no need to read apptype.properties */
	if (running_in_slurmctld())
		return SLURM_SUCCESS;

	/*
		If apptype recognition is not configured, then use init_buf(0). The reason for this is 
		that in version 22 of slurmstepd, it reads the local configuration file instead of being 
		sent by slurmd. Therefore, if buf is not initialized, it may cause the content received 
		by the slurmstep end and the content sent by the slurmd end to be mismatched, thereby 
		leading to service failure.
	*/
	if (acct_gather_parse_freq(PROFILE_APPTYPE, slurm_conf.job_acct_gather_freq) <= 0) {
		apptype_properties_apptype_buf = init_buf(0);
		return SLURM_SUCCESS;
	}

	rc += _get_option_from_file(&full_options, &full_options_cnt);
	xrealloc(full_options,
		 ((full_options_cnt + 1) * sizeof(s_p_options_t)));
	tbl = s_p_hashtbl_create_2(full_options);

	conf_path = get_extra_conf_path("apptype.properties");
	if ((conf_path == NULL) || (stat(conf_path, &buf) == -1)) {
		debug2("No apptype.properties file (%s)", conf_path);
	} else {
		debug2("Reading apptype.properties file %s", conf_path);	
		/* 
			Build the hash table from apptype.properties, passing true and changing error to debug 
			so as not to affect the ctld service 
		*/
		if (s_p_parse_file(tbl, NULL, conf_path, true, NULL) ==
		    SLURM_ERROR) {
			fatal("Could not open/read/parse apptype.properties file "
			      "%s.  Many times this is because you have "
			      "defined options for plugins that are not "
			      "loaded.  Please check your slurm.conf file "
			      "and make sure the plugins for the options "
			      "listed are loaded.",
			      conf_path);
		}
	}
	apptype_properties_apptype_buf = s_p_pack_hashtbl(
		tbl, full_options, full_options_cnt);
	for (i=0; i<full_options_cnt; i++)
		xfree(full_options[i].key);
	xfree(full_options);
	xfree(conf_path);
	s_p_hashtbl_destroy(tbl);
	return SLURM_SUCCESS;
}

extern int apptype_properties_write_conf(int fd)
{
	int len;
	apptype_properties_conf_init();
	slurm_mutex_lock(&conf_mutex2);
	len = get_buf_offset(apptype_properties_apptype_buf);

	safe_write(fd, &len, sizeof(int));
	safe_write(fd, get_buf_data(apptype_properties_apptype_buf), len);
	slurm_mutex_unlock(&conf_mutex2);
	return 0;
rwfail:
	slurm_mutex_unlock(&conf_mutex2);
	return -1;
	
}
extern int apptype_properties_read_conf(int fd)
{
	int len;

	safe_read(fd, &len, sizeof(int));
	apptype_properties_apptype_buf = init_buf(len);
	safe_read(fd, apptype_properties_apptype_buf->head, len);

	inited = true;

	return SLURM_SUCCESS;
rwfail:
	return SLURM_ERROR;
}
extern int apptype_properties_conf_reconfig(void)
{
	apptype_properties_conf_destory();
	slurm_mutex_init(&conf_mutex2);
	apptype_properties_conf_init();
	return SLURM_SUCCESS;
}
extern int apptype_properties_conf_destory(void)
{
	int rc = SLURM_SUCCESS;

	if(!inited)
		return SLURM_SUCCESS;
	
	inited = false;

	FREE_NULL_BUFFER(apptype_properties_apptype_buf);
	slurm_mutex_destroy(&conf_mutex2);
	return rc;
}

#endif
extern int jobacct_gather_fini(void)
{
	int rc = SLURM_SUCCESS;

	slurm_mutex_lock(&g_context_lock);
	if (g_context) {
		slurm_mutex_lock(&init_run_mutex);
		init_run = false;
		slurm_mutex_unlock(&init_run_mutex);

		if (watch_tasks_thread_id) {
			slurm_mutex_unlock(&g_context_lock);
			slurm_mutex_lock(&profile_timer->notify_mutex);
			slurm_cond_signal(&profile_timer->notify);
			slurm_mutex_unlock(&profile_timer->notify_mutex);
			pthread_join(watch_tasks_thread_id, NULL);
			slurm_mutex_lock(&g_context_lock);
		}
#ifdef __METASTACK_LOAD_ABNORMAL
		if (watch_stepd_thread_id) {
			slurm_mutex_unlock(&g_context_lock);
			slurm_mutex_lock(&profile_stepd->notify_mutex);
			slurm_cond_signal(&profile_stepd->notify);
			slurm_mutex_unlock(&profile_stepd->notify_mutex);
			pthread_join(watch_stepd_thread_id, NULL);
			slurm_mutex_lock(&g_context_lock);
		}
#endif
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
		if (watch_dog_thread_id) {
			slurm_mutex_unlock(&g_context_lock);
			slurm_mutex_lock(&profile_watch_dog_timer->notify_mutex);
			slurm_cond_signal(&profile_watch_dog_timer->notify);
			slurm_mutex_unlock(&profile_watch_dog_timer->notify_mutex);
			pthread_join(watch_dog_thread_id, NULL);
			slurm_mutex_lock(&g_context_lock);
		}
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
		if (apptype_recognition_thread_id) {
			slurm_mutex_unlock(&g_context_lock);
			slurm_mutex_lock(&profile_apptype->notify_mutex);
			slurm_cond_signal(&profile_apptype->notify);
			slurm_mutex_unlock(&profile_apptype->notify_mutex);
			pthread_join(apptype_recognition_thread_id, NULL);
			slurm_mutex_lock(&g_context_lock);
		}
#endif
		rc = plugin_context_destroy(g_context);
		g_context = NULL;
	}
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	apptype_properties_conf_destory();
#endif
	slurm_mutex_unlock(&g_context_lock);

	return rc;
}

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
extern int jobacct_gather_watchdog(int frequency, acct_gather_rank_t step_rank) 
{
	int retval = SLURM_SUCCESS;
	if (!plugin_polling)
		return SLURM_SUCCESS;
	if (jobacct_gather_init() < 0)
		return SLURM_ERROR;
	if (_jobacct_shutdown_test()) {
		slurm_mutex_lock(&jobacct_shutdown_mutex);
		jobacct_shutdown = false;
		slurm_mutex_unlock(&jobacct_shutdown_mutex);
		// return SLURM_SUCCESS;
	}

	if (frequency <= 0 || !(step_rank.enable_watchdog)) {   /* don't want dynamic monitoring? */
		debug2("jobacct_gather_watchdog thread disabled");
		return retval;
	}	

	acct_gather_rank_t *load_args 	= xmalloc(sizeof(acct_gather_rank_t));
	load_args->watch_dog_script 	= xstrdup(step_rank.watch_dog_script);
	load_args->watch_dog        	= xstrdup(step_rank.watch_dog);
	if(step_rank.job_stdout)
		load_args->job_stdout 		= xstrdup(step_rank.job_stdout);
	if(step_rank.job_stderr)
		load_args->job_stderr       = xstrdup(step_rank.job_stderr);
	load_args->init_time 			= step_rank.init_time;
	load_args->period 				= step_rank.period;
	load_args->enable_watchdog 		= step_rank.enable_watchdog;
	load_args->style_step 			= step_rank.style_step;
	load_args->job_id 				= step_rank.job_id;
	load_args->step_id.step_id 		= step_rank.step_id.step_id;
	load_args->head_node 			= step_rank.head_node;
	load_args->uid 					= step_rank.uid;
	load_args->gid					= step_rank.gid;
	load_args->switch_step			= step_rank.switch_step;
	
	slurm_thread_create(&watch_dog_thread_id, step_watch_dog, load_args);
	return retval;
}
#endif

#ifdef __METASTACK_LOAD_ABNORMAL
extern int	jobacct_gather_stepdpoll(uint16_t frequency, acct_gather_rank_t jobinfo) 
{
	int retval = SLURM_SUCCESS;
	if (!plugin_polling)
		return SLURM_SUCCESS;

	if (jobacct_gather_init() < 0)
		return SLURM_ERROR;

	if (_jobacct_shutdown_test()) {
		slurm_mutex_lock(&jobacct_shutdown_mutex);
		jobacct_shutdown = false;
		slurm_mutex_unlock(&jobacct_shutdown_mutex);
		// return SLURM_SUCCESS;
	}

	if (frequency == 0 || !(jobinfo.switch_step) || !(jobinfo.timer > 0)) {   /* don't want dynamic monitoring? */
		debug2("jobacct_gather send logging disabled");
		return retval;
	}
	
	acct_gather_rank_t *jobinfo_watch = NULL;
	jobinfo_watch = xmalloc(sizeof(acct_gather_rank_t));
	
	jobinfo_watch->switch_step = jobinfo.switch_step;
	jobinfo_watch->timer= jobinfo.timer;
	jobinfo_watch->cpu_min_load = jobinfo.cpu_min_load;
	jobinfo_watch->frequency = frequency;	
	jobinfo_watch->step_id = jobinfo.step_id;   
	jobinfo_watch->node_alloc_cpu = jobinfo.node_alloc_cpu;

	slurm_thread_create(&watch_stepd_thread_id, step_collect, jobinfo_watch);
	debug3("jobacct stepd gather dynamic logging enabled");

	return retval;
}
#endif

#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
extern int	jobacct_gather_apptypepoll(uint16_t frequency, acct_gather_rank_t jobinfo) 
{
	apptype_recogn_thread_args_t *args = NULL;
	if (!plugin_polling)
		return SLURM_SUCCESS;

	if (jobacct_gather_init() < 0)
		return SLURM_ERROR;

	if (_jobacct_shutdown_test()) {
		slurm_mutex_lock(&jobacct_shutdown_mutex);
		jobacct_shutdown = false;
		slurm_mutex_unlock(&jobacct_shutdown_mutex);
	}
	/* There is no need to apply apptype recognition */
	if (frequency <= 0) {
		debug2("jobacct_gather no need to apply apptype recognition");
		return SLURM_SUCCESS;
	}
	/* Non-head nodes do not start the thread */
	if (step_gather.parent_rank_gather != -1) 
		return SLURM_SUCCESS;
	/*
		Limit the number of apptype_regn threads collected
		+1 to ensure that the thread can collect one more time before exiting
	*/
	args = xmalloc(sizeof(apptype_recogn_thread_args_t));
	apptype_recongn_count = (frequency <= 0 ? 0 : ((JOBACCTGATHER_APPTYPE_DURATION / frequency) + 1));
	args->apptype_cli = xstrdup(jobinfo.apptype);
	args->profile = jobinfo.profile;
	if (args != NULL && args->apptype_cli)
		debug3("apptype recognition: apptype_client is %s", args->apptype_cli);
	else
		debug3("apptype recognition: apptype_client not recognized");
	slurm_thread_create(&apptype_recognition_thread_id, apptype_recognition, args);
	debug3("jobacct apptype gather dynamic logging enabled");

	return SLURM_SUCCESS;
}
#endif

#ifdef __METASTACK_LOAD_ABNORMAL
extern int jobacct_gather_startpoll(uint16_t frequency, acct_gather_rank_t jobinfo)
#endif
{
	int retval = SLURM_SUCCESS;

	if (!plugin_polling)
		return SLURM_SUCCESS;

	if (jobacct_gather_init() < 0)
		return SLURM_ERROR;

	if (!_jobacct_shutdown_test()) {
		error("jobacct_gather_startpoll: poll already started!");
		return retval;
	}
	slurm_mutex_lock(&jobacct_shutdown_mutex);
	jobacct_shutdown = false;
	slurm_mutex_unlock(&jobacct_shutdown_mutex);

	freq = frequency;

	task_list = list_create(jobacctinfo_destroy);
	if (frequency == 0) {	/* don't want dynamic monitoring? */
		debug2("jobacct_gather dynamic logging disabled");
		return retval;
	}

	/* create polling thread */
#ifdef __METASTACK_LOAD_ABNORMAL
	acct_gather_rank_t *jobinfo_watch = NULL;
	jobinfo_watch = xmalloc(sizeof(acct_gather_rank_t));
	
	jobinfo_watch->switch_step = jobinfo.switch_step;
	jobinfo_watch->timer= jobinfo.timer;
	jobinfo_watch->cpu_min_load = jobinfo.cpu_min_load;
	jobinfo_watch->frequency = frequency;	
	jobinfo_watch->step_id = jobinfo.step_id;   
	jobinfo_watch->node_alloc_cpu = jobinfo.node_alloc_cpu;
	slurm_thread_create(&watch_tasks_thread_id, _watch_tasks, jobinfo_watch);
#endif
	debug3("jobacct_gather dynamic logging enabled");

	return retval;
}

extern int jobacct_gather_endpoll(void)
{
	int retval = SLURM_SUCCESS;

	if (jobacct_gather_init() < 0)
		return SLURM_ERROR;

	slurm_mutex_lock(&jobacct_shutdown_mutex);
	jobacct_shutdown = true;
	slurm_mutex_unlock(&jobacct_shutdown_mutex);
	slurm_mutex_lock(&task_list_lock);
	FREE_NULL_LIST(task_list);

	retval = (*(ops.endpoll))();

	slurm_mutex_unlock(&task_list_lock);

	return retval;
}

extern int jobacct_gather_add_task(pid_t pid, jobacct_id_t *jobacct_id,
				   int poll)
{
	struct jobacctinfo *jobacct;

	if (jobacct_gather_init() < 0)
		return SLURM_ERROR;

	if (!plugin_polling)
		return SLURM_SUCCESS;

	if (_jobacct_shutdown_test())
		return SLURM_ERROR;

	jobacct = jobacctinfo_create(jobacct_id);

	slurm_mutex_lock(&task_list_lock);
	if (pid <= 0) {
		error("invalid pid given (%d) for task acct", pid);
		goto error;
	} else if (!task_list) {
		error("no task list created!");
		goto error;
	}

	jobacct->pid = pid;
	memcpy(&jobacct->id, jobacct_id, sizeof(jobacct_id_t));
	debug2("adding task %u pid %d on node %u to jobacct",
	       jobacct_id->taskid, pid, jobacct_id->nodeid);
	(*(ops.add_task))(pid, jobacct_id);
	list_push(task_list, jobacct);
	slurm_mutex_unlock(&task_list_lock);
#ifdef __METASTACK_LOAD_ABNORMAL
	if (poll == 1)
		_poll_data(1, NULL, NULL);
#endif

	return SLURM_SUCCESS;
error:
	slurm_mutex_unlock(&task_list_lock);
	jobacctinfo_destroy(jobacct);
	return SLURM_ERROR;
}

extern jobacctinfo_t *jobacct_gather_stat_task(pid_t pid)
{
	if (!plugin_polling || _jobacct_shutdown_test())
		return NULL;
#ifdef __METASTACK_LOAD_ABNORMAL
	collection_t * collect = NULL;
	collect = xmalloc(sizeof(collection_t));
	//collect->mode = true;
	_poll_data(0, collect, NULL);
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	if(collect) {
		if(collect->pids)
			xfree(collect->pids);
		xfree(collect);
	}
#endif
#endif
	if (pid) {
		struct jobacctinfo *jobacct = NULL;
		struct jobacctinfo *ret_jobacct = NULL;
		ListIterator itr = NULL;

		slurm_mutex_lock(&task_list_lock);
		if (!task_list) {
			error("no task list created!");
			goto error;
		}

		itr = list_iterator_create(task_list);
		while ((jobacct = list_next(itr))) {
			if (jobacct->pid == pid)
				break;
		}
		list_iterator_destroy(itr);
		if (jobacct == NULL)
			goto error;

		_copy_tres_usage(&ret_jobacct, jobacct);

	error:
		slurm_mutex_unlock(&task_list_lock);
		return ret_jobacct;
	}

	return NULL;
}

extern jobacctinfo_t *jobacct_gather_remove_task(pid_t pid)
{
	struct jobacctinfo *jobacct = NULL;
	ListIterator itr = NULL;

	if (!plugin_polling)
		return NULL;

	/* poll data one last time before removing task
	 * mainly for updating energy consumption */
#ifdef __METASTACK_LOAD_ABNORMAL
	_poll_data(1, NULL, NULL);
#endif

	if (_jobacct_shutdown_test())
		return NULL;

	slurm_mutex_lock(&task_list_lock);
	if (!task_list) {
		error("no task list created!");
		goto error;
	}

	itr = list_iterator_create(task_list);
	while((jobacct = list_next(itr))) {
		if (!pid || (jobacct->pid == pid)) {
			list_remove(itr);
			break;
		}
	}
	list_iterator_destroy(itr);
	if (jobacct) {
		debug2("removing task %u pid %d from jobacct",
		       jobacct->id.taskid, jobacct->pid);
	} else {
		if (pid)
			debug2("pid(%d) not being watched in jobacct!", pid);
	}
error:
	slurm_mutex_unlock(&task_list_lock);
	return jobacct;
}

extern int jobacct_gather_set_proctrack_container_id(uint64_t id)
{
	if (!plugin_polling)
		return SLURM_SUCCESS;

	if (cont_id != NO_VAL64)
		info("Warning: jobacct: set_proctrack_container_id: cont_id "
		     "is already set to %"PRIu64" you are setting it to "
		     "%"PRIu64"", cont_id, id);
	if (id <= 0) {
		error("jobacct: set_proctrack_container_id: "
		      "I was given most likely an unset cont_id %"PRIu64"",
		      id);
		return SLURM_ERROR;
	}
	cont_id = id;

	return SLURM_SUCCESS;
}

extern int jobacct_gather_set_mem_limit(slurm_step_id_t *step_id,
					uint64_t mem_limit)
{
	if (!plugin_polling)
		return SLURM_SUCCESS;

	if ((step_id->job_id == 0) || (mem_limit == 0)) {
		error("jobacct_gather_set_mem_limit: jobid:%u "
		      "mem_limit:%"PRIu64"", step_id->job_id, mem_limit);
		return SLURM_ERROR;
	}

	memcpy(&jobacct_step_id, step_id, sizeof(jobacct_step_id));
	jobacct_mem_limit   = mem_limit * 1048576; /* MB to B */
	jobacct_vmem_limit  = jobacct_mem_limit;
	jobacct_vmem_limit *= (slurm_conf.vsize_factor / 100.0);
	return SLURM_SUCCESS;
}

extern void jobacct_gather_handle_mem_limit(uint64_t total_job_mem,
					    uint64_t total_job_vsize)
{
	if (!plugin_polling)
		return;

	if (jobacct_mem_limit)
		debug("%ps memory used:%"PRIu64" limit:%"PRIu64" B",
		      &jobacct_step_id, total_job_mem, jobacct_mem_limit);

	if (jobacct_step_id.job_id && jobacct_mem_limit &&
	    (total_job_mem > jobacct_mem_limit)) {
		error("%ps exceeded memory limit (%"PRIu64" > %"PRIu64"), being killed",
		      &jobacct_step_id, total_job_mem, jobacct_mem_limit);
		_acct_kill_step();
	} else if (jobacct_step_id.job_id && jobacct_vmem_limit &&
		   (total_job_vsize > jobacct_vmem_limit)) {
		error("%ps exceeded virtual memory limit (%"PRIu64" > %"PRIu64"), being killed",
		      &jobacct_step_id, total_job_vsize, jobacct_vmem_limit);
		_acct_kill_step();
	}
}

/********************* jobacctinfo functions ******************************/

extern jobacctinfo_t *jobacctinfo_create(jobacct_id_t *jobacct_id)
{
	struct jobacctinfo *jobacct;
	jobacct_id_t temp_id;

	if (!plugin_polling)
		return NULL;

	jobacct = xmalloc(sizeof(struct jobacctinfo));

	if (!jobacct_id) {
		temp_id.taskid = NO_VAL;
		temp_id.nodeid = NO_VAL;
		jobacct_id = &temp_id;
	}

#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
   jobacct->first_acct_time.tv_sec = 0;
   jobacct->first_acct_time.tv_usec = 0;
#endif
#ifdef __METASTACK_LOAD_ABNORMAL
   jobacct->cpu_step_ave = 0.0;
   jobacct->cpu_step_max = 0.0;
   jobacct->cpu_step_min = 0.0;
   jobacct->cpu_step_real = 0.0;

   jobacct->mem_step_max = 0;
   jobacct->mem_step_min = 0;
   jobacct->mem_step = 0;

   jobacct->vmem_step_max = 0;
   jobacct->vmem_step_min = 0;
   jobacct->vmem_step = 0;

   jobacct->node_alloc_cpu = 0;
   jobacct->timer = 0;
   jobacct->cpu_threshold = 100;
#endif
	jobacct->dataset_id = -1;
	jobacct->sys_cpu_sec = 0;
	jobacct->sys_cpu_usec = 0;
	jobacct->user_cpu_sec = 0;
	jobacct->user_cpu_usec = 0;

	_jobacctinfo_create_tres_usage(jobacct_id, jobacct);
	return jobacct;
}

extern void jobacctinfo_destroy(void *object)
{
	struct jobacctinfo *jobacct = (struct jobacctinfo *)object;

	_free_tres_usage(jobacct);
	xfree(jobacct);
}

extern int jobacctinfo_setinfo(jobacctinfo_t *jobacct,
			       enum jobacct_data_type type, void *data,
			       uint16_t protocol_version)
{
	int rc = SLURM_SUCCESS;
	int *fd = (int *)data;
	struct rusage *rusage = (struct rusage *)data;
	uint64_t *uint64 = (uint64_t *) data;
	struct jobacctinfo *send = (struct jobacctinfo *) data;
	buf_t *buffer = NULL;

	if (!plugin_polling)
		return SLURM_SUCCESS;

	switch (type) {
	case JOBACCT_DATA_TOTAL:
		if (!jobacct) {
			/* Avoid possible memory leak from _copy_tres_usage() */
			error("%s: \'jobacct\' argument is NULL", __func__);
			rc = SLURM_ERROR;
		} else
			_copy_tres_usage(&jobacct, send);
		break;
	case JOBACCT_DATA_PIPE:
		if (protocol_version >= SLURM_MIN_PROTOCOL_VERSION) {
			int len;
			assoc_mgr_lock_t locks = { .tres = READ_LOCK };

			buffer = init_buf(0);

			if (jobacct) {
				assoc_mgr_lock(&locks);
				jobacct->tres_list = assoc_mgr_tres_list;
			}

			jobacctinfo_pack(jobacct, protocol_version,
					 PROTOCOL_TYPE_SLURM, buffer);
#ifdef __METASTACK_LOAD_ABNORMAL
			if(protocol_version >= META_2_1_PROTOCOL_VERSION) {
				jobacctinfo_pack_detial(jobacct, protocol_version,
						PROTOCOL_TYPE_SLURM, buffer);
			}
#endif
			if (jobacct) {
				assoc_mgr_unlock(&locks);
				jobacct->tres_list = NULL;
			}

			len = get_buf_offset(buffer);
			safe_write(*fd, &len, sizeof(int));
			safe_write(*fd, get_buf_data(buffer), len);
			FREE_NULL_BUFFER(buffer);
		}

		break;
	case JOBACCT_DATA_RUSAGE:
		if (rusage->ru_utime.tv_sec > jobacct->user_cpu_sec)
			jobacct->user_cpu_sec = rusage->ru_utime.tv_sec;
		jobacct->user_cpu_usec = rusage->ru_utime.tv_usec;
		if (rusage->ru_stime.tv_sec > jobacct->sys_cpu_sec)
			jobacct->sys_cpu_sec = rusage->ru_stime.tv_sec;
		jobacct->sys_cpu_usec = rusage->ru_stime.tv_usec;
		break;
	case JOBACCT_DATA_TOT_RSS:
		jobacct->tres_usage_in_tot[TRES_ARRAY_MEM] = *uint64;
		break;
	case JOBACCT_DATA_TOT_VSIZE:
		jobacct->tres_usage_in_tot[TRES_ARRAY_VMEM] = *uint64;
		break;
	default:
		debug("%s: data_type %d invalid", __func__, type);
	}

	return rc;

rwfail:
	FREE_NULL_BUFFER(buffer);
	return SLURM_ERROR;
}

#ifdef __METASTACK_LOAD_ABNORMAL
extern void jobacctinfo_pack_detial(jobacctinfo_t *jobacct, uint16_t rpc_version,
			     uint16_t protocol_type, buf_t *buffer)
{
	bool no_pack;
	double tmp_dbl = 0.0;
	uint64_t tmp_64 = 0;
	uint32_t tmp_32 = 0;
	no_pack = (!plugin_polling && (protocol_type != PROTOCOL_TYPE_DBD));

	if (!jobacct || no_pack) {
		pack8((uint8_t) 0, buffer);
		return;
	}
	pack8((uint8_t) 1, buffer);

	if (rpc_version >= SLURM_22_05_PROTOCOL_VERSION) {
		 if (rpc_version >= META_2_1_PROTOCOL_VERSION) {
			if ((tmp_64 = jobacct->flag) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);

            if ((tmp_dbl = jobacct->cpu_step_ave) < 0.0)
                tmp_dbl = 0.0;
            packdouble(tmp_dbl, buffer);
            if ((tmp_dbl = jobacct->cpu_step_max) < 0.0)
                tmp_dbl = 0.0;
            packdouble(tmp_dbl, buffer);
            if ((tmp_dbl = jobacct->cpu_step_min) < 0.0)
                tmp_dbl = 0.0;
            packdouble(tmp_dbl, buffer);
            if ((tmp_dbl = jobacct->cpu_step_real) < 0.0)
                tmp_dbl = 0.0;
            packdouble(tmp_dbl, buffer);

            if ((tmp_64 = jobacct->mem_step_max) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);
            if ((tmp_64 = jobacct->mem_step_min) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);
            if ((tmp_64 = jobacct->mem_step) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);

            if ((tmp_64 = jobacct->vmem_step_max) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);
            if ((tmp_64 = jobacct->vmem_step_min) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);
            if ((tmp_64 = jobacct->vmem_step) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);

            if ((tmp_64 = jobacct->step_pages) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);

            if ((tmp_64 = jobacct->acct_flag) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);	
            pack64((uint64_t)jobacct->cpu_count, buffer);
            pack64((uint64_t)jobacct->pid_count, buffer);
            pack64((uint64_t)jobacct->node_count, buffer);

            pack64_array(jobacct->cpu_start, JOBACCTINFO_START_END_ARRAY_SIZE, buffer);
            pack64_array(jobacct->cpu_end, JOBACCTINFO_START_END_ARRAY_SIZE, buffer);
            pack64_array(jobacct->pid_start, JOBACCTINFO_START_END_ARRAY_SIZE, buffer);
            pack64_array(jobacct->pid_end, JOBACCTINFO_START_END_ARRAY_SIZE, buffer);
            pack64_array(jobacct->node_start, JOBACCTINFO_START_END_ARRAY_SIZE, buffer);
            pack64_array(jobacct->node_end, JOBACCTINFO_START_END_ARRAY_SIZE, buffer);

            if ((tmp_64 = jobacct->node_alloc_cpu) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);
            if ((tmp_64 = jobacct->timer) < 0)
                tmp_64 = 0;
            pack64(tmp_64, buffer);
            if ((tmp_32 = jobacct->cpu_threshold) < 0)
                tmp_32 = 0;
            pack32(tmp_32, buffer);
		 }  else {
			info("jobacctinfo_pack version %u not supported", rpc_version);
			return;
		}
	}
	
}

extern int jobacctinfo_unpack_detial(jobacctinfo_t **jobacct, uint16_t rpc_version,
			      uint16_t protocol_type, buf_t *buffer, bool alloc) 
{
	if (jobacct_gather_init() < 0)
		return SLURM_ERROR;
	uint32_t uint32_tmp = 0;
	uint64_t tmp_int64 = 0;
	uint32_t tmp_int32 = 0;
	double tmp_double = 0.0;
	uint8_t  uint8_tmp;
	if (alloc)
		*jobacct = xmalloc(sizeof(struct jobacctinfo));
	safe_unpack8(&uint8_tmp, buffer);
	if (uint8_tmp == (uint8_t) 0)
		return SLURM_SUCCESS;

	if (rpc_version >= SLURM_22_05_PROTOCOL_VERSION) {
		if (rpc_version >= META_2_1_PROTOCOL_VERSION) {

            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->flag = tmp_int64;

            safe_unpackdouble(&tmp_double, buffer);
            (*jobacct)->cpu_step_ave = tmp_double;
            safe_unpackdouble(&tmp_double, buffer);
            (*jobacct)->cpu_step_max = tmp_double;
            safe_unpackdouble(&tmp_double, buffer);
            (*jobacct)->cpu_step_min = tmp_double;
            safe_unpackdouble(&tmp_double, buffer);
            (*jobacct)->cpu_step_real = tmp_double;

            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->mem_step_max = tmp_int64;
            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->mem_step_min = tmp_int64;
            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->mem_step = tmp_int64;

            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->vmem_step_max = tmp_int64;
            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->vmem_step_min = tmp_int64;
            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->vmem_step = tmp_int64;

            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->step_pages = tmp_int64;
            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->acct_flag = tmp_int64;	
            safe_unpack64(&(*jobacct)->cpu_count, buffer);
            safe_unpack64(&(*jobacct)->pid_count, buffer);
            safe_unpack64(&(*jobacct)->node_count, buffer);
            safe_unpack64_array(&(*jobacct)->cpu_start,&uint32_tmp, buffer);
            safe_unpack64_array(&(*jobacct)->cpu_end,&uint32_tmp, buffer);
            safe_unpack64_array(&(*jobacct)->pid_start,&uint32_tmp, buffer);
            safe_unpack64_array(&(*jobacct)->pid_end,&uint32_tmp, buffer);
            safe_unpack64_array(&(*jobacct)->node_start,&uint32_tmp, buffer);
            safe_unpack64_array(&(*jobacct)->node_end,&uint32_tmp, buffer);		

            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->node_alloc_cpu = tmp_int64;
            safe_unpack64(&tmp_int64, buffer);
            (*jobacct)->timer = tmp_int64;
            safe_unpack32(&tmp_int32, buffer);
            (*jobacct)->cpu_threshold = tmp_int32;
		} else {
			info("jobacctinfo_pack version %u not supported", rpc_version);
			return SLURM_ERROR;
		}
	}
	return SLURM_SUCCESS;
unpack_error:
	debug2("jobacctinfo_unpack: unpack_error: size_buf(buffer) %u",
	       size_buf(buffer));
	if (alloc) {
		jobacctinfo_destroy(*jobacct);
		*jobacct = NULL;
	}
	return SLURM_ERROR;
}
#endif

extern int jobacctinfo_getinfo(
	jobacctinfo_t *jobacct, enum jobacct_data_type type, void *data,
	uint16_t protocol_version)
{
	int rc = SLURM_SUCCESS;
	int *fd = (int *)data;
	uint64_t *uint64 = (uint64_t *) data;
	struct rusage *rusage = (struct rusage *)data;
	struct jobacctinfo *send = (struct jobacctinfo *) data;
	char *buf = NULL;

	if (!plugin_polling)
		return SLURM_SUCCESS;

	/* jobacct needs to be allocated before this is called.	*/
	xassert(jobacct);

	switch (type) {
	case JOBACCT_DATA_TOTAL:
		if (!send) {
			/* Avoid possible memory leak from _copy_tres_usage() */
			error("%s: \'data\' argument is NULL", __func__);
			rc = SLURM_ERROR;
		} else
			_copy_tres_usage(&send, jobacct);
		break;
	case JOBACCT_DATA_PIPE:
		if (protocol_version >= SLURM_MIN_PROTOCOL_VERSION) {
			int len;
			buf_t *buffer;

			safe_read(*fd, &len, sizeof(int));
			buf = xmalloc(len);
			safe_read(*fd, buf, len);
			buffer = create_buf(buf, len);
			jobacctinfo_unpack(&jobacct, protocol_version,
					   PROTOCOL_TYPE_SLURM, buffer, 0);
#ifdef __METASTACK_LOAD_ABNORMAL
			if(protocol_version >= META_2_1_PROTOCOL_VERSION) {
				jobacctinfo_unpack_detial(&jobacct, protocol_version,
							PROTOCOL_TYPE_SLURM, buffer, 0);
			}
#endif
			free_buf(buffer);
		}

		break;
	case JOBACCT_DATA_RUSAGE:
		memset(rusage, 0, sizeof(struct rusage));
		rusage->ru_utime.tv_sec = jobacct->user_cpu_sec;
		rusage->ru_utime.tv_usec = jobacct->user_cpu_usec;
		rusage->ru_stime.tv_sec = jobacct->sys_cpu_sec;
		rusage->ru_stime.tv_usec = jobacct->sys_cpu_usec;
		break;
	case JOBACCT_DATA_TOT_RSS:
		*uint64 = jobacct->tres_usage_in_tot[TRES_ARRAY_MEM];
		break;
	case JOBACCT_DATA_TOT_VSIZE:
		*uint64 = jobacct->tres_usage_in_tot[TRES_ARRAY_VMEM];
		break;
	default:
		debug("%s: data_type %d invalid", __func__, type);
	}
	return rc;

rwfail:
	xfree(buf);
	return SLURM_ERROR;
}

extern void jobacctinfo_pack(jobacctinfo_t *jobacct, uint16_t rpc_version,
			     uint16_t protocol_type, buf_t *buffer)
{
	bool no_pack;
	
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL	
    double tmp_dbl;
#endif
// #ifdef __METASTACK_LOAD_ABNORMAL
// 	uint64_t tmp_64;
// 	uint32_t tmp_32;
// #endif
	no_pack = (!plugin_polling && (protocol_type != PROTOCOL_TYPE_DBD));

	if (!jobacct || no_pack) {
		pack8((uint8_t) 0, buffer);
		return;
	}

	pack8((uint8_t) 1, buffer);
#ifdef __META_PROTOCOL
	if (rpc_version >= SLURM_22_05_PROTOCOL_VERSION) {
        /**
         * if META 2.1.x
         * if (rpc_version >= META_2_1_PROTOCOL_VERSION) {}
         * else if(rpc_version >= META_2_0_PROTOCOL_VERSION) {}
         * else {}
         */
        if (rpc_version >= META_2_1_PROTOCOL_VERSION) {
            pack64(jobacct->user_cpu_sec, buffer);
			pack32((uint32_t)jobacct->user_cpu_usec, buffer);
			pack64(jobacct->sys_cpu_sec, buffer);
			pack32((uint32_t)jobacct->sys_cpu_usec, buffer);
			pack32((uint32_t)jobacct->act_cpufreq, buffer);
			pack64((uint64_t)jobacct->energy.consumed_energy, buffer);

			pack32_array(jobacct->tres_ids, jobacct->tres_count, buffer);

			slurm_pack_list(jobacct->tres_list,
					slurmdb_pack_tres_rec, buffer,
					SLURM_PROTOCOL_VERSION);

			pack64_array(jobacct->tres_usage_in_max,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_max_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_max_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_min,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_min_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_min_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_tot,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_max,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_max_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_max_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_min,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_min_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_min_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_tot,
					jobacct->tres_count, buffer);
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
			if ((tmp_dbl = jobacct->cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);

			if ((tmp_dbl=jobacct->avg_cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);

			if ((tmp_dbl = jobacct->min_cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);

			if ((tmp_dbl = jobacct->max_cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);
#endif
        } else if(rpc_version >= META_2_0_PROTOCOL_VERSION) {
            /**
             * METASTACK 2.0.x(x>=3)
             */
			pack64(jobacct->user_cpu_sec, buffer);
			pack32((uint32_t)jobacct->user_cpu_usec, buffer);
			pack64(jobacct->sys_cpu_sec, buffer);
			pack32((uint32_t)jobacct->sys_cpu_usec, buffer);
			pack32((uint32_t)jobacct->act_cpufreq, buffer);
			pack64((uint64_t)jobacct->energy.consumed_energy, buffer);

			pack32_array(jobacct->tres_ids, jobacct->tres_count, buffer);

			slurm_pack_list(jobacct->tres_list,
					slurmdb_pack_tres_rec, buffer,
					SLURM_PROTOCOL_VERSION);

			pack64_array(jobacct->tres_usage_in_max,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_max_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_max_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_min,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_min_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_min_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_tot,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_max,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_max_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_max_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_min,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_min_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_min_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_tot,
					jobacct->tres_count, buffer);
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
			if ((tmp_dbl = jobacct->cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);

			if ((tmp_dbl=jobacct->avg_cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);

			if ((tmp_dbl = jobacct->min_cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);

			if ((tmp_dbl = jobacct->max_cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);
#endif		
		} else {
            /**
             * METASTACK 2.0.x(x<=2) first
             * not the offical version
             */
			pack64(jobacct->user_cpu_sec, buffer);
			pack32((uint32_t)jobacct->user_cpu_usec, buffer);
			pack64(jobacct->sys_cpu_sec, buffer);
			pack32((uint32_t)jobacct->sys_cpu_usec, buffer);
			pack32((uint32_t)jobacct->act_cpufreq, buffer);
			pack64((uint64_t)jobacct->energy.consumed_energy, buffer);

			pack32_array(jobacct->tres_ids, jobacct->tres_count, buffer);

			slurm_pack_list(jobacct->tres_list,
					slurmdb_pack_tres_rec, buffer,
					SLURM_PROTOCOL_VERSION);

			pack64_array(jobacct->tres_usage_in_max,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_max_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_max_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_min,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_min_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_min_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_in_tot,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_max,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_max_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_max_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_min,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_min_nodeid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_min_taskid,
					jobacct->tres_count, buffer);
			pack64_array(jobacct->tres_usage_out_tot,
					jobacct->tres_count, buffer);
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
			if ((tmp_dbl = jobacct->cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);

			if ((tmp_dbl=jobacct->avg_cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);

			if ((tmp_dbl = jobacct->min_cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);

			if ((tmp_dbl = jobacct->max_cpu_util) < 0)
				tmp_dbl = 0;
			packdouble(tmp_dbl, buffer);
#endif	
		}
	} else	if (rpc_version >= SLURM_21_08_PROTOCOL_VERSION) {
		pack64(jobacct->user_cpu_sec, buffer);
		pack32((uint32_t)jobacct->user_cpu_usec, buffer);
		pack64(jobacct->sys_cpu_sec, buffer);
		pack32((uint32_t)jobacct->sys_cpu_usec, buffer);
		pack32((uint32_t)jobacct->act_cpufreq, buffer);
		pack64((uint64_t)jobacct->energy.consumed_energy, buffer);

		pack32_array(jobacct->tres_ids, jobacct->tres_count, buffer);

		slurm_pack_list(jobacct->tres_list,
				slurmdb_pack_tres_rec, buffer,
				SLURM_PROTOCOL_VERSION);

		pack64_array(jobacct->tres_usage_in_max,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_max_nodeid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_max_taskid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_min,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_min_nodeid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_min_taskid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_tot,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_max,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_max_nodeid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_max_taskid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_min,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_min_nodeid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_min_taskid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_tot,
			     jobacct->tres_count, buffer);
	} else if (rpc_version >= SLURM_MIN_PROTOCOL_VERSION) {
		if (jobacct->user_cpu_sec > NO_VAL) {
			pack32((uint32_t)NO_VAL, buffer);
		} else
			pack32((uint32_t)jobacct->user_cpu_sec, buffer);
		pack32((uint32_t)jobacct->user_cpu_usec, buffer);
		if (jobacct->sys_cpu_sec > NO_VAL) {
			pack32((uint32_t)NO_VAL, buffer);
		} else
			pack32((uint32_t)jobacct->sys_cpu_sec, buffer);
		pack32((uint32_t)jobacct->sys_cpu_usec, buffer);
		pack32((uint32_t)jobacct->act_cpufreq, buffer);
		pack64((uint64_t)jobacct->energy.consumed_energy, buffer);

		pack32_array(jobacct->tres_ids, jobacct->tres_count, buffer);

		slurm_pack_list(jobacct->tres_list,
				slurmdb_pack_tres_rec, buffer,
				SLURM_PROTOCOL_VERSION);

		pack64_array(jobacct->tres_usage_in_max,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_max_nodeid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_max_taskid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_min,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_min_nodeid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_min_taskid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_in_tot,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_max,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_max_nodeid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_max_taskid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_min,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_min_nodeid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_min_taskid,
			     jobacct->tres_count, buffer);
		pack64_array(jobacct->tres_usage_out_tot,
			     jobacct->tres_count, buffer);
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
		if ((tmp_dbl = jobacct->cpu_util) < 0)
			tmp_dbl = 0;
		packdouble(tmp_dbl, buffer);

		if ((tmp_dbl=jobacct->avg_cpu_util) < 0)
			tmp_dbl = 0;
		packdouble(tmp_dbl, buffer);

		if ((tmp_dbl = jobacct->min_cpu_util) < 0)
			tmp_dbl = 0;
		packdouble(tmp_dbl, buffer);

		if ((tmp_dbl = jobacct->max_cpu_util) < 0)
			tmp_dbl = 0;
		packdouble(tmp_dbl, buffer);
#endif
	} else {
		info("jobacctinfo_pack version %u not supported", rpc_version);
		return;
	}
#endif

}

extern int jobacctinfo_unpack(jobacctinfo_t **jobacct, uint16_t rpc_version,
			      uint16_t protocol_type, buf_t *buffer, bool alloc)
{
	uint32_t uint32_tmp;
	uint8_t  uint8_tmp;

	if (jobacct_gather_init() < 0)
		return SLURM_ERROR;

	safe_unpack8(&uint8_tmp, buffer);
	if (uint8_tmp == (uint8_t) 0)
		return SLURM_SUCCESS;

	xassert(jobacct);

	if (alloc)
		*jobacct = xmalloc(sizeof(struct jobacctinfo));
	else {
		xassert(*jobacct);
		_free_tres_usage(*jobacct);
	}
#ifdef __META_PROTOCOL
	if (rpc_version >= SLURM_22_05_PROTOCOL_VERSION) {
        if(rpc_version >= META_2_1_PROTOCOL_VERSION) {
            safe_unpack64(&(*jobacct)->user_cpu_sec, buffer);
			safe_unpack32(&uint32_tmp, buffer);
			(*jobacct)->user_cpu_usec = uint32_tmp;
			safe_unpack64(&(*jobacct)->sys_cpu_sec, buffer);
			safe_unpack32(&uint32_tmp, buffer);
			(*jobacct)->sys_cpu_usec = uint32_tmp;

			safe_unpack32(&(*jobacct)->act_cpufreq, buffer);
			safe_unpack64(&(*jobacct)->energy.consumed_energy, buffer);

			safe_unpack32_array(&(*jobacct)->tres_ids,
						&(*jobacct)->tres_count, buffer);
			if (slurm_unpack_list(&(*jobacct)->tres_list,
						slurmdb_unpack_tres_rec,
						slurmdb_destroy_tres_rec,
						buffer, rpc_version) != SLURM_SUCCESS)
				goto unpack_error;
			safe_unpack64_array(&(*jobacct)->tres_usage_in_max,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_max_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_max_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_min,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_min_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_min_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_tot,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_max,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_max_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_max_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_min,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_min_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_min_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_tot,
						&uint32_tmp, buffer);
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
			double tmp_double;
			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->cpu_util = tmp_double;
			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->avg_cpu_util = tmp_double;

			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->min_cpu_util = tmp_double;

			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->max_cpu_util = tmp_double;
#endif
        } else if(rpc_version >= META_2_0_PROTOCOL_VERSION) {
			safe_unpack64(&(*jobacct)->user_cpu_sec, buffer);
			safe_unpack32(&uint32_tmp, buffer);
			(*jobacct)->user_cpu_usec = uint32_tmp;
			safe_unpack64(&(*jobacct)->sys_cpu_sec, buffer);
			safe_unpack32(&uint32_tmp, buffer);
			(*jobacct)->sys_cpu_usec = uint32_tmp;

			safe_unpack32(&(*jobacct)->act_cpufreq, buffer);
			safe_unpack64(&(*jobacct)->energy.consumed_energy, buffer);

			safe_unpack32_array(&(*jobacct)->tres_ids,
						&(*jobacct)->tres_count, buffer);
			if (slurm_unpack_list(&(*jobacct)->tres_list,
						slurmdb_unpack_tres_rec,
						slurmdb_destroy_tres_rec,
						buffer, rpc_version) != SLURM_SUCCESS)
				goto unpack_error;
			safe_unpack64_array(&(*jobacct)->tres_usage_in_max,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_max_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_max_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_min,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_min_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_min_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_tot,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_max,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_max_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_max_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_min,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_min_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_min_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_tot,
						&uint32_tmp, buffer);
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
			double tmp_double;
			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->cpu_util = tmp_double;
			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->avg_cpu_util = tmp_double;

			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->min_cpu_util = tmp_double;

			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->max_cpu_util = tmp_double;
#endif
    	} else  {
			safe_unpack64(&(*jobacct)->user_cpu_sec, buffer);
			safe_unpack32(&uint32_tmp, buffer);
			(*jobacct)->user_cpu_usec = uint32_tmp;
			safe_unpack64(&(*jobacct)->sys_cpu_sec, buffer);
			safe_unpack32(&uint32_tmp, buffer);
			(*jobacct)->sys_cpu_usec = uint32_tmp;

			safe_unpack32(&(*jobacct)->act_cpufreq, buffer);
			safe_unpack64(&(*jobacct)->energy.consumed_energy, buffer);

			safe_unpack32_array(&(*jobacct)->tres_ids,
						&(*jobacct)->tres_count, buffer);
			if (slurm_unpack_list(&(*jobacct)->tres_list,
						slurmdb_unpack_tres_rec,
						slurmdb_destroy_tres_rec,
						buffer, rpc_version) != SLURM_SUCCESS)
				goto unpack_error;
			safe_unpack64_array(&(*jobacct)->tres_usage_in_max,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_max_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_max_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_min,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_min_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_min_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_in_tot,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_max,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_max_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_max_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_min,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_min_nodeid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_min_taskid,
						&uint32_tmp, buffer);
			safe_unpack64_array(&(*jobacct)->tres_usage_out_tot,
						&uint32_tmp, buffer);
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
			double tmp_double;
			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->cpu_util = tmp_double;
			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->avg_cpu_util = tmp_double;

			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->min_cpu_util = tmp_double;

			safe_unpackdouble(&tmp_double, buffer);
			(*jobacct)->max_cpu_util = tmp_double;
#endif
		}
	} else if (rpc_version >= SLURM_21_08_PROTOCOL_VERSION) {
		safe_unpack64(&(*jobacct)->user_cpu_sec, buffer);
		safe_unpack32(&uint32_tmp, buffer);
		(*jobacct)->user_cpu_usec = uint32_tmp;
		safe_unpack64(&(*jobacct)->sys_cpu_sec, buffer);
		safe_unpack32(&uint32_tmp, buffer);
		(*jobacct)->sys_cpu_usec = uint32_tmp;

		safe_unpack32(&(*jobacct)->act_cpufreq, buffer);
		safe_unpack64(&(*jobacct)->energy.consumed_energy, buffer);

		safe_unpack32_array(&(*jobacct)->tres_ids,
				    &(*jobacct)->tres_count, buffer);
		if (slurm_unpack_list(&(*jobacct)->tres_list,
				      slurmdb_unpack_tres_rec,
				      slurmdb_destroy_tres_rec,
				      buffer, rpc_version) != SLURM_SUCCESS)
			goto unpack_error;
		safe_unpack64_array(&(*jobacct)->tres_usage_in_max,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_max_nodeid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_max_taskid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_min,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_min_nodeid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_min_taskid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_tot,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_max,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_max_nodeid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_max_taskid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_min,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_min_nodeid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_min_taskid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_tot,
				    &uint32_tmp, buffer);
	} else if (rpc_version >= SLURM_MIN_PROTOCOL_VERSION) {
		safe_unpack32(&uint32_tmp, buffer);
		(*jobacct)->user_cpu_sec = uint32_tmp;
		safe_unpack32(&uint32_tmp, buffer);
		(*jobacct)->user_cpu_usec = uint32_tmp;
		safe_unpack32(&uint32_tmp, buffer);
		(*jobacct)->sys_cpu_sec = uint32_tmp;
		safe_unpack32(&uint32_tmp, buffer);
		(*jobacct)->sys_cpu_usec = uint32_tmp;

		safe_unpack32(&(*jobacct)->act_cpufreq, buffer);
		safe_unpack64(&(*jobacct)->energy.consumed_energy, buffer);

		safe_unpack32_array(&(*jobacct)->tres_ids,
				    &(*jobacct)->tres_count, buffer);
		if (slurm_unpack_list(&(*jobacct)->tres_list,
				      slurmdb_unpack_tres_rec,
				      slurmdb_destroy_tres_rec,
				      buffer, rpc_version) != SLURM_SUCCESS)
			goto unpack_error;
		safe_unpack64_array(&(*jobacct)->tres_usage_in_max,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_max_nodeid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_max_taskid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_min,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_min_nodeid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_min_taskid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_in_tot,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_max,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_max_nodeid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_max_taskid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_min,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_min_nodeid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_min_taskid,
				    &uint32_tmp, buffer);
		safe_unpack64_array(&(*jobacct)->tres_usage_out_tot,
				    &uint32_tmp, buffer);
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
		double tmp_double;
		safe_unpackdouble(&tmp_double, buffer);
		(*jobacct)->cpu_util = tmp_double;
		safe_unpackdouble(&tmp_double, buffer);
		(*jobacct)->avg_cpu_util = tmp_double;

		safe_unpackdouble(&tmp_double, buffer);
		(*jobacct)->min_cpu_util = tmp_double;

		safe_unpackdouble(&tmp_double, buffer);
		(*jobacct)->max_cpu_util = tmp_double;
#endif
	} else {
		info("jobacctinfo_unpack version %u not supported",
		     rpc_version);
		return SLURM_ERROR;
	}
#endif

	return SLURM_SUCCESS;

unpack_error:
	debug2("jobacctinfo_unpack: unpack_error: size_buf(buffer) %u",
	       size_buf(buffer));
	if (alloc) {
		jobacctinfo_destroy(*jobacct);
		*jobacct = NULL;
	}

       	return SLURM_ERROR;
}

#ifdef __METASTACK_LOAD_ABNORMAL
extern void jobacctinfo_aggregate_2(jobacctinfo_t *dest, jobacctinfo_t *from)
{
	if (!plugin_polling)
		return;

	xassert(dest);

	if (!from)
		return;
	dest->flag =dest->flag | from->flag;
	if(from->acct_flag == 1) {
		dest->cpu_step_ave = from->cpu_step_ave;
		dest->cpu_step_max = from->cpu_step_max;
		dest->cpu_step_min = from->cpu_step_min;
		dest->cpu_step_real = from->cpu_step_real;

		dest->mem_step_max = from->mem_step_max;
		dest->mem_step_min = from->mem_step_min;
		dest->mem_step = from->mem_step;

		dest->vmem_step_max = from->vmem_step_max;
		dest->vmem_step_min = from->vmem_step_min;
		dest->vmem_step = from->vmem_step;

		dest->step_pages = from->step_pages;
		dest->acct_flag = from->acct_flag;

		dest->cpu_count = from->cpu_count;
		dest->pid_count = from->pid_count;
		dest->node_count = from->node_count;

		for (int i = 0; i < JOBACCTINFO_START_END_ARRAY_SIZE; i++) {
			if((dest->cpu_start!=NULL) && from->cpu_start!=NULL)
				dest->cpu_start[i] = from->cpu_start[i];
			if((dest->cpu_end!=NULL) && from->cpu_end!=NULL)	
				dest->cpu_end[i] = from->cpu_end[i];

			if((dest->pid_start!=NULL) && from->pid_start!=NULL)
				dest->pid_start[i] = from->pid_start[i];
			if((dest->pid_end!=NULL) && from->pid_end!=NULL)
				dest->pid_end[i] = from->pid_end[i];

			if((dest->node_start!=NULL) && from->node_start!=NULL)
				dest->node_start[i] = from->node_start[i];
			if((dest->node_end!=NULL) && from->node_end!=NULL)
				dest->node_end[i] = from->node_end[i];
		}	

		dest->node_alloc_cpu = 	from->node_alloc_cpu;
		dest->timer = 	from->timer;
		dest->cpu_threshold = 	from->cpu_threshold;		
	}	
}
#endif

extern void jobacctinfo_aggregate(jobacctinfo_t *dest, jobacctinfo_t *from)
{
	if (!plugin_polling)
		return;

	xassert(dest);

	if (!from)
		return;
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
	dest->cpu_util += from->cpu_util;
	dest->avg_cpu_util += from->avg_cpu_util;
	dest->min_cpu_util += from->min_cpu_util;
	dest->max_cpu_util += from->max_cpu_util;
#endif

	dest->user_cpu_sec	+= from->user_cpu_sec;
	dest->user_cpu_usec	+= from->user_cpu_usec;
	if (dest->user_cpu_usec >= 1E6) {
		dest->user_cpu_sec += dest->user_cpu_usec / 1E6;
		dest->user_cpu_usec = dest->user_cpu_usec % (int)1E6;
	}
	dest->sys_cpu_sec	+= from->sys_cpu_sec;
	dest->sys_cpu_usec	+= from->sys_cpu_usec;
	if (dest->sys_cpu_usec >= 1E6) {
		dest->sys_cpu_sec += dest->sys_cpu_usec / 1E6;
		dest->sys_cpu_usec = dest->sys_cpu_usec % (int)1E6;
	}
	dest->act_cpufreq 	+= from->act_cpufreq;
	if (dest->energy.consumed_energy != NO_VAL64) {
		if (from->energy.consumed_energy == NO_VAL64)
			dest->energy.consumed_energy = NO_VAL64;
		else
			dest->energy.consumed_energy +=
					from->energy.consumed_energy;
	}

	_jobacctinfo_aggregate_tres_usage(dest, from);
}

extern void jobacctinfo_2_stats(slurmdb_stats_t *stats, jobacctinfo_t *jobacct)
{
	xassert(jobacct);
	xassert(stats);

	stats->act_cpufreq = (double)jobacct->act_cpufreq;

#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
    stats->cpu_util=(double)jobacct->cpu_util;
    stats->avg_cpu_util=(double)jobacct->avg_cpu_util;
    stats->max_cpu_util=(double)jobacct->max_cpu_util;
	stats->min_cpu_util=(double)jobacct->min_cpu_util;
#endif
#ifdef __METASTACK_LOAD_ABNORMAL
	stats->flag=(uint64_t)jobacct->flag;

	stats->cpu_step_ave = (double)jobacct->cpu_step_ave;
	stats->cpu_step_max = (double)jobacct->cpu_step_max;
	stats->cpu_step_min = (double)jobacct->cpu_step_min;
	stats->cpu_step_real = (double)jobacct->cpu_step_real;

	stats->mem_step_max = (uint64_t)jobacct->mem_step_max;
	stats->mem_step_min = (uint64_t)jobacct->mem_step_min;
	stats->mem_step = (uint64_t)jobacct->mem_step;

	stats->vmem_step_max = (uint64_t)jobacct->vmem_step_max;
	stats->vmem_step_min = (uint64_t)jobacct->vmem_step_min;
	stats->vmem_step = (uint64_t)jobacct->vmem_step;

	stats->step_pages = (uint64_t)jobacct->step_pages;
	stats->acct_flag =  (uint64_t)jobacct->acct_flag;			
	stats->cpu_count = (uint64_t)jobacct->cpu_count;
	stats->pid_count = (uint64_t)jobacct->pid_count;
	stats->node_count = (uint64_t)jobacct->node_count;
#endif
	if (jobacct->energy.consumed_energy == NO_VAL64)
		stats->consumed_energy = NO_VAL64;
	else
		stats->consumed_energy =
			(double)jobacct->energy.consumed_energy;

	_jobacctinfo_2_stats_tres_usage(stats, jobacct);
}

extern long jobacct_gather_get_clk_tck()
{
	long hertz = sysconf(_SC_CLK_TCK);

	if (hertz < 1) {
		error("unable to get clock rate");
		/* 100 is default on many systems. */
		hertz = 100;
	}

	return hertz;
}

//#ifdef __METASTACK_OPT_CACHE_QUERY
/*
extern jobacctinfo_t *jobacctinfo_extract(jobacctinfo_t *src_jobacct)
{
	int i;
	jobacctinfo_t *des_jobacct = NULL;
	_copy_tres_usage(&des_jobacct, src_jobacct);

	if (src_jobacct->tres_ids) {
		i = sizeof(uint32_t) * des_jobacct->tres_count;
		des_jobacct->tres_ids = xmalloc(i);
		memcpy(des_jobacct->tres_ids,src_jobacct->tres_ids, i);
	}
	des_jobacct->tres_list = slurmdb_copy_tres_list(src_jobacct->tres_list);

	return des_jobacct;
}
*/
//#endif
