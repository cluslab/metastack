/*****************************************************************************\
 *  slurm_acct_gather_profile.c - implementation-independent job profile
 *  accounting plugin definitions
 *****************************************************************************
 *  Copyright (C) 2013 Bull S. A. S.
 *		Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois.
 *
 *  Written by Rod Schultz <rod.schultz@bull.com>
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com>.
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

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#if HAVE_SYS_PRCTL_H
#  include <sys/prctl.h>
#endif

#include "src/common/macros.h"
#include "src/common/plugin.h"
#include "src/common/plugrack.h"
#include "src/common/read_config.h"
#include "src/common/slurm_acct_gather_filesystem.h"
#include "src/common/slurm_acct_gather_interconnect.h"
#include "src/common/slurm_acct_gather_profile.h"
#include "src/common/slurm_acct_gather_energy.h"
#include "src/common/slurm_jobacct_gather.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/timers.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

/* These 2 should remain the same. */
#define SLEEP_TIME 1
#define USLEEP_TIME 1000000

typedef struct slurm_acct_gather_profile_ops {
	void (*child_forked)    (void);
	void (*conf_options)    (s_p_options_t **full_options,
				 int *full_options_cnt);
	void (*conf_set)        (s_p_hashtbl_t *tbl);
	void* (*get)            (enum acct_gather_profile_info info_type,
				 void *data);
	int (*node_step_start)  (stepd_step_rec_t*);
	int (*node_step_end)    (void);
	int (*task_start)       (uint32_t);
	int (*task_end)         (pid_t);
	int64_t (*create_group)(const char*);
	int (*create_dataset)   (const char*, int64_t,
				 acct_gather_profile_dataset_t *);
	int (*add_sample_data)  (uint32_t, void*, time_t);
#ifdef __METASTACK_LOAD_ABNORMAL
    int (*add_sample_data_stepd)  (uint32_t, void*, time_t);
#endif
	void (*conf_values)     (List *data);
	bool (*is_active)     (uint32_t);

} slurm_acct_gather_profile_ops_t;

/*
 * These strings must be kept in the same order as the fields
 * declared for slurm_acct_gather_profile_ops_t.
 */
static const char *syms[] = {
	"acct_gather_profile_p_child_forked",
	"acct_gather_profile_p_conf_options",
	"acct_gather_profile_p_conf_set",
	"acct_gather_profile_p_get",
	"acct_gather_profile_p_node_step_start",
	"acct_gather_profile_p_node_step_end",
	"acct_gather_profile_p_task_start",
	"acct_gather_profile_p_task_end",
	"acct_gather_profile_p_create_group",
	"acct_gather_profile_p_create_dataset",
	"acct_gather_profile_p_add_sample_data",
#ifdef __METASTACK_LOAD_ABNORMAL
    "acct_gather_profile_p_add_sample_data_stepd",
#endif
	"acct_gather_profile_p_conf_values",
	"acct_gather_profile_p_is_active",
};

acct_gather_profile_timer_t acct_gather_profile_timer[PROFILE_CNT];
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
acct_gather_profile_timer_t acct_gather_profile_timer_watch_dog;
static bool init_watch_dog = false;
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
/*
	This variable controls how many times the acctg_apptype thread is woked, which only collects 
	job data early in the job run to analyze the application type. When the variable is set to 
	zero, the acctg_apptype thread is woked one last time and exited to keep thread overhead as 
	low as possible
*/
int apptype_recongn_count = 0;
#endif
static bool acct_gather_profile_running = false;
static pthread_mutex_t profile_running_mutex = PTHREAD_MUTEX_INITIALIZER;

static slurm_acct_gather_profile_ops_t ops;
static plugin_context_t *g_context = NULL;
static pthread_mutex_t g_context_lock =	PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t profile_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t timer_thread_id = 0;
static pthread_mutex_t timer_thread_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t timer_thread_cond = PTHREAD_COND_INITIALIZER;
static bool init_run = false;

#ifdef __METASTACK_LOAD_ABNORMAL

/*Set the calling frequency of stepd aggregation thread*/
static void _set_freq_2(int type, char *freq, char *freq_def, acct_gather_rank_t* step_rank)
{

	slurm_mutex_lock(&step_gather.lock);
	int tmp = step_gather.max_depth_gather * 2 + 1;
	slurm_mutex_unlock(&step_gather.lock);

	/*
	 *Get the value of 'freq', with the highest priority given to the value specified in the job, 
	 *followed by slurm.conf. If neither the job nor slurm.conf specifies the value, then set it 
	 *to zero, disabling the functionality.
	 */
	if ((acct_gather_profile_timer[type].freq =
		acct_gather_parse_freq(type, freq)) == -1)
		if ((acct_gather_profile_timer[type].freq =
			acct_gather_parse_freq(type, freq_def)) == -1)
			acct_gather_profile_timer[type].freq = 0;
		
	/*
	*Set the timer interval for the anomaly detection thread based on the comparison between 
	*the sampling time and the interval for anomaly detection.		 
	*/
	if((acct_gather_profile_timer[type].freq > 0) && tmp > 0) {
		if(step_rank->timer  <= (acct_gather_profile_timer[type].freq) ) {
			step_rank->timer = ((acct_gather_profile_timer[type].freq + 59) / 60) * 60;
			if(step_rank->step != BATCH_STEP) {
				for(int i = tmp ; i <= step_rank->timer ; ++i) {
					if(step_rank->timer % i == 0){
						acct_gather_profile_timer[type].freq = step_rank->timer / i;
						break;
					}
				}
			} else
				acct_gather_profile_timer[type].freq = step_rank->timer / 3;
		} else {
			int time = 0;
			for(int i = tmp ; i <= step_rank->timer ; ++i) {
				if(step_rank->timer % i == 0) {
					time = step_rank->timer / i;
					break;
				}
			}
			if(step_rank->step != BATCH_STEP) {
				acct_gather_profile_timer[type].freq = time;
			} else
				acct_gather_profile_timer[type].freq = step_rank->timer / 3;
		}
	} else if(acct_gather_profile_timer[type].freq > 0) {
		if(step_rank->timer  <= (acct_gather_profile_timer[type].freq) ) 
			step_rank->timer = ((acct_gather_profile_timer[type].freq + 59) / 60) * 60;
		acct_gather_profile_timer[type].freq = step_rank->timer / 3;
	}
}
#endif

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
static void _set_freq_watch_dog(acct_gather_rank_t step_rank)
{
	if(step_rank.enable_watchdog) {
		if(step_rank.period < 2147483647)
			acct_gather_profile_timer_watch_dog.freq = (int)step_rank.period;
		else {
			acct_gather_profile_timer_watch_dog.freq = 0;
			debug("the period (%d) is too large. Please contact the administrator to "
					"set a reasonable value; the value must be less than 2147483647",step_rank.period);
		}

	} else
		acct_gather_profile_timer_watch_dog.freq = 0;
}
#endif

static void _set_freq(int type, char *freq, char *freq_def)
{
	if ((acct_gather_profile_timer[type].freq =
	     acct_gather_parse_freq(type, freq)) == -1)
		if ((acct_gather_profile_timer[type].freq =
		     acct_gather_parse_freq(type, freq_def)) == -1)
			acct_gather_profile_timer[type].freq = 0;
}

/*
 * This thread wakes up other profiling threads in the jobacct plugins,
 * and operates on a 1-second granularity.
 */

static void *_timer_thread(void *args)
{
	int i, now, diff;
	struct timeval tvnow;
	struct timespec abs;

#if HAVE_SYS_PRCTL_H
	if (prctl(PR_SET_NAME, "acctg_prof", NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m",
		      __func__, "acctg_prof");
	}
#endif
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	acct_gather_rank_t * rank_stepd = (acct_gather_rank_t *) args;
#endif
	/* setup timer */
	gettimeofday(&tvnow, NULL);
	abs.tv_sec = tvnow.tv_sec;
	abs.tv_nsec = tvnow.tv_usec * 1000;

	while (init_run && acct_gather_profile_test()) {
		slurm_mutex_lock(&g_context_lock);
		now = time(NULL);

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
		for (i=0; i<PROFILE_CNT + 1; i++) {
			if(i < PROFILE_CNT) {
				if (acct_gather_suspend_test()) {
					/* Handle suspended time as if it
					* didn't happen */
					if (!acct_gather_profile_timer[i].freq)
						continue;
					if (acct_gather_profile_timer[i].last_notify)
						acct_gather_profile_timer[i].
							last_notify += SLEEP_TIME;
					else
						acct_gather_profile_timer[i].
							last_notify = now;
					continue;
				}

				diff = now - acct_gather_profile_timer[i].last_notify;
				/* info ("%d is %d and %d", i, */
				/*       acct_gather_profile_timer[i].freq, */
				/*       diff); */
				if (!acct_gather_profile_timer[i].freq
					|| (diff < acct_gather_profile_timer[i].freq))
					continue;
				if (!acct_gather_profile_test())
					break;	/* Shutting down */
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
				/*
					Avoid printing invalid logs related to apptype recognition
				*/
				if (i != PROFILE_APPTYPE)
					debug2("profile signaling type %s", acct_gather_profile_type_t_name(i));
#else
				debug2("profile signaling type %s",
					acct_gather_profile_type_t_name(i));
#endif

				/* signal poller to start */
				slurm_mutex_lock(&acct_gather_profile_timer[i].
						notify_mutex);
				slurm_cond_signal(
					&acct_gather_profile_timer[i].notify);
				slurm_mutex_unlock(&acct_gather_profile_timer[i].
						notify_mutex);
				acct_gather_profile_timer[i].last_notify = now;
			} else {
				if (acct_gather_suspend_test()) {
					/* Handle suspended time as if it
						* didn't happen */
					if (!acct_gather_profile_timer_watch_dog.freq)
						continue;
					if (acct_gather_profile_timer_watch_dog.last_notify)
						acct_gather_profile_timer_watch_dog.
							last_notify += SLEEP_TIME;
					else
						acct_gather_profile_timer_watch_dog.
							last_notify = now;
					continue;
				}
				diff = now - acct_gather_profile_timer_watch_dog.last_notify;

				/* info ("%d is %d and %d", i, */
				/*       acct_gather_profile_timer[i].freq, */
				/*       diff); */
				if(!acct_gather_profile_timer_watch_dog.freq)
					continue;

				if(((rank_stepd->init_time > 0) && (!init_watch_dog))) {
					if(diff < (acct_gather_profile_timer_watch_dog.freq + rank_stepd->init_time))
						continue;
				} else if ((diff < acct_gather_profile_timer_watch_dog.freq))
					continue;

				if((!init_watch_dog) && (acct_gather_profile_timer_watch_dog.last_notify != 0))
               		 init_watch_dog = true;

				if (!acct_gather_profile_test())
					break;	/* Shutting down */

				/* signal poller to start */
				slurm_mutex_lock(&acct_gather_profile_timer_watch_dog.
							notify_mutex);
				slurm_cond_signal(
					&acct_gather_profile_timer_watch_dog.notify);
				slurm_mutex_unlock(&acct_gather_profile_timer_watch_dog.
							notify_mutex);
				acct_gather_profile_timer_watch_dog.last_notify = now;
			}

		}
#else
		for (i=0; i<PROFILE_CNT; i++) {
			if (acct_gather_suspend_test()) {
				/* Handle suspended time as if it
				 * didn't happen */
				if (!acct_gather_profile_timer[i].freq)
					continue;
				if (acct_gather_profile_timer[i].last_notify)
					acct_gather_profile_timer[i].
						last_notify += SLEEP_TIME;
				else
					acct_gather_profile_timer[i].
						last_notify = now;
				continue;
			}

			diff = now - acct_gather_profile_timer[i].last_notify;
			/* info ("%d is %d and %d", i, */
			/*       acct_gather_profile_timer[i].freq, */
			/*       diff); */
			if (!acct_gather_profile_timer[i].freq
			    || (diff < acct_gather_profile_timer[i].freq))
				continue;
			if (!acct_gather_profile_test())
				break;	/* Shutting down */
			debug2("profile signaling type %s",
			       acct_gather_profile_type_t_name(i));

			/* signal poller to start */
			slurm_mutex_lock(&acct_gather_profile_timer[i].
					 notify_mutex);
			slurm_cond_signal(
				&acct_gather_profile_timer[i].notify);
			slurm_mutex_unlock(&acct_gather_profile_timer[i].
					   notify_mutex);
			acct_gather_profile_timer[i].last_notify = now;

			
		}
#endif
		slurm_mutex_unlock(&g_context_lock);

		/*
		 * Sleep until the next second interval, or until signaled
		 * to shutdown by acct_gather_profile_fini().
		 */

		abs.tv_sec += 1;
		slurm_mutex_lock(&timer_thread_mutex);
		slurm_cond_timedwait(&timer_thread_cond, &timer_thread_mutex,
				     &abs);
		slurm_mutex_unlock(&timer_thread_mutex);
	}
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION	
	if(rank_stepd) {
		xfree(rank_stepd->watch_dog_script);
		xfree(rank_stepd);
	}
#endif
	return NULL;
}

extern int acct_gather_profile_init(void)
{
	int retval = SLURM_SUCCESS;
	char *plugin_type = "acct_gather_profile";

	if (init_run && g_context)
		return retval;

	slurm_mutex_lock(&g_context_lock);

	if (g_context)
		goto done;

	g_context = plugin_context_create(plugin_type,
					  slurm_conf.acct_gather_profile_type,
					  (void **) &ops, syms, sizeof(syms));

	if (!g_context) {
		error("cannot create %s context for %s",
		      plugin_type, slurm_conf.acct_gather_profile_type);
		retval = SLURM_ERROR;
		goto done;
	}
	init_run = true;

done:
	slurm_mutex_unlock(&g_context_lock);
	if (retval == SLURM_SUCCESS)
		retval = acct_gather_conf_init();
	if (retval != SLURM_SUCCESS)
		fatal("can not open the %s plugin",
		      slurm_conf.acct_gather_profile_type);

	return retval;
}

extern int acct_gather_profile_fini(void)
{
	int rc = SLURM_SUCCESS, i;

	if (!g_context)
		return SLURM_SUCCESS;

	slurm_mutex_lock(&g_context_lock);

	if (!g_context)
		goto done;

	init_run = false;

	for (i=0; i < PROFILE_CNT; i++) {
		switch (i) {
		case PROFILE_ENERGY:
			acct_gather_energy_fini();
			break;
		case PROFILE_TASK:
			jobacct_gather_fini();
			break;
		case PROFILE_FILESYSTEM:
			acct_gather_filesystem_fini();
			break;
		case PROFILE_NETWORK:
			acct_gather_interconnect_fini();
			break;
#ifdef __METASTACK_LOAD_ABNORMAL
		case PROFILE_STEPD:
			break;
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
		case PROFILE_APPTYPE:
			break;
#endif
		default:
			fatal("Unhandled profile option %d please update "
			      "slurm_acct_gather_profile.c "
			      "(acct_gather_profile_fini)", i);
		}
	}

	if (timer_thread_id) {
		slurm_mutex_lock(&timer_thread_mutex);
		slurm_cond_signal(&timer_thread_cond);
		slurm_mutex_unlock(&timer_thread_mutex);
		pthread_join(timer_thread_id, NULL);
	}

	rc = plugin_context_destroy(g_context);
	g_context = NULL;
done:
	slurm_mutex_unlock(&g_context_lock);

	return rc;
}

extern void acct_gather_profile_to_string_r(uint32_t profile,
					    char *profile_str)
{
	if (profile == ACCT_GATHER_PROFILE_NOT_SET)
		strcat(profile_str, "NotSet");
	else if (profile == ACCT_GATHER_PROFILE_NONE)
		strcat(profile_str, "None");
	else {
		if (profile & ACCT_GATHER_PROFILE_ENERGY)
			strcat(profile_str, "Energy");
		if (profile & ACCT_GATHER_PROFILE_LUSTRE) {
			if (profile_str[0])
				strcat(profile_str, ",");
			strcat(profile_str, "Lustre");
		}
		if (profile & ACCT_GATHER_PROFILE_NETWORK) {
			if (profile_str[0])
				strcat(profile_str, ",");
			strcat(profile_str, "Network");
		}
		if (profile & ACCT_GATHER_PROFILE_TASK) {
			if (profile_str[0])
				strcat(profile_str, ",");
			strcat(profile_str, "Task");
		}
#ifdef __METASTACK_LOAD_ABNORMAL
		/*used for function printing and api interface*/
		if (profile & ACCT_GATHER_PROFILE_STEPD) {
			if (profile_str[0])
				strcat(profile_str, ",");
			strcat(profile_str, "Jobmonitor");
		}
#endif	
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
		if (profile & ACCT_GATHER_PROFILE_APPTYPE) {
			if (profile_str[0])
				strcat(profile_str, ",");
			strcat(profile_str, "Apptype");
		}
#endif
	}
}

extern char *acct_gather_profile_to_string(uint32_t profile)
{
	static char profile_str[128];

	profile_str[0] = '\0';
	acct_gather_profile_to_string_r(profile, profile_str);

	return profile_str;
}

extern uint32_t acct_gather_profile_from_string(const char *profile_str)
{
	uint32_t profile = ACCT_GATHER_PROFILE_NOT_SET;

        if (!profile_str) {
	} else if (xstrcasestr(profile_str, "none"))
		profile = ACCT_GATHER_PROFILE_NONE;
	else if (xstrcasestr(profile_str, "all"))
		profile = ACCT_GATHER_PROFILE_ALL;
	else {
		if (xstrcasestr(profile_str, "energy"))
			profile |= ACCT_GATHER_PROFILE_ENERGY;
		if (xstrcasestr(profile_str, "task"))
			profile |= ACCT_GATHER_PROFILE_TASK;

		if (xstrcasestr(profile_str, "lustre"))
			profile |= ACCT_GATHER_PROFILE_LUSTRE;

		if (xstrcasestr(profile_str, "network"))
			profile |= ACCT_GATHER_PROFILE_NETWORK;
#ifdef __METASTACK_LOAD_ABNORMAL
		if (xstrcasestr(profile_str, "jobmonitor"))
			profile |= ACCT_GATHER_PROFILE_STEPD;
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
		if (xstrcasestr(profile_str, "apptype"))
			profile |= ACCT_GATHER_PROFILE_APPTYPE;
#endif
	}

	return profile;
}

extern char *acct_gather_profile_type_to_string(uint32_t series)
{
	if (series == ACCT_GATHER_PROFILE_ENERGY)
		return "Energy";
	else if (series == ACCT_GATHER_PROFILE_TASK)
		return "Task";
	else if (series == ACCT_GATHER_PROFILE_LUSTRE)
		return "Lustre";
	else if (series == ACCT_GATHER_PROFILE_NETWORK)
		return "Network";

	return "Unknown";
}

extern uint32_t acct_gather_profile_type_from_string(char *series_str)
{
	if (!xstrcasecmp(series_str, "energy"))
		return ACCT_GATHER_PROFILE_ENERGY;
	else if (!xstrcasecmp(series_str, "task"))
		return ACCT_GATHER_PROFILE_TASK;
	else if (!xstrcasecmp(series_str, "lustre"))
		return ACCT_GATHER_PROFILE_LUSTRE;
	else if (!xstrcasecmp(series_str, "network"))
		return ACCT_GATHER_PROFILE_NETWORK;

	return ACCT_GATHER_PROFILE_NOT_SET;
}

extern char *acct_gather_profile_type_t_name(acct_gather_profile_type_t type)
{
	switch (type) {
	case PROFILE_ENERGY:
		return "Energy";
		break;
	case PROFILE_TASK:
		return "Task";
		break;
	case PROFILE_FILESYSTEM:
		return "Lustre";
		break;
	case PROFILE_NETWORK:
		return "Network";
		break;
	case PROFILE_CNT:
		return "CNT?";
		break;
#ifdef __METASTACK_LOAD_ABNORMAL
	/*The name of the newly opened thread*/
	case PROFILE_STEPD:
		return "Jobmonitor";
		break;
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	case PROFILE_APPTYPE:
		return "Apptype";
		break;
#endif
	default:
		fatal("Unhandled profile option %d please update "
		      "slurm_acct_gather_profile.c "
		      "(acct_gather_profile_type_t_name)", type);
	}

	return "Unknown";
}

extern char *acct_gather_profile_dataset_str(
	acct_gather_profile_dataset_t *dataset, void *data,
	char *str, int str_len)
{
	int cur_loc = 0;

        while (dataset && (dataset->type != PROFILE_FIELD_NOT_SET)) {
		switch (dataset->type) {
		case PROFILE_FIELD_UINT64:
			cur_loc += snprintf(str+cur_loc, str_len-cur_loc,
					    "%s%s=%"PRIu64,
					    cur_loc ? " " : "",
					    dataset->name, *(uint64_t *)data);
			data += sizeof(uint64_t);
			break;
		case PROFILE_FIELD_DOUBLE:
			cur_loc += snprintf(str+cur_loc, str_len-cur_loc,
					    "%s%s=%lf",
					    cur_loc ? " " : "",
					    dataset->name, *(double *)data);
			data += sizeof(double);
			break;
		case PROFILE_FIELD_NOT_SET:
			break;
		}

		if (cur_loc >= str_len)
			break;
		dataset++;
	}

	return str;
}

#ifdef __METASTACK_LOAD_ABNORMAL
/*
*Used to set the parameters of the new aggregation thread, such as whether 
*to start the exception collection function in the job step.
*/
static void acct_gather_set_parameters(char *freq, char* freq_def, acct_gather_rank_t* step_rank )
{
	int enable = 0;
	step_rank->timer = -1;
	step_rank->cpu_min_load = -1;
	step_rank->switch_step = false;
    
	if(!acct_gather_parse_sw(freq_def)) {

		/*Read parameter settings*/
		step_rank->timer  = acct_gather_parse_time(freq, freq_def);

		if(step_rank->timer > 0)
			step_rank->timer = step_rank->timer *60;
		else
			step_rank->timer = 300;//set default value 
		/*Set the cpu utilization threshold size in the job step*/
		step_rank->cpu_min_load = acct_gather_parse_cpu_load(freq, freq_def);
        if((step_rank->cpu_min_load < 0 ) || (step_rank->cpu_min_load > 100 )) {
			if(step_rank->cpu_min_load < 0) {
				debug3("If the avecpuutil is not set or the value is faulty," 
				  "set it to the default value.(avecpuutil=0)");	
				  step_rank->cpu_min_load = 0;			
			} 
			if(step_rank->cpu_min_load > 100) {
				debug3("If the avecpuutil is not set or the value is faulty," 
				  "set it to the default value.(avecpuutil=100)");
				  step_rank->cpu_min_load = 100;					
			} 
			
		} 
		/*Job step enable exception detection flag*/
		enable = acct_gather_parse_monitor(freq, freq_def);
		
		/*determine job step type*/
		switch(step_rank->step) {
			case DATA_STEP:
				if(enable == ENABLE_DIG || enable == ENABLE_ALL)
					step_rank->switch_step = true;
				break;
			case EXTERN_STEP:
				step_rank->switch_step = false;
				break;
			case BATCH_STEP:
				if(enable == ENABLE_BATCH || enable == ENABLE_ALL)
					step_rank->switch_step = true;
				break;
			default:
				break;
		}
	}
}

extern int acct_gather_profile_startpoll(char *freq, char *freq_def, acct_gather_rank_t step_rank)
#endif
{
	int i;
	uint32_t profile = ACCT_GATHER_PROFILE_NOT_SET;
#ifdef __METASTACK_LOAD_ABNORMAL
	if(step_rank.step != EXTERN_STEP)
	 	acct_gather_set_parameters(freq, freq_def, &step_rank);
#endif
	if (acct_gather_profile_init() < 0) 
		return SLURM_ERROR;

	slurm_mutex_lock(&profile_running_mutex);
	if (acct_gather_profile_running) {
		slurm_mutex_unlock(&profile_running_mutex);
		error("acct_gather_profile_startpoll: poll already started!");
		return SLURM_SUCCESS;
	}
	acct_gather_profile_running = true;
	slurm_mutex_unlock(&profile_running_mutex);

	(*(ops.get))(ACCT_GATHER_PROFILE_RUNNING, &profile);
	xassert(profile != ACCT_GATHER_PROFILE_NOT_SET);
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	step_rank.profile = profile;
#endif
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	memset(&acct_gather_profile_timer_watch_dog, 0,
		       sizeof(acct_gather_profile_timer_t));
	slurm_cond_init(&acct_gather_profile_timer_watch_dog.notify, NULL);
	slurm_mutex_init(&acct_gather_profile_timer_watch_dog.notify_mutex);
#endif

	for (i=0; i < PROFILE_CNT; i++) {
		memset(&acct_gather_profile_timer[i], 0,
		       sizeof(acct_gather_profile_timer_t));
		slurm_cond_init(&acct_gather_profile_timer[i].notify, NULL);
		slurm_mutex_init(&acct_gather_profile_timer[i].notify_mutex);

		switch (i) {
		case PROFILE_ENERGY:
			if (!(profile & ACCT_GATHER_PROFILE_ENERGY))
				break;
			_set_freq(i, freq, freq_def);

			acct_gather_energy_startpoll(
				acct_gather_profile_timer[i].freq);
			break;
		case PROFILE_TASK:
			/* Always set up the task (always first) to be
			   done since it is used to control memory
			   consumption and such.  It will check
			   profile inside it's plugin.
			*/
#ifdef __METASTACK_LOAD_ABNORMAL
			_set_freq(i, freq, freq_def);
			if((step_rank.timer > 0) && (step_rank.step != EXTERN_STEP) &&
						(step_rank.timer <= acct_gather_profile_timer[i].freq)) {
				step_rank.timer = ((acct_gather_profile_timer[i].freq + 59) / 60) * 60;
			} 
			jobacct_gather_startpoll(
				acct_gather_profile_timer[i].freq, step_rank);
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
			if(step_rank.enable_watchdog) {
                /*set the frequency of new threads*/
				_set_freq_watch_dog(step_rank);
				
				jobacct_gather_watchdog(
					acct_gather_profile_timer_watch_dog.freq, step_rank);
			}
#endif
#endif
			break;
		case PROFILE_FILESYSTEM:
			if (!(profile & ACCT_GATHER_PROFILE_LUSTRE))
				break;
			_set_freq(i, freq, freq_def);

			acct_gather_filesystem_startpoll(
				acct_gather_profile_timer[i].freq);
			break;
		case PROFILE_NETWORK:
			if (!(profile & ACCT_GATHER_PROFILE_NETWORK))
				break;
			_set_freq(i, freq, freq_def);

			acct_gather_interconnect_startpoll(
				acct_gather_profile_timer[i].freq);
			break;
#ifdef __METASTACK_LOAD_ABNORMAL
		case PROFILE_STEPD:
			if((step_rank.timer > 0)  && (step_rank.step != EXTERN_STEP)) {
                /*set the frequency of new threads*/
				_set_freq_2(i, freq, freq_def, &step_rank);
				jobacct_gather_stepdpoll(
					acct_gather_profile_timer[i].freq, step_rank);
			}
			break;
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
		case PROFILE_APPTYPE:
			// Comment the code below to apply apptype recognition decoupled from the influxdb plugin
			/*
				ProfileInfluxDBDefault If apptype is not enabled, the acctg_apptype thread will 
				not be started. This is because apptype's query means are dependent on influxdb 
				plugin, independent of slurm.
				if (!(profile & ACCT_GATHER_PROFILE_APPTYPE))
					break;
			*/

			/*
				The frequency of passing NULL to represent acctg_apptype is not affected by the 
				user side and depends only on slurm.conf
			*/
			_set_freq(i, NULL, freq_def);
			jobacct_gather_apptypepoll(acct_gather_profile_timer[i].freq, step_rank);
			break;
#endif
		default:
			fatal("Unhandled profile option %d please update "
			      "slurm_acct_gather_profile.c "
			      "(acct_gather_profile_startpoll)", i);
		}
	}

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	/* create polling thread */
	acct_gather_rank_t *load_args = xmalloc(sizeof(acct_gather_rank_t));
	load_args->enable_watchdog  = true;
	load_args->watch_dog_script = xstrdup(step_rank.watch_dog_script);
	load_args->period           = step_rank.period;
	load_args->init_time        = step_rank.init_time;
	load_args->style_step       = step_rank.style_step;
	slurm_thread_create(&timer_thread_id, _timer_thread, load_args);
#endif

	debug3("acct_gather_profile_startpoll dynamic logging enabled");

	return SLURM_SUCCESS;
}

extern void acct_gather_profile_endpoll(void)
{
	int i;

	slurm_mutex_lock(&profile_running_mutex);
	if (!acct_gather_profile_running) {
		slurm_mutex_unlock(&profile_running_mutex);
		debug2("acct_gather_profile_startpoll: poll already ended!");
		return;
	}
	acct_gather_profile_running = false;
	slurm_mutex_unlock(&profile_running_mutex);

	for (i=0; i < PROFILE_CNT; i++) {
		/* end remote threads */
		slurm_mutex_lock(&acct_gather_profile_timer[i].notify_mutex);
		slurm_cond_signal(&acct_gather_profile_timer[i].notify);
		slurm_mutex_unlock(&acct_gather_profile_timer[i].notify_mutex);
		acct_gather_profile_timer[i].freq = 0;
		switch (i) {
		case PROFILE_ENERGY:
			break;
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
		case PROFILE_APPTYPE:
#endif
#ifdef __METASTACK_LOAD_ABNORMAL
		case PROFILE_STEPD:
#endif
		case PROFILE_TASK:
			jobacct_gather_endpoll();
			break;
		case PROFILE_FILESYSTEM:
			break;
		case PROFILE_NETWORK:
			break;
		default:
			fatal("Unhandled profile option %d please update "
			      "slurm_acct_gather_profile.c "
			      "(acct_gather_profile_endpoll)", i);
		}
	}
}

extern int acct_gather_profile_g_child_forked(void)
{
	if (acct_gather_profile_init() < 0)
		return SLURM_ERROR;

	(*(ops.child_forked))();
	return SLURM_SUCCESS;
}

extern int acct_gather_profile_g_conf_options(s_p_options_t **full_options,
					       int *full_options_cnt)
{
	if (acct_gather_profile_init() < 0)
		return SLURM_ERROR;

	(*(ops.conf_options))(full_options, full_options_cnt);
	return SLURM_SUCCESS;
}

extern int acct_gather_profile_g_conf_set(s_p_hashtbl_t *tbl)
{
	if (acct_gather_profile_init() < 0)
		return SLURM_ERROR;

	(*(ops.conf_set))(tbl);
	return SLURM_SUCCESS;
}

extern int acct_gather_profile_g_get(enum acct_gather_profile_info info_type,
				      void *data)
{
	if (acct_gather_profile_init() < 0)
		return SLURM_ERROR;

	(*(ops.get))(info_type, data);
	return SLURM_SUCCESS;
}

extern int acct_gather_profile_g_node_step_start(stepd_step_rec_t* job)
{
	if (acct_gather_profile_init() < 0)
		return SLURM_ERROR;

	return (*(ops.node_step_start))(job);
}

extern int acct_gather_profile_g_node_step_end(void)
{
	int retval = SLURM_ERROR;


	retval = (*(ops.node_step_end))();
	return retval;
}

extern int acct_gather_profile_g_task_start(uint32_t taskid)
{
	int retval = SLURM_ERROR;

	if (acct_gather_profile_init() < 0)
		return retval;

	slurm_mutex_lock(&profile_mutex);
	retval = (*(ops.task_start))(taskid);
	slurm_mutex_unlock(&profile_mutex);
	return retval;
}

extern int acct_gather_profile_g_task_end(pid_t taskpid)
{
	int retval = SLURM_ERROR;

	if (acct_gather_profile_init() < 0)
		return retval;

	slurm_mutex_lock(&profile_mutex);
	retval = (*(ops.task_end))(taskpid);
	slurm_mutex_unlock(&profile_mutex);
	return retval;
}

extern int64_t acct_gather_profile_g_create_group(const char *name)
{
	int64_t retval = SLURM_ERROR;

	if (acct_gather_profile_init() < 0)
		return retval;

	slurm_mutex_lock(&profile_mutex);
	retval = (*(ops.create_group))(name);
	slurm_mutex_unlock(&profile_mutex);
	return retval;
}

extern int acct_gather_profile_g_create_dataset(
	const char *name, int64_t parent,
	acct_gather_profile_dataset_t *dataset)
{
	int retval = SLURM_ERROR;

	if (acct_gather_profile_init() < 0)
		return retval;

	slurm_mutex_lock(&profile_mutex);
	retval = (*(ops.create_dataset))(name, parent, dataset);
	slurm_mutex_unlock(&profile_mutex);
	return retval;
}

#ifdef __METASTACK_LOAD_ABNORMAL
extern int acct_gather_profile_g_add_sample_data_stepd(int dataset_id, void* data,
						 time_t sample_time)
{
	int retval = SLURM_ERROR;

	if (acct_gather_profile_init() < 0)
		return retval;

	slurm_mutex_lock(&profile_mutex);
	retval = (*(ops.add_sample_data_stepd))(dataset_id, data, sample_time);
	slurm_mutex_unlock(&profile_mutex);
	return retval;
}
#endif

extern int acct_gather_profile_g_add_sample_data(int dataset_id, void* data,
						 time_t sample_time)
{
	int retval = SLURM_ERROR;

	if (acct_gather_profile_init() < 0)
		return retval;

	slurm_mutex_lock(&profile_mutex);
	retval = (*(ops.add_sample_data))(dataset_id, data, sample_time);
	slurm_mutex_unlock(&profile_mutex);
	return retval;
}

extern void acct_gather_profile_g_conf_values(void *data)
{
	if (acct_gather_profile_init() < 0)
		return;

	(*(ops.conf_values))(data);
}

extern bool acct_gather_profile_g_is_active(uint32_t type)
{
	if (acct_gather_profile_init() < 0)
		return false;

	return (*(ops.is_active))(type);
}

extern bool acct_gather_profile_test(void)
{
	bool rc;
	slurm_mutex_lock(&profile_running_mutex);
	rc = acct_gather_profile_running;
	slurm_mutex_unlock(&profile_running_mutex);
	return rc;
}
