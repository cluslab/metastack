/*****************************************************************************\
 *  jobcomp_filetxt.c - text file slurm job completion logging plugin.
 *****************************************************************************
 *  Copyright (C) 2003 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette1@llnl.gov> et. al.
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

#include <fcntl.h>
#include <grp.h>
#include <inttypes.h>
#include <pwd.h>
#include <unistd.h>

#include "src/common/slurm_protocol_defs.h"
#include "src/common/slurm_jobcomp.h"
#include "src/common/parse_time.h"
#include "src/common/slurm_time.h"
#include "src/common/uid.h"
#include "filetxt_jobcomp_process.h"

//NOTE: import header slurmctld.h for calling utils function like "slurm_get_resource_node_detail"
//#ifdef __METASTACK_OPT_RESC_NODEDETAIL
#if (defined __METASTACK_OPT_RESC_NODEDETAIL) || (defined __METASTACK_OPT_SACCT_COMMAND) || (defined __METASTACK_OPT_SACCT_OUTPUT)
#include "src/slurmctld/slurmctld.h"
#endif
#ifdef __METASTACK_OPT_HIST_COMMAND
#include <sys/param.h>
#endif

/*
 * These variables are required by the generic plugin interface.  If they
 * are not found in the plugin, the plugin loader will ignore it.
 *
 * plugin_name - a string giving a human-readable description of the
 * plugin.  There is no maximum length, but the symbol must refer to
 * a valid string.
 *
 * plugin_type - a string suggesting the type of the plugin or its
 * applicability to a particular form of data or method of data handling.
 * If the low-level plugin API is used, the contents of this string are
 * unimportant and may be anything.  Slurm uses the higher-level plugin
 * interface which requires this string to be of the form
 *
 *	<application>/<method>
 *
 * where <application> is a description of the intended application of
 * the plugin (e.g., "jobcomp" for Slurm job completion logging) and <method>
 * is a description of how this plugin satisfies that application.  Slurm will
 * only load job completion logging plugins if the plugin_type string has a
 * prefix of "jobcomp/".
 *
 * plugin_version - an unsigned 32-bit integer containing the Slurm version
 * (major.minor.micro combined into a single number).
 */
const char plugin_name[]       	= "Job completion text file logging plugin";
const char plugin_type[]       	= "jobcomp/filetxt";
const uint32_t plugin_version	= SLURM_VERSION_NUMBER;

#if defined __METASTACK_OPT_ACCOUNTING_ENHANCE
#define JOB_FORMAT "JobId=%lu UserId=%s(%lu) GroupId=%s(%lu) Name=%s JobState=%s Partition=%s "\
		"TimeLimit=%s StartTime=%s EndTime=%s NodeList=%s NodeCnt=%u ProcCnt=%u "\
		"WorkDir=%s ReservationName=%s Gres=%s Account=%s QOS=%s "\
		"WcKey=%s Cluster=%s SubmitTime=%s EligibleTime=%s%s%s "\
		"Dependency=%s McsLabel=%s TaskCnt=%u PreemptTime=%s SuspendTime=%s TotalSuspTime=%s PreSuspTime=%s StdIn=%s StdOut=%s StdErr=%s Command=%s Shared=%d Comment=%s AdminComment=%s "\
		"ResourceNodeDetail=%s DerivedExitCode=%s ExitCode=%s %s\n"
#else
#define JOB_FORMAT "JobId=%lu UserId=%s(%lu) GroupId=%s(%lu) Name=%s JobState=%s Partition=%s "\
		"TimeLimit=%s StartTime=%s EndTime=%s NodeList=%s NodeCnt=%u ProcCnt=%u "\
		"WorkDir=%s ReservationName=%s Tres=%s Account=%s QOS=%s "\
		"WcKey=%s Cluster=%s SubmitTime=%s EligibleTime=%s%s%s "\
		"DerivedExitCode=%s ExitCode=%s %s\n"
#endif

/* File descriptor used for logging */
static pthread_mutex_t  file_lock = PTHREAD_MUTEX_INITIALIZER;
static char *           log_name  = NULL;
static int              job_comp_fd = -1;

/* get the user name for the give user_id */
static void
_get_user_name(uint32_t user_id, char *user_name, int buf_size)
{
	static uint32_t cache_uid      = 0;
	static char     cache_name[32] = "root", *uname;

	if (user_id != cache_uid) {
		uname = uid_to_string((uid_t) user_id);
		snprintf(cache_name, sizeof(cache_name), "%s", uname);
		xfree(uname);
		cache_uid = user_id;
	}
	snprintf(user_name, buf_size, "%s", cache_name);
}

/* get the group name for the give group_id */
static void
_get_group_name(uint32_t group_id, char *group_name, int buf_size)
{
	static uint32_t cache_gid      = 0;
	static char     cache_name[32] = "root", *gname;

	if (group_id != cache_gid) {
		gname = gid_to_string((gid_t) group_id);
		snprintf(cache_name, sizeof(cache_name), "%s", gname);
		xfree(gname);
		cache_gid = group_id;
	}
	snprintf(group_name, buf_size, "%s", cache_name);
}

/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */
int init ( void )
{
	return SLURM_SUCCESS;
}

int fini ( void )
{
	if (job_comp_fd >= 0)
		close(job_comp_fd);
	xfree(log_name);
	return SLURM_SUCCESS;
}

/*
 * The remainder of this file implements the standard Slurm job completion
 * logging API.
 */

extern int jobcomp_p_set_location(char *location)
{
	int rc = SLURM_SUCCESS;

	if (location == NULL) {
		return SLURM_ERROR;
	}
	xfree(log_name);
	log_name = xstrdup(location);

	slurm_mutex_lock( &file_lock );
	if (job_comp_fd >= 0)
		close(job_comp_fd);
	job_comp_fd = open(location, O_WRONLY | O_CREAT | O_APPEND, 0644);
	if (job_comp_fd == -1) {
		fatal("open %s: %m", location);
		rc = SLURM_ERROR;
	} else
		fchmod(job_comp_fd, 0644);
	slurm_mutex_unlock( &file_lock );
	return rc;
}

/* This is a variation of slurm_make_time_str() in src/common/parse_time.h
 * This version uses ISO8601 format by default. */
static void _make_time_str (time_t *time, char *string, int size)
{
	struct tm time_tm;

	if ( *time == (time_t) 0 ) {
		snprintf(string, size, "Unknown");
	} else {
		/* Format YYYY-MM-DDTHH:MM:SS, ISO8601 standard format */
		localtime_r(time, &time_tm);
		strftime(string, size, "%FT%T", &time_tm);
	}
}

#ifdef __METASTACK_OPT_HIST_COMMAND
/*
 * get command string from job detail argv[]
 * IN : detail_ptr
 * OUT: buf
 */
/*void get_command_str (struct job_details *detail_ptr, char **buf)
{
	int i;
	uint32_t len = 0;
	char *tmp = NULL;

	for (i =0; detail_ptr->argv[i]; i++) {
		len += strlen(detail_ptr->argv[i]);
	}
	len += i;

	*buf = xmalloc(len*sizeof(char));
	tmp = *buf;
	for (i = 0; detail_ptr->argv[i]; i++) {
		if (i != 0) {
			*tmp = ' ';
			tmp++;
		}
		strcpy(tmp,detail_ptr->argv[i]);
		tmp += strlen(detail_ptr->argv[i]);
	}
}
*/
#endif

#ifdef __METASTACK_OPT_ACCOUNTING_ENHANCE
/*
 * get shared from job detail
 * IN : detail_ptr
 * OUT: buf
 */
void get_shared (struct job_record *job_ptr, struct job_details *detail_ptr, uint16_t *shared)
{
	if (!detail_ptr)
		*shared = NO_VAL16;
	else if (detail_ptr->share_res == 1)	/* User --share */
		*shared = 1;
	else if ((detail_ptr->share_res == 0) ||
		 (detail_ptr->whole_node == 1))
		*shared = 0;			/* User --exclusive */
	else if (detail_ptr->whole_node == WHOLE_NODE_USER)
		*shared = JOB_SHARED_USER;	/* User --exclusive=user */
	else if (detail_ptr->whole_node == WHOLE_NODE_MCS)
		*shared = JOB_SHARED_MCS;	/* User --exclusive=mcs */
	else if (job_ptr->part_ptr) {
		/* Report shared status based upon latest partition info */
		if (job_ptr->part_ptr->flags & PART_FLAG_EXCLUSIVE_USER)
			*shared = JOB_SHARED_USER;
		else if ((job_ptr->part_ptr->max_share & SHARED_FORCE) &&
			 ((job_ptr->part_ptr->max_share & (~SHARED_FORCE)) > 1))
			*shared = 1;		/* Partition Shared=force */
		else if (job_ptr->part_ptr->max_share == 0)
			*shared = 0;		/* Partition Shared=exclusive */
		else
			*shared = NO_VAL16;  /* Part Shared=yes or no */
	} else
		*shared = NO_VAL16;	/* No user or partition info */
}
#endif

extern int jobcomp_p_log_record(job_record_t *job_ptr)
{
	int rc = SLURM_SUCCESS, tmp_int, tmp_int2;
#ifdef __METASTACK_OPT_ACCOUNTING_ENHANCE
	char job_rec[32768];
#else
	char job_rec[1024];
#endif
	char usr_str[32], grp_str[32], start_str[32], end_str[32], lim_str[32];
	char *resv_name, *tres, *account, *qos, *wckey, *cluster;
#ifdef __METASTACK_OPT_ACCOUNTING_ENHANCE
	char *orig_dependency = NULL, *std_in = NULL, *std_out = NULL, *std_err = NULL, *mcs_label = NULL;
	char *command = NULL, *comment = NULL, *admin_comment = NULL;
	char *resc_node_detail = NULL;
	struct job_details *detail_ptr = job_ptr->details;
	uint16_t shared = 0;
	uint32_t num_tasks = 0;
	char suspend_time[32], preempt_time[32], pre_susp_time[32], tot_sus_time[32];
#endif
	char *exit_code_str = NULL, *derived_ec_str = NULL;
	char submit_time[32], eligible_time[32], array_id[64], het_id[64];
	char select_buf[128], *state_string, *work_dir;
	size_t offset = 0, tot_size, wrote;
	uint32_t job_state;
	uint32_t time_limit;

	if ((log_name == NULL) || (job_comp_fd < 0)) {
		error("JobCompLoc log file %s not open", log_name);
		return SLURM_ERROR;
	}

	slurm_mutex_lock( &file_lock );
	_get_user_name(job_ptr->user_id, usr_str, sizeof(usr_str));
	_get_group_name(job_ptr->group_id, grp_str, sizeof(grp_str));

	if ((job_ptr->time_limit == NO_VAL) && job_ptr->part_ptr)
		time_limit = job_ptr->part_ptr->max_time;
	else
		time_limit = job_ptr->time_limit;
	if (time_limit == INFINITE)
		strcpy(lim_str, "UNLIMITED");
	else {
		snprintf(lim_str, sizeof(lim_str), "%lu",
			 (unsigned long) time_limit);
	}

	if (job_ptr->job_state & JOB_RESIZING) {
		time_t now = time(NULL);
		state_string = job_state_string(job_ptr->job_state);
		if (job_ptr->resize_time) {
			_make_time_str(&job_ptr->resize_time, start_str,
				       sizeof(start_str));
		} else {
			_make_time_str(&job_ptr->start_time, start_str,
				       sizeof(start_str));
		}
		_make_time_str(&now, end_str, sizeof(end_str));
	} else {
		/* Job state will typically have JOB_COMPLETING or JOB_RESIZING
		 * flag set when called. We remove the flags to get the eventual
		 * completion state: JOB_FAILED, JOB_TIMEOUT, etc. */
#ifdef __METASTACK_OPT_JOBCOMP_PENDING
		job_state = (job_ptr->job_state == JOB_REQUEUE) ? JOB_REQUEUE: job_ptr->job_state & JOB_STATE_BASE;
#else
		job_state = job_ptr->job_state & JOB_STATE_BASE;
#endif
		state_string = job_state_string(job_state);
		if (job_ptr->resize_time) {
			_make_time_str(&job_ptr->resize_time, start_str,
				       sizeof(start_str));
		} else if (job_ptr->start_time > job_ptr->end_time) {
			/* Job cancelled while pending and
			 * expected start time is in the future. */
			snprintf(start_str, sizeof(start_str), "Unknown");
		} else {
			_make_time_str(&job_ptr->start_time, start_str,
				       sizeof(start_str));
		}
		_make_time_str(&job_ptr->end_time, end_str, sizeof(end_str));
	}

	if (job_ptr->details && job_ptr->details->work_dir)
		work_dir = job_ptr->details->work_dir;
	else
		work_dir = "unknown";

	if (job_ptr->resv_name && job_ptr->resv_name[0])
		resv_name = job_ptr->resv_name;
	else
		resv_name = "";

	if (job_ptr->tres_fmt_req_str && job_ptr->tres_fmt_req_str[0])
		tres = job_ptr->tres_fmt_req_str;
	else
		tres = "";

	if (job_ptr->account && job_ptr->account[0])
		account = job_ptr->account;
	else
		account = "";

	if (job_ptr->qos_ptr != NULL) {
		qos = job_ptr->qos_ptr->name;
	} else
		qos = "";

	if (job_ptr->wckey && job_ptr->wckey[0])
		wckey = job_ptr->wckey;
	else
		wckey = "";

	if (job_ptr->assoc_ptr != NULL)
		cluster = job_ptr->assoc_ptr->cluster;
	else
		cluster = "unknown";

	if (job_ptr->details && job_ptr->details->submit_time) {
		_make_time_str(&job_ptr->details->submit_time,
			       submit_time, sizeof(submit_time));
	} else {
		snprintf(submit_time, sizeof(submit_time), "unknown");
	}

	if (job_ptr->details && job_ptr->details->begin_time) {
		_make_time_str(&job_ptr->details->begin_time,
			       eligible_time, sizeof(eligible_time));
	} else {
		snprintf(eligible_time, sizeof(eligible_time), "unknown");
	}

	if (job_ptr->array_task_id != NO_VAL) {
		snprintf(array_id, sizeof(array_id),
			 " ArrayJobId=%u ArrayTaskId=%u",
			 job_ptr->array_job_id, job_ptr->array_task_id);
	} else {
		array_id[0] = '\0';
	}

	if (job_ptr->het_job_id) {
		snprintf(het_id, sizeof(het_id),
			 " HetJobId=%u HetJobOffset=%u",
			 job_ptr->het_job_id, job_ptr->het_job_offset);
	} else {
		het_id[0] = '\0';
	}

	tmp_int = tmp_int2 = 0;
	if (job_ptr->derived_ec == NO_VAL)
		;
	else if (WIFSIGNALED(job_ptr->derived_ec))
		tmp_int2 = WTERMSIG(job_ptr->derived_ec);
	else if (WIFEXITED(job_ptr->derived_ec))
		tmp_int = WEXITSTATUS(job_ptr->derived_ec);
	xstrfmtcat(derived_ec_str, "%d:%d", tmp_int, tmp_int2);

	tmp_int = tmp_int2 = 0;
	if (job_ptr->exit_code == NO_VAL)
		;
	else if (WIFSIGNALED(job_ptr->exit_code))
		tmp_int2 = WTERMSIG(job_ptr->exit_code);
	else if (WIFEXITED(job_ptr->exit_code))
		tmp_int = WEXITSTATUS(job_ptr->exit_code);
	xstrfmtcat(exit_code_str, "%d:%d", tmp_int, tmp_int2);

	select_g_select_jobinfo_sprint(job_ptr->select_jobinfo,
		select_buf, sizeof(select_buf), SELECT_PRINT_MIXED);

#ifdef __METASTACK_OPT_ACCOUNTING_ENHANCE
	if (job_ptr->details)
	{
		if (job_ptr->details->orig_dependency && job_ptr->details->orig_dependency[0])
			orig_dependency = xstrdup(job_ptr->details->orig_dependency);

		//
		#ifdef __METASTACK_OPT_HIST_OUTPUT
		char stdin_path[MAXPATHLEN];
		char stdout_path[MAXPATHLEN];
		char stderr_path[MAXPATHLEN];
		if (job_ptr->batch_flag) {
			mgr_get_job_stdin(stdin_path, sizeof(stdin_path), job_ptr);
			mgr_get_job_stdout(stdout_path, sizeof(stdout_path), job_ptr);
			mgr_get_job_stderr(stderr_path, sizeof(stderr_path), job_ptr);
			std_in = xstrdup((char*)stdin_path);
			std_out = xstrdup((char*)stdout_path);
			std_err = xstrdup((char*)stderr_path);
		}
		#else
		if (job_ptr->details->std_in)
			std_in = xstrdup(job_ptr->details->std_in);
		if (job_ptr->details->std_out)
			std_out = xstrdup(job_ptr->details->std_out);
		if (job_ptr->details->std_err)
			std_err = xstrdup(job_ptr->details->std_err);
		#endif

		#ifdef __METASTACK_OPT_HIST_COMMAND
	//	if (detail_ptr->argv) {
	//		get_command_str(detail_ptr, &command);
	//	}
		command = xstrdup(job_ptr->command);
		#endif

		num_tasks = detail_ptr->num_tasks;
	}

	mcs_label = xstrdup(job_ptr->mcs_label);

	get_shared(job_ptr, detail_ptr, &shared);

	if (job_ptr->comment)
		comment = xstrdup(job_ptr->comment);
	if (job_ptr->admin_comment)
		admin_comment = xstrdup(job_ptr->admin_comment);

	if (job_ptr->suspend_time && job_ptr->tot_sus_time) {
        time_t real_sus_time = job_ptr->suspend_time - job_ptr->tot_sus_time;
        slurm_make_time_str(&real_sus_time, suspend_time, sizeof(suspend_time));
	} else if (job_ptr->suspend_time) {
		debug2("warning: job(%u)'s suspend_time is not null, but tot_sus_time is null", job_ptr->job_id);
	} else
		sprintf(suspend_time, "None");
	
	if (job_ptr->preempt_time) {
		slurm_make_time_str(&job_ptr->preempt_time, preempt_time, sizeof(preempt_time));
	} else
		sprintf(preempt_time, "None");

	if (job_ptr->pre_sus_time) {
	//	slurm_make_time_str(&job_ptr->pre_sus_time, pre_susp_time, sizeof(pre_susp_time));
		sprintf(pre_susp_time, "%ld", job_ptr->pre_sus_time);
	} else
		sprintf(pre_susp_time, "None");

	if (job_ptr->tot_sus_time) {
	//	slurm_make_time_str(&job_ptr->tot_sus_time, tot_sus_time, sizeof(tot_sus_time));
		sprintf(tot_sus_time, "%ld", job_ptr->tot_sus_time);
	} else
		sprintf(tot_sus_time, "None");

	#ifdef __METASTACK_OPT_RESC_NODEDETAIL
	//resc_node_detail = slurm_get_resource_node_detail(job_ptr, ',', ';');
	resc_node_detail = xstrdup(job_ptr->resource_node_detail);
	#endif

#endif

	snprintf(job_rec, sizeof(job_rec), JOB_FORMAT,
		 (unsigned long) job_ptr->job_id, usr_str,
		 (unsigned long) job_ptr->user_id, grp_str,
		 (unsigned long) job_ptr->group_id, job_ptr->name,
		 state_string, job_ptr->partition, lim_str, start_str,
		 end_str, job_ptr->nodes, job_ptr->node_cnt,
		 job_ptr->total_cpus, work_dir, resv_name, tres, account, qos,
		 wckey, cluster, submit_time, eligible_time, array_id, het_id,
#ifdef __METASTACK_OPT_ACCOUNTING_ENHANCE
		orig_dependency, mcs_label, num_tasks, preempt_time, suspend_time, tot_sus_time, pre_susp_time, std_in, std_out, std_err, command, shared, comment, admin_comment, resc_node_detail,
#endif
		 derived_ec_str, exit_code_str, select_buf);
	tot_size = strlen(job_rec);
#ifdef __METASTACK_OPT_ACCOUNTING_ENHANCE
	if (tot_size == (sizeof(job_rec) - 1)) {
		job_rec[sizeof(job_rec) - 2] = '\n';
	}
#endif

	while (offset < tot_size) {
		wrote = write(job_comp_fd, job_rec + offset,
			tot_size - offset);
		if (wrote == -1) {
			if (errno == EAGAIN)
				continue;
			else {
				rc = SLURM_ERROR;
				break;
			}
		}
		offset += wrote;
	}
	xfree(derived_ec_str);
	xfree(exit_code_str);
#ifdef __METASTACK_OPT_ACCOUNTING_ENHANCE
	xfree(orig_dependency);
	xfree(std_in);
	xfree(std_out);
	xfree(std_err);
	xfree(mcs_label);
	xfree(command);
	xfree(comment);
	xfree(admin_comment);
	xfree(resc_node_detail);
#endif
	slurm_mutex_unlock( &file_lock );
	return rc;
}

/*
 * get info from the database
 * in/out job_list List of job_rec_t *
 * note List needs to be freed when called
 */
extern List jobcomp_p_get_jobs(slurmdb_job_cond_t *job_cond)
{
	return filetxt_jobcomp_process_get_jobs(job_cond);
}
