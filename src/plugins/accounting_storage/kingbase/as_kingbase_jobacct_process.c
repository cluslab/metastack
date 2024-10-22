/*****************************************************************************\
 *  as_kingbase_jobacct_process.c - functions the processing of
 *                               information from the as_kingbase jobacct
 *                               storage.
 *****************************************************************************
 *  Copyright (C) 2008-2009 Lawrence Livermore National Security.
 *  Copyright (C) 2004-2007 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
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
 *
 *  This file is patterned after jobcomp_linux.c, written by Morris Jette and
 *  Copyright (C) 2002 The Regents of the University of California.
\*****************************************************************************/

#include "as_kingbase_jobacct_process.h"

typedef struct {
	hostlist_t hl;
	time_t start;
	time_t end;
	bitstr_t *asked_bitmap;
} local_cluster_t;

/* if this changes you will need to edit the corresponding
 * enum below also t1 is job_table */
char *job_req_inx[] = {
	"t1.account",
	"t1.admin_comment",
	"t1.array_max_tasks",
	"t1.array_task_str",
	"t1.constraints",
	"t1.container",
	"t1.cpus_req",
	"t1.derived_ec",
	"t1.derived_es",
	"t1.exit_code",
	"t1.flags",
	"t1.id_array_job",
	"t1.id_array_task",
	"t1.id_assoc",
	"t1.id_block",
	"t1.id_group",
	"t1.id_job",
	"t1.het_job_id",
	"t1.het_job_offset",
	"t1.id_qos",
	"t1.id_resv",
	"t3.resv_name",
	"t1.id_user",
	"t1.id_wckey",
	"t1.job_db_inx",
	"t1.job_name",
	"t1.kill_requid",
	"t1.mem_req",
	"t1.node_inx",
	"t1.nodelist",
	"t1.nodes_alloc",
	"t1.partition",
	"t1.priority",
	"t1.state",
	"t1.state_reason_prev",
	"t1.system_comment",
	"t1.time_eligible",
	"t1.time_end",
	"t1.time_start",
	"t1.time_submit",
	"t1.time_suspended",
	"t1.timelimit",
	"t1.wckey",
	"t1.gres_used",
	"t1.tres_alloc",
	"t1.tres_req",
	"t1.work_dir",
#ifdef __METASTACK_OPT_SACCT_COMMAND
	"t1.command",
#endif
#ifdef __METASTACK_OPT_SACCT_OUTPUT
	"t1.stdout",
	"t1.stderr",
#endif
	"t1.mcs_label",
	"t4.batch_script",
	"t1.submit_line",
	"t4.env_vars",
	"t2.acct",
	"t2.lft",
#ifdef __METASTACK_OPT_RESC_NODEDETAIL
	"t1.resource_node_detail",
#endif
	"t2.user"
};

enum {
	JOB_REQ_ACCOUNT1,
	JOB_REQ_ADMIN_COMMENT,
	JOB_REQ_ARRAY_MAX,
	JOB_REQ_ARRAY_STR,
	JOB_REQ_CONSTRAINTS,
	JOB_REQ_CONTAINER,
	JOB_REQ_REQ_CPUS,
	JOB_REQ_DERIVED_EC,
	JOB_REQ_DERIVED_ES,
	JOB_REQ_EXIT_CODE,
	JOB_REQ_FLAGS,
	JOB_REQ_ARRAYJOBID,
	JOB_REQ_ARRAYTASKID,
	JOB_REQ_ASSOCID,
	JOB_REQ_BLOCKID,
	JOB_REQ_GID,
	JOB_REQ_JOBID,
	JOB_REQ_HET_JOB_ID,
	JOB_REQ_HET_JOB_OFFSET,
	JOB_REQ_QOS,
	JOB_REQ_RESVID,
	JOB_REQ_RESV_NAME,
	JOB_REQ_UID,
	JOB_REQ_WCKEYID,
	JOB_REQ_DB_INX,
	JOB_REQ_NAME,
	JOB_REQ_KILL_REQUID,
	JOB_REQ_REQ_MEM,
	JOB_REQ_NODE_INX,
	JOB_REQ_NODELIST,
	JOB_REQ_ALLOC_NODES,
	JOB_REQ_PARTITION,
	JOB_REQ_PRIORITY,
	JOB_REQ_STATE,
	JOB_REQ_STATE_REASON,
	JOB_REQ_SYSTEM_COMMENT,
	JOB_REQ_ELIGIBLE,
	JOB_REQ_END,
	JOB_REQ_START,
	JOB_REQ_SUBMIT,
	JOB_REQ_SUSPENDED,
	JOB_REQ_TIMELIMIT,
	JOB_REQ_WCKEY,
	JOB_REQ_GRES_USED,
	JOB_REQ_TRESA,
	JOB_REQ_TRESR,
	JOB_REQ_WORK_DIR,
#ifdef __METASTACK_OPT_SACCT_COMMAND
	JOB_REQ_COMMAND,
#endif
#ifdef __METASTACK_OPT_SACCT_OUTPUT
	JOB_REQ_STDOUT,
	JOB_REQ_STDERR,
#endif
	JOB_REQ_MCS_LABEL,
	JOB_REQ_SCRIPT,
	JOB_REQ_SUBMIT_LINE,
	JOB_REQ_ENV,
	JOB_REQ_ACCOUNT,
	JOB_REQ_LFT,
#ifdef __METASTACK_OPT_RESC_NODEDETAIL
	JOB_REQ_RESC_NODE,
#endif
	JOB_REQ_USER_NAME,
	JOB_REQ_COUNT
};

/* if this changes you will need to edit the corresponding
 * enum below also t1 is step_table */
char *step_req_inx[] = {
	"t1.id_step",
	"t1.step_het_comp",
	"t1.time_start",
	"t1.time_end",
	"t1.time_suspended",
	"t1.step_name",
	"t1.nodelist",
	"t1.node_inx",
	"t1.state",
	"t1.kill_requid",
	"t1.exit_code",
	"t1.nodes_alloc",
	"t1.task_cnt",
	"t1.task_dist",
	"t1.user_sec",
	"t1.user_usec",
	"t1.sys_sec",
	"t1.sys_usec",
	"t1.act_cpufreq",
	"t1.consumed_energy",
	"t1.container",
	"t1.req_cpufreq_min",
	"t1.req_cpufreq",
	"t1.req_cpufreq_gov",
	"t1.submit_line",
	"t1.tres_alloc",
	"t1.tres_usage_in_max",
	"t1.tres_usage_in_max_taskid",
	"t1.tres_usage_in_max_nodeid",
	"t1.tres_usage_in_ave",
	"t1.tres_usage_in_min",
	"t1.tres_usage_in_min_taskid",
	"t1.tres_usage_in_min_nodeid",
	"t1.tres_usage_in_tot",
	"t1.tres_usage_out_max",
	"t1.tres_usage_out_max_taskid",
	"t1.tres_usage_out_max_nodeid",
	"t1.tres_usage_out_ave",
	"t1.tres_usage_out_min",
	"t1.tres_usage_out_min_taskid",
	"t1.tres_usage_out_min_nodeid",
	"t1.tres_usage_out_tot",
};

enum {
	STEP_REQ_STEPID,
	STEP_REQ_STEP_HET_COMP,
	STEP_REQ_START,
	STEP_REQ_END,
	STEP_REQ_SUSPENDED,
	STEP_REQ_NAME,
	STEP_REQ_NODELIST,
	STEP_REQ_NODE_INX,
	STEP_REQ_STATE,
	STEP_REQ_KILL_REQUID,
	STEP_REQ_EXIT_CODE,
	STEP_REQ_NODES,
	STEP_REQ_TASKS,
	STEP_REQ_TASKDIST,
	STEP_REQ_USER_SEC,
	STEP_REQ_USER_USEC,
	STEP_REQ_SYS_SEC,
	STEP_REQ_SYS_USEC,
	STEP_REQ_ACT_CPUFREQ,
	STEP_REQ_CONSUMED_ENERGY,
	STEP_REQ_CONTAINER,
	STEP_REQ_REQ_CPUFREQ_MIN,
	STEP_REQ_REQ_CPUFREQ_MAX,
	STEP_REQ_REQ_CPUFREQ_GOV,
	STEP_REQ_SUBMIT_LINE,
	STEP_REQ_TRES,
	STEP_REQ_TRES_USAGE_IN_MAX,
	STEP_REQ_TRES_USAGE_IN_MAX_TASKID,
	STEP_REQ_TRES_USAGE_IN_MAX_NODEID,
	STEP_REQ_TRES_USAGE_IN_AVE,
	STEP_REQ_TRES_USAGE_IN_MIN,
	STEP_REQ_TRES_USAGE_IN_MIN_TASKID,
	STEP_REQ_TRES_USAGE_IN_MIN_NODEID,
	STEP_REQ_TRES_USAGE_IN_TOT,
	STEP_REQ_TRES_USAGE_OUT_MAX,
	STEP_REQ_TRES_USAGE_OUT_MAX_TASKID,
	STEP_REQ_TRES_USAGE_OUT_MAX_NODEID,
	STEP_REQ_TRES_USAGE_OUT_AVE,
	STEP_REQ_TRES_USAGE_OUT_MIN,
	STEP_REQ_TRES_USAGE_OUT_MIN_TASKID,
	STEP_REQ_TRES_USAGE_OUT_MIN_NODEID,
	STEP_REQ_TRES_USAGE_OUT_TOT,
	STEP_REQ_COUNT
};

static void _setup_job_cond_selected_steps(slurmdb_job_cond_t *job_cond,
					   char *cluster_name, char **extra)
{
	ListIterator itr = NULL;
	slurm_selected_step_t *selected_step = NULL;

	if (!job_cond || (job_cond->flags & JOBCOND_FLAG_RUNAWAY))
		return;

	if (job_cond->step_list && list_count(job_cond->step_list)) {
		char *job_ids = NULL, *sep = "";
		char *array_job_ids = NULL, *array_task_ids = NULL;
		char *het_job_ids = NULL, *het_job_offset = NULL;

		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		itr = list_iterator_create(job_cond->step_list);
		while ((selected_step = list_next(itr))) {
			if (selected_step->array_task_id != NO_VAL) {
				if (array_task_ids){
					xstrcat(array_task_ids, " ,");
				}
				xstrfmtcat(array_task_ids, "(%u, %u)",
					   selected_step->step_id.job_id,
					   selected_step->array_task_id);
			} else if (selected_step->het_job_offset != NO_VAL) {
				if (het_job_ids){
					xstrcat(het_job_ids, " ,");
				}
				if (het_job_offset){
					xstrcat(het_job_offset, " ,");
				}
				xstrfmtcat(het_job_ids, "%u",
					   selected_step->step_id.job_id);
				xstrfmtcat(het_job_offset, "%u",
					   selected_step->het_job_offset);
			} else {
				if (job_ids){
					xstrcat(job_ids, " ,");
				}
				if (array_job_ids){
					xstrcat(array_job_ids, " ,");
				}
				xstrfmtcat(job_ids, "%u",
					   selected_step->step_id.job_id);
				xstrfmtcat(array_job_ids, "%u",
					   selected_step->step_id.job_id);
			}
		}
		list_iterator_destroy(itr);

		if (job_ids) {
			if (job_cond->flags & JOBCOND_FLAG_WHOLE_HETJOB)
				xstrfmtcat(*extra, "t1.id_job in (%s) or "
					   "t1.het_job_id in (select "
					   "t4.het_job_id from `%s_%s` as "
					   "t4 where t4.id_job in (%s) and "
					   "t4.het_job_id)",
					   job_ids, cluster_name, job_table,
					   job_ids);
			else if (job_cond->flags & JOBCOND_FLAG_NO_WHOLE_HETJOB)
				xstrfmtcat(*extra, "t1.id_job in (%s)",
					   job_ids);
			else
				xstrfmtcat(*extra,
				   "t1.id_job in (%s) or t1.het_job_id in (%s)",
				   job_ids, job_ids);
			sep = " or ";
		}
		if (het_job_offset) {
			if (job_cond->flags & JOBCOND_FLAG_WHOLE_HETJOB)
				xstrfmtcat(*extra, "%s(t1.het_job_id in (%s))",
					   sep, het_job_ids);
			else
				xstrfmtcat(*extra, "%s(t1.het_job_id in (%s) "
					   "and t1.het_job_offset in (%s))",
					   sep, het_job_ids, het_job_offset);
			sep = " or ";
		}
		if (array_job_ids) {
			xstrfmtcat(*extra, "%s(t1.id_array_job in (%s))",
				   sep, array_job_ids);
			sep = " or ";
		}

		if (array_task_ids) {
			xstrfmtcat(*extra,
				   "%s((t1.id_array_job, t1.id_array_task) in (%s))",
				   sep, array_task_ids);
		}

		xstrcat(*extra, ")");
		xfree(job_ids);
		xfree(array_job_ids);
		xfree(array_task_ids);
		xfree(het_job_ids);
		xfree(het_job_offset);
	}
}

static void _state_time_string(char **extra, char *cluster_name, uint32_t state,
			       slurmdb_job_cond_t *job_cond)
{
	int base_state = state;

	if (!job_cond->usage_start && !job_cond->usage_end) {
		xstrfmtcat(*extra, "t1.state='%u'", state);
		return;
	}

	switch(base_state) {
	case JOB_PENDING:
		/*
		 * Generic Query assuming that -S and -E are properly set in
		 * slurmdb_job_cond_def_start_end
		 *
		 * (job eligible)                                   &&
		 * (( time_start &&              (-S < time_start)) ||
		 *  (!time_start &&  time_end && (-S < time_end))   || -> Cancel before start
		 *  (!time_start && !time_end && (state = PD) ))    && -> Still PD
		 * (-E > time_eligible)
		 */
		xstrfmtcat(*extra,
			   "(t1.time_eligible and "
			   "(( t1.time_start and (%ld < t1.time_start)) or "
			   " (!t1.time_start and  t1.time_end and (%ld < t1.time_end)) or "
			   " (!t1.time_start and !t1.time_end and (t1.state=%d))) and "
			   "(%ld > t1.time_eligible))",
			   job_cond->usage_start,
			   job_cond->usage_start,
			   base_state,
			   job_cond->usage_end);
		break;
	case JOB_SUSPENDED:
		xstrfmtcat(*extra,
			   "(select count(time_start) from "
			   "`%s_%s` where "
			   "(time_start <= %ld and (time_end >= %ld "
			   "or time_end = 0)) and job_db_inx=t1.job_db_inx)",
			   cluster_name, suspend_table,
			   job_cond->usage_end ?
			   job_cond->usage_end : job_cond->usage_start,
			   job_cond->usage_start);
		break;
	case JOB_RUNNING:
		/*
		 * Generic Query assuming that -S and -E are properly set in
		 * slurmdb_job_cond_def_start_end
		 *
		 * (job started)                     &&
		 * (-S < time_end || still running)  &&
		 * (-E > time_start)
		 */
		xstrfmtcat(*extra,
			   "(t1.time_start and "
			   "((%ld < t1.time_end or (!t1.time_end and t1.state=%d))) and "
			   "((%ld > t1.time_start)))",
			   job_cond->usage_start, base_state,
			   job_cond->usage_end);
		break;
	case JOB_COMPLETE:
	case JOB_CANCELLED:
	case JOB_FAILED:
	case JOB_TIMEOUT:
	case JOB_NODE_FAIL:
	case JOB_PREEMPTED:
	case JOB_BOOT_FAIL:
	case JOB_DEADLINE:
	case JOB_OOM:
	case JOB_REQUEUE:
	case JOB_RESIZING:
	case JOB_REVOKED:
		/*
		 * Query assuming that -S and -E are properly set in
		 * slurmdb_job_cond_def_start_end
		 *
		 * Job ending *in* the time window with the specified state.
		 */
		xstrfmtcat(*extra,
		           "(t1.state='%u' and (t1.time_end and "
		           "(t1.time_end between %ld and %ld)))",
		           base_state, job_cond->usage_start,
			   job_cond->usage_end);
		break;
	default:
		error("Unsupported state requested: %s",
		      job_state_string(base_state));
		xstrfmtcat(*extra, "(t1.state='%u')", base_state);
	}

	return;
}

static void _destroy_local_cluster(void *object)
{
	local_cluster_t *local_cluster = (local_cluster_t *)object;
	if (local_cluster) {
		if (local_cluster->hl)
			hostlist_destroy(local_cluster->hl);
		FREE_NULL_BITMAP(local_cluster->asked_bitmap);
		xfree(local_cluster);
	}
}

static int _cluster_get_jobs(kingbase_conn_t *kingbase_conn,
			     slurmdb_user_rec_t *user,
			     slurmdb_job_cond_t *job_cond,
			     char *cluster_name,
			     char *job_fields, char *step_fields,
			     char *sent_extra,
			     bool is_admin, int only_pending, List sent_list)
{
	char *query = NULL;
	char *extra = xstrdup(sent_extra);
	slurm_selected_step_t *selected_step = NULL;
	KCIResult *result = NULL, *step_result = NULL;
	slurmdb_job_rec_t *job = NULL;
	slurmdb_step_rec_t *step = NULL;
	time_t now = time(NULL);
	List job_list = list_create(slurmdb_destroy_job_rec);
	ListIterator itr = NULL, itr2 = NULL;
	List local_cluster_list = NULL;
	int set = 0,i=0,rows=0;
	char *prefix="t2";
	int rc = SLURM_SUCCESS;
	int last_id = -1, curr_id = -1;
	local_cluster_t *curr_cluster = NULL;

	/* This is here to make sure we are looking at only this user
	 * if this flag is set.  We also include any accounts they may be
	 * coordinator of.
	 * 
	 * This clause must be kept in sync with the access check in
	 * as_kingbase_jobacct_process_get_jobs().
	 */
	if (!is_admin && ((slurm_conf.private_data & PRIVATE_DATA_JOBS) ||
			  (job_cond->flags & JOBCOND_FLAG_SCRIPT) ||
			  (job_cond->flags & JOBCOND_FLAG_ENV))) {
		query = xstrdup_printf("select lft from `%s_%s` "
				       "where `user`='%s'",
				       cluster_name, assoc_table, user->name);
		if (user->coord_accts) {
			slurmdb_coord_rec_t *coord = NULL;
			itr = list_iterator_create(user->coord_accts);
			while ((coord = list_next(itr))) {
				xstrfmtcat(query, " or acct='%s'",
					   coord->name);
			}
			list_iterator_destroy(itr);
		}
		DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		result = kingbase_db_query_ret(kingbase_conn, query, 0);
		xfree(query);
		if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
			KCIResultDealloc(result);
			xfree(extra);
			rc = SLURM_ERROR;
			goto end_it;
		}
	
		set = 0;
        rows = KCIResultGetRowCount(result);
        for(i = 0 ; i < rows ; ++i){
			if (set) {
				xstrfmtcat(extra,
					   " or (%s between %s.lft and %s.rgt)",
					   KCIResultGetColumnValue(result,i,0), prefix, prefix);
			} else {
				set = 1;
				if (extra)
					xstrfmtcat(extra,
						   " and ((%s between %s.lft "
						   "and %s.rgt)",
						   KCIResultGetColumnValue(result,i,0), prefix,
						   prefix);
				else
					xstrfmtcat(extra,
						   " where ((%s between %s.lft "
						   "and %s.rgt)",
						   KCIResultGetColumnValue(result,i,0), prefix,
						   prefix);
			}
		}
		i = 0;
		KCIResultDealloc(result);

		if (set)
			xstrcat(extra, ")");
		else {
			xfree(extra);
			debug("User %s has no associations, and is not admin, "
			      "so not returning any jobs.", user->name);
			/* This user has no valid associations, so
			 * they will not have any jobs. */
			goto end_it;
		}
	}

	setup_job_cluster_cond_limits(kingbase_conn, job_cond,
				      cluster_name, &extra);

	query = xstrdup_printf("select %s from `%s_%s` as t1 "
			       "left join `%s_%s` as t2 "
			       "on t1.id_assoc=t2.id_assoc "
			       "left join `%s_%s` as t3 "
			       "on t1.id_resv=t3.id_resv and "
			       "((t1.time_start and "
			       "(t3.time_start < t1.time_start and "
			       "(t3.time_end >= t1.time_start or "
			       "t3.time_end = 0))) or "
			       "(t1.time_start = 0 and "
			       "((t3.time_start < t1.time_submit and "
			       "(t3.time_end >= t1.time_submit or "
			       "t3.time_end = 0)) or "
			       "(t3.time_start > t1.time_submit))))",
			       job_fields, cluster_name, job_table,
			       cluster_name, assoc_table,
			       cluster_name, resv_table);

	if (job_cond->flags & JOBCOND_FLAG_SCRIPT)
		xstrfmtcat(query,
			   " left join `%s_%s` as t4 "
			   "on t1.script_hash_inx=t4.hash_inx",
			   cluster_name, job_script_table);
	else if (job_cond->flags & JOBCOND_FLAG_ENV)
		xstrfmtcat(query,
			   " left join `%s_%s` as t4 "
			   "on t1.env_hash_inx=t4.hash_inx",
			   cluster_name, job_env_table);

	if (job_cond->flags & JOBCOND_FLAG_RUNAWAY) {
		if (extra)
			xstrcat(extra, " and (t1.time_end=0)");
		else
			xstrcat(extra, " where (t1.time_end=0)");
	}

	if (extra) {
		xstrcat(query, extra);
		xfree(extra);
	}

	/* Here we want to order them this way in such a way so it is
	   easy to look for duplicates, it is also easy to sort the
	   resized jobs.
	*/
	xstrcat(query, " order by id_job, time_submit desc");

	DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		rc = SLURM_ERROR;
		goto end_it;
	}



	/* Here we set up environment to check used nodes of jobs.
	   Since we store the bitmap of the entire cluster we can use
	   that to set up a hostlist and set up the bitmap to make
	   things work.  This should go before the setup of conds
	   since we could update the start/end time.
	*/
	if (job_cond && job_cond->used_nodes) {
		local_cluster_list = setup_cluster_list_with_inx(
			kingbase_conn, job_cond, (void **)&curr_cluster);
		if (!local_cluster_list) {
			KCIResultDealloc(result);
			rc = SLURM_ERROR;
			goto end_it;
		}
	}
	rows = KCIResultGetRowCount(result);
	for(i = 0 ; i < rows ; ++i){
		char *db_inx_char = KCIResultGetColumnValue(result,i,JOB_REQ_DB_INX);
		bool job_ended = 0;
		int start = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_START));

		curr_id = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_JOBID));

		if (job_cond && !(job_cond->flags & JOBCOND_FLAG_DUP)
		    && (curr_id == last_id)
		    && (slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_STATE)) != JOB_RESIZING))
			continue;

		/* check the bitmap to see if this is one of the jobs
		   we are looking for */
		/* Use start time instead of submit time because node
		 * indexes are determined at start time and not submit. */
		if (!good_nodes_from_inx(local_cluster_list,
					 (void **)&curr_cluster,
					 KCIResultGetColumnValue(result,i,JOB_REQ_NODE_INX), start)) {
			last_id = curr_id;
			continue;
		}

		job = slurmdb_create_job_rec();
		job->state = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_STATE));
		if (curr_id == last_id)
			/* put in reverse so we order by the submit getting
			   larger which it is given to us in reverse
			   order from the database */
			list_prepend(job_list, job);
		else
			list_append(job_list, job);
		last_id = curr_id;

		job->alloc_nodes = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_ALLOC_NODES));
		job->associd = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_ASSOCID));
		job->array_job_id = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_ARRAYJOBID));
		job->array_task_id = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_ARRAYTASKID));
		job->het_job_id = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_HET_JOB_ID));
		job->het_job_offset = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_HET_JOB_OFFSET));
		job->resvid = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_RESVID));

		/* This shouldn't happen with new jobs, but older jobs
		 * could of been added without a start and so the
		 * array_task_id would be 0 instead of it's real value */
		if (!job->array_job_id && !job->array_task_id)
			job->array_task_id = NO_VAL;

		/*
		 * This shouldn't happen with new jobs.
		 * If older jobs have het_job_id == 0 and het_job_offset == 0,
		 * then correct het_job_offset to be NO_VAL.
		 */
		if (!job->het_job_id && !job->het_job_offset)
			job->het_job_offset = NO_VAL;

		char *temp = KCIResultGetColumnValue(result,i,JOB_REQ_RESV_NAME);
		if (*temp != '\0')
			job->resv_name = xstrdup(temp);

		job->cluster = xstrdup(cluster_name);

		/* we want a blank wckey if the name is null */
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_WCKEY);
		if (*temp != '\0')
			job->wckey = xstrdup(temp);
		else
			job->wckey = xstrdup("");
		job->wckeyid = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_WCKEYID));
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_MCS_LABEL);
		if (*temp != '\0')
			job->mcs_label = xstrdup(temp);
		else
			job->mcs_label = xstrdup("");
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_USER_NAME);
		if (*temp != '\0')
			job->user = xstrdup(temp);

		temp = KCIResultGetColumnValue(result,i,JOB_REQ_UID);
		if (*temp != '\0')
			job->uid = slurm_atoul(temp);
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_LFT);
		if (*temp != '\0')
			job->lft = slurm_atoul(temp);
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_ACCOUNT);
		char *temp2 = KCIResultGetColumnValue(result,i,JOB_REQ_ACCOUNT1);
		if (*temp != '\0')
			job->account = xstrdup(temp);
		else if (*temp2 != '\0')
			job->account = xstrdup(temp2);
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_ARRAY_STR);
		if (*temp != '\0')
			job->array_task_str = xstrdup(temp);
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_ARRAY_MAX);
		if (*temp != '\0')
			job->array_max_tasks =
				slurm_atoul(temp);
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_BLOCKID);
		if (*temp != '\0')
			job->blockid = xstrdup(temp);
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_WORK_DIR);
		if (*temp != '\0')
			job->work_dir = xstrdup(temp);

#ifdef __METASTACK_OPT_SACCT_COMMAND
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_COMMAND);
		if (*temp != '\0')
			job->command = xstrdup(temp);
#endif
#ifdef __METASTACK_OPT_SACCT_OUTPUT
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_STDOUT);
		if (*temp != '\0')
			job->stdout = xstrdup(temp);
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_STDERR);
		if (*temp != '\0')
			job->stderr = xstrdup(temp);
#endif
		job->eligible = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_ELIGIBLE));
		job->submit = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_SUBMIT));
		job->start = start;
		job->end = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_END));
		job->timelimit = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_TIMELIMIT));

		job->script = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_SCRIPT));

		job->env = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_ENV));

		job->submit_line = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_SUBMIT_LINE));

		/* since the job->end could be set later end it here */
		if (job->end) {
			job_ended = 1;
			if (job->start > job->end)
				job->start = job->end;
		}

		if (job_cond && !(job_cond->flags & JOBCOND_FLAG_NO_TRUNC) ){

			if (!job_cond->usage_end ||
			    (job_cond->usage_end > now)) {
				job_cond->usage_end = now;
			}

			if (job->start && (job->start < job_cond->usage_start))
				job->start = job_cond->usage_start;

			if (!job->end || job->end > job_cond->usage_end)
				job->end = job_cond->usage_end;

			if (!job->start)
				job->start = job->end;

			job->elapsed = job->end - job->start;
			char *temp = KCIResultGetColumnValue(result,i,JOB_REQ_SUSPENDED);
			if (*temp != '\0') {
				KCIResult *result2 = NULL;
				/* get the suspended time for this job */
				query = xstrdup_printf(
					"select time_start, time_end from "
					"`%s_%s` where "
					"(time_start < %ld and (time_end >= %ld "
					"or time_end = 0)) and job_db_inx=%s "
					"order by time_start",
					cluster_name, suspend_table,
					job_cond->usage_end,
					job_cond->usage_start,
					db_inx_char);

				debug4("%d(%s:%d) query\n%s",
				       kingbase_conn->conn, THIS_FILE,
				       __LINE__, query);
				//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	   
				result2 = kingbase_db_query_ret(kingbase_conn, query, 0);
				xfree(query);
				if (KCIResultGetStatusCode(result2) != EXECUTE_TUPLES_OK) {
					KCIResultDealloc(result2);
					FREE_NULL_LIST(job_list);
					job_list = NULL;
					break;
				}
				int rows2 = KCIResultGetRowCount(result2),j = 0;
				for(j = 0 ; j < rows2 ; ++j){
					time_t local_start =
						slurm_atoul(KCIResultGetColumnValue(result2,j,0));
					time_t local_end =
						slurm_atoul(KCIResultGetColumnValue(result2,j,1));

					if (!local_start)
						continue;

					if (job->start > local_start)
						local_start = job->start;
					if (job->end < local_end)
						local_end = job->end;

					if ((local_end - local_start) < 1)
						continue;

					job->elapsed -=
						(local_end - local_start);
					job->suspended +=
						(local_end - local_start);
				}
				KCIResultDealloc(result2);

			}
		} else {
			job->suspended = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_SUSPENDED));

			/* fix the suspended number to be correct */
			if (job->state == JOB_SUSPENDED)
				job->suspended = now - job->suspended;
			if (!job->start) {
				job->elapsed = 0;
			} else if (!job->end) {
				job->elapsed = now - job->start;
			} else {
				job->elapsed = job->end - job->start;
			}

			job->elapsed -= job->suspended;
		}

		if ((int)job->elapsed < 0)
			job->elapsed = 0;

		job->db_index = slurm_atoull(db_inx_char);
		job->jobid = curr_id;
		job->jobname = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_NAME));
		job->gid = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_GID));
		job->exitcode = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_EXIT_CODE));
		job->derived_ec = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_DERIVED_EC));
		job->derived_es = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_DERIVED_ES));
		job->admin_comment = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_ADMIN_COMMENT));
		job->system_comment = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_SYSTEM_COMMENT));
#ifdef __METASTACK_OPT_RESC_NODEDETAIL
		job->resource_node_detail = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_RESC_NODE));
#endif
		job->constraints = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_CONSTRAINTS));
		job->container = xstrdup(KCIResultGetColumnValue(result,i,JOB_REQ_CONTAINER));
		job->flags = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_FLAGS));

		/*
		 * This tells us we never had a start time so the job was
		 * canceled before it ran.
		 */
		if (!job->start && job->end &&
		    (job->flags & SLURMDB_JOB_FLAG_START_R))
			job->start = NO_VAL;

		job->state_reason_prev = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_STATE_REASON));
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_PARTITION);
		if (*temp != '\0')
			job->partition = xstrdup(temp);
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_NODELIST);
		if (*temp != '\0')
			job->nodes = xstrdup(temp);

		if (!job->nodes || !xstrcmp(job->nodes, "(null)")) {
			xfree(job->nodes);
			job->nodes = xstrdup("(unknown)");
		}

		job->priority = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_PRIORITY));
		job->req_cpus = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_REQ_CPUS));
		job->req_mem = slurm_atoull(KCIResultGetColumnValue(result,i,JOB_REQ_REQ_MEM));
		if (!KCIResultGetColumnValue(result,i,JOB_REQ_KILL_REQUID))
			job->requid = INFINITE;
		else
			job->requid = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_KILL_REQUID));
		job->qosid = slurm_atoul(KCIResultGetColumnValue(result,i,JOB_REQ_QOS));
		job->show_full = 1;

		temp = KCIResultGetColumnValue(result,i,JOB_REQ_TRESA);
		if (*temp != '\0')
			job->tres_alloc_str = xstrdup(temp);
		temp = KCIResultGetColumnValue(result,i,JOB_REQ_TRESR);
		if (*temp != '\0')
			job->tres_req_str = xstrdup(temp);

		if (only_pending ||
		    (job_cond &&
		     (job_cond->flags & (JOBCOND_FLAG_NO_STEP |
					 JOBCOND_FLAG_RUNAWAY))))
			goto skip_steps;

		if (job_cond && job_cond->step_list
		    && list_count(job_cond->step_list)) {
			set = 0;
			itr = list_iterator_create(job_cond->step_list);
			while ((selected_step = list_next(itr))) {
				if ((selected_step->step_id.job_id !=
				     job->jobid) &&
				    (selected_step->step_id.job_id !=
				     job->het_job_id)&&
				    (selected_step->step_id.job_id !=
				     job->array_job_id)) {
					continue;
				} else if ((selected_step->array_task_id !=
					    NO_VAL) &&
					   (selected_step->array_task_id !=
					    job->array_task_id)) {
					continue;
				} else if ((selected_step->het_job_offset !=
					    NO_VAL) &&
					   (selected_step->het_job_offset !=
					    job->het_job_offset)) {
					continue;
				} else if (selected_step->step_id.step_id ==
					   NO_VAL) {
					job->show_full = 1;
					break;
				}
				if (set)
					xstrcat(extra, " or ");
				else
					xstrcat(extra, " and (");

				/*
				 * The stepid could be negative so use
				 * %d not %u
				 */
				xstrfmtcat(extra, "t1.id_step=%d",
					   selected_step->step_id.step_id);

				set = 1;
				job->show_full = 0;
			}
			list_iterator_destroy(itr);
			if (set)
				xstrcat(extra, ")");
		}

		query =	xstrdup_printf("select %s from `%s_%s` as t1 "
				       "where t1.job_db_inx=%s and "
				       "t1.time_start <= %ld and "
				       "(!t1.time_end or t1.time_end >= %ld)",
				       step_fields, cluster_name,
				       step_table, db_inx_char,
				       job_cond->usage_end,
				       job_cond->usage_start);

		if (extra) {
			xstrcat(query, extra);
			xfree(extra);
		}

		DB_DEBUG(DB_STEP, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		step_result = kingbase_db_query_ret(kingbase_conn, query, 0);
		xfree(query);
		if (KCIResultGetStatusCode(step_result) != EXECUTE_TUPLES_OK) {
			rc = SLURM_ERROR;
			KCIResultDealloc(result);
			KCIResultDealloc(step_result);
			goto end_it;
		}

		/* Querying the steps in the fashion was faster than
		   doing only 1 query and then matching the steps up
		   later with the job.
		*/
		int rows3 = KCIResultGetRowCount(step_result);
		for(int z = 0 ; z < rows3 ; z++){
			/* check the bitmap to see if this is one of the steps
			   we are looking for */
			if (!good_nodes_from_inx(local_cluster_list,
						 (void **)&curr_cluster,
						 KCIResultGetColumnValue(step_result,z,STEP_REQ_NODE_INX),
						 start))
				continue;

			step = slurmdb_create_step_rec();
			step->tot_cpu_sec = 0;
			step->tot_cpu_usec = 0;
			step->job_ptr = job;
			if (!job->first_step_ptr)
				job->first_step_ptr = step;
			list_append(job->steps, step);
			step->step_id.job_id = job->jobid;
			step->step_id.step_id = slurm_atoul(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_STEPID));
			step->step_id.step_het_comp =
				slurm_atoul(KCIResultGetColumnValue(step_result,z,STEP_REQ_STEP_HET_COMP));
			/* info("got %ps", &step->step_id); */
			step->state = slurm_atoul(KCIResultGetColumnValue(step_result,z,STEP_REQ_STATE));
			step->exitcode =
				slurm_atoul(KCIResultGetColumnValue(step_result,z,STEP_REQ_EXIT_CODE));
			step->nnodes = slurm_atoul(KCIResultGetColumnValue(step_result,z,STEP_REQ_NODES));

			step->ntasks = slurm_atoul(KCIResultGetColumnValue(step_result,z,STEP_REQ_TASKS));
			step->task_dist =
				slurm_atoul(KCIResultGetColumnValue(step_result,z,STEP_REQ_TASKDIST));

			step->start = slurm_atoul(KCIResultGetColumnValue(step_result,z,STEP_REQ_START));

			step->end = slurm_atoul(KCIResultGetColumnValue(step_result,z,STEP_REQ_END));
			/* if the job has ended end the step also */
			if (!step->end && job_ended) {
				step->end = job->end;
				step->state = job->state;
			}

			if (job_cond &&
			    !(job_cond->flags & JOBCOND_FLAG_NO_TRUNC)
			    && job_cond->usage_start) {
				if (step->start
				    && (step->start < job_cond->usage_start))
					step->start = job_cond->usage_start;

				if (!step->start && step->end)
					step->start = step->end;

				if (!step->end
				    || (step->end > job_cond->usage_end))
					step->end = job_cond->usage_end;

				if (step->start && step->end &&
				   (step->start > step->end))
					step->start = step->end = 0;
			}

			/* figure this out by start stop */
			step->suspended =
				slurm_atoul(KCIResultGetColumnValue(step_result,z,STEP_REQ_SUSPENDED));

			/* fix the suspended number to be correct */
			if (step->state == JOB_SUSPENDED)
				step->suspended = now - step->suspended;
			if (!step->start) {
				step->elapsed = 0;
			} else if (!step->end) {
				step->elapsed = now - step->start;
			} else {
				step->elapsed = step->end - step->start;
			}
			step->elapsed -= step->suspended;

			if ((int)step->elapsed < 0)
				step->elapsed = 0;

			step->req_cpufreq_min = slurm_atoul(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_REQ_CPUFREQ_MIN));
			step->req_cpufreq_max = slurm_atoul(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_REQ_CPUFREQ_MAX));
			step->req_cpufreq_gov =	slurm_atoul(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_REQ_CPUFREQ_GOV));

			step->stepname = xstrdup(KCIResultGetColumnValue(step_result,z,STEP_REQ_NAME));
			step->nodes = xstrdup(KCIResultGetColumnValue(step_result,z,STEP_REQ_NODELIST));
			if (!KCIResultGetColumnValue(step_result,z,STEP_REQ_KILL_REQUID))
				step->requid = INFINITE;
			else
				step->requid = slurm_atoul(
					KCIResultGetColumnValue(step_result,z,STEP_REQ_KILL_REQUID));

			step->submit_line =
				xstrdup(KCIResultGetColumnValue(step_result,z,STEP_REQ_SUBMIT_LINE));

			step->user_cpu_sec = slurm_atoull(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_USER_SEC));
			step->user_cpu_usec = slurm_atoul(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_USER_USEC));
			step->sys_cpu_sec = slurm_atoull(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_SYS_SEC));
			step->sys_cpu_usec = slurm_atoul(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_SYS_USEC));
			step->tot_cpu_sec +=
				step->user_cpu_sec + step->sys_cpu_sec;
			step->tot_cpu_usec += step->user_cpu_usec +
				step->sys_cpu_usec;
			char* temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_IN_MAX);
			if (*temp != '\0')
				step->stats.tres_usage_in_max =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_IN_MAX_TASKID);
			if (*temp != '\0')
				step->stats.tres_usage_in_max_taskid =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_IN_MAX_NODEID);
			if (*temp != '\0')
				step->stats.tres_usage_in_max_nodeid =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_IN_AVE);
			if (*temp != '\0')
				step->stats.tres_usage_in_ave =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_IN_MIN);
			if (*temp != '\0')
				step->stats.tres_usage_in_min =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_IN_MIN_TASKID);
			if (*temp != '\0')
				step->stats.tres_usage_in_min_taskid =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_IN_MIN_NODEID);
			if (*temp != '\0')
				step->stats.tres_usage_in_min_nodeid =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_IN_TOT);
			if (*temp != '\0')
				step->stats.tres_usage_in_tot =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_OUT_MAX);
			if (*temp != '\0')
				step->stats.tres_usage_out_max =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_OUT_MAX_TASKID);
			if (*temp != '\0')
				step->stats.tres_usage_out_max_taskid =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_OUT_MAX_NODEID);
			if (*temp != '\0')
				step->stats.tres_usage_out_max_nodeid =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_OUT_AVE);
			if (*temp != '\0')
				step->stats.tres_usage_out_ave =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_OUT_MIN);
			if (*temp != '\0')
				step->stats.tres_usage_out_min =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_OUT_MIN_TASKID);
			if (*temp != '\0')
				step->stats.tres_usage_out_min_taskid =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_OUT_MIN_NODEID);
			if (*temp != '\0')
				step->stats.tres_usage_out_min_nodeid =
					xstrdup(temp);
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES_USAGE_OUT_TOT);
			if (*temp != '\0')
				step->stats.tres_usage_out_tot =
					xstrdup(temp);
			step->stats.act_cpufreq =
				atof(KCIResultGetColumnValue(step_result,z,STEP_REQ_ACT_CPUFREQ));
			step->stats.consumed_energy = slurm_atoull(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_CONSUMED_ENERGY));
			step->container = xstrdup(
				KCIResultGetColumnValue(step_result,z,STEP_REQ_CONTAINER));
			temp = KCIResultGetColumnValue(step_result,z,STEP_REQ_TRES);
			if (*temp != '\0')
				step->tres_alloc_str =
					xstrdup(temp);
		}
		KCIResultDealloc(step_result);
	skip_steps:
		/* need to reset here to make the above test valid */
		step = NULL;
	}
	KCIResultDealloc(result);

end_it:
	if (itr2)
		list_iterator_destroy(itr2);

	FREE_NULL_LIST(local_cluster_list);

	if (rc == SLURM_SUCCESS)
		list_transfer(sent_list, job_list);

	FREE_NULL_LIST(job_list);
	return rc;
}

extern List setup_cluster_list_with_inx(kingbase_conn_t *kingbase_conn,
					slurmdb_job_cond_t *job_cond,
					void **curr_cluster)
{
	List local_cluster_list = NULL;
	time_t now = time(NULL);
	KCIResult *result = NULL;
	hostlist_t temp_hl = NULL;
	hostlist_iterator_t h_itr = NULL;
	int i = 0 ,rows = 0;
	char *query = NULL;
	int dims = 0;

	if (!job_cond || !job_cond->used_nodes)
		return NULL;

	if (!job_cond->cluster_list
	    || list_count(job_cond->cluster_list) != 1) {
		error("If you are doing a query against nodes "
		      "you must only have 1 cluster "
		      "you are asking for.");
		return NULL;
	}

	if (get_cluster_dims(kingbase_conn,
			     (char *)list_peek(job_cond->cluster_list),
			     &dims)){
		return NULL;
	}

	temp_hl = hostlist_create_dims(job_cond->used_nodes, dims);
	if (hostlist_count(temp_hl) <= 0) {
		error("we didn't get any real hosts to look for.");
		goto no_hosts;
	}
	h_itr = hostlist_iterator_create(temp_hl);

	query = xstrdup_printf("select cluster_nodes, time_start, "
			       "time_end from `%s_%s` where node_name='' "
			       "and cluster_nodes !=''",
			       (char *)list_peek(job_cond->cluster_list),
			       event_table);

	if (job_cond->usage_start) {
		if (!job_cond->usage_end)
			job_cond->usage_end = now;

		xstrfmtcat(query,
			   " and ((time_start < %ld) "
			   "and (time_end >= %ld or time_end = 0))",
			   job_cond->usage_end, job_cond->usage_start);
	}

	DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		goto no_hosts;
	}

	local_cluster_list = list_create(_destroy_local_cluster);
	rows = KCIResultGetRowCount(result);
	for(i = 0 ; i < rows ; ++i){
		char *host = NULL;
		int loc = 0;
		local_cluster_t *local_cluster =
			xmalloc(sizeof(local_cluster_t));
		local_cluster->hl = hostlist_create_dims(KCIResultGetColumnValue(result,i,0), dims);
		local_cluster->start = slurm_atoul(KCIResultGetColumnValue(result,i,1));
		local_cluster->end   = slurm_atoul(KCIResultGetColumnValue(result,i,2));
		local_cluster->asked_bitmap =
			bit_alloc(hostlist_count(local_cluster->hl));
		while ((host = hostlist_next_dims(h_itr, dims))) {
			if ((loc = hostlist_find_dims(
				     local_cluster->hl, host, dims)) != -1)
				bit_set(local_cluster->asked_bitmap, loc);
			free(host);
		}
		hostlist_iterator_reset(h_itr);
		if (bit_ffs(local_cluster->asked_bitmap) != -1) {
			list_append(local_cluster_list, local_cluster);
			if (local_cluster->end == 0) {
				local_cluster->end = now + 1;
				(*curr_cluster) = local_cluster;
			} else if (!(*curr_cluster)
				   || (((local_cluster_t *)(*curr_cluster))->end
				       < local_cluster->end)) {
				(*curr_cluster) = local_cluster;
			}
		} else
			_destroy_local_cluster(local_cluster);
	}
	i = 0;
	KCIResultDealloc(result);

	if (!list_count(local_cluster_list)) {
		FREE_NULL_LIST(local_cluster_list);
		local_cluster_list = NULL;
		goto no_hosts;
	}

no_hosts:

	hostlist_iterator_destroy(h_itr);
	hostlist_destroy(temp_hl);

	return local_cluster_list;
}

extern int good_nodes_from_inx(List local_cluster_list,
			       void **object, char *node_inx,
			       int start)
{
	local_cluster_t **curr_cluster = (local_cluster_t **)object;

	/* check the bitmap to see if this is one of the jobs
	   we are looking for */
	if (*curr_cluster) {
		bitstr_t *job_bitmap = NULL;
		if (!node_inx || !node_inx[0])
			return 0;
		if ((start < (*curr_cluster)->start)
		    || (start >= (*curr_cluster)->end)) {
			local_cluster_t *local_cluster = NULL;

			ListIterator itr =
				list_iterator_create(local_cluster_list);
			while ((local_cluster = list_next(itr))) {
				if ((start >= local_cluster->start)
				    && (start < local_cluster->end)) {
					*curr_cluster = local_cluster;
					break;
				}
			}
			list_iterator_destroy(itr);
			if (!local_cluster)
				return 0;
		}
		job_bitmap = bit_alloc(hostlist_count((*curr_cluster)->hl));
		bit_unfmt(job_bitmap, node_inx);
		if (!bit_overlap_any((*curr_cluster)->asked_bitmap, job_bitmap)) {
			FREE_NULL_BITMAP(job_bitmap);
			return 0;
		}
		FREE_NULL_BITMAP(job_bitmap);
	}
	return 1;
}

extern int setup_job_cluster_cond_limits(kingbase_conn_t *kingbase_conn,
					 slurmdb_job_cond_t *job_cond,
					 char *cluster_name, char **extra)
{
	int set = 0;
	ListIterator itr = NULL;
	char *object = NULL;

	if (!job_cond)
		return SLURM_SUCCESS;

	/* this must be done before resvid_list since we set
	   resvid_list up here */
	if (job_cond->resv_list && list_count(job_cond->resv_list)) {
		char *query = xstrdup_printf(
			"select distinct job_db_inx from `%s_%s` where (",
			cluster_name, job_table);
		int my_set = 0,i = 0, rows = 0;
		KCIResult *result = NULL;

		itr = list_iterator_create(job_cond->resv_list);
		while ((object = list_next(itr))) {
			if (my_set)
				xstrcat(query, " or ");
			xstrfmtcat(query, "resv_name='%s'", object);
			my_set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(query, ")");
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		result = kingbase_db_query_ret(kingbase_conn, query, 0);
		xfree(query);
		if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
			KCIResultDealloc(result);
			error("couldn't query the database");
			goto no_resv;
		}
		
		if (!job_cond->resvid_list)
			job_cond->resvid_list = list_create(xfree_ptr);
		rows = KCIResultGetRowCount(result);
		for(i = 0 ; i < rows ; ++i){
			list_append(job_cond->resvid_list, xstrdup(KCIResultGetColumnValue(result,i,0)));
		}
		i = 0;
		KCIResultDealloc(result);
	}
no_resv:

	if (job_cond->resvid_list && list_count(job_cond->resvid_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");
		itr = list_iterator_create(job_cond->resvid_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.id_resv='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->state_list && list_count(job_cond->state_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		itr = list_iterator_create(job_cond->state_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");

			_state_time_string(extra, cluster_name,
					   (uint32_t)slurm_atoul(object),
					   job_cond);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	/* Don't show revoked sibling federated jobs w/out -D */
	if (!(job_cond->flags & JOBCOND_FLAG_DUP))
		xstrfmtcat(*extra, " %s (t1.state != %"PRIu64")",
			   *extra ? "and" : "where",
			   JOB_REVOKED);

	return SLURM_SUCCESS;
}

extern int setup_job_cond_limits(slurmdb_job_cond_t *job_cond,
				 char **extra)
{
	int set = 0;
	ListIterator itr = NULL;
	char *object = NULL;

	if (!job_cond || (job_cond->flags & JOBCOND_FLAG_RUNAWAY))
		return 0;

	slurmdb_job_cond_def_start_end(job_cond);

	if (job_cond->acct_list && list_count(job_cond->acct_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");
		itr = list_iterator_create(job_cond->acct_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.account='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->associd_list && list_count(job_cond->associd_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");
		itr = list_iterator_create(job_cond->associd_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.id_assoc='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->constraint_list &&
	    list_count(job_cond->constraint_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		itr = list_iterator_create(job_cond->constraint_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " and ");
			if (object[0])
				xstrfmtcat(*extra,
					   "t1.constraints like '%%%s%%'",
					   object);
			else
				xstrcat(*extra, "t1.constraints=''");

			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->db_flags != SLURMDB_JOB_FLAG_NOTSET) {
		set = 1;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		if (job_cond->db_flags == SLURMDB_JOB_FLAG_NONE)
			xstrfmtcat(*extra, "t1.flags = %u", job_cond->db_flags);
		else
			xstrfmtcat(*extra, "t1.flags & %u", job_cond->db_flags);

		xstrcat(*extra, ")");
	}

	if (job_cond->reason_list && list_count(job_cond->reason_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		itr = list_iterator_create(job_cond->reason_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.state_reason_prev='%s'",
				   object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->userid_list && list_count(job_cond->userid_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		itr = list_iterator_create(job_cond->userid_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.id_user='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->groupid_list && list_count(job_cond->groupid_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");
		itr = list_iterator_create(job_cond->groupid_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.id_group='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->jobname_list && list_count(job_cond->jobname_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		itr = list_iterator_create(job_cond->jobname_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.job_name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->partition_list && list_count(job_cond->partition_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");
		itr = list_iterator_create(job_cond->partition_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.partition='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->qos_list && list_count(job_cond->qos_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");
		itr = list_iterator_create(job_cond->qos_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.id_qos='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (job_cond->cpus_min) {
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		xstrfmtcat(*extra,
			   "(CONVERT(SUBSTRING_INDEX(t1.tres_alloc,'%d=',-1),"
			   "UNSIGNED INTEGER)",
			   TRES_CPU);

		if (job_cond->cpus_max) {
			xstrfmtcat(*extra, " between %u and %u))",
				   job_cond->cpus_min, job_cond->cpus_max);

		} else {
			xstrfmtcat(*extra, "='%u'))",
				   job_cond->cpus_min);

		}
	}

	if (job_cond->nodes_min) {
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		if (job_cond->nodes_max) {
			xstrfmtcat(*extra,
				   "(t1.nodes_alloc between %u and %u))",
				   job_cond->nodes_min, job_cond->nodes_max);

		} else {
			xstrfmtcat(*extra, "(t1.nodes_alloc='%u'))",
				   job_cond->nodes_min);

		}
	}

	if (job_cond->timelimit_min) {
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		if (job_cond->timelimit_max) {
			xstrfmtcat(*extra, "(t1.timelimit between %u and %u))",
				   job_cond->timelimit_min,
				   job_cond->timelimit_max);

		} else {
			xstrfmtcat(*extra, "(t1.timelimit='%u'))",
				   job_cond->timelimit_min);

		}
	}

	/* Time window should be exclusive of the end time, ie [start,end) */
	if (!job_cond->state_list || !list_count(job_cond->state_list)) {
		/*
		 * There's an explicit list of jobs, so don't hide
		 * non-eligible ones. Else handle normal time query of only
		 * eligible jobs.
		 */
		if (job_cond->step_list && list_count(job_cond->step_list)) {
			if (!(job_cond->flags &
			      JOBCOND_FLAG_NO_DEFAULT_USAGE)) {
				if (*extra)
					xstrcat(*extra, " and (");
				else
					xstrcat(*extra, " where (");

				xstrfmtcat(*extra,
					   "(t1.time_submit <= %ld) and "
					   "(t1.time_end >= %ld or"
					   " t1.time_end = 0))",
					   job_cond->usage_end,
					   job_cond->usage_start);
			}
		} else if (job_cond->usage_start) {
			if (*extra)
				xstrcat(*extra, " and (");
			else
				xstrcat(*extra, " where (");

			if (!job_cond->usage_end)
				xstrfmtcat(*extra,
					   "(t1.time_end >= %ld "
					   "or t1.time_end = 0))",
					   job_cond->usage_start);
			else
				xstrfmtcat(*extra,
					   "(t1.time_eligible "
					   "and t1.time_eligible < %ld "
					   "and (t1.time_end >= %ld "
					   "or t1.time_end = 0)))",
					   job_cond->usage_end,
					   job_cond->usage_start);
		} else if (job_cond->usage_end) {
			if (*extra)
				xstrcat(*extra, " and (");
			else
				xstrcat(*extra, " where (");
			xstrfmtcat(*extra,
				   "(t1.time_eligible and "
				   "t1.time_eligible < %ld))",
				   job_cond->usage_end);
		}
	}

	if (job_cond->wckey_list && list_count(job_cond->wckey_list)) {
		set = 0;
		if (*extra)
			xstrcat(*extra, " and (");
		else
			xstrcat(*extra, " where (");

		itr = list_iterator_create(job_cond->wckey_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "t1.wckey='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}
	return set;
}

extern List as_kingbase_jobacct_process_get_jobs(kingbase_conn_t *kingbase_conn,
					      uid_t uid,
					      slurmdb_job_cond_t *job_cond)
{
	char *extra = NULL;
	char *tmp = NULL, *tmp2 = NULL;
	ListIterator itr = NULL;
	int is_admin=1;
	int i;
	List job_list = NULL;
	slurmdb_user_rec_t user;
	int only_pending = 0;
	List use_cluster_list = NULL;
	char *cluster_name;
	bool locked = false;
	assoc_mgr_lock_t locks = { NO_LOCK, NO_LOCK, NO_LOCK, NO_LOCK,
				   READ_LOCK, NO_LOCK, NO_LOCK };

	memset(&user, 0, sizeof(slurmdb_user_rec_t));
	user.uid = uid;

	/*
	 * This clause must be kept in sync with the access check in
	 * _cluster_get_jobs().
	 */
	if ((slurm_conf.private_data & PRIVATE_DATA_JOBS)  ||
	    (job_cond->flags & JOBCOND_FLAG_SCRIPT) ||
	    (job_cond->flags & JOBCOND_FLAG_ENV)) {
		if (!(is_admin = is_user_min_admin_level(
			      kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR))) {
			/*
			 * Only fill in the coordinator accounts here we will
			 * check them later when we actually try to get the jobs
			 */
			(void) is_user_any_coord(kingbase_conn, &user);
		}
		if (!is_admin && !user.name) {
			debug("User %u has no associations, and is not admin, "
			      "so not returning any jobs.", user.uid);
			return NULL;
		}
	}

	if (job_cond
	    && job_cond->state_list && (list_count(job_cond->state_list) == 1)
	    && (slurm_atoul(list_peek(job_cond->state_list)) == JOB_PENDING))
		only_pending = 1;

	if (job_cond &&
	    (!job_cond->step_list || !list_count(job_cond->step_list))) {
		char *reason = NULL;

		if (job_cond->flags & JOBCOND_FLAG_SCRIPT)
			reason = "job scripts";

		if (job_cond->flags & JOBCOND_FLAG_ENV)
			reason = "job environment";

		if (reason) {
			error("User %u is requesting %s, but no job requested, this is not allowed",
			      user.uid, reason);
			return NULL;
		}
	}

	setup_job_cond_limits(job_cond, &extra);

	xfree(tmp);
	xstrfmtcat(tmp, "%s", job_req_inx[0]);
	for (i = 1; i < JOB_REQ_COUNT; i++) {
		/* Only get the script if requesting it */
		if (((i == JOB_REQ_SCRIPT) &&
		     (!job_cond || !(job_cond->flags & JOBCOND_FLAG_SCRIPT))) ||
		    ((i == JOB_REQ_ENV) &&
		     (!job_cond || !(job_cond->flags & JOBCOND_FLAG_ENV))))
			xstrcat(tmp, ", ''");
		else
			xstrfmtcat(tmp, ", %s", job_req_inx[i]);
	}

	xfree(tmp2);
	xstrfmtcat(tmp2, "%s", step_req_inx[0]);
	for (i = 1; i < STEP_REQ_COUNT; i++) {
		xstrfmtcat(tmp2, ", %s", step_req_inx[i]);
	}

	if (job_cond
	    && job_cond->cluster_list && list_count(job_cond->cluster_list))
		use_cluster_list = job_cond->cluster_list;
	else {
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		use_cluster_list = list_shallow_copy(as_kingbase_cluster_list);
		locked = true;
	}

	assoc_mgr_lock(&locks);

	job_list = list_create(slurmdb_destroy_job_rec);
	itr = list_iterator_create(use_cluster_list);
	while ((cluster_name = list_next(itr))) {
		int rc;
		_setup_job_cond_selected_steps(job_cond, cluster_name, &extra);
		if ((rc = _cluster_get_jobs(kingbase_conn, &user, job_cond,
					    cluster_name, tmp, tmp2, extra,
					    is_admin, only_pending, job_list))
		    != SLURM_SUCCESS)
			error("Problem getting jobs for cluster %s",
			      cluster_name);
	}
	list_iterator_destroy(itr);

	assoc_mgr_unlock(&locks);

	if (locked) {
		FREE_NULL_LIST(use_cluster_list);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	}

	xfree(tmp);
	xfree(tmp2);
	xfree(extra);
	return job_list;
}
