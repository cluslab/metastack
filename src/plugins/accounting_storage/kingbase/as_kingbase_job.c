/*****************************************************************************\
 *  as_kingbase_job.c - functions dealing with jobs and job steps.
 *****************************************************************************
 *  Copyright (C) 2004-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
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
\*****************************************************************************/

#include "as_kingbase_job.h"
#include "as_kingbase_jobacct_process.h"
#include "as_kingbase_usage.h"
#include "as_kingbase_wckey.h"

#include "src/common/assoc_mgr.h"
#include "src/common/gres.h"
#include "src/common/parse_time.h"
#include "src/common/select.h"
#include "src/common/slurm_jobacct_gather.h"
#include "src/common/slurm_time.h"

#define MAX_FLUSH_JOBS 500

typedef struct {
	char *cluster;
	uint32_t new;
	uint32_t old;
} id_switch_t;

static int _find_id_switch(void *x, void *key)
{
	id_switch_t *id_switch = (id_switch_t *)x;
	uint32_t id = *(uint32_t *)key;

	if (id_switch->old == id)
		return 1;
	return 0;
}

static char *_average_tres_usage(uint32_t *tres_ids, uint64_t *tres_cnts,
				 int tres_cnt, int tasks)
{
	char *ret_str = NULL;
	int i;

	/*
	 * Don't return NULL here, we need a blank string or we will print
	 * '(null)' in the database which really isn't what we want.
	 */
	if (!tasks)
		return xstrdup("");

	for (i = 0; i < tres_cnt; i++) {
		if (tres_cnts[i] == INFINITE64)
			continue;
		xstrfmtcat(ret_str, "%s%u=%"PRIu64,
			   ret_str ? "," : "",
			   tres_ids[i], tres_cnts[i] / (uint64_t)tasks);
	}

	if (!ret_str)
		ret_str = xstrdup("");
	return ret_str;
}

/* Used in job functions for getting the database index based off the
 * submit time and job.  0 is returned if none is found
 */
static uint64_t _get_db_index(kingbase_conn_t *kingbase_conn,
			      time_t submit, uint32_t jobid)
{
	KCIResult *result = NULL;
	uint64_t db_index = 0;
	char *query = xstrdup_printf("select job_db_inx from `%s_%s` where "
				     "time_submit=%d and id_job=%u",
				     kingbase_conn->cluster_name, job_table,
				     (int)submit, jobid);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		return 0;
	}

	if (!KCIResultGetRowCount(result)) {
		KCIResultDealloc(result);
		debug4("We can't get a db_index for this combo, "
		       "time_submit=%d and id_job=%u.  "
		       "We must not have heard about the start yet, "
		       "no big deal, we will get one right after this.",
		       (int)submit, jobid);
		return 0;
	}
	db_index = slurm_atoull(KCIResultGetColumnValue(result,0,0));
	KCIResultDealloc(result);

	return db_index;
}

static char *_get_user_from_associd(kingbase_conn_t *kingbase_conn,
				    char *cluster, uint32_t associd)
{
	char *user = NULL;
	char *query = NULL;
	KCIResult *result = NULL;

	/* Just so we don't have to keep a
	   cache of the associations around we
	   will just query the db for the user
	   name of the association id.  Since
	   this should sort of be a rare case
	   this isn't too bad.
	*/
	query = xstrdup_printf("select `user` from `%s_%s` where id_assoc=%u",
			       cluster, assoc_table, associd);

	debug4("%d(%s:%d) query\n%s",
	       kingbase_conn->conn, THIS_FILE, __LINE__, query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	   
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		return NULL;
	}

	char *tmp_str = KCIResultGetColumnValue(result,0,0);
    if((KCIResultGetRowCount(result)> 0) && (*tmp_str != '\0')) {
		user = xstrdup(KCIResultGetColumnValue(result,0,0));
	}
	
	KCIResultDealloc(result);

	return user;
}

static uint32_t _get_wckeyid(kingbase_conn_t *kingbase_conn, char **name,
			     uid_t uid, char *cluster, uint32_t associd)
{
	uint32_t wckeyid = 0;

	if (slurm_get_track_wckey()) {
		/* Here we are looking for the wckeyid if it doesn't
		 * exist we will create one.  We don't need to check
		 * if it is good or not.  Right now this is the only
		 * place things are created. We do this only on a job
		 * start, not on a job submit since we don't want to
		 * slow down getting the db_index back to the
		 * controller.
		 */
		slurmdb_wckey_rec_t wckey_rec;
		char *user = NULL;

		/* since we are unable to rely on uids here (someone could
		   not have there uid in the system yet) we must
		   first get the user name from the associd */
		if (!(user = _get_user_from_associd(
			      kingbase_conn, cluster, associd))) {
			error("No user for associd %u", associd);
			goto no_wckeyid;
		}
		/* get the default key */
		if (!*name) {
			slurmdb_user_rec_t user_rec;
			memset(&user_rec, 0, sizeof(slurmdb_user_rec_t));
			user_rec.uid = NO_VAL;
			user_rec.name = user;
			if (assoc_mgr_fill_in_user(kingbase_conn, &user_rec,
						   1, NULL, false)
			    != SLURM_SUCCESS) {
				error("No user by name of %s assoc %u",
				      user, associd);
				xfree(user);
				goto no_wckeyid;
			}

			if (user_rec.default_wckey)
				*name = xstrdup_printf("*%s",
						       user_rec.default_wckey);
			else
				*name = xstrdup_printf("*");
		}

		memset(&wckey_rec, 0, sizeof(slurmdb_wckey_rec_t));
		wckey_rec.name = (*name);
		wckey_rec.uid = NO_VAL;
		wckey_rec.user = user;
		wckey_rec.cluster = cluster;
		if (assoc_mgr_fill_in_wckey(kingbase_conn, &wckey_rec,
					    ACCOUNTING_ENFORCE_WCKEYS,
					    NULL, false) != SLURM_SUCCESS) {
			List wckey_list = NULL;
			slurmdb_wckey_rec_t *wckey_ptr = NULL;
			/* we have already checked to make
			   sure this was the slurm user before
			   calling this */

			wckey_list = list_create(slurmdb_destroy_wckey_rec);

			wckey_ptr = xmalloc(sizeof(slurmdb_wckey_rec_t));
			wckey_ptr->name = xstrdup((*name));
			wckey_ptr->user = xstrdup(user);
			wckey_ptr->cluster = xstrdup(cluster);
			list_append(wckey_list, wckey_ptr);
			/* info("adding wckey '%s' '%s' '%s'", */
			/* 	     wckey_ptr->name, wckey_ptr->user, */
			/* 	     wckey_ptr->cluster); */

			if (*name[0] == '*') {
				/* make sure the non * wckey has been added */
				wckey_rec.name = (*name)+1;
				if (assoc_mgr_fill_in_wckey(
					    kingbase_conn, &wckey_rec,
					    ACCOUNTING_ENFORCE_WCKEYS,
					    NULL, false) != SLURM_SUCCESS) {
					wckey_ptr = xmalloc(
						sizeof(slurmdb_wckey_rec_t));
					wckey_ptr->name =
						xstrdup(wckey_rec.name);
					wckey_ptr->user = xstrdup(user);
					wckey_ptr->cluster = xstrdup(cluster);
					list_prepend(wckey_list, wckey_ptr);
					/* info("adding wckey '%s' '%s' " */
					/*      "'%s'", */
					/*      wckey_ptr->name, */
					/*      wckey_ptr->user, */
					/*      wckey_ptr->cluster); */
				}
				wckey_rec.name = (*name);
			}

			if (as_kingbase_add_wckeys(kingbase_conn,
			                        slurm_conf.slurm_user_id,
			                        wckey_list)
			    == SLURM_SUCCESS)
				acct_storage_p_commit(kingbase_conn, 1);
			/* If that worked lets get it */
			assoc_mgr_fill_in_wckey(kingbase_conn, &wckey_rec,
						ACCOUNTING_ENFORCE_WCKEYS,
						NULL, false);

			FREE_NULL_LIST(wckey_list);
		}
		xfree(user);
		/* info("got wckeyid of %d", wckey_rec.id); */
		wckeyid = wckey_rec.id;
	}
no_wckeyid:
	return wckeyid;
}


static uint64_t _get_hash_inx(kingbase_conn_t *kingbase_conn,
			      job_record_t *job_ptr,
			      uint64_t flag)
{
	char *query, *hash;
	char *hash_col = NULL, *type_col = NULL, *type_table = NULL;
	KCIResult *result = NULL;
	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t* data_rt = NULL;
	uint64_t hash_inx = 0;

	switch (flag) {
	case JOB_SEND_ENV:
		hash_col = "env_hash";
		type_col = "env_vars";
		type_table = job_env_table;
		hash = job_ptr->details->env_hash;
		break;
	case JOB_SEND_SCRIPT:
		hash_col = "script_hash";
		type_col = "batch_script";
		type_table = job_script_table;
		hash = job_ptr->details->script_hash;
		break;
	default:
		error("unknown hash type bit %"PRIu64, flag);
		return NO_VAL64;
		break;
	}

	if (!hash)
		return 0;

	query = xstrdup_printf(
		"select hash_inx from `%s_%s` where %s = '%s';",
		kingbase_conn->cluster_name, type_table,
		hash_col, hash);

	if (!(result = kingbase_db_query_ret(kingbase_conn, query, 0))) {
		xfree(query);
		return NO_VAL64;
	}

	xfree(query);

	if (KCIResultGetRowCount(result) != 0) {
		debug3("%u has an %s we have already seen, no need to add again",
		       job_ptr->job_id, type_col);
		hash_inx = slurm_atoull(KCIResultGetColumnValue(result, 0, 0));
	} else {
		query = xstrdup_printf(
			"insert into `%s_%s` (%s) values ('%s') "
			"on duplicate key update last_used=VALUES(last_used); ",
			// "hash_inx=LAST_INSERT_ID(hash_inx);",
			kingbase_conn->cluster_name, type_table,
			hash_col, hash);

		char *query2 = NULL;
		query2 = xstrdup_printf(
			"insert into `%s_%s` (%s) values ('%s') "
			"on duplicate key update last_used=VALUES(last_used) returning hash_inx; ",
			// "hash_inx=LAST_INSERT_ID(hash_inx);",
			kingbase_conn->cluster_name, type_table,
			hash_col, hash);
		fetch_flag = set_fetch_flag(true, false, true);
		data_rt = xmalloc(sizeof(fetch_result_t));
		
		kingbase_for_fetch2(kingbase_conn, query,fetch_flag, data_rt, query2);
		hash_inx = data_rt->insert_ret_id;
		
		if (!hash_inx)
			hash_inx = NO_VAL64;
		else
			job_ptr->bit_flags |= flag;
		xfree(query);
		xfree(query2);
		free_res_data(data_rt, fetch_flag);
	}
	KCIResultDealloc(result);

	return hash_inx;

}

/* extern functions */

extern int as_kingbase_job_start(kingbase_conn_t *kingbase_conn, job_record_t *job_ptr)
{
	int rc = SLURM_SUCCESS;
	char *nodes = NULL, *jname = NULL;
	char *partition = NULL;
	char *query = NULL;
	//char *query2 = NULL;
	int reinit = 0;
	time_t begin_time, check_time, start_time, submit_time;
	uint32_t wckeyid = 0;
	uint32_t job_state;
	uint32_t array_task_id =
		(job_ptr->array_job_id) ? job_ptr->array_task_id : NO_VAL;
	uint32_t het_job_offset =
		(job_ptr->het_job_id) ? job_ptr->het_job_offset : NO_VAL;
	uint64_t job_db_inx = job_ptr->db_index;
	job_array_struct_t *array_recs = job_ptr->array_recs;
	KCIResult *result = NULL;

	if ((!job_ptr->details || !job_ptr->details->submit_time)
	    && !job_ptr->resize_time) {
		error("as_kingbase_job_start: "
		      "Not inputing this job, it has no submit time.");
		return SLURM_ERROR;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	debug2("%s: called", __func__);

	job_state = job_ptr->job_state;

	if (job_ptr->resize_time) {
		begin_time  = job_ptr->resize_time;
		submit_time = job_ptr->resize_time;
		start_time  = job_ptr->resize_time;
	} else {
		begin_time  = job_ptr->details->begin_time;
		submit_time = job_ptr->details->submit_time;
		start_time  = job_ptr->start_time;
	}

	/* If the reason is WAIT_ARRAY_TASK_LIMIT we don't want to
	 * give the pending jobs an eligible time since it will add
	 * time to accounting where as these jobs aren't able to run
	 * until later so mark it as such.
	 */
	if (job_ptr->state_reason == WAIT_ARRAY_TASK_LIMIT)
		begin_time = INFINITE;

	/*
	 * Strip the RESIZING flag and end the job if there was a db_index.  In
	 * 21.08 we reset the db_index on the slurmctld side, previously it was
	 * done here.
	 */
	if (IS_JOB_RESIZING(job_ptr)) {
		/*
		 * If we have a db_index lets end the previous record.
		 * This should only need to be around 2 versions after 21.08.
		 */
		if (job_ptr->db_index) {
			as_kingbase_job_complete(kingbase_conn, job_ptr);
			job_ptr->db_index = 0;
		}
			
		job_state &= (~JOB_RESIZING);
	}

	job_state &= JOB_STATE_BASE;

	/* See what we are hearing about here if no start time. If
	 * this job latest time is before the last roll up we will
	 * need to reset it to look at this job. */
	if (start_time)
		check_time = start_time;
	else if (begin_time)
		check_time = begin_time;
	else
		check_time = submit_time;

	slurm_mutex_lock(&rollup_lock);
	if (check_time < global_last_rollup) {

		/* check to see if we are hearing about this time for the
		 * first time.
		 */
		query = xstrdup_printf("select job_db_inx "
				       "from `%s_%s` where id_job=%u and "
				       "time_submit=%ld and time_eligible=%ld "
				       "and time_start=%ld;",
				       kingbase_conn->cluster_name,
				       job_table, job_ptr->job_id,
				       submit_time, begin_time, start_time);
		DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		result = kingbase_db_query_ret(kingbase_conn, query, 0);
		xfree(query);
		if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
			KCIResultDealloc(result);
			slurm_mutex_unlock(&rollup_lock);
			return SLURM_ERROR;
		}

        if(KCIResultGetRowCount(result)){
			KCIResultDealloc(result);
			debug4("revieved an update for a "
			       "job (%u) already known about",
			       job_ptr->job_id);
			slurm_mutex_unlock(&rollup_lock);
			goto no_rollup_change;
		}
		KCIResultDealloc(result);

		if (job_ptr->start_time)
			debug("Need to reroll usage from %s Job %u "
			      "from %s started then and we are just "
			      "now hearing about it.",
			      slurm_ctime2(&check_time),
			      job_ptr->job_id, kingbase_conn->cluster_name);
		else if (begin_time)
			debug("Need to reroll usage from %s Job %u "
			      "from %s became eligible then and we are just "
			      "now hearing about it.",
			      slurm_ctime2(&check_time),
			      job_ptr->job_id, kingbase_conn->cluster_name);
		else
			debug("Need to reroll usage from %s Job %u "
			      "from %s was submitted then and we are just "
			      "now hearing about it.",
			      slurm_ctime2(&check_time),
			      job_ptr->job_id, kingbase_conn->cluster_name);

		global_last_rollup = check_time;
		slurm_mutex_unlock(&rollup_lock);

		/* If the times here are later than the daily_rollup
		   or monthly rollup it isn't a big deal since they
		   are always shrunk down to the beginning of each
		   time period.
		*/
		query = xstrdup_printf("update `%s_%s` set "
				       "hourly_rollup=%ld, "
				       "daily_rollup=%ld, monthly_rollup=%ld",
				       kingbase_conn->cluster_name,
				       last_ran_table, check_time,
				       check_time, check_time);
		DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

		fetch_flag_t *fetch_flag = NULL;
		fetch_result_t *data_rt = NULL;
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		free_res_data(data_rt, fetch_flag);	
		xfree(query);
	} else
		slurm_mutex_unlock(&rollup_lock);

no_rollup_change:

	if (job_ptr->name && job_ptr->name[0])
		jname = job_ptr->name;
	else 
		jname = "allocation";

	if (job_ptr->nodes && job_ptr->nodes[0])
		nodes = job_ptr->nodes;
	else
		nodes = "None assigned"; 

	/* Grab the wckey once to make sure it is placed. */
	if (job_ptr->assoc_id && (!job_ptr->db_index || job_ptr->wckey))
		wckeyid = _get_wckeyid(kingbase_conn, &job_ptr->wckey,
				       job_ptr->user_id,
				       kingbase_conn->cluster_name,
				       job_ptr->assoc_id);

	if (!IS_JOB_PENDING(job_ptr) && job_ptr->part_ptr)
		partition = job_ptr->part_ptr->name;
	else if (job_ptr->partition)
		partition = job_ptr->partition;

	if (!partition)
		partition = "";

	/* Mark the database so we know we have recieved the start record. */
	job_ptr->db_flags |= SLURMDB_JOB_FLAG_START_R;
		
	if (!job_ptr->db_index) {
		uint64_t env_hash_inx = 0, script_hash_inx = 0;
		/*
		 * Here we check to see if the env has been added to the
		 * database or not to inform the slurmctld to send it.
		 * This only happens if !db_index no need to do this on an
		 * update.
		 */
		if (job_ptr->details->env_hash) {
			env_hash_inx = _get_hash_inx(
				kingbase_conn, job_ptr, JOB_SEND_ENV);

			if (env_hash_inx == NO_VAL64)
				return SLURM_ERROR;
		}

		/*
		 * Here we check to see if the script has been added to the
		 * database or not to inform the slurmctld to send it.
		 * This only happens if !db_index no need to do this on an
		 * update.
		 */
		if (job_ptr->details->script_hash) {
			script_hash_inx = _get_hash_inx(
				kingbase_conn, job_ptr, JOB_SEND_SCRIPT);

			if (script_hash_inx == NO_VAL64)
				return SLURM_ERROR;
		}

		query = xstrdup_printf(
			"insert into `%s_%s` "
			"(id_job, mod_time, id_array_job, id_array_task, "
			"het_job_id, het_job_offset, "
			"id_assoc, id_qos, id_user, "
			"id_group, nodelist, id_resv, timelimit, "
			"time_eligible, time_submit, time_start, "
			"job_name, state, priority, cpus_req, "
			"nodes_alloc, mem_req, flags, state_reason_prev, "
			"env_hash_inx, script_hash_inx",
			kingbase_conn->cluster_name, job_table);

		if (wckeyid >= 0)
			xstrcat(query, ", id_wckey");
		if (job_ptr->mcs_label)
			xstrcat(query, ", mcs_label");
		if (job_ptr->account)
			xstrcat(query, ", account");
		if (partition)
			xstrcat(query, ", partition");
		if (job_ptr->wckey)
			xstrcat(query, ", wckey");
		if (job_ptr->network)
			xstrcat(query, ", node_inx");
		if (array_recs && array_recs->task_id_str)
			xstrcat(query, ", array_task_str, array_max_tasks, "
				"array_task_pending");
		else
			xstrcat(query, ", array_task_str, array_task_pending");

		if (job_ptr->tres_alloc_str)
			xstrcat(query, ", tres_alloc");
		if (job_ptr->tres_req_str)
			xstrcat(query, ", tres_req");
		if (job_ptr->details->work_dir)
			xstrcat(query, ", work_dir");
		if (job_ptr->details->features)
			xstrcat(query, ", constraints");
		if (job_ptr->details->submit_line)
			xstrcat(query, ", submit_line");
		if (job_ptr->container)
			xstrcat(query, ", container");

		xstrfmtcat(query,
			   ") values (%u, UNIX_TIMESTAMP(), "
			   "%u, %u, %u, %u, %u, %u, %u, %u, "
			   "'%s', %u, %u, %ld, %ld, %ld, "
			   "'%s', %u, %u, %u, %u, %"PRIu64", %u, %u, "
			   "%"PRIu64", %"PRIu64,
			   job_ptr->job_id,
			   job_ptr->array_job_id, array_task_id,
			   job_ptr->het_job_id, het_job_offset,
			   job_ptr->assoc_id, job_ptr->qos_id,
			   job_ptr->user_id, job_ptr->group_id, nodes,
			   job_ptr->resv_id, job_ptr->time_limit,
			   begin_time, submit_time, start_time,
			   jname, job_state,
			   job_ptr->priority, job_ptr->details->min_cpus,
			   job_ptr->total_nodes,
			   job_ptr->details->pn_min_memory,
			   job_ptr->db_flags,
			   job_ptr->state_reason_prev_db,
			   env_hash_inx, script_hash_inx);

		if (wckeyid >= 0)
			xstrfmtcat(query, ", %u", wckeyid);
		if (job_ptr->mcs_label)
			xstrfmtcat(query, ", '%s'", job_ptr->mcs_label);
		if (job_ptr->account)
			xstrfmtcat(query, ", '%s'", job_ptr->account);
		if (partition)
			xstrfmtcat(query, ", '%s'", partition);
		if (job_ptr->wckey)
			xstrfmtcat(query, ", '%s'", job_ptr->wckey);
		if (job_ptr->network)
			xstrfmtcat(query, ", '%s'", job_ptr->network);
		if (array_recs && array_recs->task_id_str)
			xstrfmtcat(query, ", '%s', %u, %u",
				   array_recs->task_id_str,
				   array_recs->max_run_tasks,
				   array_recs->task_cnt);
		else
			xstrcat(query, ", NULL, 0");

		if (job_ptr->tres_alloc_str)
			xstrfmtcat(query, ", '%s'", job_ptr->tres_alloc_str);
		if (job_ptr->tres_req_str)
			xstrfmtcat(query, ", '%s'", job_ptr->tres_req_str);
		if (job_ptr->details->work_dir)
			xstrfmtcat(query, ", '%s'",
				   job_ptr->details->work_dir);
		if (job_ptr->details->features)
			xstrfmtcat(query, ", '%s'",
				   job_ptr->details->features);
		if (job_ptr->details->submit_line)
			xstrfmtcat(query, ", '%s'",
				   job_ptr->details->submit_line);
		if (job_ptr->container)
			xstrfmtcat(query, ", '%s'",
				   job_ptr->container);

		xstrfmtcat(query,
			   ") on duplicate key update "
			   //"job_db_inx=nextval(job_db_inx), "
			   "id_assoc=%u, id_user=%u, id_group=%u, "
			   "nodelist='%s', id_resv=%u, timelimit=%u, "
			   "time_submit=%ld, time_eligible=%ld, "
			   "time_start=%ld, mod_time=UNIX_TIMESTAMP(), "
			   "job_name='%s', id_qos=%u, "
			   "state=greatest(state, %u), priority=%u, "
			   "cpus_req=%u, nodes_alloc=%u, "
			   "mem_req=%"PRIu64", id_array_job=%u, id_array_task=%u, "
			   "het_job_id=%u, het_job_offset=%u, flags=%u, "
			   "state_reason_prev=%u, env_hash_inx=%"PRIu64
			   ", script_hash_inx=%"PRIu64,
			   job_ptr->assoc_id, job_ptr->user_id,
			   job_ptr->group_id, nodes,
			   job_ptr->resv_id, job_ptr->time_limit,
			   submit_time, begin_time, start_time,
			   jname, job_ptr->qos_id, job_state,
			   job_ptr->priority, job_ptr->details->min_cpus,
			   job_ptr->total_nodes,
			   job_ptr->details->pn_min_memory,
			   job_ptr->array_job_id, array_task_id,
			   job_ptr->het_job_id, het_job_offset,
			   job_ptr->db_flags,
			   job_ptr->state_reason_prev_db,
			   env_hash_inx, script_hash_inx);

		if (wckeyid)
			xstrfmtcat(query, ", id_wckey=%u", wckeyid);
		if (job_ptr->mcs_label)
			xstrfmtcat(query, ", mcs_label='%s'",
				   job_ptr->mcs_label);
		if (job_ptr->account)
			xstrfmtcat(query, ", account='%s'", job_ptr->account);
		if (partition)
			xstrfmtcat(query, ", partition='%s'", partition);
		if (job_ptr->wckey)
			xstrfmtcat(query, ", wckey='%s'", job_ptr->wckey);
		if (job_ptr->network)
			xstrfmtcat(query, ", node_inx='%s'", job_ptr->network);
		if (array_recs && array_recs->task_id_str)
			xstrfmtcat(query, ", array_task_str='%s', "
				   "array_max_tasks=%u, array_task_pending=%u",
				   array_recs->task_id_str,
				   array_recs->max_run_tasks,
				   array_recs->task_cnt);
		else
			xstrfmtcat(query, ", array_task_str=NULL, "
				   "array_task_pending=0");

		if (job_ptr->tres_alloc_str)
			xstrfmtcat(query, ", tres_alloc='%s'",
				   job_ptr->tres_alloc_str);
		if (job_ptr->tres_req_str)
			xstrfmtcat(query, ", tres_req='%s'",
				   job_ptr->tres_req_str);
		if (job_ptr->details->work_dir)
			xstrfmtcat(query, ", work_dir='%s'",
				   job_ptr->details->work_dir);
		if (job_ptr->details->features)
			xstrfmtcat(query, ", constraints='%s'",
				   job_ptr->details->features);

		if (job_ptr->details->submit_line)
			xstrfmtcat(query, ", submit_line='%s'",
				   job_ptr->details->submit_line);
		if (job_ptr->container)
			xstrfmtcat(query, ", container='%s'",
				   job_ptr->container);

		DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
//try_again:
		// if (!(job_ptr->db_index = kingbase_db_insert_ret_id(
		// 	      kingbase_conn, query))) {
		// 	if (!reinit) {
		// 		error("%s: It looks like the storage has gone away trying to reconnect",
		// 		      __func__);
		// 		/* reconnect */
		// 		check_connection(kingbase_conn);
		// 		reinit = 1;
		// 		goto try_again;
		// 	} else
		// 		rc = SLURM_ERROR;
		// }
		fetch_flag_t *fetch_flag = NULL;
		fetch_result_t *data_rt = NULL;
		// query2 = xstrdup(query);
		// char *lastSemicolon = xstrrchr(query2, ';'); 
		// if (lastSemicolon != NULL) {
		// 		*lastSemicolon = ' '; // 将最后一个分号替换为空格
		// }
        // xstrfmtcat(query2," returning job_db_inx;" );

	try_again:		
		fetch_flag = set_fetch_flag(true, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		job_ptr->db_index = data_rt->insert_ret_id;
		free_res_data(data_rt, fetch_flag);
		if (rc == SLURM_ERROR) {
			if (!reinit) {
				error("%s: It looks like the storage has gone away trying to reconnect",
				      __func__);
				/* reconnect */
				check_connection(kingbase_conn);
				reinit = 1;
				goto try_again;
			} else {
				rc = SLURM_ERROR;
			}
		} else {
			uint64_t db_inx = _get_db_index(kingbase_conn,
					job_ptr->details->
					submit_time,
					job_ptr->job_id);
			if (db_inx) {
				job_ptr->db_index = db_inx;
			} else {
				rc = SLURM_ERROR;
			}
		}
     
		// if (!job_ptr->db_index) {
		// 	if (!reinit) {
		// 		error("%s: It looks like the storage has gone away trying to reconnect",
		// 		      __func__);
		// 		/* reconnect */
		// 		check_connection(kingbase_conn);
		// 		reinit = 1;
		// 		goto try_again;
		// 	} else {
		// 		rc = SLURM_ERROR;
		// 	}
		// } 
	} else {
		query = xstrdup_printf("update `%s_%s` set nodelist='%s', ",
				       kingbase_conn->cluster_name,
				       job_table, nodes);

		if (wckeyid)
			xstrfmtcat(query, "id_wckey=%u, ", wckeyid);
		if (job_ptr->mcs_label)
			xstrfmtcat(query, "mcs_label='%s', ",
				   job_ptr->mcs_label);
		if (job_ptr->account)
			xstrfmtcat(query, "account='%s', ", job_ptr->account);
		if (partition)
			xstrfmtcat(query, "partition='%s', ", partition);
		if (job_ptr->wckey)
			xstrfmtcat(query, "wckey='%s', ", job_ptr->wckey);
		if (job_ptr->network)
			xstrfmtcat(query, "node_inx='%s', ", job_ptr->network);
		if (array_recs && array_recs->task_id_str)
			xstrfmtcat(query, "array_task_str='%s', "
				   "array_max_tasks=%u, "
				   "array_task_pending=%u, ",
				   array_recs->task_id_str,
				   array_recs->max_run_tasks,
				   array_recs->task_cnt);
		else
			xstrfmtcat(query, "array_task_str=NULL, "
				   "array_task_pending=0, ");

		if (job_ptr->tres_alloc_str)
			xstrfmtcat(query, "tres_alloc='%s', ",
				   job_ptr->tres_alloc_str);
		if (job_ptr->tres_req_str)
			xstrfmtcat(query, "tres_req='%s', ",
				   job_ptr->tres_req_str);
		if (job_ptr->details->work_dir)
			xstrfmtcat(query, "work_dir='%s', ",
				   job_ptr->details->work_dir);
		if (job_ptr->details->features)
			xstrfmtcat(query, "constraints='%s', ",
				   job_ptr->details->features);

		if (job_ptr->details->submit_line)
			xstrfmtcat(query, "submit_line='%s', ",
				   job_ptr->details->submit_line);
		if (job_ptr->container)
			xstrfmtcat(query, "container='%s', ",
				   job_ptr->container);

		xstrfmtcat(query, "time_start=%ld, job_name='%s', "
			   "state=greatest(state, %u), "
			   "nodes_alloc=%u, id_qos=%u, "
			   "id_assoc=%u, id_resv=%u, "
			   "timelimit=%u, mem_req=%"PRIu64", "
			   "id_array_job=%u, id_array_task=%u, "
			   "het_job_id=%u, het_job_offset=%u, "
			   "flags=%u, state_reason_prev=%u, "
			   "time_eligible=%ld, mod_time=UNIX_TIMESTAMP() "
			   "where job_db_inx=%"PRIu64,
			   start_time, jname, job_state,
			   job_ptr->total_nodes, job_ptr->qos_id,
			   job_ptr->assoc_id,
			   job_ptr->resv_id, job_ptr->time_limit,
			   job_ptr->details->pn_min_memory,
			   job_ptr->array_job_id, array_task_id,
			   job_ptr->het_job_id, het_job_offset,
			   job_ptr->db_flags, job_ptr->state_reason_prev_db,
			   begin_time, job_ptr->db_index);

		DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

		fetch_flag_t *fetch_flag = NULL;
		fetch_result_t *data_rt = NULL;
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		free_res_data(data_rt, fetch_flag);
	}

	xfree(query);

	if (rc != SLURM_SUCCESS)
		return rc;

	/* now we will reset all the steps */
	if (IS_JOB_RESIZING(job_ptr)) {
		/* FIXME : Verify this is still needed */
		if (IS_JOB_SUSPENDED(job_ptr))
			as_kingbase_suspend(kingbase_conn, job_db_inx, job_ptr);
	}

	xfree(query);

	return rc;
}

extern int as_kingbase_job_heavy(kingbase_conn_t *kingbase_conn, job_record_t *job_ptr)
{
	char *query = NULL, *pos = NULL;
	int rc = SLURM_SUCCESS;
	struct job_details *details = job_ptr->details;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	xassert(details);

	debug2("%s() called", __func__);

	/*
	 * make sure we handle any quotes that may be in the comment
	 */
	if (details->env_hash && details->env_sup && details->env_sup[0])
		xstrfmtcatat(
			query, &pos,
			"update `%s_%s` set env_vars = '%s' "
			"where env_hash='%s';",
			kingbase_conn->cluster_name, job_env_table,
			details->env_sup[0], details->env_hash);
	if (details->script_hash && details->script)
		xstrfmtcatat(
			query, &pos,
			"update `%s_%s` set batch_script = '%s' "
			"where script_hash='%s';",
			kingbase_conn->cluster_name, job_script_table,
			details->script, details->script_hash);

	if (!query)
		return rc;

	DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
	rc = kingbase_db_query(kingbase_conn, query);
	xfree(query);

	return rc;
}

extern List as_kingbase_modify_job(kingbase_conn_t *kingbase_conn, uint32_t uid,
				slurmdb_job_cond_t *job_cond,
				slurmdb_job_rec_t *job)
{
	List ret_list = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL;
	char *vals = NULL, *cond_char = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	List job_list = NULL;
	slurmdb_job_rec_t *job_rec;
	ListIterator itr;
	List id_switch_list = NULL;
	id_switch_t *id_switch;
	bool is_admin;

	is_admin = is_user_min_admin_level(kingbase_conn, uid,
					   SLURMDB_ADMIN_OPERATOR);

	if (!job_cond || !job) {
		error("we need something to change");
		return NULL;
	} else if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_admin && (job->admin_comment || job->system_comment)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	if (job->derived_ec != NO_VAL)
		xstrfmtcat(vals, ", derived_ec=%u", job->derived_ec);

	if (job->derived_es)
		xstrfmtcat(vals, ", derived_es='%s'", job->derived_es);

	if (job->admin_comment)
		xstrfmtcat(vals, ", admin_comment='%s'", job->admin_comment);

	if (job->system_comment)
		xstrfmtcat(vals, ", system_comment='%s'", job->system_comment);

	if (job->wckey)
		xstrfmtcat(vals, ", wckey='%s'", job->wckey);

	if (!vals) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		error("No change specified for job modification");
		return NULL;
	}
	job_cond->flags |= JOBCOND_FLAG_NO_STEP;
	job_cond->flags |= JOBCOND_FLAG_NO_DEFAULT_USAGE;

	job_list = as_kingbase_jobacct_process_get_jobs(kingbase_conn, uid, job_cond);

	if (!job_list || !list_count(job_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_JOB, kingbase_conn->conn, "%s: Job(s) not found\n",
		         __func__);
		xfree(vals);
		FREE_NULL_LIST(job_list);
		return NULL;
	}

	user_name = uid_to_string((uid_t) uid);

	itr = list_iterator_create(job_list);
	while ((job_rec = list_next(itr))) {
		char tmp_char[25];
		char *vals_mod = NULL;

		if ((uid != job_rec->uid) && !is_admin) {
			errno = ESLURM_ACCESS_DENIED;
			rc = SLURM_ERROR;
			break;
		}

		slurm_make_time_str(&job_rec->submit,
				    tmp_char, sizeof(tmp_char));

		xstrfmtcat(cond_char, "job_db_inx=%"PRIu64, job_rec->db_index);
		object = xstrdup_printf("%u submitted at %s",
					job_rec->jobid, tmp_char);

		if (!ret_list)
			ret_list = list_create(xfree_ptr);
		list_append(ret_list, object);

		/*
		 * Grab the wckey id to update the job now.
		 */
		if (job->wckey) {
			uint32_t wckeyid = _get_wckeyid(kingbase_conn,
							&job->wckey,
							job_rec->uid,
							job_rec->cluster,
							job_rec->associd);
			if (!wckeyid) {
				rc = SLURM_ERROR;
				break;
			}
			vals_mod = xstrdup_printf("%s, id_wckey='%u'",
						  vals, wckeyid);
			id_switch = NULL;
			if (!id_switch_list)
				id_switch_list = list_create(xfree_ptr);
			else {
				id_switch = list_find_first(
					id_switch_list,
					_find_id_switch,
					&job_rec->wckeyid);
			}

			if (!id_switch) {
				id_switch = xmalloc(sizeof(id_switch_t));
				id_switch->cluster = job_rec->cluster;
				id_switch->old = job_rec->wckeyid;
				id_switch->new = wckeyid;
				list_append(id_switch_list, id_switch);
			}
		} else
			vals_mod = vals;

		rc = modify_common(kingbase_conn, DBD_MODIFY_JOB, now, user_name,
				   job_table, cond_char, vals_mod,
				   job_rec->cluster);
		xfree(cond_char);

		if (job->wckey)
			xfree(vals_mod);

		if (rc != SLURM_SUCCESS)
			break;
	}
	list_iterator_destroy(itr);

	xfree(vals);
	xfree(user_name);

	if (rc == SLURM_ERROR) {
		error("Couldn't modify job(s)");
		FREE_NULL_LIST(ret_list);
		ret_list = NULL;
	} else if (id_switch_list) {
		struct tm hour_tm;
		time_t usage_start, usage_end;
		char *time_str = NULL;
		char *query = NULL;

		if (!job_cond->usage_end)
			job_cond->usage_end = now;

		if (!localtime_r(&job_cond->usage_end, &hour_tm)) {
			error("Couldn't get localtime from end %ld",
			      job_cond->usage_end);
			FREE_NULL_LIST(ret_list);
			ret_list = NULL;
			goto endit;
		}
		hour_tm.tm_sec = 0;
		hour_tm.tm_min = 0;

		usage_end = slurm_mktime(&hour_tm);

		if (!job_cond->usage_start)
			usage_start = 0;
		else {
			if (!localtime_r(&job_cond->usage_start, &hour_tm)) {
				error("Couldn't get localtime from start %ld",
				      job_cond->usage_start);
				FREE_NULL_LIST(ret_list);
				ret_list = NULL;
				goto endit;
			}
			hour_tm.tm_sec = 0;
			hour_tm.tm_min = 0;

			usage_start = slurm_mktime(&hour_tm);
		}

		time_str = xstrdup_printf(
			"(time_start < %ld && time_start >= %ld)",
			usage_end, usage_start);

		itr = list_iterator_create(id_switch_list);
		while ((id_switch = list_next(itr))) {
			char *use_table = NULL;

			for (int i = 0; i < 3; i++) {
				switch (i) {
				case 0:
					use_table = wckey_hour_table;
					break;
				case 1:
					use_table = wckey_day_table;
					break;
				case 2:
					use_table = wckey_month_table;
					break;
				}

				use_table = xstrdup_printf(
					"%s_%s",
					id_switch->cluster, use_table);
				/*
				 * Move any of the new id lines into the old id.
				 */
				query = xstrdup_printf(
					"insert into `%s` (creation_time, mod_time, id, id_tres, time_start, alloc_secs) "
					"select creation_time, %ld, %u, id_tres, time_start, @ASUM:=SUM(alloc_secs) from `%s` where (id=%u || id=%u) && %s group by id_tres, time_start on duplicate key update alloc_secs=@ASUM;",
					use_table,
					now, id_switch->old, use_table,
					id_switch->new, id_switch->old,
					time_str);

				/* Delete all traces of the new id */
				xstrfmtcat(query,
					   "delete from `%s` where id=%u && %s;",
					   use_table, id_switch->new, time_str);

				/* Now we just need to switch the ids */
				xstrfmtcat(query,
					   "update `%s` set mod_time=%ld, id=%u where id=%u && %s;",
					   use_table, now, id_switch->new, id_switch->old, time_str);


				xfree(use_table);
				DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s",
				         query);
				//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
				fetch_flag_t *fetch_flag = NULL;
				fetch_result_t *data_rt = NULL;
				fetch_flag = set_fetch_flag(false, false, false);
				data_rt = xmalloc(sizeof(fetch_result_t));
				rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
				free_res_data(data_rt, fetch_flag);

				xfree(query);
				if (rc == SLURM_ERROR) {
					break;
				}
			}
			if (rc != SLURM_SUCCESS) {
				FREE_NULL_LIST(ret_list);
				ret_list = NULL;
				break;
			}
		}
		list_iterator_destroy(itr);
		xfree(time_str);
	}
endit:
	FREE_NULL_LIST(job_list);
	FREE_NULL_LIST(id_switch_list);
	return ret_list;
}

extern int as_kingbase_job_complete(kingbase_conn_t *kingbase_conn,
				 job_record_t *job_ptr)
{
	char *query = NULL;
	int rc = SLURM_SUCCESS, job_state;
	time_t submit_time, end_time;
	uint32_t exit_code = 0;

	if (!job_ptr->db_index
	    && ((!job_ptr->details || !job_ptr->details->submit_time)
		&& !job_ptr->resize_time)) {
		error("as_kingbase_job_complete: "
		      "Not inputing this job, it has no submit time.");
		return SLURM_ERROR;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	debug2("%s() called", __func__);

	if (job_ptr->resize_time)
		submit_time = job_ptr->resize_time;
	else
		submit_time = job_ptr->details->submit_time;

	if (IS_JOB_RESIZING(job_ptr)) {
		end_time = job_ptr->resize_time;
		job_state = JOB_RESIZING;
	} else {
		if (job_ptr->end_time == 0) {
			if (job_ptr->start_time) {
				error("%s: We are trying to end a job (%u) with no end time, setting it to the start time (%ld) of the job.",
				      __func__,
				      job_ptr->job_id, job_ptr->start_time);
				job_ptr->end_time = job_ptr->start_time;
			} else {
				error("%s: job %u never started",
				      __func__, job_ptr->job_id);

				/* If we get an error with this just
				 * fall through to avoid an infinite loop */
				return SLURM_SUCCESS;
			}
		}
		end_time = job_ptr->end_time;

		if (IS_JOB_REQUEUED(job_ptr))
			job_state = JOB_REQUEUE;
		else if (IS_JOB_REVOKED(job_ptr))
			job_state = JOB_REVOKED;
		else
			job_state = job_ptr->job_state & JOB_STATE_BASE;
	}

	if (trigger_reroll(kingbase_conn, end_time))
		debug("Need to reroll usage from %s Job %u from %s %s then and we are just now hearing about it.",
		      slurm_ctime2(&end_time),
		      job_ptr->job_id, kingbase_conn->cluster_name,
		      IS_JOB_RESIZING(job_ptr) ? "resized" : "ended");

	if (!job_ptr->db_index) {
		if (!(job_ptr->db_index =
		      _get_db_index(kingbase_conn,
				    submit_time,
				    job_ptr->job_id))) {
			/* Comment is overloaded in job_start to be
			   the block_id, so we will need to store this
			   for later.
			*/
			char *comment = job_ptr->comment;
			job_ptr->comment = NULL;
			/* If we get an error with this just fall
			 * through to avoid an infinite loop
			 */
			if (as_kingbase_job_start(
				    kingbase_conn, job_ptr) == SLURM_ERROR) {
				job_ptr->comment = comment;
				error("couldn't add job %u at job completion",
				      job_ptr->job_id);
				return SLURM_SUCCESS;
			}
			job_ptr->comment = comment;
		}
	}

	/*
	 * make sure we handle any quotes that may be in the comment
	 */

	query = xstrdup_printf("update `%s_%s` set "
			       "mod_time=UNIX_TIMESTAMP(), "
			       "time_end=%ld, state=%d",
			       kingbase_conn->cluster_name, job_table,
			       end_time, job_state);

	if (job_ptr->derived_ec != NO_VAL)
		xstrfmtcat(query, ", derived_ec=%u", job_ptr->derived_ec);

	if (job_ptr->tres_alloc_str)
		xstrfmtcat(query, ", tres_alloc='%s'", job_ptr->tres_alloc_str);

	if (job_ptr->comment)
		xstrfmtcat(query, ", derived_es='%s'", job_ptr->comment);

#ifdef __METASTACK_OPT_SACCT_COMMAND
	if (job_ptr->command)
		xstrfmtcat(query, ", command='%s'", job_ptr->command);
#endif
#ifdef __METASTACK_OPT_SACCT_OUTPUT
	if (job_ptr->details->std_out)
		xstrfmtcat(query, ", stdout='%s'", job_ptr->details->std_out);
	//	xstrfmtcat(query, ", stdout='%s'", job_ptr->stdout);
	if (job_ptr->details->std_err)
		xstrfmtcat(query, ", stderr='%s'", job_ptr->details->std_err);
	//	xstrfmtcat(query, ", stderr='%s'", job_ptr->stderr);
#endif

#ifdef __METASTACK_OPT_RESC_NODEDETAIL
	if (job_ptr->resource_node_detail)
		xstrfmtcat(query, ", resource_node_detail='%s'", job_ptr->resource_node_detail);
#endif

	if (job_ptr->admin_comment)
		xstrfmtcat(query, ", admin_comment='%s'",
			   job_ptr->admin_comment);

	if (job_ptr->system_comment)
		xstrfmtcat(query, ", system_comment='%s'",
			   job_ptr->system_comment);

	exit_code = job_ptr->exit_code;
	if (exit_code == 1) {
		/* This wasn't signaled, it was set by Slurm so don't
		 * treat it like a signal.
		 */
		exit_code = 256;
	}
	xstrfmtcat(query, ", exit_code=%d, ", exit_code);

	if (job_ptr->requid == (uid_t) -1)
		xstrfmtcat(query, "kill_requid=null ");
	else
		xstrfmtcat(query, "kill_requid=%u ", job_ptr->requid);

	xstrfmtcat(query, "where job_db_inx=%"PRIu64";", job_ptr->db_index);

	DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

	fetch_flag_t *fetch_flag = NULL;
	fetch_result_t *data_rt = NULL;
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);

	xfree(query);

	return rc;
}

extern int as_kingbase_step_start(kingbase_conn_t *kingbase_conn,
			       step_record_t *step_ptr)
{
	int tasks = 0, nodes = 0, task_dist = 0;
	int rc = SLURM_SUCCESS;
	char temp_bit[BUF_SIZE];
	char *node_list = NULL;
	char *node_inx = NULL;
	time_t start_time, submit_time;
	char *query = NULL;

	if (!step_ptr->job_ptr->db_index
	    && ((!step_ptr->job_ptr->details
		 || !step_ptr->job_ptr->details->submit_time)
		&& !step_ptr->job_ptr->resize_time)) {
		error("as_kingbase_step_start: "
		      "Not inputing this job, it has no submit time.");
		return SLURM_ERROR;
	}

	if (step_ptr->job_ptr->resize_time) {
		submit_time = start_time = step_ptr->job_ptr->resize_time;
		if (step_ptr->start_time > submit_time)
			start_time = step_ptr->start_time;
	} else {
		start_time = step_ptr->start_time;
		submit_time = step_ptr->job_ptr->details->submit_time;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;
	if (slurmdbd_conf) {
		if (step_ptr->job_ptr->details)
			tasks = step_ptr->job_ptr->details->num_tasks;
		else
			tasks = step_ptr->cpu_count;
		node_list = step_ptr->job_ptr->nodes;
		nodes = step_ptr->step_layout->node_cnt;
		task_dist = step_ptr->step_layout->task_dist;
		node_inx = step_ptr->network;
	} else if (step_ptr->step_id.step_id == SLURM_BATCH_SCRIPT) {
		if (step_ptr->step_node_bitmap) {
			node_inx = bit_fmt(temp_bit, sizeof(temp_bit),
					   step_ptr->step_node_bitmap);
		}
		/*
		 * We overload tres_per_node with the node name of where the
		 * script was running.
		 */
		node_list = step_ptr->tres_per_node;
		nodes = tasks = 1;
		if (!step_ptr->tres_alloc_str)
			xstrfmtcat(step_ptr->tres_alloc_str,
				   "%s%u=%u,%u=%u",
				   step_ptr->tres_alloc_str ? "," : "",
				   TRES_CPU, 1,
				   TRES_NODE, 1);
	} else {
		if (step_ptr->step_node_bitmap) {
			node_inx = bit_fmt(temp_bit, sizeof(temp_bit),
					   step_ptr->step_node_bitmap);
		}

		if (!step_ptr->step_layout
		    || !step_ptr->step_layout->task_cnt) {
			if (step_ptr->cpu_count)
				tasks = step_ptr->cpu_count;
			else {
				if ((tasks = slurmdb_find_tres_count_in_string(
					     step_ptr->tres_alloc_str,
					     TRES_CPU)) == INFINITE64) {
					if ((tasks =
					     slurmdb_find_tres_count_in_string(
						     step_ptr->job_ptr->
						     tres_alloc_str,
						     TRES_CPU)) == INFINITE64)
						tasks = step_ptr->job_ptr->
							total_nodes;
				}
			}

			nodes = step_ptr->job_ptr->total_nodes;
			node_list = step_ptr->job_ptr->nodes;
		} else {
			tasks = step_ptr->step_layout->task_cnt;
			nodes = step_ptr->step_layout->node_cnt;
			task_dist = step_ptr->step_layout->task_dist;
			node_list = step_ptr->step_layout->node_list;
		}
	}

	if (!step_ptr->job_ptr->db_index) {
		if (!(step_ptr->job_ptr->db_index =
		      _get_db_index(kingbase_conn,
				    submit_time,
				    step_ptr->job_ptr->job_id))) {
			/* If we get an error with this just fall
			 * through to avoid an infinite loop
			 */
			if (as_kingbase_job_start(kingbase_conn, step_ptr->job_ptr)
			    == SLURM_ERROR) {
				error("couldn't add job %u at step start",
				      step_ptr->job_ptr->job_id);
				return SLURM_SUCCESS;
			}
		}
	}

	/* we want to print a -1 for the requid so leave it a
	   %d */
	/* The stepid could be negative so use %d not %u */
	query = xstrdup_printf(
		"insert into `%s_%s` (job_db_inx, id_step, step_het_comp, "
		"time_start, step_name, state, tres_alloc, "
		"nodes_alloc, task_cnt, nodelist, node_inx, "
		"task_dist, req_cpufreq, req_cpufreq_min, req_cpufreq_gov",
		kingbase_conn->cluster_name, step_table);

	if (step_ptr->submit_line)
		xstrcat(query, ", submit_line");
	if (step_ptr->container)
		xstrcat(query, ", container");

	xstrfmtcat(query,
		   ") values (%"PRIu64", %d, %u, %d, '%s', %d, '%s', %d, %d, "
		   "'%s', '%s', %d, %u, %u, %u",
		   step_ptr->job_ptr->db_index,
		   step_ptr->step_id.step_id,
		   step_ptr->step_id.step_het_comp,
		   (int)start_time, step_ptr->name,
		   JOB_RUNNING, step_ptr->tres_alloc_str,
		   nodes, tasks, node_list, node_inx, task_dist,
		   step_ptr->cpu_freq_max, step_ptr->cpu_freq_min,
		   step_ptr->cpu_freq_gov);

	if (step_ptr->submit_line)
		xstrfmtcat(query, ", '%s'", step_ptr->submit_line);
	if (step_ptr->container)
		xstrfmtcat(query, ", '%s'", step_ptr->container);

	xstrfmtcat(query,
		   ") on duplicate key update "
		   "nodes_alloc=%d, task_cnt=%d, time_end=0, state=%d, "
		   "nodelist='%s', node_inx='%s', task_dist=%d, "
		   "req_cpufreq=%u, req_cpufreq_min=%u, req_cpufreq_gov=%u,"
		   "tres_alloc='%s'",
		   nodes, tasks, JOB_RUNNING,
		   node_list, node_inx, task_dist, step_ptr->cpu_freq_max,
		   step_ptr->cpu_freq_min, step_ptr->cpu_freq_gov,
		   step_ptr->tres_alloc_str);

	if (step_ptr->submit_line)
		xstrfmtcat(query, ", submit_line='%s'", step_ptr->submit_line);

	if (step_ptr->container)
		xstrfmtcat(query, ", container='%s'", step_ptr->container);

	DB_DEBUG(DB_STEP, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

	fetch_flag_t *fetch_flag = NULL;
	fetch_result_t *data_rt = NULL;
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);
	xfree(query);
	return rc;
}

extern int as_kingbase_step_complete(kingbase_conn_t *kingbase_conn,
				  step_record_t *step_ptr)
{
	time_t now;
	uint16_t comp_status;
	int tasks = 0;
	struct jobacctinfo *jobacct = (struct jobacctinfo *)step_ptr->jobacct;
	char *query = NULL;
	int rc = SLURM_SUCCESS;
	uint32_t exit_code = 0;
	time_t submit_time;

	if (!step_ptr->job_ptr->db_index
	    && ((!step_ptr->job_ptr->details
		 || !step_ptr->job_ptr->details->submit_time)
		&& !step_ptr->job_ptr->resize_time)) {
		error("as_kingbase_step_complete: "
		      "Not inputing this job, it has no submit time.");
		return SLURM_ERROR;
	}

	if (step_ptr->job_ptr->resize_time)
		submit_time = step_ptr->job_ptr->resize_time;
	else
		submit_time = step_ptr->job_ptr->details->submit_time;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (slurmdbd_conf) {
		now = step_ptr->job_ptr->end_time;
		if (step_ptr->job_ptr->details)
			tasks = step_ptr->job_ptr->details->num_tasks;
		else
			tasks = step_ptr->cpu_count;
	} else if (step_ptr->step_id.step_id == SLURM_BATCH_SCRIPT) {
		now = time(NULL);
		tasks = 1;
	} else {
		now = time(NULL);
		if (!step_ptr->step_layout
		    || !step_ptr->step_layout->task_cnt) {
			if (step_ptr->cpu_count)
				tasks = step_ptr->cpu_count;
			else {
				if ((tasks = slurmdb_find_tres_count_in_string(
					     step_ptr->tres_alloc_str,
					     TRES_CPU)) == INFINITE64) {
					if ((tasks =
					     slurmdb_find_tres_count_in_string(
						     step_ptr->job_ptr->
						     tres_alloc_str,
						     TRES_CPU)) == INFINITE64)
						tasks = step_ptr->job_ptr->
							total_nodes;
				}
			}
		} else
			tasks = step_ptr->step_layout->task_cnt;
	}

	exit_code = step_ptr->exit_code;
	comp_status = step_ptr->state & JOB_STATE_BASE;
	if (comp_status < JOB_COMPLETE) {
		if (exit_code == SIG_OOM) {
			comp_status = JOB_OOM;
		} else if (WIFSIGNALED(exit_code)) {
			comp_status = JOB_CANCELLED;
		} else if (exit_code)
			comp_status = JOB_FAILED;
		else {
			step_ptr->requid = -1;
			comp_status = JOB_COMPLETE;
		}
	}

	if (!step_ptr->job_ptr->db_index) {
		if (!(step_ptr->job_ptr->db_index =
		      _get_db_index(kingbase_conn,
				    submit_time,
				    step_ptr->job_ptr->job_id))) {
			/* If we get an error with this just fall
			 * through to avoid an infinite loop
			 */
			if (as_kingbase_job_start(kingbase_conn, step_ptr->job_ptr)
			    == SLURM_ERROR) {
				error("couldn't add job %u "
				      "at step completion",
				      step_ptr->job_ptr->job_id);
				return SLURM_SUCCESS;
			}
		}
	}

	/* The stepid could be negative so use %d not %u */
	query = xstrdup_printf(
		"update `%s_%s` set time_end=%d, state=%u, exit_code=%d, ",
		kingbase_conn->cluster_name, step_table, (int)now,
		comp_status,
		exit_code);

	if (step_ptr->requid == (uid_t) -1)
		xstrfmtcat(query, "kill_requid=null");
	else
		xstrfmtcat(query, "kill_requid=%u", step_ptr->requid);

	if (jobacct) {
		slurmdb_stats_t stats;

		memset(&stats, 0, sizeof(slurmdb_stats_t));

		/* figure out the ave of the totals sent */
		if (tasks > 0) {
			stats.tres_usage_in_ave =
				_average_tres_usage(jobacct->tres_ids,
						    jobacct->tres_usage_in_tot,
						    jobacct->tres_count,
						    tasks);
			stats.tres_usage_out_ave =
				_average_tres_usage(jobacct->tres_ids,
						    jobacct->tres_usage_out_tot,
						    jobacct->tres_count,
						    tasks);
		}

		/*
		 * We can't trust the assoc_mgr here as the tres may have
		 * changed, we have to go off what was sent us.  We can just use
		 * the _average_tres_usage to do this by dividing by 1.
		 */
		stats.tres_usage_in_max = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_in_max,
			jobacct->tres_count, 1);
		stats.tres_usage_in_max_nodeid = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_in_max_nodeid,
			jobacct->tres_count, 1);
		stats.tres_usage_in_max_taskid = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_in_max_taskid,
			jobacct->tres_count, 1);
		stats.tres_usage_in_min = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_in_min,
			jobacct->tres_count, 1);
		stats.tres_usage_in_min_nodeid = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_in_min_nodeid,
			jobacct->tres_count, 1);
		stats.tres_usage_in_min_taskid = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_in_min_taskid,
			jobacct->tres_count, 1);
		stats.tres_usage_in_tot = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_in_tot,
			jobacct->tres_count, 1);
		stats.tres_usage_out_max = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_out_max,
			jobacct->tres_count, 1);
		stats.tres_usage_out_max_nodeid = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_out_max_nodeid,
			jobacct->tres_count, 1);
		stats.tres_usage_out_max_taskid = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_out_max_taskid,
			jobacct->tres_count, 1);
		stats.tres_usage_out_min = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_out_min,
			jobacct->tres_count, 1);
		stats.tres_usage_out_min_nodeid = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_out_min_nodeid,
			jobacct->tres_count, 1);
		stats.tres_usage_out_min_taskid = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_out_min_taskid,
			jobacct->tres_count, 1);
		stats.tres_usage_out_tot = _average_tres_usage(
			jobacct->tres_ids,
			jobacct->tres_usage_out_tot,
			jobacct->tres_count, 1);

		xstrfmtcat(query,
			   ", user_sec=%"PRIu64", user_usec=%u, "
			   "sys_sec=%"PRIu64", sys_usec=%u, "
			   "act_cpufreq=%u, consumed_energy=%"PRIu64", "
			   "tres_usage_in_ave='%s', "
			   "tres_usage_out_ave='%s', "
			   "tres_usage_in_max='%s', "
			   "tres_usage_in_max_taskid='%s', "
			   "tres_usage_in_max_nodeid='%s', "
			   "tres_usage_in_min='%s', "
			   "tres_usage_in_min_taskid='%s', "
			   "tres_usage_in_min_nodeid='%s', "
			   "tres_usage_in_tot='%s', "
			   "tres_usage_out_max='%s', "
			   "tres_usage_out_max_taskid='%s', "
			   "tres_usage_out_max_nodeid='%s', "
			   "tres_usage_out_min='%s', "
			   "tres_usage_out_min_taskid='%s', "
			   "tres_usage_out_min_nodeid='%s', "
			   "tres_usage_out_tot='%s'",
			   /* user seconds */
			   jobacct->user_cpu_sec,
			   /* user microseconds */
			   jobacct->user_cpu_usec,
			   /* system seconds */
			   jobacct->sys_cpu_sec,
			   /* system microsecs */
			   jobacct->sys_cpu_usec,
			   jobacct->act_cpufreq,
			   jobacct->energy.consumed_energy,
			   stats.tres_usage_in_ave,
			   stats.tres_usage_out_ave,
			   stats.tres_usage_in_max,
			   stats.tres_usage_in_max_taskid,
			   stats.tres_usage_in_max_nodeid,
			   stats.tres_usage_in_min,
			   stats.tres_usage_in_min_taskid,
			   stats.tres_usage_in_min_nodeid,
			   stats.tres_usage_in_tot,
			   stats.tres_usage_out_max,
			   stats.tres_usage_out_max_taskid,
			   stats.tres_usage_out_max_nodeid,
			   stats.tres_usage_out_min,
			   stats.tres_usage_out_min_taskid,
			   stats.tres_usage_out_min_nodeid,
			   stats.tres_usage_out_tot);

		slurmdb_free_slurmdb_stats_members(&stats);
	}

	/* id_step has to be %d here to handle the negative values for the batch
	   and extern steps.  Don't change it to a %u.
	*/
	xstrfmtcat(query,
		   " where job_db_inx=%"PRIu64" and id_step=%d and step_het_comp=%u",
		   step_ptr->job_ptr->db_index, step_ptr->step_id.step_id,
		   step_ptr->step_id.step_het_comp);
	DB_DEBUG(DB_STEP, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

	fetch_flag_t *fetch_flag = NULL;
	fetch_result_t *data_rt = NULL;
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);
	xfree(query);

	/* set the energy for the entire job. */
	if (step_ptr->job_ptr->tres_alloc_str) {
		query = xstrdup_printf(
			"update `%s_%s` set tres_alloc='%s' where "
			"job_db_inx=%"PRIu64,
			kingbase_conn->cluster_name, job_table,
			step_ptr->job_ptr->tres_alloc_str,
			step_ptr->job_ptr->db_index);
		DB_DEBUG(DB_STEP, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

		fetch_flag_t *fetch_flag = NULL;
		fetch_result_t *data_rt = NULL;
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		free_res_data(data_rt, fetch_flag);
		xfree(query);
	}

	return rc;
}

extern int as_kingbase_suspend(kingbase_conn_t *kingbase_conn, uint64_t old_db_inx,
			    job_record_t *job_ptr)
{
	char *query = NULL;
	int rc = SLURM_SUCCESS;
	time_t submit_time;
	uint64_t job_db_inx;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (job_ptr->resize_time)
		submit_time = job_ptr->resize_time;
	else
		submit_time = job_ptr->details->submit_time;

	if (!job_ptr->db_index) {
		if (!(job_ptr->db_index =
		      _get_db_index(kingbase_conn,
				    submit_time,
				    job_ptr->job_id))) {
			/* If we get an error with this just fall
			 * through to avoid an infinite loop
			 */
			if (as_kingbase_job_start(
				    kingbase_conn, job_ptr) == SLURM_ERROR) {
				error("couldn't suspend job %u",
				      job_ptr->job_id);
				return SLURM_SUCCESS;
			}
		}
	}

	if (IS_JOB_RESIZING(job_ptr)) {
		if (!old_db_inx) {
			error("No old db inx given for job %u cluster %s, "
			      "can't update suspend table.",
			      job_ptr->job_id, kingbase_conn->cluster_name);
			return SLURM_ERROR;
		}
		job_db_inx = old_db_inx;
		xstrfmtcat(query,
			   "update `%s_%s` set time_end=%d where "
			   "job_db_inx=%"PRIu64" && time_end=0;",
			   kingbase_conn->cluster_name, suspend_table,
			   (int)job_ptr->suspend_time, job_db_inx);

	} else
		job_db_inx = job_ptr->db_index;

	/* use job_db_inx for this one since we want to update the
	   supend time of the job before it was resized.
	*/
	xstrfmtcat(query,
		   "update `%s_%s` set time_suspended=%d-time_suspended, "
		   "state=%d where job_db_inx=%"PRIu64";",
		   kingbase_conn->cluster_name, job_table,
		   (int)job_ptr->suspend_time,
		   job_ptr->job_state & JOB_STATE_BASE,
		   job_db_inx);
	if (IS_JOB_SUSPENDED(job_ptr))
		xstrfmtcat(query,
			   "insert into `%s_%s` (job_db_inx, id_assoc, "
			   "time_start, time_end) "
			   "values (%"PRIu64", %u, %d, 0);",
			   kingbase_conn->cluster_name, suspend_table,
			   job_ptr->db_index, job_ptr->assoc_id,
			   (int)job_ptr->suspend_time);
	else
		xstrfmtcat(query,
			   "update `%s_%s` set time_end=%d where "
			   "job_db_inx=%"PRIu64" && time_end=0;",
			   kingbase_conn->cluster_name, suspend_table,
			   (int)job_ptr->suspend_time, job_ptr->db_index);
	DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

	fetch_flag_t *fetch_flag = NULL;
	fetch_result_t *data_rt = NULL;
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);
	xfree(query);

	if (rc != SLURM_ERROR) {
		xstrfmtcat(query,
			   "update `%s_%s` set "
			   "time_suspended=%u-time_suspended, "
			   "state=%d where job_db_inx=%"PRIu64" and time_end=0",
			   kingbase_conn->cluster_name, step_table,
			   (int)job_ptr->suspend_time,
			   job_ptr->job_state, job_ptr->db_index);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	   

		fetch_flag_t *fetch_flag = NULL;
		fetch_result_t *data_rt = NULL;
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		free_res_data(data_rt, fetch_flag);
		xfree(query);
	}

	return rc;
}

extern int as_kingbase_flush_jobs_on_cluster(
	kingbase_conn_t *kingbase_conn, time_t event_time)
{
	int rc = SLURM_SUCCESS;
	/* put end times for a clean start */
	KCIResult *result = NULL;
	char *query = NULL;
	char *id_char = NULL;
	char *suspended_char = NULL;
	size_t count;

again:
	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	/* First we need to get the job_db_inx's and states so we can clean up
	 * the suspend table and the step table
	 */
	query = xstrdup_printf(
		"select distinct t1.job_db_inx, t1.state from `%s_%s` "
		"as t1 where t1.time_end=0 LIMIT %u;",
		kingbase_conn->cluster_name, job_table, MAX_FLUSH_JOBS);
	DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		return SLURM_ERROR;
	}


    count = KCIResultGetRowCount(result);
    int i =0;
    for(i = 0 ; i < count ; i++){
        int state = slurm_atoul(KCIResultGetColumnValue(result,i,1));
		if (state == JOB_SUSPENDED) {
			if (suspended_char)
				xstrfmtcat(suspended_char,
					   ", %s", KCIResultGetColumnValue(result,i,0));
			else
				xstrfmtcat(suspended_char, "job_db_inx in (%s",
					   KCIResultGetColumnValue(result,i,0));
		}

		if (id_char)
			xstrfmtcat(id_char, ", %s", KCIResultGetColumnValue(result,i,0));
		else
			xstrfmtcat(id_char, "job_db_inx in (%s", KCIResultGetColumnValue(result,i,0));
    }
	KCIResultDealloc(result);

	if (suspended_char) {
		xstrfmtcat(suspended_char, ")");
		xstrfmtcat(query,
			   "update `%s_%s` set "
			   "time_suspended=%ld-time_suspended "
			   "where %s;",
			   kingbase_conn->cluster_name, job_table,
			   event_time, suspended_char);
		xstrfmtcat(query,
			   "update `%s_%s` set "
			   "time_suspended=%ld-time_suspended "
			   "where %s;",
			   kingbase_conn->cluster_name, step_table,
			   event_time, suspended_char);
		xstrfmtcat(query,
			   "update `%s_%s` set time_end=%ld where (%s) "
			   "&& time_end=0;",
			   kingbase_conn->cluster_name, suspend_table,
			   event_time, suspended_char);
		xfree(suspended_char);
	}
	if (id_char) {
		xstrfmtcat(id_char, ")");
		xstrfmtcat(query,
			   "update `%s_%s` set state=%d, "
			   "time_end=%ld where %s;",
			   kingbase_conn->cluster_name, job_table,
			   JOB_CANCELLED, event_time, id_char);
		xstrfmtcat(query,
			   "update `%s_%s` set state=%d, "
			   "time_end=%ld where %s;",
			   kingbase_conn->cluster_name, step_table,
			   JOB_CANCELLED, event_time, id_char);
		xfree(id_char);
	}

	if (query) {
		DB_DEBUG(DB_JOB, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

		fetch_flag_t *fetch_flag = NULL;
		fetch_result_t *data_rt = NULL;
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		free_res_data(data_rt, fetch_flag);
		xfree(query);
	}

	/* all rows were returned, there may be more to check */
	if (!rc && (count >= MAX_FLUSH_JOBS)) {
		DB_DEBUG(DB_JOB, kingbase_conn->conn,
			 "%s: possible missed jobs. Running query again.",
			 __func__);
		goto again;
	}

	return rc;
}