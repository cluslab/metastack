/*****************************************************************************\
 *  as_kingbase_fix_runaway_jobs.c - functions dealing with runaway jobs.
 *****************************************************************************
 *  Copyright (C) 2016 SchedMD LLC.
 *  Written by Nathan Yee <nyee32@schedmd.com>
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

#include "as_kingbase_fix_runaway_jobs.h"
#include "src/common/list.h"
#include "src/common/slurmdb_defs.h"

static int _first_job_roll_up(kingbase_conn_t *kingbase_conn, time_t first_start)
{
	int rc = SLURM_SUCCESS;
	char *query;
	struct tm start_tm;
	time_t month_start;

	/* set up the month period */
	if (!localtime_r(&first_start, &start_tm)) {
		error("mktime for start failed for rollup\n");
		return SLURM_ERROR;
	}

	// Go to the last day of the previous month for rollup start
	start_tm.tm_sec = 0;
	start_tm.tm_min = 0;
	start_tm.tm_hour = 0;
	start_tm.tm_mday = 0;
	month_start = slurm_mktime(&start_tm);

	debug("Need to reroll usage from %s in cluster %s because of runaway job(s)",
	      slurm_ctime2(&month_start), kingbase_conn->cluster_name);

	query = xstrdup_printf("UPDATE `%s_%s` SET hourly_rollup = %ld, "
			       "daily_rollup = %ld, monthly_rollup = %ld;",
			       kingbase_conn->cluster_name, last_ran_table,
			       month_start, month_start, month_start);

	/*
	 * Delete allocated time from the assoc and wckey usage tables.
	 * If the only usage during those times was runaway jobs, then rollup
	 * won't clear that usage, so we have to clear it here. Rollup will
	 * re-create the correct rows in these tables.
	 */
	xstrfmtcat(query, "DELETE FROM `%s_%s` where time_start >= %ld;",
		   kingbase_conn->cluster_name, assoc_hour_table,
		   month_start);
	xstrfmtcat(query, "DELETE FROM `%s_%s` where time_start >= %ld;",
		   kingbase_conn->cluster_name, assoc_day_table,
		   month_start);
	xstrfmtcat(query, "DELETE FROM `%s_%s` where time_start >= %ld;",
		   kingbase_conn->cluster_name, assoc_month_table,
		   month_start);
	xstrfmtcat(query, "DELETE FROM `%s_%s` where time_start >= %ld;",
		   kingbase_conn->cluster_name, wckey_hour_table,
		   month_start);
	xstrfmtcat(query, "DELETE FROM `%s_%s` where time_start >= %ld;",
		   kingbase_conn->cluster_name, wckey_day_table,
		   month_start);
	xstrfmtcat(query, "DELETE FROM `%s_%s` where time_start >= %ld;",
		   kingbase_conn->cluster_name, wckey_month_table,
		   month_start);

	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

	fetch_flag_t *fetch_flag = NULL;
	fetch_result_t *data_rt = NULL;
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);	

	if (rc == SLURM_ERROR) {
		error("%s Failed to rollup at the end of previous month",
		      __func__);
	}

	xfree(query);
	return rc;
}

extern int as_kingbase_fix_runaway_jobs(kingbase_conn_t *kingbase_conn, uint32_t uid,
				     List runaway_jobs)
{
	char *query = NULL, *job_ids = NULL;
	slurmdb_job_rec_t *job = NULL;
	ListIterator iter = NULL;
	int rc = SLURM_SUCCESS;
	slurmdb_job_rec_t *first_job;
	char *temp_cluster_name = kingbase_conn->cluster_name;

	if (!runaway_jobs) {
		error("%s: No List of runaway jobs to fix given.",
		      __func__);
		rc = SLURM_ERROR;
		goto bail;
	}

	list_sort(runaway_jobs, slurmdb_job_sort_by_submit_time);

	if (!(first_job = list_peek(runaway_jobs))) {
		error("%s: List of runaway jobs to fix is unexpectedly empty",
		      __func__);
		rc = SLURM_ERROR;
		goto bail;
	}

	if (!first_job->submit) {
		error("Runaway jobs all have time_submit=0, something is wrong! Aborting fix runaway jobs");
		rc = SLURM_ERROR;
		goto bail;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS) {
		rc = ESLURM_DB_CONNECTION;
		goto bail;
	}

	/*
	 * Temporarily use kingbase_conn->cluster_name for potentially non local
	 * cluster name, change back before return
	 */
	kingbase_conn->cluster_name = first_job->cluster;

	/*
	 * Double check if we are at least an operator, this check should had
	 * already happened in the slurmdbd.
	 */
	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		rc = ESLURM_ACCESS_DENIED;
		goto bail;
	}

	iter = list_iterator_create(runaway_jobs);
	while ((job = list_next(iter))) {
		/*
		 * Currently you can only fix one cluster at a time, so we need
		 * to verify we don't have multiple cluster names.
		 */
		if (xstrcmp(job->cluster, first_job->cluster)) {
			error("%s: You can only fix runaway jobs on one cluster at a time.",
			      __func__);
			rc = SLURM_ERROR;
			goto bail;
		}

		xstrfmtcat(job_ids, "%s%d", ((job_ids) ? "," : ""), job->jobid);
	}
	list_iterator_destroy(iter);

	debug("Fixing runaway jobs: %s", job_ids);

	query = xstrdup_printf("UPDATE `%s_%s` SET time_end="
			       "GREATEST(time_start, time_eligible, time_submit), "
			       "state=%d WHERE time_end=0 and id_job IN (%s);",
			       kingbase_conn->cluster_name, job_table,
			       JOB_COMPLETE, job_ids);

	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

	fetch_flag_t *fetch_flag = NULL;
	fetch_result_t *data_rt = NULL;
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);

	xfree(query);

	if (rc == SLURM_ERROR) {
		error("Failed to fix runaway jobs: update query failed");
		goto bail;
	}

	/* Set rollup to the last day of the previous month of the first
	 * runaway job */
	rc = _first_job_roll_up(kingbase_conn, first_job->submit);
	if (rc != SLURM_SUCCESS)
		error("Failed to fix runaway jobs");

bail:
	xfree(job_ids);
	kingbase_conn->cluster_name = temp_cluster_name;
	return rc;
}
