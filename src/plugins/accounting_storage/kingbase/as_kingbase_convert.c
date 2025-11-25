/*****************************************************************************\
 *  as_kingbase_convert.c - functions dealing with converting from tables in
 *                    slurm <= 17.02.
 *****************************************************************************
 *  Copyright (C) 2015 SchedMD LLC.
 *  Written by Danny Auble <da@schedmd.com>
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

#include "as_kingbase_convert.h"
#include "as_kingbase_tres.h"
#include "src/common/slurm_jobacct_gather.h"

/*
 * Any time you have to add to an existing convert update this number.
 * NOTE: 9 was the first version of 20.11.
 * NOTE: 10 was the first version of 21.08.
 * NOTE: 11 was the first version of 22.05.
 * NOTE: 12 was the second version of 22.05.
 */
#define CONVERT_VERSION 12

#define JOB_CONVERT_LIMIT_CNT 1000

typedef enum {
	MOVE_ENV,
	MOVE_BATCH
} move_large_type_t;

typedef struct {
	uint64_t count;
	uint32_t id;
} local_tres_t;

static uint32_t db_curr_ver = NO_VAL;

static int _convert_step_table_post(
	kingbase_conn_t *kingbase_conn, char *cluster_name)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL;

	if (db_curr_ver < 9) {
		/*
		 * Change the names pack_job_id and pack_job_offset to be het_*
		 */
		query = xstrdup_printf(
			"update `%s_%s` set id_step = %d where id_step = -2;"
			"update `%s_%s` set id_step = %d where id_step = -1;",
			cluster_name, step_table, SLURM_BATCH_SCRIPT,
			cluster_name, step_table, SLURM_EXTERN_CONT);
	}

	if (query) {
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
			error("%s: Can't convert %s_%s info: %m",
			      __func__, cluster_name, step_table);
		}
	}

	return rc;
}

static int _rename_usage_columns(kingbase_conn_t *kingbase_conn, char *table)
{
	char *query = NULL;
	int rc = SLURM_SUCCESS;


	/*
	 * Change the names pack_job_id and pack_job_offset to be het_*
	 */
	query = xstrdup_printf(
		"alter table %s change resv_secs plan_secs bigint "
		"default 0 not null;",
		table);

	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	if ((rc = as_kingbase_convert_alter_query(kingbase_conn, query)) !=
	    SLURM_SUCCESS)
		error("Can't update %s %m", table);
	xfree(query);

	return rc;
}

static int _convert_usage_table_pre(kingbase_conn_t *kingbase_conn,
				    char *cluster_name)
{
	int rc = SLURM_SUCCESS;

	if (db_curr_ver < 10) {
		char table[200];

		snprintf(table, sizeof(table), "`%s_%s`",
			 cluster_name, cluster_day_table);
		if ((rc = _rename_usage_columns(kingbase_conn, table))
		    != SLURM_SUCCESS)
			return rc;

		snprintf(table, sizeof(table), "`%s_%s`",
			 cluster_name, cluster_hour_table);
		if ((rc = _rename_usage_columns(kingbase_conn, table))
		    != SLURM_SUCCESS)
			return rc;

		snprintf(table, sizeof(table), "`%s_%s`",
			 cluster_name, cluster_month_table);
		if ((rc = _rename_usage_columns(kingbase_conn, table))
		    != SLURM_SUCCESS)
			return rc;
	}

	return rc;
}

static int _insert_into_hash_table(kingbase_conn_t *kingbase_conn, char *cluster_name,
				   move_large_type_t type)
{
	char *query, *hash_inx_col;
	char *hash_col = NULL, *type_col = NULL, *type_table = NULL;
	int rc;

	switch (type) {
	case MOVE_ENV:
		hash_col = "env_hash";
		hash_inx_col = "env_hash_inx";
		type_col = "env_vars";
		type_table = job_env_table;
		break;
	case MOVE_BATCH:
		hash_col = "script_hash";
		hash_inx_col = "script_hash_inx";
		type_col = "batch_script";
		type_table = job_script_table;
		break;
	default:
		return SLURM_ERROR;
		break;
	}

	info("Starting insert from job_table into %s", type_table);
	/*
	 * Do the transfer inside kingbase.  This results in a much quicker
	 * transfer instead of doing a select then an insert after the fact.
	 */
	query = xstrdup_printf(
		"insert into `%s_%s` (%s, %s) "
		"select distinct %s, %s from `%s_%s` "
		"where %s is not NULL and "
		"(id_array_job=id_job or !id_array_job) "
		"on duplicate key update last_used=UNIX_TIMESTAMP();",
		cluster_name, type_table,
		hash_col, type_col,
		hash_col, type_col,
		cluster_name, job_table,
		type_col);
	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	rc = kingbase_db_query(kingbase_conn, query);
	xfree(query);

	if (rc != SLURM_SUCCESS)
		return rc;

	query = xstrdup_printf(
		"update `%s_%s` as jobs inner join `%s_%s` as hash "
		"on jobs.%s = hash.%s set jobs.%s = hash.hash_inx;",
		cluster_name, job_table,
		cluster_name, type_table,
		hash_col, hash_col,
		hash_inx_col);
	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	rc = kingbase_db_query(kingbase_conn, query);
	xfree(query);

	info("Done");
	return rc;
}

static int _convert_job_table_pre(kingbase_conn_t *kingbase_conn, char *cluster_name)
{
	int rc = SLURM_SUCCESS;
	storage_field_t job_table_fields_21_08[] = {
		{ "job_db_inx", "bigserial not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "account", "tinytext" },
		{ "admin_comment", "text" },
		{ "array_task_str", "text" },
		{ "array_max_tasks", "bigint default 0 not null" },
		{ "array_task_pending", "bigint default 0 not null" },
		{ "batch_script", "longtext" },
		{ "constraints", "text default ''" },
		{ "container", "text" },
		{ "cpus_req", "bigint not null" },
		{ "derived_ec", "bigint default 0 not null" },
		{ "derived_es", "text" },
		{ "env_vars", "longtext" },
		{ "env_hash", "text" },
		{ "env_hash_inx", "bigint default 0 not null" },
		{ "exit_code", "bigint default 0 not null" },
		{ "flags", "bigint default 0 not null" },
		{ "job_name", "tinytext not null" },
		{ "id_assoc", "bigint not null" },
		{ "id_array_job", "bigint default 0 not null" },
		{ "id_array_task", "bigint default 0xfffffffe not null" },
		{ "id_block", "tinytext" },
		{ "id_job", "bigint not null" },
		{ "id_qos", "bigint default 0 not null" },
		{ "id_resv", "bigint not null" },
		{ "id_wckey", "bigint not null" },
		{ "id_user", "bigint not null" },
		{ "id_group", "bigint not null" },
		{ "het_job_id", "bigint not null" },
		{ "het_job_offset", "bigint not null" },
		{ "kill_requid", "int default null" },
		{ "state_reason_prev", "bigint not null" },
		{ "mcs_label", "tinytext default ''" },
		{ "mem_req", "bigint default 0 not null" },
		{ "nodelist", "text" },
		{ "nodes_alloc", "bigint not null" },
		{ "node_inx", "text" },
		{ "partition", "tinytext not null" },
		{ "priority", "bigint not null" },
		{ "script_hash", "text" },
		{ "script_hash_inx", "bigint default 0 not null" },
		{ "state", "bigint not null" },
		{ "timelimit", "bigint default 0 not null" },
		{ "time_submit", "bigint default 0 not null" },
		{ "time_eligible", "bigint default 0 not null" },
		{ "time_start", "bigint default 0 not null" },
		{ "time_end", "bigint default 0 not null" },
		{ "time_suspended", "bigint default 0 not null" },
		{ "gres_used", "text not null default ''" },
		{ "wckey", "tinytext not null default ''" },
		{ "work_dir", "text not null default ''" },
		{ "submit_line", "text" },
		{ "system_comment", "text" },
		{ "track_steps", "tinyint not null" },
		{ "tres_alloc", "text not null default ''" },
		{ "tres_req", "text not null default ''" },
		{ NULL, NULL}
	};

	storage_field_t job_env_table_fields[] = {
		{ "hash_inx", "bigserial not null" },
		{ "last_used", "timestamp DEFAULT CURRENT_TIMESTAMP not null" },
		{ "env_hash", "text not null" },
		{ "env_vars", "longtext" },
		{ NULL, NULL}
	};

	storage_field_t job_script_table_fields[] = {
		{ "hash_inx", "bigserial not null" },
		{ "last_used", "timestamp DEFAULT CURRENT_TIMESTAMP not null" },
		{ "script_hash", "text not null" },
		{ "batch_script", "longtext" },
		{ NULL, NULL}
	};

	if (db_curr_ver == 10) {
		/* This only needs to happen for 21.08 databases */
		char table_name[200];
		char *query;
		char *end = NULL;

		snprintf(table_name, sizeof(table_name), "`%s_%s`",
			 cluster_name, job_table);
		xstrfmtcat(end, ", primary key (job_db_inx));"
						"create unique index job_submit_%s_%s on %s_%s (id_job, time_submit); "
						"create index old_tuple_%s_%s on %s_%s (id_job, id_assoc, time_submit); "
						"create index rollup_%s_%s on %s_%s (time_eligible, time_end); "
						"create index rollup2_%s_%s on %s_%s (time_end, time_eligible); "
						"create index nodes_alloc_%s_%s on %s_%s (nodes_alloc); "
						"create index wckey_%s_%s on %s_%s (id_wckey); "
						"create index qos_%s_%s on %s_%s (id_qos); "
						"create index association_%s_%s on %s_%s (id_assoc); "
						"create index array_job_%s_%s on %s_%s (id_array_job); "
						"create index het_job_%s_%s on %s_%s (het_job_id); "
						"create index reserv_%s_%s on %s_%s (id_resv); "
						"create index sacct_def_%s_%s on %s_%s (id_user, time_start, time_end); "
						"create index sacct_def2_%s_%s on %s_%s (id_user, time_end, time_eligible); "
						"create index env_hash_inx_%s_%s on %s_%s (env_hash_inx); "
						"create index script_hash_inx_%s_%s on %s_%s (script_hash_inx); "
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						, cluster_name, job_table, cluster_name, job_table
						);
		if (kingbase_db_create_table(
			    kingbase_conn, table_name, job_table_fields_21_08,
			    end)
		    == SLURM_ERROR){
				xfree(end); 
				return SLURM_ERROR;
			}
		xfree(end); 	


		snprintf(table_name, sizeof(table_name), "`%s_%s`",
			 cluster_name, job_env_table);
		xstrfmtcat(end, ", primary key (hash_inx));"
						"create unique index env_hash_inx_%s_%s on %s_%s (env_hash); "
						, cluster_name, job_env_table, cluster_name, job_env_table);
		if (kingbase_db_create_table(kingbase_conn, table_name,
					  job_env_table_fields, end)
		    == SLURM_ERROR){
				xfree(end); 
				return SLURM_ERROR;
			}
		xfree(end); 	

		snprintf(table_name, sizeof(table_name), "`%s_%s`",
			 cluster_name, job_script_table);
		xstrfmtcat(end, ", primary key (hash_inx));"
						"create unique index script_hash_inx_%s_%s on %s_%s (script_hash); "
						, cluster_name, job_script_table, cluster_name, job_script_table);
		if (kingbase_db_create_table(kingbase_conn, table_name,
					  job_script_table_fields, end)
		    == SLURM_ERROR){
				xfree(end); 
				return SLURM_ERROR;
			}
		xfree(end); 	

		/*
		 * Using SHA256 here inside kingbase as it will make the conversion
		 * dramatically faster.  This will cause these tables to
		 * potentially be 2x in size as all future things will be K12,
		 * but in theory that shouldn't be as big a deal as this
		 * conversion should save much room more than that.
		 */
		info("Creating env and batch script hashes in the job_table");
		query = xstrdup_printf(
			"update `%s_%s` set "
			"env_hash = concat('%d:', SHA256(env_vars)), "
			"script_hash = concat('%d:', SHA256(batch_script));",
			cluster_name, job_table,
			HASH_PLUGIN_SHA256, HASH_PLUGIN_SHA256);
		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
		rc = kingbase_db_query(kingbase_conn, query);
		xfree(query);
		info("Done");

		if ((rc = _insert_into_hash_table(
			     kingbase_conn, cluster_name,
			     MOVE_ENV)) != SLURM_SUCCESS)
			return rc;

		if ((rc = _insert_into_hash_table(
			     kingbase_conn, cluster_name,
			     MOVE_BATCH)) != SLURM_SUCCESS)
			return rc;
	}

	if (db_curr_ver < 12) {
		char *table_name;
		char *query;

		table_name = xstrdup_printf("`%s_%s`",
					    cluster_name, job_table);
		/* Update kill_requid to NULL instead of -1 for not set */
		query = xstrdup_printf("alter table %s modify kill_requid "
				       "int default null;", table_name);
		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
		if ((rc = kingbase_db_query(kingbase_conn, query)) != SLURM_SUCCESS) {
			xfree(query);
			return rc;
		}
		xfree(query);
		query = xstrdup_printf("update %s set kill_requid=null where "
				       "kill_requid=-1;", table_name);
		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
		rc = kingbase_db_query(kingbase_conn, query);
		xfree(query);
		xfree(table_name);
	}

	return rc;
}

static int _convert_step_table_pre(kingbase_conn_t *kingbase_conn, char *cluster_name)
{
	int rc = SLURM_SUCCESS;

	if (db_curr_ver < 12) {
		char *table_name;
		char *query;

		table_name = xstrdup_printf("`%s_%s`",
					    cluster_name, step_table);
		/* temporarily "not null" from req_uid */
		query = xstrdup_printf("alter table %s modify kill_requid "
				       "int default null;", table_name);
		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
		if ((rc = kingbase_db_query(kingbase_conn, query)) != SLURM_SUCCESS) {
			xfree(query);
			return rc;
		}
		xfree(query);
		/* update kill_requid = -1 to NULL */
		query = xstrdup_printf("update %s set kill_requid=null where "
				       "kill_requid=-1;", table_name);
		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
		rc = kingbase_db_query(kingbase_conn, query);
		xfree(query);
		xfree(table_name);
	}

	return rc;
}
static int _set_db_curr_ver(kingbase_conn_t *kingbase_conn)
{
	char *query;
	KCIResult *result = NULL;
	int rc = SLURM_SUCCESS;

	if (db_curr_ver != NO_VAL)
		return SLURM_SUCCESS;

	query = xstrdup_printf("select version from %s", convert_version_table);
	debug4("%d(%s:%d) query\n%s", kingbase_conn->conn,
	       THIS_FILE, __LINE__, query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	   
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return SLURM_ERROR;
	}
    xfree(query);
	if (KCIResultGetRowCount(result) != 0) {
		db_curr_ver = slurm_atoul(KCIResultGetColumnValue(result, 0, 0));
	} else {
		int tmp_ver = 0;
		/* no valid clusters, just return */
		if (as_kingbase_total_cluster_list &&
		    !list_count(as_kingbase_total_cluster_list))
			tmp_ver = CONVERT_VERSION;

		query = xstrdup_printf("insert into %s (version) values (%d);",
				       convert_version_table, tmp_ver);
		debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		fetch_flag_t *fetch_flag = NULL;
		fetch_result_t *data_rt = NULL;
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		free_res_data(data_rt, fetch_flag);		
		xfree(query);
		if (rc == SLURM_ERROR) {
			KCIResultDealloc(result);
			return rc;
		}	
		db_curr_ver = tmp_ver;
	}
    KCIResultDealloc(result);

	return rc;
}

extern int as_kingbase_convert_tables_pre_create(kingbase_conn_t *kingbase_conn)
{
	int rc = SLURM_SUCCESS;
	ListIterator itr;
	char *cluster_name;

	xassert(as_kingbase_total_cluster_list);

	if ((rc = _set_db_curr_ver(kingbase_conn)) != SLURM_SUCCESS)
		return rc;

	if (db_curr_ver == CONVERT_VERSION) {
		debug4("%s: No conversion needed, Horray!", __func__);
		return SLURM_SUCCESS;
	} else if (backup_dbd) {
		/*
		 * We do not want to create/check the database if we are the
		 * backup (see Bug 3827). This is only handled on the primary.
		 *
		 * To avoid situations where someone might upgrade the database
		 * through the backup we want to fatal so they know what
		 * happened instead of potentially starting with the older
		 * database.
		 */
		fatal("Backup DBD can not convert database, please start the primary DBD before starting the backup.");
		return SLURM_ERROR;
	}

	/* make it up to date */
	itr = list_iterator_create(as_kingbase_total_cluster_list);
	while ((cluster_name = list_next(itr))) {
		/*
		 * When calling alters on tables here please remember to use
		 * as_kingbase_convert_alter_query instead of kingbase_db_query to be
		 * able to detect a previous failed conversion.
		 */
		info("pre-converting usage table for %s", cluster_name);
		if ((rc = _convert_usage_table_pre(kingbase_conn, cluster_name)
		     != SLURM_SUCCESS))
			break;
		info("pre-converting job table for %s", cluster_name);
		if ((rc = _convert_job_table_pre(kingbase_conn, cluster_name)
		     != SLURM_SUCCESS))
			break;
		info("pre-converting step table for %s", cluster_name);
		if ((rc = _convert_step_table_pre(kingbase_conn, cluster_name)
		     != SLURM_SUCCESS))
			break;
	}
	list_iterator_destroy(itr);

	return rc;
}

extern int as_kingbase_convert_tables_post_create(kingbase_conn_t *kingbase_conn)
{
	int rc = SLURM_SUCCESS;
	ListIterator itr;
	char *cluster_name;

	xassert(as_kingbase_total_cluster_list);

	if ((rc = _set_db_curr_ver(kingbase_conn)) != SLURM_SUCCESS)
		return rc;

	if (db_curr_ver == CONVERT_VERSION) {
		debug4("%s: No conversion needed, Horray!", __func__);
		return SLURM_SUCCESS;
	} else if (backup_dbd) {
		/*
		 * We do not want to create/check the database if we are the
		 * backup (see Bug 3827). This is only handled on the primary.
		 *
		 * To avoid situations where someone might upgrade the database
		 * through the backup we want to fatal so they know what
		 * happened instead of potentially starting with the older
		 * database.
		 */
		fatal("Backup DBD can not convert database, please start the primary DBD before starting the backup.");
		return SLURM_ERROR;
	}

	/* make it up to date */
	itr = list_iterator_create(as_kingbase_total_cluster_list);
	while ((cluster_name = list_next(itr))) {
		info("post-converting step table for %s", cluster_name);
		if ((rc = _convert_step_table_post(kingbase_conn, cluster_name)
		     != SLURM_SUCCESS))
			break;
	}
	list_iterator_destroy(itr);

	return rc;
}

extern int as_kingbase_convert_non_cluster_tables_post_create(
	kingbase_conn_t *kingbase_conn)
{
	int rc = SLURM_SUCCESS;

	if ((rc = _set_db_curr_ver(kingbase_conn)) != SLURM_SUCCESS)
		return rc;

	if (db_curr_ver == CONVERT_VERSION) {
		debug4("%s: No conversion needed, Horray!", __func__);
		return SLURM_SUCCESS;
	}

	if (rc != SLURM_ERROR) {
		char *query = xstrdup_printf(
			"update %s set version=%d, mod_time=UNIX_TIMESTAMP()",
			convert_version_table, CONVERT_VERSION);

		info("Conversion done: success!");

		debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
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

/*
 * Only use this when running "ALTER TABLE" during an upgrade.  This is to get
 * around that kingbase cannot rollback an "ALTER TABLE", but its possible that the
 * rest of the upgrade transaction was aborted.
 *
 * We may not always use this function, but don't delete it just in case we
 * need to alter tables in the future.
 */
extern int as_kingbase_convert_alter_query(kingbase_conn_t *kingbase_conn, char *query)
{
	int rc = SLURM_SUCCESS;

	rc = kingbase_db_query(kingbase_conn, query);
	if ((rc != SLURM_SUCCESS) && (strstr(KCIConnectionGetLastError(kingbase_conn->db_conn),"不存在")
			|| strstr(KCIConnectionGetLastError(kingbase_conn->db_conn),"not exist"))) {
		errno = 0;
		rc = SLURM_SUCCESS;
		info("The database appears to have been altered by a previous upgrade attempt, continuing with upgrade.");
	}

	return rc;
}
