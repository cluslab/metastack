/*****************************************************************************\
 *  accounting_storage_kingbase.c - accounting interface to as_kingbase.
 *****************************************************************************
 *  Copyright (C) 2004-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Copyright (C) 2011-2018 SchedMD LLC.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@schedmd.com, da@llnl.gov>
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
 *****************************************************************************
 * Notes on as_kingbase configuration
 *	Assumes  is installed as user root
 *	Assumes SlurmUser is configured as user slurm
 * #  --user=root -p
 * > GRANT ALL ON *.* TO 'slurm'@'localhost' IDENTIFIED BY PASSWORD 'pw';
 * > GRANT SELECT, INSERT ON *.* TO 'slurm'@'localhost';
\*****************************************************************************/

#include "accounting_storage_kingbase.h"
#include "as_kingbase_acct.h"
#include "as_kingbase_tres.h"
#include "as_kingbase_archive.h"
#include "as_kingbase_assoc.h"
#include "as_kingbase_cluster.h"
#include "as_kingbase_convert.h"
#include "as_kingbase_federation.h"
#include "as_kingbase_fix_runaway_jobs.h"
#include "as_kingbase_job.h"
#include "as_kingbase_jobacct_process.h"
#include "as_kingbase_problems.h"
#include "as_kingbase_qos.h"
#include "as_kingbase_resource.h"
#include "as_kingbase_resv.h"
#include "as_kingbase_rollup.h"
#include "as_kingbase_txn.h"
#include "as_kingbase_usage.h"
#include "as_kingbase_user.h"
#include "as_kingbase_wckey.h"

#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
#include "as_kingbase_usage.h"
#endif


List as_kingbase_cluster_list = NULL;
/* This total list is only used for converting things, so no
   need to keep it upto date even though it lives until the
   end of the life of the slurmdbd.
*/
List as_kingbase_total_cluster_list = NULL;
pthread_rwlock_t as_kingbase_cluster_list_lock = PTHREAD_RWLOCK_INITIALIZER;

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
 * the plugin (e.g., "accounting_storage" for Slurm job completion
 * logging) and <method>
 * is a description of how this plugin satisfies that application.  Slurm will
 * only load job completion logging plugins if the plugin_type string has a
 * prefix of "accounting_storage/".
 *
 * plugin_version - an unsigned 32-bit integer containing the Slurm version
 * (major.minor.micro combined into a single number).
 */
const char plugin_name[] = "Accounting storage KINGBASE plugin";
const char plugin_type[] = "accounting_storage/as_kingbase";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;

//static bool update_id_init = false;
static kingbase_db_info_t *kingbase_db_info = NULL;
static char *kingbase_db_name = NULL;

#define DELETE_SEC_BACK 86400

char *acct_coord_table = "acct_coord_table";
char *acct_table = "acct_table";
char *tres_table = "tres_table";
char *assoc_day_table = "assoc_usage_day_table";
char *assoc_hour_table = "assoc_usage_hour_table";
char *assoc_month_table = "assoc_usage_month_table";
char *assoc_table = "assoc_table";
char *clus_res_table = "clus_res_table";
char *cluster_day_table = "usage_day_table";
char *cluster_hour_table = "usage_hour_table";
char *cluster_month_table = "usage_month_table";
char *cluster_table = "cluster_table";
char *convert_version_table = "convert_version_table";
char *federation_table = "federation_table";
char *event_table = "event_table";
char *job_table = "job_table";
char *job_env_table = "job_env_table";
char *job_script_table = "job_script_table";
char *last_ran_table = "last_ran_table";
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
char *node_borrow_table = "node_borrow_table";
#endif
char *qos_table = "qos_table";
char *resv_table = "resv_table";
char *res_table = "res_table";
char *step_table = "step_table";
char *txn_table = "txn_table";
char *user_table = "user_table";
char *suspend_table = "suspend_table";
char *wckey_day_table = "wckey_usage_day_table";
char *wckey_hour_table = "wckey_usage_hour_table";
char *wckey_month_table = "wckey_usage_month_table";
char *wckey_table = "wckey_table";

char *event_view = "event_view";
char *event_ext_view = "event_ext_view";
char *job_view = "job_view";
char *job_ext_view = "job_ext_view";
char *resv_view = "resv_view";
char *resv_ext_view = "resv_ext_view";
char *step_view = "step_view";
char *step_ext_view = "step_ext_view";

bool backup_dbd = 0;
//typedef char *KINGBASE_ROW;
static char *default_qos_str = NULL;
enum {
	JASSOC_JOB,
	JASSOC_ACCT,
	JASSOC_USER,
	JASSOC_PART,
	JASSOC_COUNT
};

extern int acct_storage_p_close_connection(kingbase_conn_t **kingabse_conn);

static List _get_cluster_names(kingbase_conn_t *kingbase_conn, bool with_deleted)
{
	KCIResult *result = NULL;
	//KCIResult *row;
	List ret_list = NULL;
    int row = 0;
	char *query = xstrdup_printf("select name from %s", cluster_table);

	if (!with_deleted)
		xstrcat(query, " where deleted=0");

	/*sql语句通过kingbase_db_query_ret函数查询,kingbase_conn为当前连接，query为sql语句，0为拿第一个结果集*/
	result = kingbase_db_query_ret(kingbase_conn, query, 0); 
	/*获取sql语句的查询状态*/
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return NULL;
	}
	xfree(query);
	ret_list = list_create(xfree_ptr);
	/* KCIResultGetRowCount 从结果集中获取行数*/
    row = KCIResultGetRowCount(result);
    for(int i = 0; i< row; i++) {
		char *tmp_str=KCIResultGetColumnValue(result, i, 0);
		if(*tmp_str != '\0')
			list_append(ret_list, xstrdup(KCIResultGetColumnValue(result, i, 0)));
	}
	KCIResultDealloc(result);
	return ret_list;
}

static int _set_qos_cnt(kingbase_conn_t *kingbase_conn)
{
	KCIResult *result = NULL;
	bool tmp_flag = false;
	//KCIResult *row = NULL;
	char *query = xstrdup_printf("select MAX(id) from %s", qos_table);
	assoc_mgr_lock_t locks = { NO_LOCK, NO_LOCK, WRITE_LOCK, NO_LOCK,
				   NO_LOCK, NO_LOCK, NO_LOCK };

	/*获取qos table中的最大qos id*/
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return SLURM_ERROR;
	}
	xfree(query);

    int row = KCIResultGetRowCount(result);
	/*判断有元素返回*/
	for (int i = 0; i < row ;i++) {
		if(KCIResultGetColumnValue(result, 0, i)) {
			tmp_flag = true;
		}
	}
	if (!(tmp_flag)) {
		KCIResultDealloc(result);
		return SLURM_ERROR;
	}

	if (!KCIResultGetColumnValue(result, 0, 0)) {
		error("No QoS present in the DB, start the primary slurmdbd to create the DefaultQOS.");
		KCIResultDealloc(result);
		return SLURM_ERROR;
	}

	/* Set the current qos_count on the system for
	   generating bitstr of that length.  Since 0 isn't
	   possible as an id we add 1 to the total to burn 0 and
	   start at the 1 bit.
	*/
	assoc_mgr_lock(&locks);
	g_qos_count = atoi(KCIResultGetColumnValue(result, 0, 0));//这里需要测试
	assoc_mgr_unlock(&locks);
	KCIResultDealloc(result);

	return SLURM_SUCCESS;
}

/*
 * If we are removing the association with a user's default account, don't
 * unless are removing all of a user's assocs then removing the default assoc
 * is ok.
 */
static int _check_is_def_acct_before_remove(kingbase_conn_t *kingbase_conn,
					    char *cluster_name,
					    char *assoc_char,
					    List ret_list,
					    bool *default_account)
{
	char *query, *tmp_char = NULL, *as_statement = "", *last_user = NULL;
	KCIResult *result = NULL;
	int row = 0;
	int i;
	bool other_assoc = false;

	char *dassoc_inx[] = {
		"is_def",
		"`user`",
		"acct",
	};

	enum {
		DASSOC_IS_DEF,
		DASSOC_USER,
		DASSOC_ACCT,
		DASSOC_COUNT
	};

	xassert(default_account);

	xstrcat(tmp_char, dassoc_inx[0]);
	for (i = 1; i < DASSOC_COUNT; i++)
		xstrfmtcat(tmp_char, ", %s", dassoc_inx[i]);
	if (!xstrncmp(assoc_char, "t2.", 3))
		as_statement = "as t2 ";

	/* Query all the user associations given */
	query = xstrdup_printf("select %s from `%s_%s` %swhere deleted=0 and `user`!='' and (%s) order by `user`, is_def asc",
			       tmp_char, cluster_name, assoc_table,
			       as_statement, assoc_char);
	xfree(tmp_char);
	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);

	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);

	if (!result)
		return *default_account;

	row = KCIResultGetRowCount(result);
	for(int i=0; i < row; i++) {
		if (!xstrcmp(last_user, KCIResultGetColumnValue(result, i, DASSOC_USER))) {
			other_assoc = false;
			last_user = KCIResultGetColumnValue(result, i,DASSOC_USER);
		}
		char *tmp_string = KCIResultGetColumnValue(result, i, DASSOC_IS_DEF);
		char first_char = *tmp_string;
		if (first_char == '0') {
			other_assoc = true;
			continue;
		} else if (!other_assoc) {
			/*
			 * We have no other association, we are just removing
			 * this from the mix.
			 */
			continue;
		}

		DB_DEBUG(DB_ASSOC,  kingbase_conn->conn,
			 "Attempted removing default account (%s) of user: %s",
			 KCIResultGetColumnValue(result, i, DASSOC_ACCT), KCIResultGetColumnValue(result, i, DASSOC_USER));
		if (!(*default_account)) {
			*default_account = true;
			list_flush(ret_list);
			reset_kingbase_conn(kingbase_conn);
		}
		tmp_char = xstrdup_printf("C = %-15s A = %-10s U = %-9s",
					  cluster_name, KCIResultGetColumnValue(result, i, DASSOC_ACCT),
					  KCIResultGetColumnValue(result, i, DASSOC_USER));
		list_append(ret_list, tmp_char);
	}

	KCIResultDealloc(result);
	return *default_account;

}

static void _process_running_jobs_result(char *cluster_name,
					 KCIResult *result, List ret_list)
{
	int row = 0;
	char *object;
    row = KCIResultGetRowCount(result);
	for(int i=0; i < row; i++) {
		char *tmp_string = KCIResultGetColumnValue(result, i, JASSOC_USER);
		if ((strlen(tmp_string)<=0)) {
			/* This should never happen */
			error("How did we get a job running on an association "
			      "that isn't a user association job %s cluster "
			      "'%s' acct '%s'?", KCIResultGetColumnValue(result, i, JASSOC_JOB),
			      cluster_name, KCIResultGetColumnValue(result, i, JASSOC_ACCT));
			continue;
		}
		object = xstrdup_printf(
			"JobID = %-10s C = %-10s A = %-10s U = %-9s",
			KCIResultGetColumnValue(result, i, JASSOC_JOB), cluster_name,  KCIResultGetColumnValue(result, i, JASSOC_ACCT),
			KCIResultGetColumnValue(result, i, JASSOC_USER));
		char *tmp_string2 = KCIResultGetColumnValue(result, i, JASSOC_PART);
		if (strlen(tmp_string2) > 0) {
			// see if there is a partition name
			xstrfmtcat(object, " P = %s", KCIResultGetColumnValue(result, i, JASSOC_PART));
		}
		list_append(ret_list, object);
	}
}

/* this function is here to see if any of what we are trying to remove
 * has jobs that are not completed.  If we have jobs and the object is less
 * than a day old we don't want to delete it, only set the deleted flag.
 */
static bool _check_jobs_before_remove(kingbase_conn_t *kingbase_conn,
				      char *cluster_name,
				      char *assoc_char,
				      List ret_list,
				      bool *already_flushed)
{
	char *query = NULL, *object = NULL;
	bool rc = 0;
	int i;
	KCIResult *result = NULL;

	/* if this changes you will need to edit the corresponding
	 * enum above in the global settings */
	static char *jassoc_req_inx[] = {
		"t0.id_job",
		"t1.acct",
		"t1.user",
		"t1.partition"
	};
	if (ret_list) {
		xstrcat(object, jassoc_req_inx[0]);
		for(i=1; i<JASSOC_COUNT; i++)
			xstrfmtcat(object, ", %s", jassoc_req_inx[i]);

		query = xstrdup_printf(
			"select distinct %s "
			"from `%s_%s` as t0, "
			"`%s_%s` as t1, `%s_%s` as t2 "
			"where t1.lft between "
			"t2.lft and t2.rgt and (%s) "
			"and t0.id_assoc=t1.id_assoc "
			"and t0.time_end=0 and t0.state<%d;",
			object, cluster_name, job_table,
			cluster_name, assoc_table,
			cluster_name, assoc_table,
			assoc_char, JOB_COMPLETE);
		xfree(object);
	} else {
		query = xstrdup_printf(
			"select t0.id_assoc from `%s_%s` as t2 inner join "
			"`%s_%s` as t1 inner join `%s_%s` as t0 "
			"where t1.lft between "
			"t2.lft and t2.rgt and (%s) "
			"and t0.id_assoc=t1.id_assoc limit 1;",
			cluster_name, assoc_table,
			cluster_name, assoc_table,
			cluster_name, job_table,
			assoc_char);
	}

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return rc;
	}
	xfree(query);

	if (KCIResultGetRowCount(result)) {
		debug4("We have jobs for this combo");
		rc = true;
		if (ret_list && !(*already_flushed)) {
			list_flush(ret_list);
			(*already_flushed) = 1;
			reset_kingbase_conn(kingbase_conn);
		}
		if (ret_list)
			_process_running_jobs_result(cluster_name, result,
						     ret_list);
	}

	KCIResultDealloc(result);
	return rc;
}

/* Same as above but for associations instead of other tables */
static bool _check_jobs_before_remove_assoc(kingbase_conn_t *kingbase_conn,
					    char *cluster_name,
					    char *assoc_char,
					    List ret_list,
					    bool *already_flushed)
{
	char *query = NULL, *object = NULL;
	bool rc = 0;
	int i;
	KCIResult *result = NULL;

	/* if this changes you will need to edit the corresponding
	 * enum above in the global settings */
	static char *jassoc_req_inx[] = {
		"t1.id_job",
		"t2.acct",
		"t2.user",
		"t2.partition"
	};

	if (ret_list) {
		xstrcat(object, jassoc_req_inx[0]);
		for(i=1; i<JASSOC_COUNT; i++)
			xstrfmtcat(object, ", %s", jassoc_req_inx[i]);

		query = xstrdup_printf("select %s "
				       "from `%s_%s` as t1, `%s_%s` as t2 "
				       "where (%s) and t1.id_assoc=t2.id_assoc "
				       "and t1.time_end=0 and t1.state<%d;",
				       object, cluster_name, job_table,
				       cluster_name, assoc_table,
				       assoc_char, JOB_COMPLETE);
		xfree(object);
	} else {
		query = xstrdup_printf(
			"select t1.id_assoc from `%s_%s` as t1, "
			"`%s_%s` as t2 where (%s) "
			"and t1.id_assoc=t2.id_assoc limit 1;",
			cluster_name, job_table,
			cluster_name, assoc_table,
			assoc_char);
	}

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return rc;
	}
	xfree(query);

	if (KCIResultGetRowCount(result)) {
		debug4("We have jobs for this combo");
		rc = true;
		if (ret_list && !(*already_flushed)) {
			list_flush(ret_list);
			(*already_flushed) = 1;
			reset_kingbase_conn(kingbase_conn);
		}
	}

	if (ret_list)
		_process_running_jobs_result(cluster_name, result, ret_list);

	KCIResultDealloc(result);
	return rc;
}

/* Same as above but for things having nothing to do with associations
 * like qos or wckey */
static bool _check_jobs_before_remove_without_assoctable(
	kingbase_conn_t *kingbase_conn, char *cluster_name, char *where_char)
{
	char *query = NULL;
	bool rc = 0;
	KCIResult *result = NULL;

	query = xstrdup_printf("select id_assoc from `%s_%s` "
			       "where (%s) limit 1;",
			       cluster_name, job_table, where_char);

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return rc;
	}
	xfree(query);

	if (KCIResultGetRowCount(result)) {
		debug4("We have jobs for this combo");
		rc = true;
	}

	KCIResultDealloc(result);
	return rc;
}


/* Any time a new table is added set it up here */
static int _as_kingbase_acct_check_tables(kingbase_conn_t *kingbase_conn)
{
	storage_field_t acct_coord_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0" },
		{ "acct", "tinytext not null" },
		{ "`user`", "tinytext not null" },
		{ NULL, NULL}
	};

	storage_field_t acct_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0" },
		{ "name", "tinytext not null" },
		{ "description", "text not null" },
		{ "organization", "text not null" },
		{ NULL, NULL}
	};

	storage_field_t tres_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "id", "serial not null" },
		{ "type", "tinytext not null" },
		{ "name", "tinytext not null default ''" },
		{ NULL, NULL}
	};

	storage_field_t cluster_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint  default 0 not null" },
		{ "deleted", "tinyint default 0" },
		{ "name", "tinytext not null" },
		{ "control_host", "tinytext not null default ''" },
		{ "control_port", "bigint not null default 0" },
		{ "last_port", "bigint not null default 0" },
		{ "rpc_version", "int not null default 0" },
		{ "classification", "int default 0" },
		{ "dimensions", "int default 1" },
		{ "plugin_id_select", "int default 0" },
		{ "flags", "bigint default 0" },
		{ "federation", "tinytext not null" },
		{ "features", "text not null default ''" },
		{ "fed_id", "bigint default 0 not null" },
		{ "fed_state", "int not null" },
		{ NULL, NULL}
	};

	storage_field_t clus_res_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0" },
		{ "cluster", "tinytext not null" },
		{ "res_id", "int not null" },
		{ "percent_allowed", "bigint default 0" },
		{ NULL, NULL}
	};

	storage_field_t convert_version_table_fields[] = {
		{ "mod_time", "bigint default 0 not null" },
		{ "version", "int default 0" },
		{ NULL, NULL}
	};

	storage_field_t federation_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0" },
		{ "name", "tinytext not null" },
		{ "flags", "bigint default 0" },
		{ NULL, NULL}
	};

	/*
	 * Note: if the preempt field changes, _alter_table_after_upgrade() will
	 * need to be updated.
	 */
	storage_field_t qos_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0" },
		{ "id", "serial not null" },
		{ "name", "tinytext not null" },
		{ "description", "text" },
		{ "flags", "bigint default 0" },
		{ "grace_time", "bigint default NULL" },
		{ "max_jobs_pa", "int default NULL" },
		{ "max_jobs_per_user", "int default NULL" },
		{ "max_jobs_accrue_pa", "int default NULL" },
		{ "max_jobs_accrue_pu", "int default NULL" },
		{ "min_prio_thresh", "int default NULL" },
		{ "max_submit_jobs_pa", "int default NULL" },
		{ "max_submit_jobs_per_user", "int default NULL" },
		{ "max_tres_pa", "text not null default ''" },
		{ "max_tres_pj", "text not null default ''" },
		{ "max_tres_pn", "text not null default ''" },
		{ "max_tres_pu", "text not null default ''" },
		{ "max_tres_mins_pj", "text not null default ''" },
		{ "max_tres_run_mins_pa", "text not null default ''" },
		{ "max_tres_run_mins_pu", "text not null default ''" },
		{ "min_tres_pj", "text not null default ''" },
		{ "max_wall_duration_per_job", "int default NULL" },
		{ "grp_jobs", "int default NULL" },
		{ "grp_jobs_accrue", "int default NULL" },
		{ "grp_submit_jobs", "int default NULL" },
		{ "grp_tres", "text not null default ''" },
		{ "grp_tres_mins", "text not null default ''" },
		{ "grp_tres_run_mins", "text not null default ''" },
		{ "grp_wall", "int default NULL" },
		{ "preempt", "text not null default ''" },
		{ "preempt_mode", "int default 0" },
		{ "preempt_exempt_time", "bigint default NULL" },
		{ "priority", "bigint default 0" },
		{ "usage_factor", "double default 1.0 not null" },
		{ "usage_thres", "double default NULL" },
		{ "limit_factor", "double default NULL"},
		{ NULL, NULL}
	};

	storage_field_t res_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0" },
		{ "id", "serial not null " },
		{ "name", "tinytext not null" },
		{ "description", "text default null" },
		{ "manager", "tinytext not null" },
		{ "server", "tinytext not null" },
		{ "count", "bigint default 0" },
		{ "type", "bigint default 0"},
		{ "flags", "bigint default 0"},
		{ NULL, NULL}
	};

	storage_field_t txn_table_fields[] = {
		{ "id", "serial not null " },
		{ "timestamp", "bigint default 0 not null" },
		{ "action", "smallint not null" },
		{ "name", "text not null" },
		{ "actor", "tinytext not null" },
		{ "cluster", "tinytext not null default ''" },
		{ "info", "blob" },
		{ NULL, NULL}
	};

	storage_field_t user_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0" },
		{ "name", "tinytext not null" },
		{ "admin_level", "smallint default 1 not null" },
		{ NULL, NULL}
	};

	/*
	 * If more limits are added here they need to be added to
	 * get_parent_limits_select in as_kingbase_assoc.c
	 */
	/*
	char *get_parent_proc =
	"CREATE OR REPLACE PROCEDURE get_parent_limits ( my_table IN VARCHAR2, acct IN VARCHAR2, cluster IN VARCHAR2, without_limits IN NUMBER ) AS par_id NUMBER;"
	"mj NUMBER;"
	"mja NUMBER;"
	"mpt NUMBER;"
	"msj NUMBER;"
	"mwpj NUMBER;"
	"mtpj VARCHAR2 ( 4000 );"
	"mtpn VARCHAR2 ( 4000 );"
	"mtmpj VARCHAR2 ( 4000 );"
	"mtrm VARCHAR2 ( 4000 );"
	"prio NUMBER;"
	"def_qos_id NUMBER;"
	"qos VARCHAR2 ( 4000 );"
	"delta_qos VARCHAR2 ( 4000 );"
	"my_acct VARCHAR2 ( 4000 );"
	"my_acct_new VARCHAR2 ( 4000 );	"
"BEGIN "
		"par_id := NULL;"
		"mj := NULL;"
		"mja := NULL;"
		"mpt := NULL;"
		"msj := NULL;"
		"mwpj := NULL;"
		"mtpj := \'\';"
		"mtpn := \'\';"
		"mtmpj := \'\';"
		"mtrm := \'\';"
		"prio := NULL;"
		"def_qos_id := NULL;"
		"qos := \'\';"
		"delta_qos := \'\';"
		"my_acct := acct;"
	"IF "
		"without_limits = 1 THEN "
			"mj := 0; "
		"msj := 0; "
		"mwpj := 0; "
		"prio := 0; "
		"def_qos_id := 0; "
		"qos := \'1\'; "
	"END IF; "
	"LOOP "
			"FOR c IN ( "
			"SELECT "
				"id_assoc, "
				"max_jobs, "
				"max_jobs_accrue, "
				"min_prio_thresh, "
				"max_submit_jobs, "
				"max_wall_pj, "
				"priority, "
				"def_qos_id, "
				"qos, "
				"delta_qos, "
				"max_tres_pj, "
				"max_tres_pn, "
				"max_tres_mins_pj, "
				"max_tres_run_mins, "
				"parent_acct  "
			"FROM "
				"CONCAT(cluster, \'_\', my_table)"
			"WHERE "
				"my_table = my_table  "
				"AND acct = my_acct  "
				"AND user_id IS NULL  "
			")LOOP "
			"IF "
				"par_id IS NULL THEN "
					"par_id := c.id_assoc; "
			"END IF; "
			"IF "
				"mj IS NULL THEN "
					"mj := c.max_jobs; "
			"END IF; "
			"IF "
				"mja IS NULL THEN "
					"mja := c.max_jobs_accrue; "
			"END IF; "
			"IF "
				"mpt IS NULL THEN "
					"mpt := c.min_prio_thresh; "
			"END IF; "
			"IF "
				"msj IS NULL THEN "
					"msj := c.max_submit_jobs; "
			"END IF; "
			"IF "
				"mwpj IS NULL THEN "
					"mwpj := c.max_wall_pj; "
			"END IF; "
			"IF "
				"prio IS NULL THEN "
					"prio := c.priority; "
			"END IF; "
			"IF "
				"def_qos_id IS NULL THEN "
					"def_qos_id := c.def_qos_id; "
			"END IF; "
			"IF "
				"qos = \'\' THEN "
					"qos := c.qos; "
				"delta_qos := REPLACE ( CONCAT( delta_qos, c.delta_qos ), \',,\', \',\' ); "
			"END IF; "
			"mtpj := CONCAT( mtpj, CASE WHEN mtpj != \'\' AND c.max_tres_pj != \'\' THEN \',\' END, c.max_tres_pj ); "
			"mtpn := CONCAT( mtpn, CASE WHEN mtpn != \'\' AND c.max_tres_pn != \'\' THEN \',\' END, c.max_tres_pn ); "
			"mtmpj := CONCAT( mtmpj, CASE WHEN mtmpj != \'\' AND c.max_tres_mins_pj != \'\' THEN \',\' END, c.max_tres_mins_pj ); "
			"mtrm := CONCAT( mtrm, CASE WHEN mtrm != \'\' AND c.max_tres_run_mins != \'\' THEN \',\' END, c.max_tres_run_mins ); "
			"my_acct_new := c.parent_acct; "
		"END LOOP; "
		"my_acct := my_acct_new; "
		"EXIT  "
			"WHEN without_limits = 1 OR my_acct = \'\'; "
	"END LOOP; "
"END;";

	char *get_coord_qos =
	"CREATE OR REPLACE PROCEDURE get_coord_qos(my_table IN VARCHAR2, acct IN VARCHAR2, cluster IN VARCHAR2, coord IN VARCHAR2) AS "
    "qos VARCHAR2(4000); "
    "delta_qos VARCHAR2(4000); "
    "found_coord VARCHAR2(4000); "
    "my_acct VARCHAR2(4000); "
    "s VARCHAR2(32767); "
"BEGIN "
    "qos := \'\'; "
    "delta_qos := \'\'; "
    "found_coord := NULL; "
    "my_acct := acct; "

    "LOOP "
        "s := \'SELECT t1.qos, REPLACE(CONCAT(t1.delta_qos, :delta_qos), \'\',,\'\'), parent_acct, t2.user \'; "
        "s := s || \'FROM `\' || cluster || \'_\' || my_table || \'` t1 LEFT OUTER JOIN acct_coord_table t2 ON t1.acct = t2.acct WHERE t1.acct = :my_acct AND t1.user = \'\'\'\' AND (t2.user = \'\'\' || coord || \'\'\' OR t2.user IS NULL)\'; "
        "EXECUTE IMMEDIATE s INTO qos, delta_qos, my_acct, found_coord; "
        "IF found_coord IS NOT NULL THEN "
            "EXIT; "
        "END IF; "
        "IF found_coord IS NULL THEN "
            "qos := \'\'; "
            "delta_qos := \'\'; "
        "END IF; "
        "EXIT WHEN qos != \'\' OR my_acct = \'\'; "
    "END LOOP; "
    "DBMS_OUTPUT.PUT_LINE(REPLACE(CONCAT(qos, delta_qos), \',,\', \',\')); "
"END;";
	*/
	char *query = NULL;
	time_t now = time(NULL);
	char *cluster_name = NULL;
	int rc = SLURM_SUCCESS;
	ListIterator itr = NULL;

	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t*  data_rt = NULL;

     /* Make the convert version table since we will check that going
	 * forward to see if we need to update or not.
	 */
     /*创建table_defs_table及convert_version_table表，表的两列分别为
	  *mod_time及version，并将version设置为主键
	  */
	if (kingbase_db_create_table(kingbase_conn, convert_version_table,
				  convert_version_table_fields,
				  ", primary key (version))") == SLURM_ERROR)
		return SLURM_ERROR;

	/* Make the cluster table first since we build other tables
	   built off this one */
	if (kingbase_db_create_table(kingbase_conn, cluster_table,
				  cluster_table_fields,
				  ", primary key (name))") == SLURM_ERROR)
		return SLURM_ERROR;

	/* This table needs to be made before conversions also since
	   we add a cluster column.
	*/
	char *end = NULL;
	xstrfmtcat(end, ", primary key (id)); create index archive_purge_%s on %s (timestamp, cluster);", txn_table, txn_table);
	if (kingbase_db_create_table(kingbase_conn, txn_table, txn_table_fields,
				  end) == SLURM_ERROR){
			xfree(end);
			return SLURM_ERROR;
		}
	xfree(end);
    
	xstrfmtcat(end, ", primary key (id));create unique index udex_%s on %s (type, name);select setval((select sys_get_serial_sequence('%s','id')), 1001, false);", tres_table, tres_table, tres_table);
	if (kingbase_db_create_table(kingbase_conn, tres_table,
				tres_table_fields,
                end)
	    == SLURM_ERROR){
            xfree(end); 
            return SLURM_ERROR; 
        }
	xfree(end);

	if (!backup_dbd) {
		/* We always want CPU to be the first one, so create
		   it now.  We also add MEM here, the others tres
		   are site specific and could vary.  None but CPU
		   matter on order though.  CPU always has to be 1.

		   TRES_OFFSET is needed since there's no way to force
		   the number of first automatic id in Kingbase. auto_increment
		   value is lost on kingbase restart. Bug 4553.
		*/
		query = xstrdup_printf(
			"insert into %s (creation_time, id, deleted, type) values "
			"(%ld, %d, 0, 'cpu'), "
			"(%ld, %d, 0, 'mem'), "
			"(%ld, %d, 0, 'energy'), "
			"(%ld, %d, 0, 'node'), "
			"(%ld, %d, 0, 'billing'), "
			"(%ld, %d, 0, 'vmem'), "
			"(%ld, %d, 0, 'pages'), "
			"(%ld, %d, 1, 'dynamic_offset') "
			"as new on duplicate key update deleted=new.deleted, type=new.type, id=new.id;",
			tres_table,
			now, TRES_CPU,
			now, TRES_MEM,
			now, TRES_ENERGY,
			now, TRES_NODE,
			now, TRES_BILLING,
			now, TRES_VMEM,
			now, TRES_PAGES,
			now, TRES_OFFSET);
		DB_DEBUG(DB_TRES, kingbase_conn->conn, "%s", query);

     	fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
        xfree(query);
		free_res_data(data_rt, fetch_flag);
		if (rc == SLURM_ERROR ) {
			fatal("problem adding static tres");
		}
		
		/* Now insert TRES that have a name */
		query = xstrdup_printf(
			"insert into %s (creation_time, id, deleted, type, name) values "
			"(%ld, %d, 0, 'fs', 'disk') "
			"as new on duplicate key update deleted=new.deleted, type=new.type, name=new.name, id=new.id;",
			tres_table,
			now, TRES_FS_DISK);
		DB_DEBUG(DB_TRES, kingbase_conn->conn, "%s", query);

     	fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		xfree(query);
		free_res_data(data_rt, fetch_flag);
		if (rc == SLURM_ERROR ) {
			fatal("problem adding static tres");
		}
	}

	slurm_rwlock_wrlock(&as_kingbase_cluster_list_lock);
	if (!(as_kingbase_cluster_list = _get_cluster_names(kingbase_conn, 0))) {
		error("issue getting contents of %s", cluster_table);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
		return SLURM_ERROR;
	}

	/* This total list is only used for converting things, so no
	   need to keep it upto date even though it lives until the
	   end of the life of the slurmdbd.
	*/
	if (!(as_kingbase_total_cluster_list =
	      _get_cluster_names(kingbase_conn, 1))) {
		error("issue getting total contents of %s", cluster_table);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
		return SLURM_ERROR;
	}
    /*设置convert_version_table中version值，表转换的依据就是该值*/
	if ((rc = as_kingbase_convert_tables_pre_create(kingbase_conn)) !=
	    SLURM_SUCCESS) {
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
		error("issue converting tables before create");
		return rc;
	} else if (backup_dbd) {
		/*
		 * We do not want to create/check the database if we are the
		 * backup (see Bug 3827). This is only handled on the primary.
		 */

		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

		/* We do want to set the QOS count though. */
		if (rc == SLURM_SUCCESS)
			rc = _set_qos_cnt(kingbase_conn);

		return rc;
	}

	/* might as well do all the cluster centric tables inside this
	 * lock.  We need to do this on all the clusters deleted or
	 * other wise just to make sure everything is kept up to
	 * date. */
	itr = list_iterator_create(as_kingbase_total_cluster_list);
	while ((cluster_name = list_next(itr))) {
		if ((rc = create_cluster_tables(kingbase_conn, cluster_name))
		    != SLURM_SUCCESS)
			break;
	}
	list_iterator_destroy(itr);
	if (rc != SLURM_SUCCESS) {
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
		return rc;
	}

	rc = as_kingbase_convert_tables_post_create(kingbase_conn);

	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	if (rc != SLURM_SUCCESS) {
		error("issue converting tables after create");
		return rc;
	}

    xstrfmtcat(end, ", primary key (acct, `user`));create index user_%s on %s (`user`);", acct_coord_table, acct_coord_table);
	if (kingbase_db_create_table(kingbase_conn, acct_coord_table,
				  acct_coord_table_fields,
				  end)
	    == SLURM_ERROR){
            xfree(end);
            return SLURM_ERROR;
        }
	xfree(end);	

	if (kingbase_db_create_table(kingbase_conn, acct_table, acct_table_fields,
				  ", primary key (name))") == SLURM_ERROR)
		return SLURM_ERROR;

    xstrfmtcat(end, ", primary key (id));create unique index udex_%s on %s (name, server, type);", res_table, res_table);
	if (kingbase_db_create_table(kingbase_conn, res_table,
				  res_table_fields,
				  end)
	    == SLURM_ERROR){
            xfree(end);	
            return SLURM_ERROR;
        }
	xfree(end);		

    xstrfmtcat(end, ", primary key (res_id, cluster));create unique index udex_%s on %s (res_id, cluster);", clus_res_table, clus_res_table);
	if (kingbase_db_create_table(kingbase_conn, clus_res_table,
				  clus_res_table_fields,
				  end)
	    == SLURM_ERROR){
            xfree(end);	
            return SLURM_ERROR;
        }
	xfree(end);		

    xstrfmtcat(end, ", primary key (id));create unique index udex_%s on %s (name);", qos_table, qos_table);
	if (kingbase_db_create_table(kingbase_conn, qos_table,
				  qos_table_fields,
				  end)
	    == SLURM_ERROR){
            xfree(end);	
            return SLURM_ERROR;
    } else {
        xfree(end);	
		int qos_id = 0;
		if (slurmdbd_conf && slurmdbd_conf->default_qos) {
			List char_list = list_create(xfree_ptr);
			char *qos = NULL;

			slurm_addto_char_list(char_list,
					      slurmdbd_conf->default_qos);
			/*
			 * NOTE: You have to use slurm_list_pop here, since
			 * kingbase is exporting something of the same type as a
			 * macro, which messes everything up
			 * (my_list.h is the bad boy).
			 */
			while ((qos = slurm_list_pop(char_list))) {
				query = xstrdup_printf(
					"insert into %s "
					"(creation_time, mod_time, name, "
					"description) "
					"values (%ld, %ld, '%s', "
					"'Added as default') "
					"on duplicate key update "
					//"id=nextval(id), deleted=0;",
					"deleted=0;",
					qos_table, now, now, qos);
				DB_DEBUG(DB_QOS, kingbase_conn->conn, "%s", query);
				char *query2 = xstrdup_printf(
					"insert into %s "
					"(creation_time, mod_time, name, "
					"description) "
					"values (%ld, %ld, '%s', "
					"'Added as default') "
					"on duplicate key update "
					//"id=nextval(id), deleted=0;",
					"deleted=0 returning id;",
					qos_table, now, now, qos);

				fetch_flag = set_fetch_flag(true, false, false);
				data_rt = xmalloc(sizeof(fetch_result_t));
				rc = kingbase_for_fetch2(kingbase_conn, query, fetch_flag, data_rt, query2);
				xfree(query2);
				qos_id = data_rt->insert_ret_id;
				free_res_data(data_rt, fetch_flag); 

				if (!qos_id) {
					fatal("problem added qos '%s", qos);
				}
				xstrfmtcat(default_qos_str, ",%d", qos_id);
				xfree(query);
				xfree(qos);
				if(rc == SLURM_ERROR)
					return rc;
			}
			FREE_NULL_LIST(char_list);
		} else {
			query = xstrdup_printf(
				"insert into %s "
				"(creation_time, mod_time, name, description) "
				"values (%ld, %ld, 'normal', "
				"'Normal QOS default') "
				"on duplicate key update "
				//"id=nextval(id), deleted=0;",
				"deleted=0;",
				qos_table, now, now);
			DB_DEBUG(DB_QOS, kingbase_conn->conn, "%s", query);
			char* query2 = xstrdup_printf(
				"insert into %s "
				"(creation_time, mod_time, name, description) "
				"values (%ld, %ld, 'normal', "
				"'Normal QOS default') "
				"on duplicate key update "
				//"id=nextval(id), deleted=0;",
				"deleted=0 returning id;",
				qos_table, now, now);
			fetch_flag = set_fetch_flag(true, false, false);
			data_rt = xmalloc(sizeof(fetch_result_t));
			rc = kingbase_for_fetch2(kingbase_conn, query, fetch_flag, data_rt,query2);		
			xfree(query2);
			qos_id = data_rt->insert_ret_id;
			free_res_data(data_rt, fetch_flag); 
			if (!qos_id) {
				fatal("problem added qos 'normal");
			}
			xfree(query);
			if(rc == SLURM_ERROR)
				return rc;
			xstrfmtcat(default_qos_str, ",%d", qos_id);

		}

		if (_set_qos_cnt(kingbase_conn) != SLURM_SUCCESS)
			return SLURM_ERROR;
	}

	/* This must be ran after create_cluster_tables() */
	if (kingbase_db_create_table(kingbase_conn, user_table, user_table_fields,
				  ", primary key (name))") == SLURM_ERROR)
		return SLURM_ERROR;

	if (kingbase_db_create_table(kingbase_conn, federation_table,
				  federation_table_fields,
				  ", primary key (name))") == SLURM_ERROR)
		return SLURM_ERROR;

	rc = as_kingbase_convert_non_cluster_tables_post_create(kingbase_conn);

	if (rc != SLURM_SUCCESS) {
		error("issue converting non-cluster tables after create");
		return rc;
	}
	//info("[query] line %d, %s: get_parent_proc: %s", __LINE__, __func__, get_parent_proc);
	/*
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, get_parent_proc, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);
	if (rc == SLURM_ERROR ) {
		fatal("problem adding static tres");
	}

	//info("[query] line %d, %s: get_coord_qos: %s", __LINE__, __func__, get_coord_qos);	
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, get_coord_qos, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);
	if (rc == SLURM_ERROR ) {
		fatal("problem adding static tres");
	}
	*/

	/* Add user root to be a user by default and have this default
	 * account be root.  If already there just update
	 * name='root'.  That way if the admins delete it it will
	 * remain deleted. Creation time will be 0 so it will never
	 * really be deleted.
	 */
	query = xstrdup_printf(
		"insert into %s (creation_time, mod_time, name, "
		"admin_level) values (%ld, %ld, 'root', %d) "
		"on duplicate key update name='root';",
		user_table, (long)now, (long)now, SLURMDB_ADMIN_SUPER_USER);
	xstrfmtcat(query,
		   "insert into %s (creation_time, mod_time, name, "
		   "description, organization) values (%ld, %ld, 'root', "
		   "'default root account', 'root') on duplicate key "
		   "update name='root';",
		   acct_table, (long)now, (long)now);

	//DB_DEBUG(kingbase_conn->conn, "%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);   
	if (rc == SLURM_ERROR ) {
		fatal("problem adding static tres");
	}

	xfree(query);

	return rc;
}

/* This should be added to the beginning of each function to make sure
 * we have a connection to the database before we try to use it.
 */
extern int check_connection(kingbase_conn_t *kingbase_conn)
{
	if (!kingbase_conn) {
		error("We need a connection to run this");
		errno = ESLURM_DB_CONNECTION;
		return ESLURM_DB_CONNECTION;
	} else if (kingbase_db_ping(kingbase_conn) != 0) {
		/* avoid memory leak and end thread */
		kingbase_db_close_db_connection(kingbase_conn);
		if (kingbase_db_get_db_connection(
			    kingbase_conn, kingbase_db_name, kingbase_db_info)
		    != SLURM_SUCCESS) {
			error("unable to re-connect to as_kingbase database");
			errno = ESLURM_DB_CONNECTION;
			return ESLURM_DB_CONNECTION;
		}
	}

	if (kingbase_conn->cluster_deleted) {
		errno = ESLURM_CLUSTER_DELETED;
		return ESLURM_CLUSTER_DELETED;
	}

	return SLURM_SUCCESS;
}

/* Let me know if the last statement had rows that were affected.
 * This only gets called by a non-threaded connection, so there is no
 * need to worry about locks.
 */
extern int last_affected_rows(KCIResult *res)
{
	int status = 0, rows = 0;
	char *tmp_str = KCIResultGetAffectedCount(res);
	if(tmp_str)
		status = atoi(tmp_str);
	if (status > 0) 
		rows = status;
	if (res)
		KCIResultDealloc(res);
	return rows;
}

/*回滚*/
extern void reset_kingbase_conn(kingbase_conn_t *kingbase_conn)
{
	if (kingbase_conn->rollback)
		kingbase_db_rollback(kingbase_conn);
	xfree(kingbase_conn->pre_commit_query);
	list_flush(kingbase_conn->update_list);
}

/*创建assoc表*/
extern int create_cluster_assoc_table(
	kingbase_conn_t *kingbase_conn, char *cluster_name)
{
storage_field_t assoc_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "is_def", "tinyint default 0 not null" },
		{ "id_assoc", "serial not null" },
		{ "`user`", "tinytext not null default ''" },
		{ "acct", "tinytext not null" },
		{ "partition", "tinytext not null default ''" },
		{ "parent_acct", "tinytext not null default ''" },
		{ "lft", "int not null" },
		{ "rgt", "int not null" },
		{ "shares", "int default 1 not null" },
		{ "max_jobs", "int default NULL" },
		{ "max_jobs_accrue", "int default NULL" },
		{ "min_prio_thresh", "int default NULL" },
		{ "max_submit_jobs", "int default NULL" },
		{ "max_tres_pj", "text not null default ''" },
		{ "max_tres_pn", "text not null default ''" },
		{ "max_tres_mins_pj", "text not null default ''" },
		{ "max_tres_run_mins", "text not null default ''" },
		{ "max_wall_pj", "int default NULL" },
		{ "grp_jobs", "int default NULL" },
		{ "grp_jobs_accrue", "int default NULL" },
		{ "grp_submit_jobs", "int default NULL" },
		{ "grp_tres", "text not null default ''" },
		{ "grp_tres_mins", "text not null default ''" },
		{ "grp_tres_run_mins", "text not null default ''" },
		{ "grp_wall", "int default NULL" },
		{ "priority", "bigint default NULL" },
		{ "def_qos_id", "int default NULL" },
		{ "qos", "blob not null default ''" },
		{ "delta_qos", "blob not null default ''" },
		{ NULL, NULL}
	};

	char table_name[200];

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, assoc_table);
	char *end = NULL;
	xstrfmtcat(end, ", primary key (id_assoc));create unique index udex_%s_%s on %s_%s (`user`,acct,partition);create index lft_%s_%s on %s_%s (lft);create index account_%s_%s on %s_%s (acct);",cluster_name, assoc_table, cluster_name, assoc_table, cluster_name, assoc_table, cluster_name, assoc_table, cluster_name, assoc_table, cluster_name, assoc_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  assoc_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end); 
			return SLURM_ERROR;
		}
	xfree(end);
	return SLURM_SUCCESS;
}

extern int create_cluster_tables(kingbase_conn_t *kingbase_conn, char *cluster_name)
{
	storage_field_t cluster_usage_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "id_tres", "int not null" },
		{ "time_start", "bigint not null" },
		{ "count", "numeric(21,0) default 0 not null" },
		{ "alloc_secs", "bigint default 0 not null" },
		{ "down_secs", "bigint default 0 not null" },
		{ "pdown_secs", "bigint default 0 not null" },
		{ "idle_secs", "bigint default 0 not null" },
		{ "plan_secs", "bigint default 0 not null" },
		{ "over_secs", "bigint default 0 not null" },
		{ NULL, NULL}
	};

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	storage_field_t node_borrow_table_fields[] = {
		{ "time_start", "bigint not null" },
		{ "time_end", "bigint default 0 not null" },
		{ "node_name", "tinytext default '' not null" },
		{ "reason", "tinytext not null" },
		{ "reason_uid", "bigint default 0xfffffffe not null" },
		{ "state", "bigint default 0 not null" },
		{ "expansion", "text" },
		{ NULL, NULL}
	};
#endif

	storage_field_t event_table_fields[] = {
		{ "time_start", "bigint not null" },
		{ "time_end", "bigint default 0 not null" },
		{ "node_name", "tinytext default '' not null" },
		{ "cluster_nodes", "text not null default ''" },
		{ "reason", "tinytext not null" },
		{ "reason_uid", "bigint default 4294967294 not null" },
		{ "state", "bigint default 0 not null" },
		{ "tres", "text not null default ''" },
		{ NULL, NULL}
	};

	storage_field_t id_usage_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "id", "bigint not null" },
		{ "id_tres", "int default 1 not null" },
		{ "time_start", "bigint not null" },
		{ "alloc_secs", "bigint default 0 not null" },
		{ NULL, NULL}
	};

	storage_field_t job_table_fields[] = {
		{ "job_db_inx", "bigserial not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "account", "tinytext" },
		{ "admin_comment", "text" },
		{ "array_task_str", "text" },
		{ "array_max_tasks", "bigint default 0 not null" },
		{ "array_task_pending", "bigint default 0 not null" },
		{ "constraints", "text default ''" },
		{ "container", "text" },
		{ "cpus_req", "bigint not null" },
		{ "derived_ec", "bigint default 0 not null" },
		{ "derived_es", "text" },
		{ "env_hash_inx", "bigint default 0 not null" },
		{ "exit_code", "bigint default 0 not null" },
		{ "flags", "bigint default 0 not null" },
		{ "job_name", "tinytext not null" },
		{ "id_assoc", "bigint not null" },
		{ "id_array_job", "bigint default 0 not null" },
		{ "id_array_task", "bigint default 4294967294 not null" },
		{ "id_block", "tinytext" },
		{ "id_job", "bigint not null" },
		{ "id_qos", "bigint default 0 not null" },
		{ "id_resv", "bigint not null" },
		{ "id_wckey", "bigint not null" },
		{ "id_user", "bigint not null" },
		{ "id_group", "bigint not null" },
		{ "het_job_id", "bigint not null" },
		{ "het_job_offset", "bigint not null" },
		{ "kill_requid", "bigint default null" },
		{ "state_reason_prev", "bigint not null" },
		{ "mcs_label", "tinytext default ''" },
		{ "mem_req", "numeric(21,0) default 0 not null" },
		{ "nodelist", "text" },
		{ "nodes_alloc", "bigint not null" },
		{ "node_inx", "text" },
		{ "partition", "tinytext not null" },
		{ "priority", "bigint not null" },
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
		{ "tres_alloc", "text not null default ''" },
		{ "tres_req", "text not null default ''" },
#ifdef __METASTACK_OPT_RESC_NODEDETAIL
		{ "resource_node_detail", "text not null default ''"},
#endif
#ifdef __METASTACK_OPT_SACCT_COMMAND
		{ "command", "text not null default ''" },
#endif
#ifdef __METASTACK_OPT_SACCT_OUTPUT
		{ "stdout", "text not null default ''" },
		{ "stderr", "text not null default ''" },
#endif
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

	storage_field_t last_ran_table_fields[] = {
		{ "hourly_rollup", "bigint default 0 not null" },
		{ "daily_rollup", "bigint default 0 not null" },
		{ "monthly_rollup", "bigint default 0 not null" },
		{ NULL, NULL}
	};

	storage_field_t resv_table_fields[] = {
		{ "id_resv", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "assoclist", "text not null default ''" },
		{ "flags", "numeric(21,0) default 0 not null" },
		{ "nodelist", "text not null default ''" },
		{ "node_inx", "text not null default ''" },
		{ "resv_name", "text not null" },
		{ "time_start", "bigint default 0 not null"},
		{ "time_end", "bigint default 0 not null" },
		{ "tres", "text not null default ''" },
		{ "unused_wall", "double default 0.0 not null" },
		{ NULL, NULL}
	};

	storage_field_t step_table_fields[] = {
		{ "job_db_inx", "numeric(21,0) not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "exit_code", "int default 0 not null" },
		{ "id_step", "int not null" },
		{ "step_het_comp", "bigint default 4294967294 not null" },
		{ "kill_requid", "bigint default null" },
		{ "nodelist", "text not null" },
		{ "nodes_alloc", "bigint not null" },
		{ "node_inx", "text" },
		{ "state", "int not null" },
		{ "step_name", "text not null" },
		{ "task_cnt", "bigint not null" },
		{ "task_dist", "int default 0 not null" },
		{ "time_start", "bigint default 0 not null" },
		{ "time_end", "bigint default 0 not null" },
		{ "time_suspended", "bigint default 0 not null" },
		{ "user_sec", "bigint default 0 not null" },
		{ "user_usec", "bigint default 0 not null" },
		{ "sys_sec", "bigint default 0 not null" },
		{ "sys_usec", "bigint default 0 not null" },
		{ "act_cpufreq", "DOUBLE default 0.0 not null" },
		{ "consumed_energy", "numeric(21,0) default 0 not null" },
		{ "container", "text" },
		{ "req_cpufreq_min", "bigint default 0 not null" },
		{ "req_cpufreq", "bigint default 0 not null" }, /* max */
		{ "req_cpufreq_gov", "bigint default 0 not null" },
		{ "tres_alloc", "text not null default ''" },
		{ "submit_line", "text" },
		{ "tres_usage_in_ave", "text not null default ''" },
		{ "tres_usage_in_max", "text not null default ''" },
		{ "tres_usage_in_max_taskid", "text not null default ''" },
		{ "tres_usage_in_max_nodeid", "text not null default ''" },
		{ "tres_usage_in_min", "text not null default ''" },
		{ "tres_usage_in_min_taskid", "text not null default ''" },
		{ "tres_usage_in_min_nodeid", "text not null default ''" },
		{ "tres_usage_in_tot", "text not null default ''" },
		{ "tres_usage_out_ave", "text not null default ''" },
		{ "tres_usage_out_max", "text not null default ''" },
		{ "tres_usage_out_max_taskid", "text not null default ''" },
		{ "tres_usage_out_max_nodeid", "text not null default ''" },
		{ "tres_usage_out_min", "text not null default ''" },
		{ "tres_usage_out_min_taskid", "text not null default ''" },
		{ "tres_usage_out_min_nodeid", "text not null default ''" },
		{ "tres_usage_out_tot", "text not null default ''" },
		{ NULL, NULL}
	};

	storage_field_t suspend_table_fields[] = {
		{ "job_db_inx", "numeric(21,0) not null" },
		{ "id_assoc", "int not null" },
		{ "time_start", "bigint default 0 not null" },
		{ "time_end", "bigint default 0 not null" },
		{ NULL, NULL}
	};

	storage_field_t wckey_table_fields[] = {
		{ "creation_time", "bigint not null" },
		{ "mod_time", "bigint default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "is_def", "tinyint default 0 not null" },
		{ "id_wckey", "serial not null" },
		{ "wckey_name", "tinytext not null default ''" },
		{ "`user`", "tinytext not null" },
		{ NULL, NULL}
	};

	char table_name[200];
	char *end = NULL;

	if (create_cluster_assoc_table(kingbase_conn, cluster_name)
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, assoc_day_table);
	xstrfmtcat(end, ", primary key (id, id_tres, time_start));"
					"create index time_start_%s_%s on %s_%s (time_start);"
					, cluster_name, assoc_day_table, cluster_name, assoc_day_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  id_usage_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end);
			return SLURM_ERROR;
		}
	xfree(end); 

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, assoc_hour_table);
	xstrfmtcat(end, ", primary key (id, id_tres, time_start));"
					"create index time_start_%s_%s on %s_%s (time_start);"
					, cluster_name, assoc_hour_table, cluster_name, assoc_hour_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  id_usage_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end);
			return SLURM_ERROR;
		}
	xfree(end);
	
	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, assoc_month_table);
	xstrfmtcat(end, ", primary key (id, id_tres, time_start));"
					"create index time_start_%s_%s on %s_%s (time_start);"
					, cluster_name, assoc_month_table, cluster_name, assoc_month_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  id_usage_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end);
			return SLURM_ERROR;
		}
	xfree(end);

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, cluster_day_table);
	xstrfmtcat(end, ", primary key (id_tres, time_start));"
					"create index time_start_%s_%s on %s_%s (time_start);"
					, cluster_name, cluster_day_table, cluster_name, cluster_day_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  cluster_usage_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end);
			return SLURM_ERROR;
		}
	xfree(end);

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, cluster_hour_table);
	xstrfmtcat(end, ", primary key (id_tres, time_start));"
					"create index time_start_%s_%s on %s_%s (time_start);"
					, cluster_name, cluster_hour_table, cluster_name, cluster_hour_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  cluster_usage_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end);
			return SLURM_ERROR;
		}
	xfree(end);

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, cluster_month_table);
	xstrfmtcat(end, ", primary key (id_tres, time_start));"
					"create index time_start_%s_%s on %s_%s (time_start);"
					, cluster_name, cluster_month_table, cluster_name, cluster_month_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  cluster_usage_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end);
			return SLURM_ERROR;
		}
	xfree(end);

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, event_table);

	xstrfmtcat(end, ", primary key (node_name, time_start));"
					"create index rollup_%s_%s on %s_%s (node_name, time_start, time_end, state);"
					"create index time_start_end_%s_%s on %s_%s (time_start, time_end);"
					, cluster_name, event_table, cluster_name, event_table
					, cluster_name, event_table, cluster_name, event_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  event_table_fields,
				  end) == SLURM_ERROR){
			xfree(end); 
			return SLURM_ERROR;
		}
	xfree(end); 	

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, job_table);

	/*
	 * sacct_def is the index for query's with state as time_start is used
	 * in these queries. sacct_def2 is for plain sacct queries.
	 */
	xstrfmtcat(end, ", primary key (job_db_inx));"
					"create unique index udex_%s_%s on %s_%s (id_job, time_submit);"
					"create index old_tuple_%s_%s on %s_%s (id_job, id_assoc, time_submit);"
					"create index rollup_%s_%s on %s_%s (time_eligible, time_end);"
					"create index rollup2_%s_%s on %s_%s (time_end, time_eligible);"
					"create index nodes_alloc_%s_%s on %s_%s (nodes_alloc);"
					"create index wckey_%s_%s on %s_%s (id_wckey);"
					"create index qos_%s_%s on %s_%s (id_qos);"
					"create index association_%s_%s on %s_%s (id_assoc);"
					"create index array_job_%s_%s on %s_%s (id_array_job);"
					"create index het_job_%s_%s on %s_%s (het_job_id);"
					"create index reserv_%s_%s on %s_%s (id_resv);"
					"create index sacct_def_%s_%s on %s_%s (id_user, time_start, time_end);"
					"create index sacct_def2_%s_%s on %s_%s (id_user, time_end, time_eligible);"
					"create index env_hash_inx_%s_%s on %s_%s (env_hash_inx);"
					"create index script_hash_inx_%s_%s on %s_%s (script_hash_inx);"
					"create index archive_purge_%s_%s on %s_%s (time_submit, time_end);"
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
					, cluster_name, job_table, cluster_name, job_table
					);
	if (kingbase_db_create_table(kingbase_conn, table_name, job_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end); 
			return SLURM_ERROR;
		}
	xfree(end); 	

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, job_env_table);
	xstrfmtcat(end, ", primary key (hash_inx));"
					"create unique index env_hash_inx_%s_%s on %s_%s (env_hash);"
					, cluster_name, job_env_table, cluster_name, job_env_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  job_env_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end); 
			return SLURM_ERROR;
		}
	xfree(end);

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, job_script_table);
	xstrfmtcat(end, ", primary key (hash_inx));"
					"create unique index script_hash_inx_%s_%s on %s_%s (script_hash);"
					, cluster_name, job_script_table, cluster_name, job_script_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  job_script_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end); 
			return SLURM_ERROR;
		}
	xfree(end);

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, last_ran_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  last_ran_table_fields,
				  ", primary key (hourly_rollup, "
				  "daily_rollup, monthly_rollup))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, node_borrow_table);
	xstrfmtcat(end, ", primary key (node_name, time_start));"
					"create index rollup_%s_%s on %s_%s (node_name, time_start, time_end, reason);"
					"create index time_start_end_%s_%s on %s_%s (time_start, time_end);"
					, cluster_name, node_borrow_table, cluster_name, node_borrow_table
					, cluster_name, node_borrow_table, cluster_name, node_borrow_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  node_borrow_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end); 
			return SLURM_ERROR;
		}
	xfree(end);
#endif

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, resv_table);
	xstrfmtcat(end, ", primary key (id_resv, time_start));"
					"create index time_start_end_%s_%s on %s_%s (time_start, time_end);"
					, cluster_name, resv_table, cluster_name, resv_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  resv_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end); 
			return SLURM_ERROR;
		}
	xfree(end);

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, step_table);
	xstrfmtcat(end, ", primary key (job_db_inx, id_step, step_het_comp));"
					"create index no_step_comp_%s_%s on %s_%s (job_db_inx, id_step);"
					"create index time_start_end_%s_%s on %s_%s (time_start, time_end);"
					, cluster_name, step_table, cluster_name, step_table
					, cluster_name, step_table, cluster_name, step_table
					);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  step_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end);
			return SLURM_ERROR;
		}
	xfree(end);	

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, suspend_table);
	xstrfmtcat(end, ", primary key (job_db_inx, time_start));"
					"create index job_db_inx_times_%s_%s on %s_%s (job_db_inx, time_start, time_end);"
					, cluster_name, suspend_table, cluster_name, suspend_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  suspend_table_fields,
				  end) == SLURM_ERROR){
			xfree(end); 
			return SLURM_ERROR;
		}
	xfree(end);

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, wckey_table);
	xstrfmtcat(end, ", primary key (id_wckey));"
					"create unique index udex_%s_%s on %s_%s (wckey_name, `user`);"
					, cluster_name, wckey_table, cluster_name, wckey_table);
	if (kingbase_db_create_table(kingbase_conn, table_name,
				  wckey_table_fields,
				  end)
	    == SLURM_ERROR){
			xfree(end); 
			return SLURM_ERROR;
		}
	xfree(end);	

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, wckey_day_table);

	if (kingbase_db_create_table(kingbase_conn, table_name,
				  id_usage_table_fields,
				  ", primary key (id, id_tres, time_start))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, wckey_hour_table);

	if (kingbase_db_create_table(kingbase_conn, table_name,
				  id_usage_table_fields,
				  ", primary key (id, id_tres, time_start))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "%s_%s",
		 cluster_name, wckey_month_table);

	if (kingbase_db_create_table(kingbase_conn, table_name,
				  id_usage_table_fields,
				  ", primary key (id, id_tres, time_start))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	return SLURM_SUCCESS;
}

/*移除cluster name*/
extern int remove_cluster_tables(kingbase_conn_t *kingbase_conn, char *cluster_name)
{
	char *query = NULL;
	int rc = SLURM_SUCCESS;
	KCIResult *result = NULL;

	query = xstrdup_printf("select id_assoc from `%s_%s` limit 1;",
			       cluster_name, assoc_table);
	debug4("%d(%s:%d) query\n%s",
	       kingbase_conn->conn, THIS_FILE, __LINE__, query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		error("no result given when querying cluster %s", cluster_name);
		return SLURM_ERROR;
	}
	xfree(query);

	if (KCIResultGetRowCount(result)) {
		KCIResultDealloc(result);
		debug4("we still have associations, can't remove tables");
		return SLURM_SUCCESS;
	}
	KCIResultDealloc(result);
	xstrfmtcat(kingbase_conn->pre_commit_query,
		   "drop table `%s_%s`, `%s_%s`, `%s_%s`, "
		   "`%s_%s`, `%s_%s`, `%s_%s`, `%s_%s`, "
		   "`%s_%s`, `%s_%s`, `%s_%s`, `%s_%s`, "
		   "`%s_%s`, `%s_%s`, `%s_%s`, `%s_%s`, "
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
		   "`%s_%s`, "
#endif
		   "`%s_%s`, `%s_%s`;",
		   cluster_name, assoc_table,
		   cluster_name, assoc_day_table,
		   cluster_name, assoc_hour_table,
		   cluster_name, assoc_month_table,
		   cluster_name, cluster_day_table,
		   cluster_name, cluster_hour_table,
		   cluster_name, cluster_month_table,
		   cluster_name, event_table,
		   cluster_name, job_table,
		   cluster_name, last_ran_table,
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
		   cluster_name, node_borrow_table,
#endif
		   cluster_name, resv_table,
		   cluster_name, step_table,
		   cluster_name, suspend_table,
		   cluster_name, wckey_table,
		   cluster_name, wckey_day_table,
		   cluster_name, wckey_hour_table,
		   cluster_name, wckey_month_table);
	/* Since we could possibly add this exact cluster after this
	   we will require a commit before doing anything else.  This
	   flag will give us that.
	*/
	kingbase_conn->cluster_deleted = 1;
	return rc;
}

extern int setup_assoc_limits(slurmdb_assoc_rec_t *assoc,
			      char **cols, char **vals,
			      char **extra, qos_level_t qos_level,
			      bool for_add)
{
	uint32_t tres_str_flags = TRES_STR_FLAG_REMOVE |
		TRES_STR_FLAG_SORT_ID | TRES_STR_FLAG_SIMPLE |
		TRES_STR_FLAG_NO_NULL;

	assoc_mgr_lock_t locks = { NO_LOCK, NO_LOCK, READ_LOCK, NO_LOCK,
				   NO_LOCK, NO_LOCK, NO_LOCK };
	if (!assoc)
		return SLURM_ERROR;

	if (for_add) {
		/* If we are adding we should make sure we don't get
		   old reside sitting around from a former life.
		*/
		if (assoc->shares_raw == NO_VAL)
			assoc->shares_raw = INFINITE;
		if (assoc->grp_jobs == NO_VAL)
			assoc->grp_jobs = INFINITE;
		if (assoc->grp_jobs_accrue == NO_VAL)
			assoc->grp_jobs_accrue = INFINITE;
		if (assoc->grp_submit_jobs == NO_VAL)
			assoc->grp_submit_jobs = INFINITE;
		if (assoc->grp_wall == NO_VAL)
			assoc->grp_wall = INFINITE;
		if (assoc->max_jobs == NO_VAL)
			assoc->max_jobs = INFINITE;
		if (assoc->max_jobs_accrue == NO_VAL)
			assoc->max_jobs_accrue = INFINITE;
		if (assoc->min_prio_thresh == NO_VAL)
			assoc->min_prio_thresh = INFINITE;
		if (assoc->max_submit_jobs == NO_VAL)
			assoc->max_submit_jobs = INFINITE;
		if (assoc->max_wall_pj == NO_VAL)
			assoc->max_wall_pj = INFINITE;
		if (assoc->priority == NO_VAL)
			assoc->priority = INFINITE;
		if (assoc->def_qos_id == NO_VAL)
			assoc->def_qos_id = INFINITE;
	}

	if (assoc->shares_raw == INFINITE) {
		xstrcat(*cols, ", shares");
		xstrcat(*vals, ", 1");
		xstrcat(*extra, ", shares=1");
		assoc->shares_raw = 1;
	} else if ((assoc->shares_raw != NO_VAL)
		   && (int32_t)assoc->shares_raw >= 0) {
		xstrcat(*cols, ", shares");
		xstrfmtcat(*vals, ", %u", assoc->shares_raw);
		xstrfmtcat(*extra, ", shares=%u", assoc->shares_raw);
	}

	if (assoc->grp_jobs == INFINITE) {
		xstrcat(*cols, ", grp_jobs");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", grp_jobs=NULL");
	} else if ((assoc->grp_jobs != NO_VAL)
		   && ((int32_t)assoc->grp_jobs >= 0)) {
		xstrcat(*cols, ", grp_jobs");
		xstrfmtcat(*vals, ", %u", assoc->grp_jobs);
		xstrfmtcat(*extra, ", grp_jobs=%u", assoc->grp_jobs);
	}

	if (assoc->grp_jobs_accrue == INFINITE) {
		xstrcat(*cols, ", grp_jobs_accrue");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", grp_jobs_accrue=NULL");
	} else if ((assoc->grp_jobs_accrue != NO_VAL)
		   && ((int32_t)assoc->grp_jobs_accrue >= 0)) {
		xstrcat(*cols, ", grp_jobs_accrue");
		xstrfmtcat(*vals, ", %u", assoc->grp_jobs_accrue);
		xstrfmtcat(*extra, ", grp_jobs_accrue=%u",
			   assoc->grp_jobs_accrue);
	}

	if (assoc->grp_submit_jobs == INFINITE) {
		xstrcat(*cols, ", grp_submit_jobs");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", grp_submit_jobs=NULL");
	} else if ((assoc->grp_submit_jobs != NO_VAL)
		   && ((int32_t)assoc->grp_submit_jobs >= 0)) {
		xstrcat(*cols, ", grp_submit_jobs");
		xstrfmtcat(*vals, ", %u", assoc->grp_submit_jobs);
		xstrfmtcat(*extra, ", grp_submit_jobs=%u",
			   assoc->grp_submit_jobs);
	}

	if (assoc->grp_wall == INFINITE) {
		xstrcat(*cols, ", grp_wall");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", grp_wall=NULL");
	} else if ((assoc->grp_wall != NO_VAL)
		   && ((int32_t)assoc->grp_wall >= 0)) {
		xstrcat(*cols, ", grp_wall");
		xstrfmtcat(*vals, ", %u", assoc->grp_wall);
		xstrfmtcat(*extra, ", grp_wall=%u", assoc->grp_wall);
	}

	/* this only gets set on a user's association and is_def
	 * could be NO_VAL only 1 is accepted */
	if ((assoc->is_def == 1)
	    && ((qos_level == QOS_LEVEL_MODIFY)
		|| (assoc->user && assoc->cluster && assoc->acct))) {
		xstrcat(*cols, ", is_def");
		xstrcat(*vals, ", 1");
		xstrcat(*extra, ", is_def=1");
	}

	if (assoc->max_jobs == INFINITE) {
		xstrcat(*cols, ", max_jobs");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_jobs=NULL");
	} else if ((assoc->max_jobs != NO_VAL)
		   && ((int32_t)assoc->max_jobs >= 0)) {
		xstrcat(*cols, ", max_jobs");
		xstrfmtcat(*vals, ", %u", assoc->max_jobs);
		xstrfmtcat(*extra, ", max_jobs=%u", assoc->max_jobs);
	}

	if (assoc->max_jobs_accrue == INFINITE) {
		xstrcat(*cols, ", max_jobs_accrue");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_jobs_accrue=NULL");
	} else if ((assoc->max_jobs_accrue != NO_VAL)
		   && ((int32_t)assoc->max_jobs_accrue >= 0)) {
		xstrcat(*cols, ", max_jobs_accrue");
		xstrfmtcat(*vals, ", %u", assoc->max_jobs_accrue);
		xstrfmtcat(*extra, ", max_jobs_accrue=%u",
			   assoc->max_jobs_accrue);
	}

	if (assoc->min_prio_thresh == INFINITE) {
		xstrcat(*cols, ", min_prio_thresh");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", min_prio_thresh=NULL");
	} else if ((assoc->min_prio_thresh != NO_VAL)
		   && ((int32_t)assoc->min_prio_thresh >= 0)) {
		xstrcat(*cols, ", min_prio_thresh");
		xstrfmtcat(*vals, ", %u", assoc->min_prio_thresh);
		xstrfmtcat(*extra, ", min_prio_thresh=%u",
			   assoc->min_prio_thresh);
	}

	if (assoc->max_submit_jobs == INFINITE) {
		xstrcat(*cols, ", max_submit_jobs");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_submit_jobs=NULL");
	} else if ((assoc->max_submit_jobs != NO_VAL)
		   && ((int32_t)assoc->max_submit_jobs >= 0)) {
		xstrcat(*cols, ", max_submit_jobs");
		xstrfmtcat(*vals, ", %u", assoc->max_submit_jobs);
		xstrfmtcat(*extra, ", max_submit_jobs=%u",
			   assoc->max_submit_jobs);
	}

	if (assoc->max_wall_pj == INFINITE) {
		xstrcat(*cols, ", max_wall_pj");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_wall_pj=NULL");
	} else if ((assoc->max_wall_pj != NO_VAL)
		   && ((int32_t)assoc->max_wall_pj >= 0)) {
		xstrcat(*cols, ", max_wall_pj");
		xstrfmtcat(*vals, ", %u", assoc->max_wall_pj);
		xstrfmtcat(*extra, ", max_wall_pj=%u", assoc->max_wall_pj);
	}

	if (assoc->priority == INFINITE) {
		xstrcat(*cols, ", priority");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", priority=NULL");
	} else if ((assoc->priority != NO_VAL)
		   && ((int32_t)assoc->priority >= 0)) {
		xstrcat(*cols, ", priority");
		xstrfmtcat(*vals, ", %u", assoc->priority);
		xstrfmtcat(*extra, ", priority=%u", assoc->priority);
	}

	if (assoc->def_qos_id == INFINITE) {
		xstrcat(*cols, ", def_qos_id");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", def_qos_id=NULL");
	} else if ((assoc->def_qos_id != NO_VAL)
		   && ((int32_t)assoc->def_qos_id > 0)) {
		assoc_mgr_lock(&locks);
		if (!list_find_first(assoc_mgr_qos_list,
		    slurmdb_find_qos_in_list, &(assoc->def_qos_id))) {
			assoc_mgr_unlock(&locks);
			return ESLURM_INVALID_QOS;
		}
		assoc_mgr_unlock(&locks);
		xstrcat(*cols, ", def_qos_id");
		xstrfmtcat(*vals, ", %u", assoc->def_qos_id);
		xstrfmtcat(*extra, ", def_qos_id=%u", assoc->def_qos_id);
	}

	/* When modifying anything below this comment it happens in
	 * the actual function since we have to wait until we hear
	 * about the parent first.
	 * What we do to make it known something needs to be changed
	 * is we cat "" onto extra which will inform the caller
	 * something needs changing.
	 */

	if (assoc->grp_tres) {
		if (qos_level == QOS_LEVEL_MODIFY) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", grp_tres");
		slurmdb_combine_tres_strings(
			&assoc->grp_tres, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", assoc->grp_tres);
		xstrfmtcat(*extra, ", grp_tres='%s'", assoc->grp_tres);
	}

	if (assoc->grp_tres_mins) {
		if (qos_level == QOS_LEVEL_MODIFY) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", grp_tres_mins");
		slurmdb_combine_tres_strings(
			&assoc->grp_tres_mins, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", assoc->grp_tres_mins);
		xstrfmtcat(*extra, ", grp_tres_mins='%s'",
			   assoc->grp_tres_mins);
	}

	if (assoc->grp_tres_run_mins) {
		if (qos_level == QOS_LEVEL_MODIFY) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", grp_tres_run_mins");
		slurmdb_combine_tres_strings(
			&assoc->grp_tres_run_mins, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", assoc->grp_tres_run_mins);
		xstrfmtcat(*extra, ", grp_tres_run_mins='%s'",
			   assoc->grp_tres_run_mins);
	}

	if (assoc->max_tres_pj) {
		if (qos_level == QOS_LEVEL_MODIFY) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_pj");
		slurmdb_combine_tres_strings(
			&assoc->max_tres_pj, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", assoc->max_tres_pj);
		xstrfmtcat(*extra, ", max_tres_pj='%s'", assoc->max_tres_pj);
	}

	if (assoc->max_tres_pn) {
		if (qos_level == QOS_LEVEL_MODIFY) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_pn");
		slurmdb_combine_tres_strings(
			&assoc->max_tres_pn, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", assoc->max_tres_pn);
		xstrfmtcat(*extra, ", max_tres_pn='%s'", assoc->max_tres_pn);
	}

	if (assoc->max_tres_mins_pj) {
		if (qos_level == QOS_LEVEL_MODIFY) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_mins_pj");
		slurmdb_combine_tres_strings(
			&assoc->max_tres_mins_pj, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", assoc->max_tres_mins_pj);
		xstrfmtcat(*extra, ", max_tres_mins_pj='%s'",
			   assoc->max_tres_mins_pj);
	}

	if (assoc->max_tres_run_mins) {
		if (qos_level == QOS_LEVEL_MODIFY) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_run_mins");
		slurmdb_combine_tres_strings(
			&assoc->max_tres_run_mins, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", assoc->max_tres_run_mins);
		xstrfmtcat(*extra, ", max_tres_run_mins='%s'",
			   assoc->max_tres_run_mins);
	}

	if (assoc->qos_list && list_count(assoc->qos_list)) {
		char *qos_type = "qos";
		char *qos_val = NULL;
		char *tmp_char = NULL;
		int set = 0;
		ListIterator qos_itr;

		if (qos_level == QOS_LEVEL_MODIFY) {
			xstrcat(*extra, "");
			goto end_modify;
		}

		qos_itr = list_iterator_create(assoc->qos_list);
		while ((tmp_char = list_next(qos_itr))) {
			/* we don't want to include blank names */
			if (!tmp_char[0])
				continue;
			if (!set) {
				if (tmp_char[0] == '+' || tmp_char[0] == '-')
					qos_type = "delta_qos";
				set = 1;
			}
			xstrfmtcat(qos_val, ",%s", tmp_char);
		}

		list_iterator_destroy(qos_itr);
		if (qos_val) {
			xstrfmtcat(*cols, ", %s", qos_type);
			xstrfmtcat(*vals, ", '%s,'", qos_val);
			xstrfmtcat(*extra, ", %s='%s,'", qos_type, qos_val);
			xfree(qos_val);
		}
	} else if ((qos_level == QOS_LEVEL_SET) && default_qos_str) {
		/* Add default qos to the account */
		xstrcat(*cols, ", qos");
		xstrfmtcat(*vals, ", '%s,'", default_qos_str);
		xstrfmtcat(*extra, ", qos='%s,'", default_qos_str);
		if (!assoc->qos_list)
			assoc->qos_list = list_create(xfree_ptr);
		slurm_addto_char_list(assoc->qos_list, default_qos_str);
	} else if (qos_level != QOS_LEVEL_MODIFY) {
		/* clear the qos */
		xstrcat(*cols, ", qos, delta_qos");
		xstrcat(*vals, ", '', ''");
		xstrcat(*extra, ", qos='', delta_qos=''");
	}
end_modify:

	return SLURM_SUCCESS;

}

/* This is called by most modify functions to alter the table and
 * insert a new line in the transaction table.
 */
extern int modify_common(kingbase_conn_t *kingbase_conn,
			 uint16_t type,
			 time_t now,
			 char *user_name,
			 char *table,
			 char *cond_char,
			 char *vals,
			 char *cluster_name)
{
	char *query = NULL;
	int rc = SLURM_SUCCESS;
	char *tmp_cond_char = slurm_add_slash_to_quotes2(cond_char);
	char *tmp_vals = NULL;
	bool cluster_centric = true;

	/* figure out which tables we need to append the cluster name to */
	if ((table == cluster_table) || (table == acct_coord_table)
	    || (table == acct_table) || (table == qos_table)
	    || (table == txn_table) || (table == user_table)
	    || (table == res_table) || (table == clus_res_table)
	    || (table == federation_table))
		cluster_centric = false;

	if (vals && vals[1])
		tmp_vals = slurm_add_slash_to_quotes2(vals+2);

	if (cluster_centric) {
		xassert(cluster_name);
		xstrfmtcat(query,
			   "update `%s_%s` set mod_time=%ld%s "
			   "where deleted=0 and %s;",
			   cluster_name, table, now, vals, cond_char);
		xstrfmtcat(query,
			   "insert into %s "
			   "(timestamp, action, name, cluster, actor, info) "
			   "values (%ld, %d, '%s', '%s', '%s', '%s');",
			   txn_table,
			   now, type, tmp_cond_char, cluster_name,
			   user_name, tmp_vals);
	} else {
		xstrfmtcat(query,
			   "update %s set mod_time=%ld%s "
			   "where deleted=0 and %s;",
			   table, now, vals, cond_char);
		xstrfmtcat(query,
			   "insert into %s "
			   "(timestamp, action, name, actor, info) "
			   "values (%ld, %d, '%s', '%s', '%s');",
			   txn_table,
			   now, type, tmp_cond_char, user_name, tmp_vals);
	}
	xfree(tmp_cond_char);
	xfree(tmp_vals);
	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	
     
	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t* data_rt = NULL;
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag); 
	xfree(query);
	if (rc == SLURM_ERROR ) {
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

/* Every option in assoc_char should have a 't1.' infront of it. */
extern int remove_common(kingbase_conn_t *kingbase_conn,
			 uint16_t type,
			 time_t now,
			 char *user_name,
			 char *table,
			 char *name_char,
			 char *assoc_char,
			 char *cluster_name,
			 List ret_list,
			 bool *jobs_running,
			 bool *default_account)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL;
	char *loc_assoc_char = NULL, *loc_usage_id_char = NULL;
	KCIResult *result = NULL;
	int row;
	time_t day_old = now - DELETE_SEC_BACK;
	bool has_jobs = false;
	char *tmp_name_char = NULL;
	bool cluster_centric = true;
	uint32_t smallest_lft = 0xFFFFFFFF;

	/* figure out which tables we need to append the cluster name to */
	if ((table == cluster_table) || (table == acct_coord_table)
	    || (table == acct_table) || (table == qos_table)
	    || (table == txn_table) || (table == user_table)
	    || (table == res_table) || (table == clus_res_table)
	    || (table == federation_table))
		cluster_centric = false;

	if (((table == assoc_table) || (table == acct_table))) {
		if (_check_is_def_acct_before_remove(kingbase_conn,
						     cluster_name,
						     assoc_char,
						     ret_list,
						     default_account))
			return SLURM_SUCCESS;
	}

	/* If we have jobs associated with this we do not want to
	 * really delete it for accounting purposes.  This is for
	 * corner cases most of the time this won't matter.
	 */
	if ((table == acct_coord_table) || (table == res_table)
	    || (table == clus_res_table) || (table == federation_table)) {
		/* This doesn't apply for these tables since we are
		 * only looking for association type tables.
		 */
	} else if ((table == qos_table) || (table == wckey_table)) {
		if (cluster_name)
			has_jobs = _check_jobs_before_remove_without_assoctable(
				kingbase_conn, cluster_name, assoc_char);
	} else if (table != assoc_table) {
		/* first check to see if we are running jobs now */
		if (_check_jobs_before_remove(
			    kingbase_conn, cluster_name, assoc_char,
			    ret_list, jobs_running) || (*jobs_running))
			return SLURM_SUCCESS;

		has_jobs = _check_jobs_before_remove(
			kingbase_conn, cluster_name, assoc_char, NULL, NULL);
	} else {
		/* first check to see if we are running jobs now */
		if (_check_jobs_before_remove_assoc(
			    kingbase_conn, cluster_name, name_char,
			    ret_list, jobs_running) || (*jobs_running))
			return SLURM_SUCCESS;

		/* now check to see if any jobs were ever run. */
		has_jobs = _check_jobs_before_remove_assoc(
			kingbase_conn, cluster_name, name_char,
			NULL, NULL);
	}
	/* we want to remove completely all that is less than a day old */
	if (!has_jobs && table != assoc_table) {
		if (cluster_centric) {
			query = xstrdup_printf("delete from `%s_%s` where "
					       "creation_time>%ld and (%s);",
					       cluster_name, table, day_old,
					       name_char);
		} else {
			query = xstrdup_printf("delete from %s where "
					       "creation_time>%ld and (%s);",
					       table, day_old, name_char);
		}
	}

	if (table != assoc_table) {
		if (cluster_centric) {
			xstrfmtcat(query,
				   "update `%s_%s` set mod_time=%ld, "
				   "deleted=1 where deleted=0 and (%s);",
				   cluster_name, table, now, name_char);
		} else if (table == federation_table) {
			xstrfmtcat(query,
				   "update %s set "
				   "mod_time=%ld, deleted=1, "
				   "flags=DEFAULT "
				   "where deleted=0 and (%s);",
				   federation_table, now,
				   name_char);
		} else if (table == qos_table) {
			xstrfmtcat(query,
				   "update %s set "
				   "mod_time=%ld, deleted=1, "
				   "grace_time=DEFAULT, "
				   "max_jobs_pa=DEFAULT, "
				   "max_jobs_per_user=DEFAULT, "
				   "max_jobs_accrue_pa=DEFAULT, "
				   "max_jobs_accrue_pu=DEFAULT, "
				   "min_prio_thresh=DEFAULT, "
				   "max_submit_jobs_pa=DEFAULT, "
				   "max_submit_jobs_per_user=DEFAULT, "
				   "max_tres_pa=DEFAULT, "
				   "max_tres_pj=DEFAULT, "
				   "max_tres_pn=DEFAULT, "
				   "max_tres_pu=DEFAULT, "
				   "max_tres_mins_pj=DEFAULT, "
				   "max_tres_run_mins_pa=DEFAULT, "
				   "max_tres_run_mins_pu=DEFAULT, "
				   "min_tres_pj=DEFAULT, "
				   "max_wall_duration_per_job=DEFAULT, "
				   "grp_jobs=DEFAULT, grp_submit_jobs=DEFAULT, "
				   "grp_jobs_accrue=DEFAULT, grp_tres=DEFAULT, "
				   "grp_tres_mins=DEFAULT, "
				   "grp_tres_run_mins=DEFAULT, "
				   "grp_wall=DEFAULT, "
				   "preempt=DEFAULT, "
				   "preempt_exempt_time=DEFAULT, "
				   "priority=DEFAULT, "
				   "usage_factor=DEFAULT, "
				   "usage_thres=DEFAULT, "
				   "limit_factor=DEFAULT "
				   "where deleted=0 and (%s);",
				   qos_table, now, name_char);
		} else {
			xstrfmtcat(query,
				   "update %s set mod_time=%ld, deleted=1 "
				   "where deleted=0 and (%s);",
				   table, now, name_char);
		}
	}

	/* If we are removing assocs use the assoc_char since the
	   name_char has lft between statements that can change over
	   time.  The assoc_char has the actual ids of the assocs
	   which never change.
	*/
	if (type == DBD_REMOVE_ASSOCS && assoc_char)
		tmp_name_char = slurm_add_slash_to_quotes2(assoc_char);
	else
		tmp_name_char = slurm_add_slash_to_quotes2(name_char);

	if (cluster_centric)
		xstrfmtcat(query,
			   "insert into %s (timestamp, action, name, "
			   "actor, cluster) values "
			   "(%ld, %d, '%s', '%s', '%s');",
			   txn_table,
			   now, type, tmp_name_char, user_name, cluster_name);
	else
		xstrfmtcat(query,
			   "insert into %s (timestamp, action, name, actor) "
			   "values (%ld, %d, '%s', '%s');",
			   txn_table,
			   now, type, tmp_name_char, user_name);

	xfree(tmp_name_char);

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	
	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t* data_rt = NULL;

	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);
	xfree(query);
	if (rc == SLURM_ERROR ) {
		reset_kingbase_conn(kingbase_conn);
		return SLURM_ERROR;
	}else if((table == acct_coord_table)
		|| (table == wckey_table)
		|| (table == clus_res_table)
		|| (table == res_table)
		|| (table == federation_table)
		|| (table == qos_table)) { 
			return SLURM_SUCCESS;
	}
	/* mark deleted=1 or remove completely the accounting tables
	 */
	if (table != assoc_table) {
		if (!assoc_char) {
			error("no assoc_char");
			if (kingbase_conn->rollback) {
				kingbase_db_rollback(kingbase_conn);
			}
			list_flush(kingbase_conn->update_list);
			return SLURM_ERROR;
		}

		/* If we are doing this on an assoc_table we have
		   already done this, so don't */
		query = xstrdup_printf("select distinct t1.id_assoc "
				       "from `%s_%s` as t1, `%s_%s` as t2 "
				       "where (%s) and t1.lft between "
				       "t2.lft and t2.rgt and t1.deleted=0 "
				       "and t2.deleted=0 order by t1.id_assoc;",
				       cluster_name, assoc_table,
				       cluster_name, assoc_table, assoc_char);

		DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		result = kingbase_db_query_ret(kingbase_conn, query, 0);
		if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
			xfree(query);
			if (kingbase_conn->rollback) {
				kingbase_db_rollback(kingbase_conn);
			}
			list_flush(kingbase_conn->update_list);
			KCIResultDealloc(result);
			return SLURM_ERROR;
		}
		xfree(query);

		rc = 0;
		xfree(loc_assoc_char);
		int tmp_inx = 0;
        row = KCIResultGetRowCount(result);
		while (tmp_inx < row) {
			slurmdb_assoc_rec_t *rem_assoc = NULL;
			if (loc_assoc_char)
				xstrcat(loc_assoc_char, " or ");
			xstrfmtcat(loc_assoc_char, "id_assoc=%s", KCIResultGetColumnValue(result,tmp_inx,0));

			rem_assoc = xmalloc(sizeof(slurmdb_assoc_rec_t));
			rem_assoc->id = slurm_atoul(KCIResultGetColumnValue(result, tmp_inx, 0 ));
			rem_assoc->cluster = xstrdup(cluster_name);
			if (addto_update_list(kingbase_conn->update_list,
					      SLURMDB_REMOVE_ASSOC,
					      rem_assoc) != SLURM_SUCCESS)
				error("couldn't add to the update list");
			tmp_inx++;
		}
		KCIResultDealloc(result);
	} else
		loc_assoc_char = assoc_char;

	if (!loc_assoc_char) {
		debug2("No associations with object being deleted");
		return rc;
	}

	loc_usage_id_char = xstrdup(loc_assoc_char);
	xstrsubstituteall(loc_usage_id_char, "id_assoc", "id");

	/* We should not have to delete from usage table, only flag since we
	 * only delete things that are typos.
	 */
	xstrfmtcat(query,
		   "update `%s_%s` set mod_time=%ld, deleted=1 where (%s);"
		   "update `%s_%s` set mod_time=%ld, deleted=1 where (%s);"
		   "update `%s_%s` set mod_time=%ld, deleted=1 where (%s);",
		   cluster_name, assoc_day_table, now, loc_usage_id_char,
		   cluster_name, assoc_hour_table, now, loc_usage_id_char,
		   cluster_name, assoc_month_table, now, loc_usage_id_char);
	xfree(loc_usage_id_char);

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s %zu",
	         query, strlen(query));
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	xfree(query);
	free_res_data(data_rt, fetch_flag); 
	if (rc != SLURM_SUCCESS) {
		reset_kingbase_conn(kingbase_conn);
		return SLURM_ERROR;
	}
	/* If we have jobs that have ran don't go through the logic of
	 * removing the associations. Since we may want them for
	 * reports in the future since jobs had ran.
	 */
	if (has_jobs)
		goto just_update;

	/* remove completely all the associations for this added in the last
	 * day, since they are most likely nothing we really wanted in
	 * the first place.
	 */
	query = xstrdup_printf("select id_assoc from `%s_%s` as t1 where "
			       "creation_time>%ld and (%s);",
			       cluster_name, assoc_table,
			       day_old, loc_assoc_char);

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		reset_kingbase_conn(kingbase_conn);
		return SLURM_ERROR;
	}
	xfree(query);

	row = 0 ;
    row = KCIResultGetRowCount(result);
	int tmp_inx2 = 0;
	while (tmp_inx2 < row) {
        int tmp_count = tmp_inx2;
		KCIResult *result2 = NULL;
		int row2;
		uint32_t lft;
        tmp_inx2++;
		/* we have to do this one at a time since the lft's and rgt's
		   change. If you think you need to remove this make
		   sure your new way can handle changing lft and rgt's
		   in the association. */
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
		xstrfmtcat(query,
			   "SELECT lft, rgt, (rgt - lft + 1) "
			   "FROM `%s_%s` WHERE id_assoc = %s FOR UPDATE;",
			   cluster_name, assoc_table, KCIResultGetColumnValue(result,tmp_count,0));
#else
		xstrfmtcat(query,
			   "SELECT lft, rgt, (rgt - lft + 1) "
			   "FROM `%s_%s` WHERE id_assoc = %s;",
			   cluster_name, assoc_table, KCIResultGetColumnValue(result,tmp_count,0));
#endif	
		DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		result2 = kingbase_db_query_ret(kingbase_conn, query, 0);
		if (KCIResultGetStatusCode(result2) != EXECUTE_TUPLES_OK) {
			KCIResultDealloc(result2);
			xfree(query);
			rc = SLURM_ERROR;
			break;
		}
		xfree(query);
		row2 = KCIResultGetRowCount(result2);
		if (!row2) {
			KCIResultDealloc(result2);
			continue;
		}

		xstrfmtcat(query,
			   "delete from `%s_%s` where "
			   "lft between %s AND %s;",
			   cluster_name, assoc_table, KCIResultGetColumnValue(result2,0,0), KCIResultGetColumnValue(result2,0,1));

		xstrfmtcat(query,
			   "UPDATE `%s_%s` SET rgt = rgt - %s WHERE rgt > %s;"
			   "UPDATE `%s_%s` SET "
			   "lft = lft - %s WHERE lft > %s;",
			   cluster_name, assoc_table, KCIResultGetColumnValue(result2,0,2), KCIResultGetColumnValue(result2,0,1),
			   cluster_name, assoc_table, KCIResultGetColumnValue(result2,0,2), KCIResultGetColumnValue(result2,0,1));

		lft = slurm_atoul(KCIResultGetColumnValue(result2,0,0));
		if (lft < smallest_lft)
			smallest_lft = lft;

		KCIResultDealloc(result2);

		DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);


		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		xfree(query);
		if (rc != SLURM_SUCCESS) {
			free_res_data(data_rt, fetch_flag); 
			error("couldn't remove assoc");
			break;
		}
		free_res_data(data_rt, fetch_flag); 
	}

	KCIResultDealloc(result);
	/* This already happened before, but we need to run it again
	   since the first time we ran it we didn't know if we were
	   going to remove the above associations.
	*/
	if (rc == SLURM_SUCCESS)
		rc = as_kingbase_get_modified_lfts(kingbase_conn,
						cluster_name, smallest_lft);

	if (rc == SLURM_ERROR) {
		reset_kingbase_conn(kingbase_conn);
		return rc;
	}

just_update:
	/* now update the associations themselves that are still
	 * around clearing all the limits since if we add them back
	 * we don't want any residue from past associations lingering
	 * around.
	 */
	query = xstrdup_printf("update `%s_%s` as t1 set "
			       "mod_time=%ld, deleted=1, def_qos_id=DEFAULT, "
			       "shares=DEFAULT, max_jobs=DEFAULT, "
			       "max_jobs_accrue=DEFAULT, "
			       "min_prio_thresh=DEFAULT, "
			       "max_submit_jobs=DEFAULT, "
			       "max_wall_pj=DEFAULT, "
			       "max_tres_pj=DEFAULT, "
			       "max_tres_pn=DEFAULT, "
			       "max_tres_mins_pj=DEFAULT, "
			       "max_tres_run_mins=DEFAULT, "
			       "grp_jobs=DEFAULT, grp_submit_jobs=DEFAULT, "
			       "grp_jobs_accrue=DEFAULT, grp_wall=DEFAULT, "
			       "grp_tres=DEFAULT, "
			       "grp_tres_mins=DEFAULT, "
			       "grp_tres_run_mins=DEFAULT, "
			       "qos=DEFAULT, delta_qos=DEFAULT, "
			       "priority=DEFAULT, is_def=DEFAULT "
			       "where (%s);",
			       cluster_name, assoc_table, now,
			       loc_assoc_char);

	if (table != assoc_table)
		xfree(loc_assoc_char);

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	xfree(query);
	if (rc != SLURM_SUCCESS) {
		reset_kingbase_conn(kingbase_conn);
	}
    free_res_data(data_rt, fetch_flag); 
	return rc;
}

extern void mod_tres_str(char **out, char *mod, char *cur,
			 char *cur_par, char *name, char **vals,
			 uint32_t id, bool assoc)
{
	uint32_t tres_str_flags = TRES_STR_FLAG_REMOVE |
		TRES_STR_FLAG_SORT_ID | TRES_STR_FLAG_SIMPLE |
		TRES_STR_FLAG_NO_NULL;

	xassert(out);
	xassert(name);

	if (!mod)
		return;

	/* We have to add strings in waves or we will not be able to
	 * get removes to work correctly.  We want the string returned
	 * after the first slurmdb_combine_tres_strings to be put in
	 * the database.
	 */
	xfree(*out); /* just to make sure */
	*out = xstrdup(mod);
	slurmdb_combine_tres_strings(out, cur, tres_str_flags);

	if (xstrcmp(*out, cur)) {
		if (vals) {
			/* This logic is here because while the change
			 * we are doing on the limit is the same for
			 * each limit the other limits on the
			 * associations might not be.  What this does
			 * is only change the limit on the association
			 * given the id.  I'm hoping someone in the
			 * future comes up with a better way to do
			 * this since this seems like a hack, but it
			 * does do the job.
			 */
			xstrfmtcat(*vals, ", %s = "
				   "if (%s=%u, '%s', %s)",
				   name, assoc ? "id_assoc" : "id", id,
				   *out, name);
			/* xstrfmtcat(*vals, ", %s='%s%s')", */
			/* 	   name, */
			/* 	   *out[0] ? "," : "", */
			/* 	   *out); */
		}
		if (cur_par)
			slurmdb_combine_tres_strings(
				out, cur_par, tres_str_flags);
	} else
		xfree(*out);
}

/*
KINGBASE_ROW kingbase_fetch_row(KCIResult *result,int row, int column)
{
	//Get the number of rows and columns of result
	int row = 0;
	int column = 0;
	char *element;
	row = KCIResultGetRowCount(result);
	column = KCIResultGetColumnCount(result);
    element = KCIResultGetColumnValue(result, row, column);
	return element;
}
*/

int _get_database_variable(kingbase_conn_t *kingbase_conn,
				  const char *variable_name, uint64_t *value)
{
	KCIResult *result = NULL;
	char *err_check = NULL;
	char *query;
	//int row = 0;

	query = xstrdup_printf("select name,setting from sys_settings where name =\'%s\';",
			        variable_name);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);				
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		error("%s: null result from query `%s`", __func__, query);
		xfree(query);
		return SLURM_ERROR;
	}

	if (KCIResultGetRowCount(result) != 1) {
		error("%s: invalid results from query `%s`", __func__, query);
		xfree(query);
		KCIResultDealloc(result);
		return SLURM_ERROR;
	}

	xfree(query);

	//row = KCIResultGetRowCount(result);
	*value = (uint64_t) strtoll(KCIResultGetColumnValue(result, 0, 1), &err_check, 10);

	if (*err_check) {
		error("%s: error parsing string to int `%s`", __func__, KCIResultGetColumnValue(result, 0, 1));
		KCIResultDealloc(result);
		return SLURM_ERROR;
	}
	KCIResultDealloc(result);

	return SLURM_SUCCESS;
}

/*
 * kingbase version 5.6.48 and 5.7.30 introduced a regression in the
 * implementation of CONCAT() that will lead to incorrect NULL values.
 *
 * We cannot safely work around this mistake without restructing our stored
 * procedures, and thus fatal() here to avoid a segfault.
 *
 * Test that concat() is working as expected, rather than trying to blacklist
 * specific versions.
 */
static void _check_kingbase_concat_is_sane(kingbase_conn_t *kingbase_conn)
{
	//kingbase_ROW row = NULL;
	int row = 0;
	KCIResult *result = NULL;
	char *query = "select concat('');";
	int version = KCIConnectionGetServerVersion(kingbase_conn->db_conn);

	info("kingbase server version is: %d", version);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		fatal("%s: null result from query `%s`", __func__, query);
	}
	if (KCIResultGetRowCount(result) != 1) {
		fatal("%s: invalid results from query `%s`", __func__, query);
	}
    row = KCIResultGetRowCount(result);
	if ((!row) || !KCIResultGetColumnValue(result, 0, 0)) {
		fatal("kingbase concat() function is defective. Please upgrade to a fixed version. See https://bugs.kingbase.com/bug.php?id=99485.");
	}

	KCIResultDealloc(result);
}

/*
 * init sql func last_insert_id
 */
// static int last_insert_id_init(kingbase_conn_t *kingbase_conn)
// {
// 	int rc = SLURM_SUCCESS;
// 	if (!update_id_init) {
//		update_id_init = true;
// 		char *query2 = "set SQLTERM /";
// 		char tmp[1] = {'\\'};
// 		char *tmp2 = xmalloc(strlen(query2) + 2);
// 		sprintf(tmp2,"%c%s",tmp[0],query2);

// 		rc = kingbase_db_query(kingbase_conn, tmp2);
// 		if (rc == SLURM_SUCCESS) {
// 			xfree(tmp2);
// 			rc = kingbase_db_query(kingbase_conn, 
// 				"create or replace function last_insert_id() returns int as begin return lastval();exception when others then return 0;end;");
// 			if (rc == SLURM_SUCCESS) {
// 				rc = kingbase_db_query(kingbase_conn, "/");	
// 				if ( rc != SLURM_SUCCESS) {
// 					fatal("last_insert_id init failed");					
// 					return rc;
// 				}
// 				return rc;
// 			} else {
// 				fatal("last_insert_id init failed");
// 				return rc;			
// 			}
// 		} else {
// 			fatal("last_insert_id init failed");
// 			xfree(tmp2);
// 			return rc;
// 		}
// 	}
// 	return rc;	
// }

/*
 * Check the values of innodb global database variables, and print
 * an error if the values are not at least half the recommendation.
 */
int _check_database_variables(kingbase_conn_t *kingbase_conn)
{
	 ///effective_cache_size,log_rotation_size, log_lock_waits
	const char buffer_var[] = "effective_cache_size";
	const char logfile_var[] = "log_rotation_size";
	const char lockwait_var[] = "deadlock_timeout";
	//const uint64_t lockwait_timeout = 900;

	uint64_t  value ;

	char *error_msg = xstrdup("Database settings not recommended values:");

	if (_get_database_variable(kingbase_conn, buffer_var, &value)) 
		goto error;
    debug2("%s: %"PRIu64, buffer_var, value);
	if (_get_database_variable(kingbase_conn, logfile_var, &value))
		goto error;
	debug2("%s: %"PRIu64, logfile_var, value);

	if (_get_database_variable(kingbase_conn, lockwait_var, &value)) 
		goto error;
	debug2("%s: %"PRIu64, lockwait_var, value);
	
	xfree(error_msg);
	return SLURM_SUCCESS;

error:

	xfree(error_msg);
	return SLURM_ERROR;
}

/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */

extern int init(void)
{
	int rc = SLURM_SUCCESS;
	kingbase_conn_t *kingbase_conn = NULL;
	if (slurmdbd_conf->dbd_backup) {
		char node_name_short[128];
		char node_name_long[128];
		if (gethostname(node_name_long, sizeof(node_name_long))) {
			fatal("getnodename: %m");
		}	
		if (gethostname_short(node_name_short, sizeof(node_name_short))) {
			fatal("getnodename_short: %m");
		}

		if (!xstrcmp(node_name_short, slurmdbd_conf->dbd_backup) ||
		    !xstrcmp(node_name_long, slurmdbd_conf->dbd_backup) ||
		    !xstrcmp(slurmdbd_conf->dbd_backup, "localhost")) {
			backup_dbd = true;
		}	
	}

	kingbase_db_info = create_kingbase_db_info(SLURM_KINGBASE_PLUGIN_AS);
    kingbase_db_name = acct_get_db_name();

	debug2("kingbase_connect() called for db %s", kingbase_db_name);
	kingbase_conn = create_kingbase_conn(0, 1, NULL);
	while (kingbase_db_get_db_connection(
		       kingbase_conn, kingbase_db_name, kingbase_db_info)
	       != SLURM_SUCCESS) {
		error("The database must be up when starting "
		      "the kingbase plugin.  Trying again in 5 seconds.");
		sleep(5);
	}

	// rc = last_insert_id_init(kingbase_conn);
	// if (rc != SLURM_SUCCESS) {
	// 	error("last_insert_id_init failed");
	// 	verbose("%s failed", plugin_name);
	// 	return rc;
	// }

    _check_kingbase_concat_is_sane(kingbase_conn);
	_check_database_variables(kingbase_conn);

	rc = _as_kingbase_acct_check_tables(kingbase_conn);

	if (rc == SLURM_SUCCESS) {
		if (kingbase_db_commit(kingbase_conn)) {
			error("commit failed, meaning %s failed", plugin_name);
			rc = SLURM_ERROR;
		} else
			verbose("%s loaded", plugin_name);
	} else {
		verbose("%s failed", plugin_name);
		if (kingbase_db_rollback(kingbase_conn))
			error("rollback failed");
	}

	destroy_kingbase_conn(kingbase_conn);
	return rc;
}


extern int fini ( void )
{
	slurm_rwlock_wrlock(&as_kingbase_cluster_list_lock);
	FREE_NULL_LIST(as_kingbase_cluster_list);
	FREE_NULL_LIST(as_kingbase_total_cluster_list);
	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	slurm_rwlock_destroy(&as_kingbase_cluster_list_lock);
	destroy_kingbase_db_info(kingbase_db_info);
	xfree(kingbase_db_name);
	xfree(default_qos_str);

	kingbase_db_cleanup();
	return SLURM_SUCCESS;
}

/*
 * Get the dimensions of this cluster so we know how to deal with the hostlists.
 *
 * IN kingbase_conn - kingbase connection
 * IN cluster_name - name of cluster to get dimensions for
 * OUT dims - dimenions of cluster
 *
 * RET return SLURM_SUCCESS on success, SLURM_FAILURE otherwise.
 */
extern int get_cluster_dims(kingbase_conn_t *kingbase_conn, char *cluster_name,
			    int *dims)
{
	char *query;
	int row = 0;
	bool tmp_flag = false;
	KCIResult *result = NULL;

	query = xstrdup_printf("select dimensions from %s where name='%s'",
			       cluster_table, cluster_name);

	debug4("%d(%s:%d) query\n%s",
	       kingbase_conn->conn, THIS_FILE, __LINE__, query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	   
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return SLURM_ERROR;
	}
	xfree(query);

    row = KCIResultGetRowCount(result);
	for (int i = 0; i < row ;i++) {
		if(KCIResultGetColumnValue(result, 0, i)) {
			tmp_flag = true;
		}
	}
	if (!tmp_flag) {
		error("Couldn't get the dimensions of cluster '%s'.",
		      cluster_name);
			KCIResultDealloc(result);
		return SLURM_ERROR;
	}
	*dims = atoi(KCIResultGetColumnValue(result, 0, 0));
	KCIResultDealloc(result);

	return SLURM_SUCCESS;
}

extern void *acct_storage_p_get_connection(
	int conn_num, uint16_t *persist_conn_flags,
	bool rollback, char *cluster_name)
{
	kingbase_conn_t *kingbase_conn = NULL;

	debug2("acct_storage_p_get_connection: request new connection %d",
	       rollback);

	if (!(kingbase_conn = create_kingbase_conn(
		      conn_num, rollback, cluster_name))) {
		fatal("couldn't get a kingbase_conn");
		return NULL;	/* Fix CLANG false positive error */
	}

	errno = SLURM_SUCCESS;
	kingbase_db_get_db_connection(kingbase_conn, kingbase_db_name, kingbase_db_info);

	if (kingbase_conn->db_conn)
		errno = SLURM_SUCCESS;

	return (void *)kingbase_conn;
}

extern int acct_storage_p_close_connection(kingbase_conn_t **kingbase_conn)
{
	int rc;

	if (!kingbase_conn || !(*kingbase_conn))
		return SLURM_SUCCESS;

	acct_storage_p_commit((*kingbase_conn), 0);
	rc = destroy_kingbase_conn(*kingbase_conn);
	*kingbase_conn = NULL;

	return rc;
}

extern int acct_storage_p_commit(kingbase_conn_t *kingbase_conn, bool commit)
{
	int rc = check_connection(kingbase_conn);
	List update_list = NULL;

	/* always reset this here */
	if (kingbase_conn)
		kingbase_conn->cluster_deleted = 0;

	if ((rc != SLURM_SUCCESS) && (rc != ESLURM_CLUSTER_DELETED))
		return rc;
	/*
	 * We should never get here since check_connection will return
	 * ESLURM_DB_CONNECTION when !kingbase_conn, but Coverity doesn't
	 * understand that. CID 44841.
	 */
	xassert(kingbase_conn);

	update_list = list_create(slurmdb_destroy_update_object);
	list_transfer(update_list, kingbase_conn->update_list);
	debug4("got %d commits", list_count(update_list));

	if (kingbase_conn->rollback) {
		if (!commit) {
			if (kingbase_db_rollback(kingbase_conn))
				error("rollback failed");
		} else {
			int rc = SLURM_SUCCESS;
			/*
			 * Handle anything here we were unable to do
			 * because of rollback issues.
			 */
			if (kingbase_conn->pre_commit_query) {
				DB_DEBUG(DB_ASSOC, kingbase_conn->conn,
				         "query\n%s",
				         kingbase_conn->pre_commit_query);
				//info("[query] line %d, %s: query: %s", __LINE__, __func__, kingbase_conn->pre_commit_query);		

				fetch_flag_t* fetch_flag = NULL;
				fetch_result_t* data_rt = NULL;
				fetch_flag = set_fetch_flag(false, false, false);
				data_rt = xmalloc(sizeof(fetch_result_t));
				rc = kingbase_for_fetch(kingbase_conn, kingbase_conn->pre_commit_query, fetch_flag, data_rt);	
				free_res_data(data_rt, fetch_flag); 			 
			}

			if (rc != SLURM_SUCCESS) {
				if (kingbase_db_rollback(kingbase_conn))
					error("rollback failed");
			} else{
				if (kingbase_db_commit(kingbase_conn))
					error("commit failed");
			}
		}
	}

	if (commit && list_count(update_list)) {
		char *query = NULL;
		KCIResult *result = NULL;
		int row = 0;
		ListIterator itr = NULL;
		slurmdb_update_object_t *object = NULL;

		xstrfmtcat(query, "select control_host, control_port, "
			   "name, rpc_version, flags "
			   "from %s where deleted=0 and control_port != 0",
			   cluster_table);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	
		result = kingbase_db_query_ret(kingbase_conn, query, 0);		   
		if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
			KCIResultDealloc(result);
			xfree(query);
			goto skip;
		}
		xfree(query);
		row = KCIResultGetRowCount(result);
		for(int i=0; i< row; i++) {
			if (slurm_atoul(KCIResultGetColumnValue(result, i, 4)) & CLUSTER_FLAG_EXT)
				continue;
			(void) slurmdb_send_accounting_update(
				update_list,
				KCIResultGetColumnValue(result, i, 2), KCIResultGetColumnValue(result, i, 0),
				slurm_atoul(KCIResultGetColumnValue(result, i, 1)),
				slurm_atoul(KCIResultGetColumnValue(result, i, 3)));
		}
		KCIResultDealloc(result);
	skip:
		(void) assoc_mgr_update(update_list, 0);

		slurm_rwlock_wrlock(&as_kingbase_cluster_list_lock);
		itr = list_iterator_create(update_list);
		while ((object = list_next(itr))) {
			if (!object->objects || !list_count(object->objects))
				continue;
			/* We only care about clusters removed here. */
			switch (object->type) {
			case SLURMDB_REMOVE_CLUSTER:
			{
				ListIterator rem_itr = NULL;
				char *rem_cluster = NULL;
				rem_itr = list_iterator_create(object->objects);
				while ((rem_cluster = list_next(rem_itr))) {
					list_delete_all(as_kingbase_cluster_list,
							slurm_find_char_in_list,
							rem_cluster);
				}
				list_iterator_destroy(rem_itr);
				break;
			}
			default:
				break;
			}
		}
		list_iterator_destroy(itr);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	}
	xfree(kingbase_conn->pre_commit_query);
	FREE_NULL_LIST(update_list);

	return SLURM_SUCCESS;
}

extern int acct_storage_p_add_users(kingbase_conn_t *kingbase_conn, uint32_t uid,
				    List user_list)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_add_users(kingbase_conn, uid, user_list);
}

extern int acct_storage_p_add_coord(kingbase_conn_t *kingbase_conn, uint32_t uid,
				    List acct_list,
				    slurmdb_user_cond_t *user_cond)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_add_coord(kingbase_conn, uid, acct_list, user_cond);
}

extern int acct_storage_p_add_accts(kingbase_conn_t *kingbase_conn, uint32_t uid,
				    List acct_list)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_add_accts(kingbase_conn, uid, acct_list);
}

extern int acct_storage_p_add_clusters(kingbase_conn_t *kingbase_conn, uint32_t uid,
				       List cluster_list)
{	
	return as_kingbase_add_clusters(kingbase_conn, uid, cluster_list);
}

extern int acct_storage_p_add_federations(kingbase_conn_t *kingbase_conn,
					  uint32_t uid, List federation_list)
{		
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}	
	return as_kingbase_add_federations(kingbase_conn, uid, federation_list);
}

extern int acct_storage_p_add_tres(kingbase_conn_t *kingbase_conn,
				   uint32_t uid, List tres_list_in)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_add_tres(kingbase_conn, uid, tres_list_in);
}

extern int acct_storage_p_add_assocs(kingbase_conn_t *kingbase_conn,
				     uint32_t uid,
				     List assoc_list)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_add_assocs(kingbase_conn, uid, assoc_list);
}

extern int acct_storage_p_add_qos(kingbase_conn_t *kingbase_conn, uint32_t uid,
				  List qos_list)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_add_qos(kingbase_conn, uid, qos_list);
}

extern int acct_storage_p_add_res(kingbase_conn_t *kingbase_conn, uint32_t uid,
				  List res_list)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_add_res(kingbase_conn, uid, res_list);
}

extern int acct_storage_p_add_wckeys(kingbase_conn_t *kingbase_conn, uint32_t uid,
				     List wckey_list)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_add_wckeys(kingbase_conn, uid, wckey_list);
}

extern int acct_storage_p_add_reservation(kingbase_conn_t *kingbase_conn,
					  slurmdb_reservation_rec_t *resv)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_add_resv(kingbase_conn, resv);
}

extern List acct_storage_p_modify_users(kingbase_conn_t *kingbase_conn, uint32_t uid,
					slurmdb_user_cond_t *user_cond,
					slurmdb_user_rec_t *user)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_modify_users(kingbase_conn, uid, user_cond, user);
}

extern List acct_storage_p_modify_accts(kingbase_conn_t *kingbase_conn, uint32_t uid,
					slurmdb_account_cond_t *acct_cond,
					slurmdb_account_rec_t *acct)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_modify_accts(kingbase_conn, uid, acct_cond, acct);
}

extern List acct_storage_p_modify_clusters(kingbase_conn_t *kingbase_conn,
					   uint32_t uid,
					   slurmdb_cluster_cond_t *cluster_cond,
					   slurmdb_cluster_rec_t *cluster)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_modify_clusters(kingbase_conn, uid, cluster_cond, cluster);
}

extern List acct_storage_p_modify_assocs(
	kingbase_conn_t *kingbase_conn, uint32_t uid,
	slurmdb_assoc_cond_t *assoc_cond,
	slurmdb_assoc_rec_t *assoc)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_modify_assocs(kingbase_conn, uid, assoc_cond, assoc);
}

extern List acct_storage_p_modify_federations(
				kingbase_conn_t *kingbase_conn, uint32_t uid,
				slurmdb_federation_cond_t *fed_cond,
				slurmdb_federation_rec_t *fed)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_modify_federations(kingbase_conn, uid, fed_cond, fed);
}

extern List acct_storage_p_modify_job(kingbase_conn_t *kingbase_conn, uint32_t uid,
				      slurmdb_job_cond_t *job_cond,
				      slurmdb_job_rec_t *job)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_modify_job(kingbase_conn, uid, job_cond, job);
}

extern List acct_storage_p_modify_qos(kingbase_conn_t *kingbase_conn, uint32_t uid,
				      slurmdb_qos_cond_t *qos_cond,
				      slurmdb_qos_rec_t *qos)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_modify_qos(kingbase_conn, uid, qos_cond, qos);
}

extern List acct_storage_p_modify_res(kingbase_conn_t *kingbase_conn,
				      uint32_t uid,
				      slurmdb_res_cond_t *res_cond,
				      slurmdb_res_rec_t *res)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_modify_res(kingbase_conn, uid, res_cond, res);
}

extern List acct_storage_p_modify_wckeys(kingbase_conn_t *kingbase_conn,
					 uint32_t uid,
					 slurmdb_wckey_cond_t *wckey_cond,
					 slurmdb_wckey_rec_t *wckey)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_modify_wckeys(kingbase_conn, uid, wckey_cond, wckey);
}

extern int acct_storage_p_modify_reservation(kingbase_conn_t *kingbase_conn,
					     slurmdb_reservation_rec_t *resv)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_modify_resv(kingbase_conn, resv);
}

extern List acct_storage_p_remove_users(kingbase_conn_t *kingbase_conn, uint32_t uid,
					slurmdb_user_cond_t *user_cond)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_remove_users(kingbase_conn, uid, user_cond);
}

extern List acct_storage_p_remove_coord(kingbase_conn_t *kingbase_conn, uint32_t uid,
					List acct_list,
					slurmdb_user_cond_t *user_cond)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_remove_coord(kingbase_conn, uid, acct_list, user_cond);
}

extern List acct_storage_p_remove_accts(kingbase_conn_t *kingbase_conn, uint32_t uid,
					slurmdb_account_cond_t *acct_cond)
{
	
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_remove_accts(kingbase_conn, uid, acct_cond);
}

extern List acct_storage_p_remove_clusters(kingbase_conn_t *kingbase_conn,
					   uint32_t uid,
					   slurmdb_cluster_cond_t *cluster_cond)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_remove_clusters(kingbase_conn, uid, cluster_cond);
}

extern List acct_storage_p_remove_assocs(
	kingbase_conn_t *kingbase_conn, uint32_t uid,
	slurmdb_assoc_cond_t *assoc_cond)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_remove_assocs(kingbase_conn, uid, assoc_cond);
}

extern List acct_storage_p_remove_federations(
					kingbase_conn_t *kingbase_conn, uint32_t uid,
					slurmdb_federation_cond_t *fed_cond)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_remove_federations(kingbase_conn, uid, fed_cond);
}

extern List acct_storage_p_remove_qos(kingbase_conn_t *kingbase_conn, uint32_t uid,
				      slurmdb_qos_cond_t *qos_cond)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_remove_qos(kingbase_conn, uid, qos_cond);
}

extern List acct_storage_p_remove_res(kingbase_conn_t *kingbase_conn,
				      uint32_t uid,
				      slurmdb_res_cond_t *res_cond)
{
	
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_remove_res(kingbase_conn, uid, res_cond);
}

extern List acct_storage_p_remove_wckeys(kingbase_conn_t *kingbase_conn,
					 uint32_t uid,
					 slurmdb_wckey_cond_t *wckey_cond)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return NULL;
	}
	return as_kingbase_remove_wckeys(kingbase_conn, uid, wckey_cond);
}

extern int acct_storage_p_remove_reservation(kingbase_conn_t *kingbase_conn,
					     slurmdb_reservation_rec_t *resv)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_remove_resv(kingbase_conn, resv);
}

extern List acct_storage_p_get_users(kingbase_conn_t *kingbase_conn, uid_t uid,
				     slurmdb_user_cond_t *user_cond)
{
	return as_kingbase_get_users(kingbase_conn, uid, user_cond);
}

extern List acct_storage_p_get_accts(kingbase_conn_t *kingbase_conn, uid_t uid,
				     slurmdb_account_cond_t *acct_cond)
{
	return as_kingbase_get_accts(kingbase_conn, uid, acct_cond);
}

extern List acct_storage_p_get_clusters(kingbase_conn_t *kingbase_conn, uid_t uid,
					slurmdb_cluster_cond_t *cluster_cond)
{
	return as_kingbase_get_clusters(kingbase_conn, uid, cluster_cond);
}

extern List acct_storage_p_get_federations(kingbase_conn_t *kingbase_conn, uid_t uid,
					   slurmdb_federation_cond_t *fed_cond)
{	
	return as_kingbase_get_federations(kingbase_conn, uid, fed_cond);
}

extern List acct_storage_p_get_tres(
	kingbase_conn_t *kingbase_conn, uid_t uid,
	slurmdb_tres_cond_t *tres_cond)
{	
	return as_kingbase_get_tres(kingbase_conn, uid, tres_cond);
}

extern List acct_storage_p_get_assocs(
	kingbase_conn_t *kingbase_conn, uid_t uid,
	slurmdb_assoc_cond_t *assoc_cond)
{	

#ifdef __METASTACK_OPT_LIST_USER
	return as_kingbase_get_assocs(kingbase_conn, uid, assoc_cond, false);
#else
	return as_kingbase_get_assocs(kingbase_conn, uid, assoc_cond);
#endif
}

extern List acct_storage_p_get_events(kingbase_conn_t *kingbase_conn, uint32_t uid,
				      slurmdb_event_cond_t *event_cond)
{
	return as_kingbase_get_cluster_events(kingbase_conn, uid, event_cond);
}

extern List acct_storage_p_get_problems(kingbase_conn_t *kingbase_conn, uint32_t uid,
					slurmdb_assoc_cond_t *assoc_cond)
{
	List ret_list = NULL;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	ret_list = list_create(slurmdb_destroy_assoc_rec);

	if (as_kingbase_acct_no_assocs(kingbase_conn, assoc_cond, ret_list)
	    != SLURM_SUCCESS)
		goto end_it;

	if (as_kingbase_acct_no_users(kingbase_conn, assoc_cond, ret_list)
	    != SLURM_SUCCESS)
		goto end_it;

	if (as_kingbase_user_no_assocs_or_no_uid(kingbase_conn, assoc_cond, ret_list)
	    != SLURM_SUCCESS)
		goto end_it;

end_it:

	return ret_list;
}

extern List acct_storage_p_get_config(void *db_conn, char *config_name)
{
	return NULL;
}

extern List acct_storage_p_get_qos(kingbase_conn_t *kingbase_conn, uid_t uid,
				   slurmdb_qos_cond_t *qos_cond)
{
	return as_kingbase_get_qos(kingbase_conn, uid, qos_cond);
}

extern List acct_storage_p_get_res(kingbase_conn_t *kingbase_conn, uid_t uid,
				   slurmdb_res_cond_t *res_cond)
{
	return as_kingbase_get_res(kingbase_conn, uid, res_cond);
}

extern List acct_storage_p_get_wckeys(kingbase_conn_t *kingbase_conn, uid_t uid,
				      slurmdb_wckey_cond_t *wckey_cond)
{
	return as_kingbase_get_wckeys(kingbase_conn, uid, wckey_cond);
}

extern List acct_storage_p_get_reservations(
	kingbase_conn_t *kingbase_conn, uid_t uid,
	slurmdb_reservation_cond_t *resv_cond)
{
	return as_kingbase_get_resvs(kingbase_conn, uid, resv_cond);
}

extern List acct_storage_p_get_txn(kingbase_conn_t *kingbase_conn, uid_t uid,
				   slurmdb_txn_cond_t *txn_cond)
{
	return as_kingbase_get_txn(kingbase_conn, uid, txn_cond);
}

extern int acct_storage_p_get_usage(kingbase_conn_t *kingbase_conn, uid_t uid,
				    void *in, slurmdbd_msg_type_t type,
				    time_t start, time_t end)
{	
	return as_kingbase_get_usage(kingbase_conn, uid, in, type, start, end);
}

extern int acct_storage_p_roll_usage(kingbase_conn_t *kingbase_conn,
				     time_t sent_start, time_t sent_end,
				     uint16_t archive_data,
				     List *rollup_stats_list_in)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_roll_usage(kingbase_conn, sent_start, sent_end,
				   archive_data, rollup_stats_list_in);
}

extern int acct_storage_p_fix_runaway_jobs(void *db_conn, uint32_t uid,
					List jobs)
{
	if (kingbase_db_query(db_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_fix_runaway_jobs(db_conn, uid, jobs);
}

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
extern List acct_storage_p_get_borrow(kingbase_conn_t *kingbase_conn, uint32_t uid,
				      slurmdb_borrow_cond_t *borrow_cond)
{
	return as_kingbase_get_cluster_borrow(kingbase_conn, uid, borrow_cond);
}

extern int clusteracct_storage_p_node_borrow(kingbase_conn_t *kingbase_conn,
					   node_record_t *node_ptr,
					   time_t event_time, char *reason,
					   uint32_t reason_uid)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_node_borrow(kingbase_conn, node_ptr,
				  event_time, reason, reason_uid);
}

extern int clusteracct_storage_p_node_return(kingbase_conn_t *kingbase_conn,
					 node_record_t *node_ptr,
					 time_t event_time)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_node_return(kingbase_conn, node_ptr, event_time);
}
#endif

extern int clusteracct_storage_p_node_down(kingbase_conn_t *kingbase_conn,
					   node_record_t *node_ptr,
					   time_t event_time, char *reason,
					   uint32_t reason_uid)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_node_down(kingbase_conn, node_ptr,
				  event_time, reason, reason_uid);
}

extern char *acct_storage_p_node_inx(void *db_conn, char *nodes)
{
	return NULL;
}

extern int clusteracct_storage_p_node_up(kingbase_conn_t *kingbase_conn,
					 node_record_t *node_ptr,
					 time_t event_time)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_node_up(kingbase_conn, node_ptr, event_time);
}

/* This is only called when not running from the slurmdbd so we can
 * assumes some things like rpc_version.
 */
extern int clusteracct_storage_p_register_ctld(kingbase_conn_t *kingbase_conn,
					       uint16_t port)
{
	return as_kingbase_register_ctld(
		kingbase_conn, kingbase_conn->cluster_name, port);
}

extern uint16_t clusteracct_storage_p_register_disconn_ctld(
	kingbase_conn_t *kingbase_conn, char *control_host)
{
	uint16_t control_port = 0;
	char *query = NULL;
	KCIResult *result = NULL;
    int row = 0;
	bool tmp_flag =false;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!kingbase_conn->cluster_name) {
		error("%s:%d no cluster name", THIS_FILE, __LINE__);
		return control_port;
	} else if (!control_host) {
		error("%s:%d no control host for cluster %s",
		      THIS_FILE, __LINE__, kingbase_conn->cluster_name);
		return control_port;
	}

	query = xstrdup_printf("select last_port from %s where name='%s';",
			       cluster_table, kingbase_conn->cluster_name);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	
	result = kingbase_db_query_ret(kingbase_conn, query, 0);		   
	if(KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		error("register_disconn_ctld: no result given for cluster %s",
		      kingbase_conn->cluster_name);
		return control_port;
	}
	xfree(query);
    row = KCIResultGetRowCount(result);
	for(int i = 0; i < row ;i++) {
		if(KCIResultGetColumnValue(result, 0, i)) {
			tmp_flag = true;
		}
	}
	if (tmp_flag) {
		control_port = slurm_atoul(KCIResultGetColumnValue(result, 0, 0));
		/* If there is ever a network issue talking to the DBD, and
		   both the DBD and the ctrl stay up when the ctld goes to
		   talk to the DBD again it may not re-register (<=2.2).
		   Since the slurmctld didn't go down we can presume the port
		   is still the same and just use the last information as the
		   information we should use and go along our merry way.
		*/
		query = xstrdup_printf(
			"update %s set control_host='%s', "
			"control_port=%u where name='%s';",
			cluster_table, control_host, control_port,
			kingbase_conn->cluster_name);
		DB_DEBUG(DB_EVENT, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

		fetch_flag_t* fetch_flag = NULL;
		fetch_result_t* data_rt = NULL;

		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		int rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		free_res_data(data_rt, fetch_flag); 

		if(rc != SLURM_SUCCESS){
			control_port = 0;
		}
	}
	KCIResultDealloc(result);

	return control_port;
}

extern int clusteracct_storage_p_fini_ctld(kingbase_conn_t *kingbase_conn,
					   slurmdb_cluster_rec_t *cluster_rec)
{
	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!cluster_rec || (!kingbase_conn->cluster_name && !cluster_rec->name)) {
		error("%s:%d no cluster name", THIS_FILE, __LINE__);
		return SLURM_ERROR;
	}

	if (!cluster_rec->name)
		cluster_rec->name = kingbase_conn->cluster_name;

	return as_kingbase_fini_ctld(kingbase_conn, cluster_rec);
}

extern int clusteracct_storage_p_cluster_tres(kingbase_conn_t *kingbase_conn,
					      char *cluster_nodes,
					      char *tres_str_in,
					      time_t event_time,
					      uint16_t rpc_version)
{
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	return as_kingbase_cluster_tres(kingbase_conn,
				     cluster_nodes, &tres_str_in,
				     event_time, rpc_version);
}

/*
 * load into the storage the start of a job
 */
extern int jobacct_storage_p_job_start(kingbase_conn_t *kingbase_conn,
				       job_record_t *job_ptr)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_job_start(kingbase_conn, job_ptr);
}

/*
 * load into the storage the start of a job
 */
extern int jobacct_storage_p_job_heavy(kingbase_conn_t *kingbase_conn,
				       job_record_t *job_ptr)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_job_heavy(kingbase_conn, job_ptr);
}

/*
 * load into the storage the end of a job
 */
extern int jobacct_storage_p_job_complete(kingbase_conn_t *kingbase_conn,
					  job_record_t *job_ptr)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_job_complete(kingbase_conn, job_ptr);
}

/*
 * load into the storage the start of a job step
 */
extern int jobacct_storage_p_step_start(kingbase_conn_t *kingbase_conn,
					step_record_t *step_ptr)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_step_start(kingbase_conn, step_ptr);
}

/*
 * load into the storage the end of a job step
 */
extern int jobacct_storage_p_step_complete(kingbase_conn_t *kingbase_conn,
					   step_record_t *step_ptr)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_step_complete(kingbase_conn, step_ptr);
}

/*
 * load into the storage a suspension of a job
 */
extern int jobacct_storage_p_suspend(kingbase_conn_t *kingbase_conn,
				     job_record_t *job_ptr)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_suspend(kingbase_conn, 0, job_ptr);
}

/*
 * get info from the storage
 * returns List of job_rec_t *
 * note List needs to be freed when called
 */
extern List jobacct_storage_p_get_jobs_cond(kingbase_conn_t *kingbase_conn,
					    uid_t uid,
					    slurmdb_job_cond_t *job_cond)
{
	List job_list = NULL;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS) {
		return NULL;
	}

	job_list = as_kingbase_jobacct_process_get_jobs(kingbase_conn, uid, job_cond);

	return job_list;
}

/*
 * expire old info from the storage
 */
extern int jobacct_storage_p_archive(kingbase_conn_t *kingbase_conn,
				     slurmdb_archive_cond_t *arch_cond)
{
	int rc;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	/* Make sure only 1 archive is happening at a time. */
	slurm_mutex_lock(&usage_rollup_lock);
	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}		
	rc = as_kingbase_jobacct_process_archive(kingbase_conn, arch_cond);
	slurm_mutex_unlock(&usage_rollup_lock);

	return rc;
}

/*
 * load old info into the storage
 */
extern int jobacct_storage_p_archive_load(kingbase_conn_t *kingbase_conn,
					  slurmdb_archive_rec_t *arch_rec)
{
	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if(kingbase_db_query(kingbase_conn, "BEGIN") != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}	
	return as_kingbase_jobacct_process_archive_load(kingbase_conn, arch_rec);
}

extern int acct_storage_p_update_shares_used(kingbase_conn_t *kingbase_conn,
					     List shares_used)
{
	/* No plans to have the database hold the used shares */
	return SLURM_SUCCESS;
}

extern int acct_storage_p_flush_jobs_on_cluster(
	kingbase_conn_t *kingbase_conn, time_t event_time)
{
	if (kingbase_db_query(kingbase_conn, "BEGIN")!= SLURM_SUCCESS) {
		return SLURM_ERROR;
	}
	return as_kingbase_flush_jobs_on_cluster(kingbase_conn, event_time);
}

extern int acct_storage_p_reconfig(kingbase_conn_t *kingbase_conn, bool dbd)
{
	return SLURM_SUCCESS;
}

extern int acct_storage_p_reset_lft_rgt(kingbase_conn_t *kingbase_conn, uid_t uid,
					List cluster_list)
{
	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	return as_kingbase_reset_lft_rgt(kingbase_conn, uid, cluster_list);
}

extern int acct_storage_p_get_stats(void *db_conn, bool dbd)
{
	return SLURM_SUCCESS;
}

extern int acct_storage_p_clear_stats(void *db_conn, bool dbd)
{
	return SLURM_SUCCESS;
}

extern int acct_storage_p_get_data(void *db_conn, acct_storage_info_t dinfo,
				   void *data)
{
	return SLURM_SUCCESS;
}

extern void acct_storage_p_send_all(void *db_conn, time_t event_time,
				    slurm_msg_type_t msg_type)
{
	return;
}

extern int acct_storage_p_shutdown(void *db_conn, bool dbd)
{
	return SLURM_SUCCESS;
}
