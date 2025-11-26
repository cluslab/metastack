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
#include "src/interfaces/jobacct_gather.h"

/*
 * Any time you have to add to an existing convert update this number.
 * NOTE: 13 was the first version of 23.02.
 * NOTE: 14 was the first version of 23.11.
 * NOTE: 15 was the second version of 23.11.
 */
#define CONVERT_VERSION 15

#ifdef __META_PROTOCOL
#define MIN_CONVERT_VERSION 11
#else
#define MIN_CONVERT_VERSION 13
#endif

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

static int _rename_clus_res_columns(kingbase_conn_t *kingbase_conn)
{
	char *query = NULL;
	int rc = SLURM_SUCCESS;

	/*
	 * Change the name 'percent_allowed' to be 'allowed'
	 */
	query = xstrdup_printf(
		"alter table %s change percent_allowed allowed "
		"bigint default 0;",
		clus_res_table);

	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	if ((rc = as_kingbase_convert_alter_query(kingbase_conn, query)) !=
	    SLURM_SUCCESS)
		error("Can't update %s %m", clus_res_table);
	xfree(query);

	return rc;
}

static int _convert_clus_res_table_pre(kingbase_conn_t *kingbase_conn)
{
	int rc = SLURM_SUCCESS;

	if (db_curr_ver < 13) {
		if ((rc = _rename_clus_res_columns(kingbase_conn)) !=
		    SLURM_SUCCESS)
			return rc;
	}

	return rc;
}

#ifdef __METASTACK_OPT_SACCT_OUTPUT
static int _rename_job_table_columns(kingbase_conn_t *kingbase_conn, char *cluster_name)
{
	char *query = NULL;
	int rc = SLURM_SUCCESS;
	KCIResult *result = NULL;

	query = xstrdup_printf(
	"SELECT 1 FROM information_schema.columns WHERE table_schema = (SELECT current_schema()) AND table_name = '%s_%s' AND (column_name = 'stdout' or column_name = 'stderr');",
	cluster_name, job_table);
	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		xfree(query);
		error("couldn't query the database");
		return SLURM_ERROR;
	}
	xfree(query);
	if (!KCIResultGetRowCount(result)) {
		KCIResultDealloc(result);
		return rc;
	}
	KCIResultDealloc(result);

	/*
	 * Change the name 'percent_allowed' to be 'allowed'
	 */
	debug("alter table %s_%s, change column stdout to std_out, change column stderr to std_err", cluster_name, job_table);
	query = xstrdup_printf(
		"alter table `%s_%s` change column stderr std_err TEXT NOT NULL;"
		"alter table `%s_%s` change column stdout std_out TEXT NOT NULL;",
		cluster_name, job_table, cluster_name, job_table);

	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	if ((rc = as_kingbase_convert_alter_query(kingbase_conn, query)) !=
	    SLURM_SUCCESS)
		error("Can't update %s_%s %m", cluster_name, job_table);
	xfree(query);

	return rc;
}
#endif

static int _convert_job_table_pre(kingbase_conn_t *kingbase_conn, char *cluster_name)
{
	int rc = SLURM_SUCCESS;

#ifdef __METASTACK_OPT_SACCT_OUTPUT
	if (db_curr_ver < 13) {
		if ((rc = _rename_job_table_columns(kingbase_conn, cluster_name)) !=
		    SLURM_SUCCESS)
			return rc;
	}
#endif

	return rc;
}

static int _convert_step_table_pre(kingbase_conn_t *kingbase_conn, char *cluster_name)
{
	int rc = SLURM_SUCCESS;

	return rc;
}
static int _set_db_curr_ver(kingbase_conn_t *kingbase_conn)
{
	char *query = NULL;
	KCIResult *result = NULL;
	int rc = SLURM_SUCCESS;

	if (db_curr_ver != NO_VAL)
		return SLURM_SUCCESS;

	query = xstrdup_printf("select version from %s", convert_version_table);
	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
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
		int tmp_ver = CONVERT_VERSION;

		query = xstrdup_printf("insert into %s (version) values (%d);",
				       convert_version_table, tmp_ver);
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
			KCIResultDealloc(result);
			return rc;
		}	
		db_curr_ver = tmp_ver;
	}
    KCIResultDealloc(result);

	return rc;
}

extern void as_kingbase_convert_possible(kingbase_conn_t *kingbase_conn)
{
	(void) _set_db_curr_ver(kingbase_conn);

	/*
	 * Check to see if conversion is possible.
	 */
	if (db_curr_ver == NO_VAL) {
		/*
		 * Check if the cluster_table exists before deciding if this is
		 * a new database or a database that predates the
		 * convert_version_table.
		 */
		KCIResult *result = NULL;
		char *query = xstrdup_printf("select name from %s limit 1",
					     cluster_table);
		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
		result = kingbase_db_query_ret(kingbase_conn, query, 0);
		if (KCIResultGetRowCount(result)) {
			/*
			 * knowing that the table exists is enough to say this
			 * is an old database.
			 */
			xfree(query);
			KCIResultDealloc(result);
			fatal("Database schema is too old for this version of Slurm to upgrade.");
		}
		xfree(query);
		KCIResultDealloc(result);
		debug4("Database is new, conversion is not required");
	} else if (db_curr_ver < MIN_CONVERT_VERSION) {
		fatal("Database schema is too old for this version of Slurm to upgrade.");
	} else if (db_curr_ver > CONVERT_VERSION) {
		char *err_msg = "Database schema is from a newer version of Slurm, downgrading is not possible.";
		/*
		 * If we are configured --enable-debug only make this a
		 * debug statement instead of fatal to allow developers
		 * easier bisects.
		 */
#ifdef NDEBUG
		fatal("%s", err_msg);
#else
		debug("%s", err_msg);
#endif
	}
}

extern int as_kingbase_convert_tables_pre_create(kingbase_conn_t *kingbase_conn)
{
	int rc = SLURM_SUCCESS;
	list_itr_t *itr;
	char *cluster_name = NULL;

	xassert(as_kingbase_total_cluster_list);

	if ((rc = _set_db_curr_ver(kingbase_conn)) != SLURM_SUCCESS)
		return rc;

	if (db_curr_ver == CONVERT_VERSION) {
		debug4("No conversion needed, Horray!");
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

	/*
	 * At this point, its clear an upgrade is being performed.
	 * Setup the galera cluster specific options if applicable.
	 *
	 * If this fails for whatever reason, it does not mean that the upgrade
	 * will fail, but it might.
	 */
	/*
 	 * Galera is a synchronous multi master cluster software for MySQL (also supports MariaDB, Percona). 
 	 * Kingbase does not support Galera scheme.
 	 */
	// mysql_db_enable_streaming_replication(mysql_conn);

	info("pre-converting cluster resource table");
	if ((rc = _convert_clus_res_table_pre(kingbase_conn)) != SLURM_SUCCESS)
		return rc;

	/* make it up to date */
	itr = list_iterator_create(as_kingbase_total_cluster_list);
	while ((cluster_name = list_next(itr))) {
		/*
		 * When calling alters on tables here please remember to use
		 * as_kingbase_convert_alter_query instead of kingbase_db_query to be
		 * able to detect a previous failed conversion.
		 */
		info("pre-converting job table for %s", cluster_name);
		if ((rc = _convert_job_table_pre(kingbase_conn, cluster_name))
		     != SLURM_SUCCESS)
			break;
		info("pre-converting step table for %s", cluster_name);
		if ((rc = _convert_step_table_pre(kingbase_conn, cluster_name))
		     != SLURM_SUCCESS)
			break;
	}
	list_iterator_destroy(itr);

	return rc;
}

static int _foreach_set_lineage(void *x, void *arg)
{
	char *query = x;
	kingbase_conn_t *kingbase_conn = arg;

	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	if (kingbase_db_query(kingbase_conn, query) != SLURM_SUCCESS)
		return -1; /* Abort list_for_each */

	return 0; /* Continue list_for_each */
}

static int _convert_assoc_table_post(kingbase_conn_t *kingbase_conn,
				     char *cluster_name)
{
	int rc = SLURM_SUCCESS;

	if (db_curr_ver < 14) {

		KCIResult *result = NULL;
		char *insert_pos = NULL;
		uint64_t max_query_size = 0;
		char *table_name = xstrdup_printf("`%s_%s`",
						  cluster_name, assoc_table);;
		list_t *query_list = list_create(xfree_ptr);
		/* fill in the id_parent */
		char *query = xstrdup_printf(
			"update %s as t1 inner join %s as t2 on t1.acct=t2.acct and t1.`user`!='' and t1.id_assoc!=t2.id_assoc set t1.id_parent=t2.id_assoc;",
			table_name, table_name);
		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
		if ((rc = kingbase_db_query(kingbase_conn, query)) != SLURM_SUCCESS)
			goto endit;
		xfree(query);
		query = xstrdup_printf(
			"update %s as t1 inner join %s as t2 on t1.parent_acct=t2.acct and t1.parent_acct!='' and t2.`user`='' set t1.id_parent=t2.id_assoc;",
			table_name, table_name);
		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
		if ((rc = kingbase_db_query(kingbase_conn, query)) != SLURM_SUCCESS)
			goto endit;
		xfree(query);

		/*
		 * Determine max query size to avoid possibly generating
		 * something too long for the sql server to process.
		 *
		 * This is primarily to support older MySQL servers, but also
		 * supports very large association tables.
		 */
		// if (mysql_db_get_var_u64(mysql_conn, "max_allowed_packet",
		// 			 &max_query_size))
			max_query_size = 1024 * 1024;
		/*
		 * Safety margin of 10% of the possible size.  A single set
		 * lineage call should not exceeed 1KiB.
		 */
		max_query_size = (max_query_size * 0.9);

		/*
		 * Now set the lineage for the associations.
		 * It would be nice to be able to call a function here to do the
		 * set, but MySQL/MariaDB does not allow dynamic SQL. Since the
		 * update would require the cluster name to set set the table
		 * correctly we can do this in a function.
		 *
		 * I also though about having a different function per cluster
		 * and just call that instead, but the problem there is you
		 * can't have a '-' in a function name which makes clusters like
		 * 'smd-server' not able to create a valid function name
		 * (get_lineage_smd-server() is not valid).
		 *
		 * So this is the best I could figure out at the moment.
		 */
		query = xstrdup_printf("select id_assoc, acct, `user`, partition from %s",
				       table_name);
		if (!(result = kingbase_db_query_ret(kingbase_conn, query, 1))) {
			xfree(query);
			rc = SLURM_ERROR;
			goto endit;
		}
		xfree(query);
		for(int i = 0; i < KCIResultGetRowCount(result); i++){
			xstrfmtcatat(query, &insert_pos,
				     "select set_lineage(%s, '%s', '%s', '%s', '%s');",
				     KCIResultGetColumnValue(result,i,0), KCIResultGetColumnValue(result,i,1), KCIResultGetColumnValue(result,i,2), KCIResultGetColumnValue(result,i,3),
				     table_name);
			if ((insert_pos - query) > max_query_size) {
				list_append(query_list, query);
				query = NULL;
				insert_pos = NULL;
			}
		}
		if (query) {
			list_append(query_list, query);
			query = NULL;
		}
		KCIResultDealloc(result);
		if (list_for_each(query_list, _foreach_set_lineage,
				  kingbase_conn) < 0)
			rc = SLURM_ERROR;
	endit:
		FREE_NULL_LIST(query_list);
		xfree(table_name);
	} else if (db_curr_ver < 15) {
		/*
		 * There was a bug in version 14 that didn't add the partition
		 * to the lineage. This fixes that.
		 */
		char *query = xstrdup_printf(
			"update `%s_%s` set lineage=concat(lineage, partition, '/') where partition!='' and (partition is not null) and (lineage not like concat('%%/', partition, '/'));",
			cluster_name, assoc_table);
		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
		rc = kingbase_db_query(kingbase_conn, query);
		xfree(query);
	}

	return rc;
}

static int _foreach_post_create(void *x, void *arg)
{
	char *cluster_name = x;
	kingbase_conn_t *kingbase_conn = arg;
	int rc;

	info("post-converting assoc table for %s", cluster_name);
	if ((rc = _convert_assoc_table_post(kingbase_conn, cluster_name)) !=
	     SLURM_SUCCESS)
		return rc;

	return SLURM_SUCCESS;
}

extern int as_kingbase_convert_tables_post_create(kingbase_conn_t *kingbase_conn)
{
	int rc = SLURM_SUCCESS;

	xassert(as_kingbase_total_cluster_list);

	if ((rc = _set_db_curr_ver(kingbase_conn)) != SLURM_SUCCESS)
		return rc;

	if (db_curr_ver == CONVERT_VERSION) {
		debug4("No conversion needed, Horray!");
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
	if (list_for_each_ro(as_kingbase_total_cluster_list,
			     _foreach_post_create, kingbase_conn) < 0)
		return SLURM_ERROR;

	return SLURM_SUCCESS;
}

extern int as_kingbase_convert_non_cluster_tables_post_create(
	kingbase_conn_t *kingbase_conn)
{
	int rc = SLURM_SUCCESS;

	if ((rc = _set_db_curr_ver(kingbase_conn)) != SLURM_SUCCESS)
		return rc;

	if (db_curr_ver == CONVERT_VERSION) {
		debug4("No conversion needed, Horray!");
		return SLURM_SUCCESS;
	}

	if (rc != SLURM_ERROR) {
		char *query = xstrdup_printf(
			"update %s set version=%d, mod_time=UNIX_TIMESTAMP()",
			convert_version_table, CONVERT_VERSION);

		info("Conversion done: success!");

		DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
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
