/*****************************************************************************\
 *  kingbase_common.c - common functions for the  storage plugin.
 *****************************************************************************
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
#include <stdlib.h>
#include <stdio.h>
#include "config.h"

#include "kingbase_common.h"
#include "src/common/log.h"
#include "src/common/xstring.h"
#include "src/common/xmalloc.h"
#include "src/common/timers.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/read_config.h"

#define MAX_DEADLOCK_ATTEMPTS 10 // 最大死锁次数

static char *table_defs_table = "table_defs_table";

typedef struct
{
	char *name;
	char *columns;
} db_key_t;

static void _destroy_db_key(void *arg)
{
	db_key_t *db_key = (db_key_t *)arg;

	if (db_key) {
		xfree(db_key->name);
		xfree(db_key->columns);
		xfree(db_key);
	}
}
static int _kingbase_query_internal(KCIConnection *db_conn, char *query);
extern int kingbase_for_fetch(kingbase_conn_t *kingbase_conn, char *query, fetch_flag_t* flag, fetch_result_t* data_rt)
{
	int rc = SLURM_SUCCESS;
	if (!kingbase_conn || !kingbase_conn->db_conn) {
		fatal("You haven't inited this storage yet.");
		return 0; /* For CLANG false positive */
	}
	slurm_mutex_lock(&kingbase_conn->lock);
	rc = _kingbase_query_internal(kingbase_conn->db_conn, query);

	/*获取sql执行状态*/
	data_rt->rc = rc;
	if (rc != SLURM_SUCCESS) {
		error("%s line: %d, send query error: %s", __func__, __LINE__, query);
     	slurm_mutex_unlock(&kingbase_conn->lock);
    	goto endit;
	}
	KCIResult *result = NULL;
	result = KCIConnectionFetchResult(kingbase_conn->db_conn);
	if(flag->insert_ret_id) {	
		rc = _kingbase_query_internal(kingbase_conn->db_conn, "select last_insert_id()");
		if (rc != SLURM_SUCCESS) {
			data_rt->rc = rc;
			slurm_mutex_unlock(&kingbase_conn->lock);
			goto endit;
		}

		KCIResult *res = KCIConnectionFetchResult(kingbase_conn->db_conn);		
		if(KCIResultGetStatusCode(res) != EXECUTE_TUPLES_OK) {
			rc = SLURM_ERROR;
			data_rt->rc = rc;
			KCIResultDealloc(res);
			slurm_mutex_unlock(&kingbase_conn->lock);
			goto endit;
		}
      
		char *value = KCIResultGetColumnValue(res, 0, 0);
        int new_id = value == NULL ? 0 : atoi(value);
		if (!new_id) {
				/* should have new id */
				error("%s: We should have gotten a new id: %s",
					  __func__, KCIConnectionGetLastError(kingbase_conn->db_conn));
		}
		data_rt->insert_ret_id = new_id;
		KCIResultDealloc(res);
	}
	slurm_mutex_unlock(&kingbase_conn->lock);

	if((KCIResultGetStatusCode(result) != EXECUTE_COMMAND_OK) && (KCIResultGetStatusCode(result) !=EXECUTE_TUPLES_OK)) {
		rc = SLURM_ERROR;
		data_rt->rc = rc;
		error("%s line: %d, exec query error: %s", __func__, __LINE__, query);
		KCIResultDealloc(result);
		goto endit;
 	}

	/*获取行数\列数*/
    data_rt->RowCount = KCIResultGetRowCount(result);
    data_rt->ColumnCount = KCIResultGetColumnCount(result);


    /*获取受影响的行数*/
    if(flag->rows_affect) {
		int status = 0, rows = 0;
		char *tmp_str = KCIResultGetAffectedCount(result);
		if(tmp_str)
			status = atoi(tmp_str);
		if (status > 0) 
			rows = status;
		data_rt->rows_affect = rows;
	}

	KCIResultDealloc(result);
endit:
	return rc;
}

extern int kingbase_for_fetch2(kingbase_conn_t *kingbase_conn, char *query, fetch_flag_t* flag, fetch_result_t* data_rt,char *query2)
{
	int rc = SLURM_SUCCESS;
	if (!kingbase_conn || !kingbase_conn->db_conn) {
		fatal("You haven't inited this storage yet.");
		return 0; /* For CLANG false positive */
	}
	slurm_mutex_lock(&kingbase_conn->lock);
	rc = _kingbase_query_internal(kingbase_conn->db_conn, query);

	/*获取sql执行状态*/
	data_rt->rc = rc;
	if (rc != SLURM_SUCCESS) {
		error("%s line: %d, send query error: %s", __func__, __LINE__, query);
     	slurm_mutex_unlock(&kingbase_conn->lock);
    	goto endit;
	}
	KCIResult *result = NULL;
	result = KCIConnectionFetchResult(kingbase_conn->db_conn);
	if(flag->rows_affect) {
		int status = 0, rows = 0;
		char *tmp_str = KCIResultGetAffectedCount(result);
		if(tmp_str)
			status = atoi(tmp_str);
		if (status > 0) 
			rows = status;
		data_rt->rows_affect = rows;
	}
	if(flag->insert_ret_id) {	
		//debug4("query2: %s", query2);
		rc = _kingbase_query_internal(kingbase_conn->db_conn, query2);
		//error("rc = %d",rc);
		if (rc != SLURM_SUCCESS) {
			data_rt->rc = rc;
			slurm_mutex_unlock(&kingbase_conn->lock);
			goto endit;
		}

		KCIResult *res = KCIConnectionFetchResult(kingbase_conn->db_conn);
		
		if(KCIResultGetStatusCode(res) != EXECUTE_TUPLES_OK) {
			rc = SLURM_ERROR;
			data_rt->rc = rc;
			KCIResultDealloc(res);
			slurm_mutex_unlock(&kingbase_conn->lock);
			goto endit;
		}
       
		char *value = KCIResultGetColumnValue(res, 0, 0);
        int new_id = value == NULL ? 0 : atoi(value);
		if (!new_id) {
				/* should have new id */
				error("%s: We should have gotten a new id: %s",
					  __func__, KCIConnectionGetLastError(kingbase_conn->db_conn));
		}
		data_rt->insert_ret_id = new_id;
		KCIResultDealloc(res);
	}
	slurm_mutex_unlock(&kingbase_conn->lock);

	if((KCIResultGetStatusCode(result) != EXECUTE_COMMAND_OK) && (KCIResultGetStatusCode(result) !=EXECUTE_TUPLES_OK)) {
		rc = SLURM_ERROR;
		data_rt->rc = rc;
		error("%s line: %d, exec query error: %s", __func__, __LINE__, query);
		KCIResultDealloc(result);
		goto endit;
 	}

	/*获取行数\列数*/
    data_rt->RowCount = KCIResultGetRowCount(result);
    data_rt->ColumnCount = KCIResultGetColumnCount(result);

	KCIResultDealloc(result);

endit:
	return rc;
}



extern fetch_flag_t* set_fetch_flag(bool insert_ret_id, bool result, bool rows_affect)
{
	fetch_flag_t *fetch_flag = xmalloc(sizeof(fetch_flag_t));;
	fetch_flag->insert_ret_id = insert_ret_id;
	fetch_flag->result = result;
	fetch_flag->rows_affect = rows_affect;
	return fetch_flag;
}

extern void free_res_data(fetch_result_t* data_rt, fetch_flag_t *fetch_flag)
{
	xfree(data_rt);	
	if (fetch_flag) {
		xfree(fetch_flag);
	}
}

extern char *slurm_add_slash_to_quotes2(char *str)
{
	char *dup, *copy = NULL;
	int len = 0;
	bool reserve = false, discard = false;
	if (!str || !(len = strlen(str)))
		return NULL;

	/* make a buffer 2 times the size just to be safe */
	copy = dup = xmalloc((2 * len) + 1);
	if (copy) {
		do {
			/* 保留连续单引号 */
			if (reserve) {
				reserve = false;
				*dup++ = '\'';
				*dup++ = '\'';
				continue;
			}
			/* 原始处理 */
			if (*str == '\\') {
			   *dup++ = '\\';
			} else if ((*(str) == '\'')) {
				if ((*(str+1) == '\'')) {
						reserve = true;
				} else {
						discard = true;
				}
			}
			/* 丢弃单个单引号 */
			if (discard) {
					discard = false;
					str++;
			}			
		} while ((*dup++ = *str++));
	}
	return copy;
}

/* NOTE: Ensure that kingbase_conn->lock is set on function entry */
static int _clear_results(KCIConnection *db_conn) // 清理未处理的查询结果
{
	KCIResult *result = NULL;
    if (KCIConnectionGetStatus(db_conn)!=CONNECTION_OK) {
		return SLURM_ERROR;
	}
	while ((result = KCIConnectionFetchResult(db_conn))) {
		KCIResultDealloc(result);
	}
	return SLURM_SUCCESS;
}

/* NOTE: Ensure that kingbase_conn->lock is set on function entry */
static KCIResult *_get_first_result(KCIConnection *db_conn) // 获取第一次查询的结果
{
	KCIResult *result = NULL;
	if ((result = KCIConnectionFetchResult(db_conn)))
		return result;
	return NULL;
}

/* NOTE: Ensure that kingbase_conn->lock is set on function entry */
static KCIResult *_get_last_result(KCIConnection *db_conn)
{
	KCIResult *result = NULL;
	KCIResult *last_result = NULL;
	while ((result = KCIConnectionFetchResult(db_conn))) {
		if (last_result) {
			KCIResultDealloc(last_result); // 每次查询完成后，释放上一次查询的内容
		}
		last_result = result;
	}
	return last_result;
}

/* NOTE: Ensure that kingbase_conn->lock is set on function entry */
static int _kingbase_query_internal(KCIConnection *db_conn, char *query)
{
	int rc = SLURM_SUCCESS;
	// int deadlock_attempt = 0;

	if (!db_conn) {
		fatal("You haven't inited this storage yet.");
	}

	/* clear out the old results so we don't get a 2014 error */
	_clear_results(db_conn);
	if (!KCIStatementSend(db_conn, query)) { // 返回值非零表示发送成功
		const char *err_str = KCIConnectionGetLastError(db_conn);
		debug4("This could happen often and is expected.\n"
			   "KCIStatementSend failed: %s\n%s",
			   err_str, query);
		errno = 0;
		rc = SLURM_ERROR;
	}
	/*
	 * Starting in MariaDB 10.2 many of the api commands started
	 * setting errno erroneously.
	 */
	if (!rc)
		errno = 0;
	return rc;
}

/*
 * Determine if a database server upgrade has taken place and if so, check to
 * see if the candidate table alteration query should be used to alter the table
 * to its expected settings. Returns true if so.
 *
 * Background:
 *
 * From the MariaDB docs:
 *  Before MariaDB 10.2.1, BLOB and TEXT columns could not be assigned a DEFAULT
 *  value. This restriction was lifted in MariaDB 10.2.1.
 *
 * If a site begins using MariaDB >= 10.2.1 and is either using an existing
 * Slurm database from an earlier version or has restored one from a dump from
 * an earlier version or from any version of MySQL, some text/blob default
 * values will need to be altered to avoid failures from subsequent queries from
 * slurmdbd that set affected fields to DEFAULT (see bug#13606).
 *
 * Note that only one column from one table ('preempt' from qos_table) is
 * checked to determine if an upgrade has taken place with the assumption that
 * if its default value is not correct then the same is true for similar
 * text/blob columns from other tables and they will also need to be altered.
 *
 * The qos_table has been chosen for this check because it is the last table
 * with condition to be created. If that condition changes this should be
 * re-evaluated.
 */
// static bool _alter_table_after_upgrade(mysql_conn_t *mysql_conn,
// 				       char *table_alter_query)
// {
// 	static bool have_value = false, upgraded = false;
// 	MYSQL_RES *result = NULL;
// 	MYSQL_ROW row;

// 	/* check to see if upgrade has happened */
// 	if (!have_value) {
// 		const char *info;
// 		char *query;
// 		/*
// 		 * confirm MariaDB is being used to avoid any ambiguity with
// 		 * MySQL versions
// 		 */
// 		info = mysql_get_server_info(mysql_conn->db_conn);
// 		if (xstrcasestr(info, "mariadb") &&
// 		    (mysql_get_server_version(mysql_conn->db_conn) >= 100201)) {
// 			query = "show columns from `qos_table` like 'preempt'";
// 			result = mysql_db_query_ret(mysql_conn, query, 0);
// 			if (result) {
// 				/*
// 				 * row[4] holds the column's default value and
// 				 * if it's NULL then it will need to be altered
// 				 * and an upgrade is assumed
// 				 */
// 				if ((row = mysql_fetch_row(result)) &&
// 				    !xstrcasecmp(row[1], "text") &&
// 				    !row[4])
// 					upgraded = true;
// 				mysql_free_result(result);
// 			}
// 		}
// 		have_value = true;
// 	}

// 	/*
// 	 * If upgrade detected and the table alter query string contains an
// 	 * emtpy string default then the query should be executed. The latter
// 	 * check avoids unnecessary table alterations.
// 	 */
// 	if (upgraded && xstrcasestr(table_alter_query, "default ''"))
// 		return true;

// 	return false;
// }


/* NOTE: Ensure that kingbase_conn->lock is NOT set on function entry */
static int _kingbase_make_table_current(kingbase_conn_t *kingbase_conn, char *table_name,
										storage_field_t *fields, char *ending)
{
	char *query = NULL;
	char *correct_query = NULL;
	KCIResult *result = NULL;
	int i = 0;
	List columns = NULL;
	ListIterator itr = NULL;
	char *col = NULL;
	int adding = 0;
	int run_update = 0;
	char *primary_key = NULL;
	char *unique_index = NULL;
	int old_primary = 0;
	char *old_index = NULL;
	char *temp = NULL, *temp2 = NULL;
	List keys_list = NULL;
	db_key_t *db_key = NULL;

	DEF_TIMERS;

	/* figure out the unique keys in the table */ // select ui.index_name,ui.uniqueness,column_name from USER_INDEXES ui left join DBA_IND_COLUMNS di on ui.index_name=di.index_name where ui.table_name = '%s';
	/*替换mysql方言show index from convert_version_table where non_unique=0;,查看该表的indisunique是否为true
	*mysql
	*+-----------------------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
    *| Table                 | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
    *+-----------------------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
    *| convert_version_table |          0 | PRIMARY  |            1 | version     | A         |           1 |     NULL | NULL   |      | BTREE      |         |               |
    *+-----------------------+------------+----------+--------------+-------------+-----------+-------------+----------+----
	*kingbase
    *relname           | indisprimary | indisunique | indisclustered | indisvalid |                                        pg_get_indexdef                                        | pg_get_constraintdef  | contype | condeferrable | condeferred | indisreplident | reltablespace | indisready 
    *----------------------------+--------------+-------------+----------------+------------+-----------------------------------------------------------------------------------------------+-----------------------+---------+---------------+-------------+----------------+---------------+------------
    *convert_version_table_pkey | t            | t           | f              | t          | CREATE UNIQUE INDEX convert_version_table_pkey ON convert_version_table USING btree (version) | PRIMARY KEY (version) | p       | f             | f           | f              |             0 | t
	*/
	query = xstrdup_printf("SELECT a.attname, c2.relname, i.indisprimary, "
			"i.indisunique, i.indisclustered, i.indisvalid, "
			"pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), " 
			"pg_catalog.pg_get_constraintdef(con.oid, true), contype, "
			"condeferrable, condeferred, i.indisreplident, c2.reltablespace, "
			"i.indisready FROM pg_catalog.pg_attribute a, pg_catalog.pg_class c, pg_catalog.pg_class c2, "
			"pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con "
			"ON (conrelid = i.indrelid AND conindid = i.indexrelid "
			"AND contype IN ('p','u','x')) WHERE a.attrelid = c.oid AND a.attnum = ANY(i.indkey) AND c.oid = i.indrelid AND "
			"i.indisunique='t' AND i.indexrelid = c2.oid " 
			"AND c.relname ='%s' ORDER BY i.indisprimary DESC, "
			"i.indisunique DESC, c2.relname;", table_name);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		return SLURM_ERROR;
	}

	/* KCIResultGetColumnValue(result, c, 1) is the key name*/
	for (int c = 0; c < KCIResultGetRowCount(result); c++) {   
		if (!xstrncmp(KCIResultGetColumnValue(result, c, 7), "PRIMARY",7))
			old_primary = 1;
		else if (!old_index)
			old_index = xstrdup(KCIResultGetColumnValue(result, c, 1));
	}

	KCIResultDealloc(result);

	/* figure out the non-unique keys in the table */
	/*替换mysql方言show index from convert_version_table where non_unique=1;和上述查询语句类似
	 *获取Column_name为version
	 */
	query = xstrdup_printf("SELECT a.attname, c2.relname, i.indisprimary, i.indisunique, i.indisclustered, "
						   "i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), "
						   "pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, "
						   "condeferred, i.indisreplident, c2.reltablespace, i.indisready FROM pg_catalog.pg_attribute a, pg_catalog.pg_class c, "
						   "pg_catalog.pg_class c2, pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con "
						   "ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x')) "
						   "WHERE a.attrelid = c.oid AND a.attnum = ANY(i.indkey) AND c.oid = i.indrelid AND i.indisunique='f' AND i.indexrelid = c2.oid "
						   "AND c.relname ='%s' ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;",
						   table_name);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(old_index);
		return SLURM_ERROR;
	}
	
	itr = NULL;
	keys_list = list_create(_destroy_db_key);
	for (int  c = 0; c < KCIResultGetRowCount(result); c++) {
		if (!itr)
			itr = list_iterator_create(keys_list);
		else
			list_iterator_reset(itr);
		while ((db_key = list_next(itr))) {
			if (!xstrcmp(db_key->name, KCIResultGetColumnValue(result, c, 1))){
				break;
			}
		}

		if (db_key) {
			xstrfmtcat(db_key->columns, ", %s", KCIResultGetColumnValue(result, c, 0));
		}
		else {
			db_key = xmalloc(sizeof(db_key_t));
			db_key->name = xstrdup(KCIResultGetColumnValue(result, c, 1));	   // name
			db_key->columns = xstrdup(KCIResultGetColumnValue(result, c, 0)); // column name
			list_append(keys_list, db_key);								   // don't use list_push
		}
	}
	KCIResultDealloc(result);

	if (itr) {
		list_iterator_destroy(itr);
		itr = NULL;
	}

	/* figure out the existing columns in the table */
	/*
		SELECT column_name, data_type, character_maximum_length
		FROM information_schema.columns
		WHERE table_name = 'orders';
	*/
	query = xstrdup_printf("SELECT column_name FROM information_schema.columns "
						   "WHERE table_name = '%s';", table_name); // SELECT column_name, data_type, character_maximum_length, is_nullable, column_default FROM information_schema.columns WHERE table_name = '%s';
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(old_index);
		FREE_NULL_LIST(keys_list);
		return SLURM_ERROR;
	}
	
	columns = list_create(xfree_ptr);

	for (int c = 0; c < KCIResultGetRowCount(result); c++){
		col = xstrdup(KCIResultGetColumnValue(result, c, 0));
		list_append(columns, col);
	}

	KCIResultDealloc(result);

	itr = list_iterator_create(columns);
	/* In MySQL 5.7.4 we lost the ability to run 'alter ignore'.  This was
	 * needed when converting old tables to new schemas.  If people convert
	 * in the future from an older version of Slurm that needed the ignore
	 * to work they will have to downgrade mysql to <= 5.7.3 to make things
	 * work correctly or manually edit the database to get things to work.
	 */
	/*
	 * `query` is compared against the current table_defs_table.definition
	 * and run if they are different. `correct_query` is inserted into the
	 * table, so it must be what future `query` schemas will be.
	 * In other words, `query` transitions the table to the new schema,
	 * `correct_query` represents the new schema
	 */
	query = xstrdup_printf("alter table %s", table_name);
	correct_query = xstrdup(query);
	START_TIMER;
	/*遍历Field（表头）*/
	while (fields[i].name) {
		int found = 0;
        list_iterator_reset(itr);
		while ((col = list_next(itr))) {
			if (!xstrcmp(col, "user"))
				col = "`user`";
            if (!xstrcmp(col, fields[i].name)) {
                char options[50];
                strcpy(options,fields[i].options);

                char *token = strtok(options," ");
				/*
                if(!(strcmp(token,"serial")))
                    xstrfmtcat(query, " modify %s integer,",
                                    fields[i].name);
                else if(!(strcmp(token,"bigserial")))
                    xstrfmtcat(query, " modify %s bigint,",
                                    fields[i].name);
                else
                    xstrfmtcat(query, " modify %s %s,",
                                    fields[i].name,
                                    token);
				*/
                if(!(strcmp(token,"serial"))) {
                        xstrfmtcat(query, " alter column %s type integer,",
                                        fields[i].name);
                        xstrfmtcat(correct_query, " alter column %s type integer,",
                                        fields[i].name);
                } else if(!(strcmp(token,"bigserial"))) {
                        xstrfmtcat(query, " alter column %s type bigint,",
                                        fields[i].name);
                        xstrfmtcat(correct_query, " alter column %s type bigint,",
                                        fields[i].name);
                } else {
                        xstrfmtcat(query, " modify %s %s,",
                                        fields[i].name,
                                        token);
                        xstrfmtcat(correct_query, " modify %s %s,",
                                        fields[i].name,
                                        token);
                }
                token = strtok(NULL," ");
                while(token) {
                        if(!(strcmp(token,"not"))) {
                                xstrfmtcat(query, "modify %s not null,",
                                                fields[i].name);
                                xstrfmtcat(correct_query, "modify %s not null,",
                                                fields[i].name);
                        }
                        if(!(strcmp(token,"default"))) {
                                token = strtok(NULL," ");
                                xstrfmtcat(query, " alter column %s set default %s,",
                                                fields[i].name,
                                                token);
                                xstrfmtcat(correct_query, " alter column %s set default %s,",
                                                fields[i].name,
                                                token);
                        }
                        token = strtok(NULL," ");
                }

                //xstrfmtcat(correct_query, " modify %s %s,",
                //                      fields[i].name,
                //                      fields[i].options);
				list_delete_item(itr);
				found = 1;
				break;
			}
		}
		if (!found) {
			if (i) {
				info("adding column %s after %s in table %s",
					 fields[i].name,
					 fields[i - 1].name,
					 table_name);
				//xstrfmtcat(query, " add `%s` %s after %s,",
				xstrfmtcat(query, " add %s %s ,",
						   fields[i].name,
						   fields[i].options);
				//		   fields[i - 1].name);
				xstrfmtcat(correct_query, " modify %s %s,",
						   fields[i].name,
						   fields[i].options);
			} else {
				info("adding column %s at the beginning "
					 "of table %s",
					 fields[i].name,
					 table_name);
				//xstrfmtcat(query, " add `%s` %s first,",
				xstrfmtcat(query, " add %s %s ,",
						   fields[i].name,
						   fields[i].options);
				xstrfmtcat(correct_query, " modify %s %s,",
						   fields[i].name,
						   fields[i].options);
			}
			adding = 1;
		}

		i++;
	}

	list_iterator_reset(itr);
	while ((col = list_next(itr))) {
		adding = 1;
		info("dropping column %s from table %s", col, table_name);
		xstrfmtcat(query, " drop `%s`,", col);
	}

	list_iterator_destroy(itr);
	FREE_NULL_LIST(columns);

	if ((temp = strstr(ending, "primary key ("))) {
		int open = 0, close = 0;
		int end = 0;
		while (temp[end++]) {
			if (temp[end] == '(')
				open++;
			else if (temp[end] == ')')
				close++;
			else
				continue;
			if (open == close)
				break;
		}
		if (temp[end]) {
			end++;
			primary_key = xstrndup(temp, end);
			if (old_primary){
				query[strlen(query) - 1] = ';';
				xstrfmtcat(query, " alter table %s drop CONSTRAINT %s_pkey,", table_name, table_name);
			}
			correct_query[strlen(correct_query) - 1] = ';';
			xstrfmtcat(correct_query, " alter table %s drop CONSTRAINT %s_pkey,", table_name, table_name);
			xstrfmtcat(query, " add %s,", primary_key);
			xstrfmtcat(correct_query, " add %s,", primary_key);

			xfree(primary_key);
		}
	}

	if ((temp = strstr(ending, "unique index"))) {
		int open = 0, close = 0;
		/* sizeof includes NULL, and end should start 1 back */
		int end = sizeof("unique index") - 2;
		char *udex_name = NULL, *name_marker = NULL;
		while (temp[end++])	{
			/*
			 * Extracts the index name, which is given explicitly
			 * or is the name of the first field included in the
			 * index.
			 * "unique index indexname (field1, field2)"
			 * "unique index (indexname, field2)"
			 * indexname is started by the first non '(' or ' '
			 *     after "unique index"
			 * indexname is terminated by '(' ')' ' ' or ','
			 */
			if (name_marker) {
				if (!udex_name && (temp[end] == '(' ||
								   temp[end] == ')' ||
								   temp[end] == ' ' ||
								   temp[end] == ','))
					udex_name = xstrndup(name_marker,
										 temp + end - name_marker);
			}
			else if (temp[end] != '(' && temp[end] != ' ') {
				name_marker = temp + end;
			}

			/* find the end of the parenthetical expression */
			if (temp[end] == '(')
				open++;
			else if (temp[end] == ')')
				close++;
			else
				continue;
			if (open == close)
				break;
		}
		if (temp[end]) {
			end++;
			unique_index = xstrndup(temp, end);
			if (old_index) {
				query[strlen(query) - 1] = ';';
				xstrfmtcat(query, " drop index %s,", old_index);
			}
			correct_query[strlen(correct_query) - 1] = ';';
			xstrfmtcat(correct_query, " drop index %s,", udex_name);
			query[strlen(query) - 1] = ';';
			xstrfmtcat(query, " create %s,", unique_index);
			correct_query[strlen(correct_query) - 1] = ';';
			xstrfmtcat(correct_query, " create %s,", unique_index);
			xfree(unique_index);
		}
		xfree(udex_name);
	}
	xfree(old_index);

	temp2 = ending;
	itr = list_iterator_create(keys_list);
	while ((temp = strstr(temp2, "create index"))) {
		int open = 0, close = 0, name_end = 0;
		int end = 13;
		char *new_key_name = NULL, *new_key = NULL;
		while (temp[end++]) {
			if (!name_end && (temp[end] == ' ')) {
				name_end = end;
				continue;
			}
			else if (temp[end] == '(') {
				open++;
				if (!name_end)
					name_end = end;
			}
			else if (temp[end] == ')')
				close++;
			else
				continue;
			if (open == close)
				break;
		}
		if (temp[end]) {
			end++;
			new_key_name = xstrndup(temp + 13, name_end - 13);
			new_key = xstrndup(temp + 13, end - 13); // skip ', '
			while ((db_key = list_next(itr))) {
				if (!xstrcmp(db_key->name, new_key_name)) {
					list_remove(itr);
					break;
				}
			}
			list_iterator_reset(itr);
			if (db_key) {
				query[strlen(query) - 1] = ';';
				xstrfmtcat(query,
						   " drop index %s,", db_key->name);
				_destroy_db_key(db_key);
			}
			else
				info("creating index %s to table %s",
					 new_key, table_name);
			correct_query[strlen(correct_query) - 1] = ';';
			xstrfmtcat(correct_query,
					   " drop index %s,", new_key_name);

			query[strlen(query) - 1] = ';';
			xstrfmtcat(query, " create index %s,", new_key);
			correct_query[strlen(correct_query) - 1] = ';';
			xstrfmtcat(correct_query, " create index %s,", new_key);

			xfree(new_key);
			xfree(new_key_name);
		}
		temp2 = temp + end;
	}

	/* flush extra (old) keys */
	while ((db_key = list_next(itr))) {
		info("dropping index %s from table %s", db_key->name, table_name);
		query[strlen(query) - 1] = ';';
		xstrfmtcat(query, " drop index %s,", db_key->name);
	}
	list_iterator_destroy(itr);

	FREE_NULL_LIST(keys_list);

	query[strlen(query) - 1] = ';';
	correct_query[strlen(correct_query) - 1] = ';';
	// info("%d query\n%s", __LINE__, query);

	/* see if we have already done this definition */
	if (!adding && !run_update) {
		char *quoted = slurm_add_slash_to_quotes2(query);
		char *query2 = xstrdup_printf("select table_name from "
									  "%s where definition='%s'",
									  table_defs_table, quoted);
		KCIResult *result = NULL;
		//KCIResult *row;

		xfree(quoted);
		run_update = 1;
		result = kingbase_db_query_ret(kingbase_conn, query2, 0);
		
		if (KCIResultGetStatusCode(result) == EXECUTE_TUPLES_OK) {
			if ((KCIResultGetRowCount(result)> 0))
				run_update = 0;
		}
		KCIResultDealloc(result);
		xfree(query2);
		if (run_update) {
			run_update = 2;
			query2 = xstrdup_printf("select table_name from "
									"%s where table_name='%s'",
									table_defs_table, table_name);
			result = kingbase_db_query_ret(kingbase_conn, query2, 0);
			if (KCIResultGetStatusCode(result)  == EXECUTE_TUPLES_OK) {
				if ((KCIResultGetRowCount(result)> 0))
					run_update = 1;
			}
			KCIResultDealloc(result);
			xfree(query2);
		}
		
	}

	/* if something has changed run the alter line */
	if (run_update || adding) {
		time_t now = time(NULL);
		char *query2 = NULL;
		char *quoted = NULL;

		if (run_update == 2)
			debug4("Table %s doesn't exist, adding", table_name);
		else
			debug("Table %s has changed.  Updating...", table_name);
		
		bool rc = 0;
		fetch_flag_t *fetch_flag = NULL;
		fetch_result_t *data_rt = NULL;
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		debug2("query\n%s", query);
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		free_res_data(data_rt, fetch_flag);		

		if (rc != SLURM_SUCCESS) {
			xfree(query);
			return SLURM_ERROR;
		}
		
		quoted = slurm_add_slash_to_quotes2(correct_query);
		query2 = xstrdup_printf("insert into %s (creation_time, "
								"mod_time, table_name, definition) "
								"values (%ld, %ld, '%s', '%s') "
								"on duplicate key update "
								"definition='%s', mod_time=%ld;",
								table_defs_table, now, now,
								table_name, quoted,
								quoted, now);
		xfree(quoted);
		debug3("query\n%s", query2);
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query2, fetch_flag, data_rt);
		xfree(query2);
		free_res_data(data_rt, fetch_flag);	
		if (rc != SLURM_SUCCESS) {
			return SLURM_ERROR;
		}
	}

	xfree(query);
	xfree(correct_query);
	query = xstrdup_printf("make table current %s", table_name);
	END_TIMER2(query);
	xfree(query);
	return SLURM_SUCCESS;
}

void _set_kingbase_ssl_opts(KCIConnection *db_conn, const char *options, SSL_Options *ssl_options)
{
	char *tmp_opts, *token, *save_ptr = NULL;
	ssl_options->key = NULL;
	ssl_options->cert = NULL;
	ssl_options->ca = NULL;
	ssl_options->ca_path = NULL;
	ssl_options->cipher = NULL;

	if (!options)
		return;

	tmp_opts = xstrdup(options);
	token = strtok_r(tmp_opts, ",", &save_ptr);
	while (token) {
		char *opt_str, *val_str = NULL;

		opt_str = strtok_r(token, "=", &val_str);

		if (!opt_str || !val_str)
		{
			error("Invalid storage option/val");
			goto next;
		}
		else if (!xstrcasecmp(opt_str, "SSL_CERT"))
			ssl_options->cert = val_str;
		else if (!xstrcasecmp(opt_str, "SSL_CA"))
			ssl_options->ca = val_str;
		else if (!xstrcasecmp(opt_str, "SSL_CAPATH"))
			ssl_options->ca_path = val_str;
		else if (!xstrcasecmp(opt_str, "SSL_KEY"))
			ssl_options->key = val_str;
		else if (!xstrcasecmp(opt_str, "SSL_CIPHER"))
			ssl_options->cipher = val_str;
		else {
			error("Invalid storage option '%s'", opt_str);
			goto next;
		}
	next:
		token = strtok_r(NULL, ",", &save_ptr);
	}
	xfree(tmp_opts);
}

/* NOTE: Ensure that kingbase_conn->lock is set on function entry */
static int _create_db(char *db_name, kingbase_db_info_t *db_info)
{
	int rc = SLURM_ERROR;
	char *options = NULL;

	KCIConnection *db_ptr = NULL;
	char *db_host = NULL;
	char Kingbase_port[32] = {0};

	while (rc == SLURM_ERROR) {
		rc = SLURM_SUCCESS;

		SSL_Options *ssl_options = (SSL_Options *)xmalloc(sizeof(SSL_Options));
		_set_kingbase_ssl_opts(db_ptr, db_info->params, ssl_options);
		options = xstrdup_printf("dbname=test sslcert=%s sslkey=%s sslrootcert=%s", ssl_options->cert, ssl_options->key, ssl_options->ca);
		if (ssl_options != NULL) {
			xfree(ssl_options);
		}

		db_host = db_info->host;
		sprintf(Kingbase_port, "%d", db_info->port);
		db_ptr = KCIConnectionCreateDeprecated(db_host,
											   Kingbase_port, NULL,
											   NULL, options,
											   db_info->user, db_info->pass);

		if (!db_ptr && db_info->backup) {
				info("Connection failed to host = %s "
                "user = %s port = %s",
                db_host, db_info->user,
                Kingbase_port);
            db_host = db_info->backup;
            db_ptr = KCIConnectionCreateDeprecated(db_host,
                        Kingbase_port, NULL,
                        NULL, options,
                        db_info->user, db_info->pass);
		}

		if (db_ptr) {
				char* create_line = NULL;
				xstrfmtcat(create_line, "create database %s", db_name);
				_clear_results(db_ptr);
				if (KCIStatementSend(db_ptr, create_line)) {
					KCIResult *res = NULL;
					res = KCIConnectionFetchResult(db_ptr);
					int err = KCIResultGetStatusCode(res);
					if (err != EXECUTE_COMMAND_OK) {
						fatal("KCIStatementSend failed: %d %s\n%s",
							  KCIResultGetStatusCode(res),
							  KCIConnectionGetLastError(db_ptr), create_line);
					}
					KCIResultDealloc(res);
				} else {
					fatal("KCIStatementSend failed:  %s\n%s",
						  KCIConnectionGetLastError(db_ptr), create_line);
				}
				if(create_line)
					xfree(create_line);
				KCIConnectionDestory(db_ptr);
		} else {
				info("Connection failed to host = %s "
					 "user = %s port = %s",
					 db_host, db_info->user,
					 Kingbase_port);
				error("KCIConnectionCreateDeprecated failed: %s",
					  KCIConnectionGetLastError(db_ptr));
				rc = SLURM_ERROR;
		}
		if (rc == SLURM_ERROR)
			sleep(3);
	}
	return rc;
}

extern kingbase_conn_t *create_kingbase_conn(int conn_num, bool rollback,
											 char *cluster_name)
{
	kingbase_conn_t *kingbase_conn = xmalloc(sizeof(kingbase_conn_t));

	kingbase_conn->rollback = rollback;
	kingbase_conn->conn = conn_num;
	kingbase_conn->cluster_name = xstrdup(cluster_name);
	slurm_mutex_init(&kingbase_conn->lock);
	kingbase_conn->update_list = list_create(slurmdb_destroy_update_object);

	return kingbase_conn;
}

extern int destroy_kingbase_conn(kingbase_conn_t *kingbase_conn)
{
	if (kingbase_conn) {
		kingbase_db_close_db_connection(kingbase_conn);
		xfree(kingbase_conn->pre_commit_query);
		xfree(kingbase_conn->cluster_name);
		slurm_mutex_destroy(&kingbase_conn->lock);
		FREE_NULL_LIST(kingbase_conn->update_list);
		xfree(kingbase_conn);
	}

	return SLURM_SUCCESS;
}

extern kingbase_db_info_t *create_kingbase_db_info(slurm_kingbase_plugin_type_t type)
{
	kingbase_db_info_t *db_info = xmalloc(sizeof(kingbase_db_info_t));

	switch (type) {
		case SLURM_KINGBASE_PLUGIN_AS:
			db_info->port = slurm_conf.accounting_storage_port;
			db_info->host = xstrdup(slurm_conf.accounting_storage_host);
			db_info->backup =
				xstrdup(slurm_conf.accounting_storage_backup_host);
			db_info->user = xstrdup(slurm_conf.accounting_storage_user);
			db_info->pass = xstrdup(slurm_conf.accounting_storage_pass);
			db_info->params = xstrdup(slurm_conf.accounting_storage_params);
			break;
		case SLURM_KINGBASE_PLUGIN_JC:
			if (!slurm_conf.job_comp_port) {
				slurm_conf.job_comp_port = 54321;
			}
			db_info->port = slurm_conf.job_comp_port;
			db_info->host = xstrdup(slurm_conf.job_comp_host);
			db_info->user = xstrdup(slurm_conf.job_comp_user);
			db_info->pass = xstrdup(slurm_conf.job_comp_pass);
			db_info->params = xstrdup(slurm_conf.accounting_storage_params);
			break;
		default:
			xfree(db_info);
			fatal("Unknown kingbase_db_info %d", type);
	}
	return db_info;
}

extern int destroy_kingbase_db_info(kingbase_db_info_t *db_info)
{
	if (db_info) {
		xfree(db_info->backup);
		xfree(db_info->host);
		xfree(db_info->user);
		xfree(db_info->pass);
		xfree(db_info);
	}
	return SLURM_SUCCESS;
}

extern int kingbase_db_get_db_connection(kingbase_conn_t *kingbase_conn, char *db_name,
										 kingbase_db_info_t *db_info)
{
	int rc = SLURM_SUCCESS;
	bool storage_init = false;
	char *db_host = db_info->host;
	unsigned int my_timeout = 30;
	// bool reconnect = 0;
	char *options = NULL;
	char kingbase_port[32] = {0};

	xassert(kingbase_conn);

	slurm_mutex_lock(&kingbase_conn->lock);

	// mysql_options(mysql_conn->db_conn, MYSQL_OPT_RECONNECT, &reconnect);

	/*
	 * If this ever changes you will need to alter
	 * src/common/slurmdbd_defs.c function _send_init_msg to
	 * handle a different timeout when polling for the
	 * response.
	 */
	SSL_Options *ssl_options = (SSL_Options *)xmalloc(sizeof(SSL_Options));
	_set_kingbase_ssl_opts(kingbase_conn->db_conn, db_info->params, ssl_options);
	options = xstrdup_printf("dbname=%s sslcert=%s sslkey=%s sslrootcert=%s connect_timeout=%d",
							 db_name, ssl_options->cert, ssl_options->key, ssl_options->ca, my_timeout);
	if (ssl_options != NULL)
		xfree(ssl_options);
	sprintf(kingbase_port, "%d", db_info->port);
	while (!storage_init) {
		debug2("Attempting to connect to %s:%s", db_host,
			   kingbase_port);
		kingbase_conn->db_conn = KCIConnectionCreateDeprecated(db_host,
															   kingbase_port, NULL,
															   NULL, options,
															   db_info->user, db_info->pass);
		int err = KCIConnectionGetStatus(kingbase_conn->db_conn);
		/**/
		if (err != CONNECTION_OK) {
			const char *err_str = NULL;
			bool create_flag = false;

			err_str = KCIConnectionGetLastError(kingbase_conn->db_conn);
			if ((strstr(err_str, db_name)))
				create_flag = true;
			if (create_flag) {
				debug("Database %s not created.  Creating",
						db_name);
				rc = _create_db(db_name, db_info);
				continue;
			} else if (err != CONNECTION_OK) {
				if ((db_host == db_info->host) && db_info->backup) {
					debug2("KCIConnectionCreateDeprecated failed: %d %s because database not exist or sqlv connect fail",
							err, err_str);
					db_host = db_info->backup;
					continue;
				}
				error("KCIConnectionCreateDeprecated failed: %d %s",
						err, err_str);
				rc = ESLURM_DB_CONNECTION;
				KCIConnectionDestory(kingbase_conn->db_conn);
				kingbase_conn->db_conn = NULL;
				break;
			}
		}
		storage_init = true;
	}
	xfree(options);
	slurm_mutex_unlock(&kingbase_conn->lock);
	errno = rc;
	return rc;
}

extern int kingbase_db_close_db_connection(kingbase_conn_t *kingbase_conn)
{
	slurm_mutex_lock(&kingbase_conn->lock);
	if (kingbase_conn && kingbase_conn->db_conn) {
		KCIConnectionDestory(kingbase_conn->db_conn);
		kingbase_conn->db_conn = NULL;
	}
	slurm_mutex_unlock(&kingbase_conn->lock);
	return SLURM_SUCCESS;
}

extern int kingbase_db_cleanup()
{
	return SLURM_SUCCESS;
}

extern int kingbase_db_query(kingbase_conn_t *kingbase_conn, char *query)
{
	int rc = SLURM_SUCCESS;

	if (!kingbase_conn || !kingbase_conn->db_conn) {
		fatal("You haven't inited this storage yet.");
		return 0; /* For CLANG false positive */
	}
	slurm_mutex_lock(&kingbase_conn->lock);
	rc = _kingbase_query_internal(kingbase_conn->db_conn, query);
	slurm_mutex_unlock(&kingbase_conn->lock);
	return rc;
}

/*
 * Executes a single delete sql query.
 * Returns the number of deleted rows, <0 for failure.
 */
extern int kingbase_db_delete_affected_rows(kingbase_conn_t *kingbase_conn, char *query)
{
	int rc = SLURM_SUCCESS;

	if (!kingbase_conn || !kingbase_conn->db_conn) {
		fatal("You haven't inited this storage yet.");
		return 0; /* For CLANG false positive */
	}
	slurm_mutex_lock(&kingbase_conn->lock);
	if (!(rc = _kingbase_query_internal(kingbase_conn->db_conn, query))) {
		char *value = KCIResultGetAffectedCount(KCIConnectionFetchResult(kingbase_conn->db_conn));
		rc = value == NULL ? 0 : atoi(value);
	}
	slurm_mutex_unlock(&kingbase_conn->lock);
	return rc;
}

extern int kingbase_db_ping(kingbase_conn_t *kingbase_conn)
{
	int rc;

	if (!kingbase_conn->db_conn)
		return -1;

	/* clear out the old results so we don't get a 2014 error */
	slurm_mutex_lock(&kingbase_conn->lock);
	_clear_results(kingbase_conn->db_conn);
	rc = KCIConnectionGetStatus(kingbase_conn->db_conn);
	/*
	 * Starting in MariaDB 10.2 many of the api commands started
	 * setting errno erroneously.
	 */
	if (!rc)
		errno = 0;
	slurm_mutex_unlock(&kingbase_conn->lock);
	return rc;
}

extern int kingbase_db_commit(kingbase_conn_t *kingbase_conn)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL;

	if (!kingbase_conn->db_conn)
		return SLURM_ERROR;

	query = xstrdup_printf("COMMIT");
	slurm_mutex_lock(&kingbase_conn->lock);
	/* clear out the old results so we don't get a 2014 error */
	_clear_results(kingbase_conn->db_conn);
	if (KCIStatementSend(kingbase_conn->db_conn, query)) {
		KCIResult *res = NULL;
		res = KCIConnectionFetchResult(kingbase_conn->db_conn);
		int err = KCIResultGetStatusCode(res);
		if (err != EXECUTE_COMMAND_OK) {
				error("kingbase_commit failed: %d %s\n%s",
					  KCIResultGetStatusCode(res),
					  KCIConnectionGetLastError(kingbase_conn->db_conn), query);
				rc = SLURM_ERROR;
		}
		KCIResultDealloc(res);
	} else {
		/*
		 * Starting in MariaDB 10.2 many of the api commands started
		 * setting errno erroneously.
		 */
		errno = 0;
		fatal("KCIStatementSend failed:  %s\n%s",
			  KCIConnectionGetLastError(kingbase_conn->db_conn), query);
	}
	slurm_mutex_unlock(&kingbase_conn->lock);
	xfree(query);
	return rc;
}

extern int kingbase_db_rollback(kingbase_conn_t *kingbase_conn)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL;

	if (!kingbase_conn->db_conn)
		return SLURM_ERROR;

	query = xstrdup_printf("ROLLBACK");
	slurm_mutex_lock(&kingbase_conn->lock);
	/* clear out the old results so we don't get a 2014 error */
	_clear_results(kingbase_conn->db_conn);
	if (KCIStatementSend(kingbase_conn->db_conn, query)) {
		KCIResult *res = NULL;
		res = KCIConnectionFetchResult(kingbase_conn->db_conn);
		int err = KCIResultGetStatusCode(res);
		if (err != EXECUTE_COMMAND_OK) {
			error("kingbase_rollback failed: %d %s\n%s",
					KCIResultGetStatusCode(res),
					KCIConnectionGetLastError(kingbase_conn->db_conn), query);
			rc = SLURM_ERROR;
		}
		KCIResultDealloc(res);
	} else {
		/*
		 * Starting in MariaDB 10.2 many of the api commands started
		 * setting errno erroneously.
		 */
		//errno = 0;
		fatal("KCIStatementSend failed:  %s\n%s",
			  KCIConnectionGetLastError(kingbase_conn->db_conn), query);
	}
	slurm_mutex_unlock(&kingbase_conn->lock);
	xfree(query);
	return rc;
}

extern KCIResult *kingbase_db_query_ret(kingbase_conn_t *kingbase_conn,
										char *query, bool last)
{
	KCIResult *result = NULL;

	slurm_mutex_lock(&kingbase_conn->lock);
	if (_kingbase_query_internal(kingbase_conn->db_conn, query) != SLURM_ERROR) {
		if (last)
			result = _get_last_result(kingbase_conn->db_conn);
		else
			result = _get_first_result(kingbase_conn->db_conn);
		/*
		 * Starting in MariaDB 10.2 many of the api commands started
		 * setting errno erroneously.
		 */
		errno = 0;
		if (!result && KCIResultGetColumnCount(result)) {
			/* should have returned data */
			error("We should have gotten a result: '%m' '%s'",
					KCIConnectionGetLastError(kingbase_conn->db_conn));
		}
	}
	slurm_mutex_unlock(&kingbase_conn->lock);
	return result;
}

extern int kingbase_db_query_check_after(kingbase_conn_t *kingbase_conn, char *query)
{
	int rc = SLURM_SUCCESS;

	slurm_mutex_lock(&kingbase_conn->lock);
	if ((rc = _kingbase_query_internal(
			 kingbase_conn->db_conn, query)) != SLURM_ERROR)
		rc = _clear_results(kingbase_conn->db_conn);
	slurm_mutex_unlock(&kingbase_conn->lock);
	return rc;
}

// extern uint64_t kingbase_db_insert_ret_id(kingbase_conn_t *kingbase_conn, char *query)
// {
// 	uint64_t new_id = 0;
// 	int rc = 0;
// 	slurm_mutex_lock(&kingbase_conn->lock);

// 	rc = _kingbase_query_internal(kingbase_conn->db_conn, query);

// 	if ((rc != SLURM_ERROR) && 
// 		_kingbase_query_internal(kingbase_conn->db_conn, "select last_insert_id()") != SLURM_ERROR) {
// 		KCIResult *res = KCIConnectionFetchResult(kingbase_conn->db_conn);
// 		char *value = KCIResultGetColumnValue(res, 0, 0);
//         new_id = value == NULL ? 0 : atoi(value);
// 		if (!new_id) {
// 				/* should have new id */
// 				error("%s: We should have gotten a new id: %s",
// 					  __func__, KCIConnectionGetLastError(kingbase_conn->db_conn));
// 		}
// 	}
// 	slurm_mutex_unlock(&kingbase_conn->lock);
// 	return new_id;
// }

extern int kingbase_db_create_table(kingbase_conn_t *kingbase_conn, char *table_name,
									storage_field_t *fields, char *ending)
{
	char *query = NULL;
	int i = 0, rc;
	storage_field_t *first_field = fields;

	if (!fields || !fields->name) {
		error("Not creating an empty table");
		return SLURM_ERROR;
	}

	/* We have an internal table called table_defs_table which
	 * contains the definition of each table in the database.  To
	 * speed things up we just check against that to see if
	 * anything has changed.
	 */
	/*kingbase暂不支持设置主键长度，这里和mysql有所区别*/
	query = xstrdup_printf("create table if not exists %s "
						   "(creation_time bigint not null, "
						   "mod_time bigint default 0 not null, "
						   "table_name text not null, "
						   "definition text not null, "
						   "primary key (table_name))",
						   table_defs_table);
	
	fetch_flag_t *fetch_flag = NULL;
	fetch_result_t *data_rt = NULL;
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);	
	xfree(query);
	if (rc != SLURM_SUCCESS)  {
		return SLURM_ERROR;
	}

    /*拼接sql语句*/
	query = xstrdup_printf("create table if not exists %s (%s %s",
						   table_name, fields->name, fields->options);
	i = 1;
	fields++;

	while (fields && fields->name) {
		xstrfmtcat(query, ", %s %s", fields->name, fields->options);
		fields++;
		i++;
	}
	xstrcat(query, ending);

	/* make sure we can do a rollback */
	//xstrcat(query, " engine='innodb'");
    /*执行sql语句，mysql这里设置：关闭autocommit，开启事务，可以进行回滚操作*/
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	free_res_data(data_rt, fetch_flag);		
	xfree(query);
	if (rc != SLURM_SUCCESS) {
		return SLURM_ERROR;
	}

	rc = _kingbase_make_table_current(
		kingbase_conn, table_name, first_field, ending);
	return rc;
}
