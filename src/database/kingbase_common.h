/*****************************************************************************\
 *  kingbase_common.h - common functions for the kingbase storage plugin.
 *****************************************************************************
 *
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
#ifndef _KINGBASE_COMMON_H
#define _KINGBASE_COMMON_H

#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>

#include "slurm/slurm_errno.h"
#include "src/common/list.h"
#include "src/common/xstring.h"

// #include <mysql.h>
// #include <mysql_error.h>

// libkci header files
#include "src/database/libkci/include/libkci_fe.h"
#include "src/database/libkci/include/libkci_fs.h"



typedef enum
{
	SLURM_KINGBASE_PLUGIN_NOTSET,
	SLURM_KINGBASE_PLUGIN_AS, /* accounting_storage */
	SLURM_KINGBASE_PLUGIN_JC, /* jobcomp */
} slurm_kingbase_plugin_type_t;

typedef struct
{
	bool cluster_deleted;
	char *cluster_name;
	KCIConnection *db_conn;
	pthread_mutex_t lock;
	char *pre_commit_query;
	bool rollback;
	List update_list;
	int conn;
} kingbase_conn_t;

typedef struct
{
	char *backup;
	uint32_t port;
	char *host;
	const char *user;
	char *params;
	const char *pass;
} kingbase_db_info_t;

typedef struct
{
	char *name;
	char *options;
} storage_field_t;

typedef struct {
	const char *key;
	const char *cert;
	const char *ca;
	const char *ca_path;
	const char *cipher;
} SSL_Options;

typedef struct fetch_result {
	int rc;
    int RowCount; /*结果集行数*/
	int ColumnCount; /*结果集列数*/
	int rows_affect;  /*受影响的行数*/
	int insert_ret_id; /*返回的id*/
} fetch_result_t;

typedef struct fetch_flag{
    bool rows_affect; /*受影响的行数falg*/
	bool insert_ret_id; /*返回的id flag*/
	bool result; /*结果集flag*/
} fetch_flag_t;
extern int kingbase_for_fetch2(kingbase_conn_t *kingbase_conn, char *query, fetch_flag_t* flag, fetch_result_t* data_rt,char *query2);
extern fetch_flag_t* set_fetch_flag(bool insert_ret_id, bool result, bool rows_affect);
extern void free_res_data(fetch_result_t* data_rt, fetch_flag_t *fetch_flag);
extern int kingbase_for_fetch(kingbase_conn_t *kingbase_conn, char *query, fetch_flag_t* flag, fetch_result_t* data_rt);
extern char *slurm_add_slash_to_quotes2(char *str);

extern kingbase_conn_t *create_kingbase_conn(int conn_num, bool rollback,
											 char *cluster_name);
extern int destroy_kingbase_conn(kingbase_conn_t *kingbase_conn);
extern kingbase_db_info_t *create_kingbase_db_info(slurm_kingbase_plugin_type_t type);
extern int destroy_kingbase_db_info(kingbase_db_info_t *db_info);

extern int kingbase_db_get_db_connection(kingbase_conn_t *kingbase_conn, char *db_name,
										 kingbase_db_info_t *db_info);
extern int kingbase_db_close_db_connection(kingbase_conn_t *kingbase_conn);
extern int kingbase_db_cleanup();
extern int kingbase_db_query(kingbase_conn_t *kingbase_conn, char *query);
extern int kingbase_db_delete_affected_rows(kingbase_conn_t *kingbase_conn, char *query);
extern int kingbase_db_ping(kingbase_conn_t *kingbase_conn);
extern int kingbase_db_commit(kingbase_conn_t *kingbase_conn);
extern int kingbase_db_rollback(kingbase_conn_t *kingbase_conn);

extern KCIResult *kingbase_db_query_ret(kingbase_conn_t *kingbase_conn,
										char *query, bool last);
extern int kingbase_db_query_check_after(kingbase_conn_t *kingbase_conn, char *query);

//extern uint64_t kingbase_db_insert_ret_id(kingbase_conn_t *kingbase_conn, char *query);

extern int kingbase_db_create_table(kingbase_conn_t *kingbase_conn, char *table_name,
									storage_field_t *fields, char *ending);

#endif