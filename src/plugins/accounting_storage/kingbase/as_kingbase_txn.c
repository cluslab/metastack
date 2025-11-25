/*****************************************************************************\
 *  as_kingbase_txn.c - functions dealing with transactions.
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

#include "as_kingbase_txn.h"

extern List as_kingbase_get_txn(kingbase_conn_t *kingbase_conn, uid_t uid,
			     slurmdb_txn_cond_t *txn_cond)
{
	char *query = NULL;
	char *assoc_extra = NULL;
	char *name_extra = NULL;
	char *extra = NULL;
	char *tmp = NULL;
	List txn_list = NULL;
	ListIterator itr = NULL;
	char *object = NULL;
	int set = 0;
	int i=0;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	List use_cluster_list = NULL;
	bool locked = 0;

	/* if this changes you will need to edit the corresponding enum */
	char *txn_req_inx[] = {
		"id",
		"timestamp",
		"action",
		"name",
		"actor",
		"info",
		"cluster"
	};
	enum {
		TXN_REQ_ID,
		TXN_REQ_TS,
		TXN_REQ_ACTION,
		TXN_REQ_NAME,
		TXN_REQ_ACTOR,
		TXN_REQ_INFO,
		TXN_REQ_CLUSTER,
		TXN_REQ_COUNT
	};

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!txn_cond)
		goto empty;

	/* handle query for associations first */
	if (txn_cond->acct_list && list_count(txn_cond->acct_list)) {
		set = 0;
		if (assoc_extra)
			xstrcat(assoc_extra, " and (");
		else
			xstrcat(assoc_extra, " where (");

		if (name_extra)
			xstrcat(name_extra, " and (");
		else
			xstrcat(name_extra, " (");
		itr = list_iterator_create(txn_cond->acct_list);
		while ((object = list_next(itr))) {
			if (set) {
				xstrcat(assoc_extra, " or ");
				xstrcat(name_extra, " or ");
			}

			xstrfmtcat(assoc_extra, "acct='%s'", object);

			xstrfmtcat(name_extra, "(name like E'%%\\'%s\\'%%'"
				   " or name='%s')"
				   " or (info like E'%%acct=\\'%s\\'%%')",
				   object, object, object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(assoc_extra, ")");
		xstrcat(name_extra, ")");
	}

	if (txn_cond->cluster_list && list_count(txn_cond->cluster_list)) {
		set = 0;
		if (name_extra)
			xstrcat(name_extra, " and (");
		else
			xstrcat(name_extra, "(");

		itr = list_iterator_create(txn_cond->cluster_list);
		while ((object = list_next(itr))) {
			if (set) {
				xstrcat(name_extra, " or ");
			}
			xstrfmtcat(name_extra, "(cluster='%s' or "
				   "name like E'%%\\'%s\\'%%' or name='%s')"
				   " or (info like E'%%cluster=\\'%s\\'%%')",
				   object, object, object, object); 
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(name_extra, ")");
		use_cluster_list = txn_cond->cluster_list;
	}

	if (txn_cond->user_list && list_count(txn_cond->user_list)) {
		set = 0;
		if (assoc_extra)
			xstrcat(assoc_extra, " and (");
		else
			xstrcat(assoc_extra, " where (");

		if (name_extra)
			xstrcat(name_extra, " and (");
		else
			xstrcat(name_extra, "(");

		itr = list_iterator_create(txn_cond->user_list);
		while ((object = list_next(itr))) {
			if (set) {
				xstrcat(assoc_extra, " or ");
				xstrcat(name_extra, " or ");
			}
			xstrfmtcat(assoc_extra, "`user`='%s'", object);

			xstrfmtcat(name_extra, "(name like E'%%\\'%s\\'%%'"
				   " or name='%s')"
				   " or (info like E'%%user=\\'%s\\'%%')",
				   object, object, object);

			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(assoc_extra, ")");
		xstrcat(name_extra, ")");
	}

	if (assoc_extra) {
		if (!locked && !use_cluster_list) {
			slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
			use_cluster_list = list_shallow_copy(
				as_kingbase_cluster_list);
			locked = 1;
		}

		itr = list_iterator_create(use_cluster_list);
		while ((object = list_next(itr))) {
			xstrfmtcat(query, "select id_assoc from `%s_%s`%s",
				   object, assoc_table, assoc_extra);
			DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s",
			         query);
			//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);		
			result = kingbase_db_query_ret(kingbase_conn, query, 0); 
			xfree(query);
			if(KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
				KCIResultDealloc(result);
				break;
			}

			if (KCIResultGetRowCount(result)) {
				if (extra)
					xstrfmtcat(extra,
						   " or (cluster='%s' and (",
						   object);
				else
					xstrfmtcat(extra,
						   " where (cluster='%s' and (",
						   object);

				set = 0;
                cnt = KCIResultGetRowCount(result);
				for(int j = 0; j < cnt; j++) {
					if (set)
						xstrcat(extra, " or ");

					xstrfmtcat(extra,
						   "(name like "
						   "'%%id_assoc=%s %%' "
						   "or name like "
						   "'%%id_assoc=%s')",
						   KCIResultGetColumnValue(result, j, 0), KCIResultGetColumnValue(result, j, 0));
					set = 1;
				}
				xstrcat(extra, "))");
			}
			KCIResultDealloc(result);
		}
		list_iterator_destroy(itr);

		xfree(assoc_extra);
	}

	if (name_extra) {
		if (extra)
			xstrfmtcat(extra, " or (%s)", name_extra);
		else
			xstrfmtcat(extra, " where (%s)", name_extra);
		xfree(name_extra);
	}
	/*******************************************/

	if (txn_cond->action_list && list_count(txn_cond->action_list)) {
		set = 0;
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, " where (");
		itr = list_iterator_create(txn_cond->action_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "action='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (txn_cond->actor_list && list_count(txn_cond->actor_list)) {
		set = 0;
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, " where (");
		itr = list_iterator_create(txn_cond->actor_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "actor='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (txn_cond->id_list && list_count(txn_cond->id_list)) {
		set = 0;
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, " where (");
		itr = list_iterator_create(txn_cond->id_list);
		while ((object = list_next(itr))) {
			char *ptr = NULL;
			long num = strtol(object, &ptr, 10);
			if ((num == 0) && ptr && ptr[0]) {
				error("Invalid value for txn id (%s)",
				      object);
				xfree(extra);
				list_iterator_destroy(itr);
				goto end_it;
			}

			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "id=%s", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (txn_cond->info_list && list_count(txn_cond->info_list)) {
		set = 0;
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, " where (");
		itr = list_iterator_create(txn_cond->info_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "info like '%%%s%%'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (txn_cond->name_list && list_count(txn_cond->name_list)) {
		set = 0;
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, " where (");
		itr = list_iterator_create(txn_cond->name_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "name like '%%%s%%'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (txn_cond->time_start && txn_cond->time_end) {
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, " where (");
		xstrfmtcat(extra, "timestamp < %ld and timestamp >= %ld)",
			   txn_cond->time_end, txn_cond->time_start);
	} else if (txn_cond->time_start) {
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, " where (");
		xstrfmtcat(extra, "timestamp >= %ld)", txn_cond->time_start);

	} else if (txn_cond->time_end) {
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, " where (");
		xstrfmtcat(extra, "timestamp < %ld)", txn_cond->time_end);
	}

	/* make sure we can get the max length out of the database
	 * when grouping the names
	 */
	// if (txn_cond->with_assoc_info) {
	// 	//info("[query] line %d, query: set session group_concat_max_len=65536;", __LINE__);
	// 	fetch_flag_t* fetch_flag = NULL;
	// 	fetch_result_t* data_rt = NULL;

	// 	fetch_flag = set_fetch_flag(false, false, false);
	// 	data_rt = xmalloc(sizeof(fetch_result_t));
		
	// 	char *tmp = xstrdup("set session group_concat_max_len=65536;");
	// 	kingbase_for_fetch(kingbase_conn, tmp, fetch_flag, data_rt);
	// 	free_res_data(data_rt, fetch_flag); 
	// 	xfree(tmp);
	// }

empty:
	if (!locked && !use_cluster_list) {
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		use_cluster_list = list_shallow_copy(as_kingbase_cluster_list);
		locked = 1;
	}

	xfree(tmp);
	xstrfmtcat(tmp, "%s", txn_req_inx[i]);
	for(i = 1; i < TXN_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", txn_req_inx[i]);
	}

	query = xstrdup_printf("select %s from %s", tmp, txn_table);

	if (extra) {
		xstrfmtcat(query, "%s", extra);
		xfree(extra);
	}
	xstrcat(query, " order by timestamp;");

	xfree(tmp);

	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		goto end_it;
	}

	txn_list = list_create(slurmdb_destroy_txn_rec);
    cnt = KCIResultGetRowCount(result);
	for(int j = 0; j < cnt; j++) {
		slurmdb_txn_rec_t *txn = xmalloc(sizeof(slurmdb_txn_rec_t));

		list_append(txn_list, txn);

		txn->action = slurm_atoul(KCIResultGetColumnValue(result, j, TXN_REQ_ACTION));
		txn->actor_name = xstrdup(KCIResultGetColumnValue(result, j, TXN_REQ_ACTOR));
		txn->id = slurm_atoul(KCIResultGetColumnValue(result, j, TXN_REQ_ID));
		txn->set_info = xstrdup(KCIResultGetColumnValue(result, j, TXN_REQ_INFO));
		txn->timestamp = slurm_atoul(KCIResultGetColumnValue(result, j, TXN_REQ_TS));
		txn->where_query = xstrdup(KCIResultGetColumnValue(result, j, TXN_REQ_NAME));
		txn->clusters = xstrdup(KCIResultGetColumnValue(result, j, TXN_REQ_CLUSTER));

		if (txn_cond && txn_cond->with_assoc_info
		    && (txn->action == DBD_ADD_ASSOCS
			|| txn->action == DBD_MODIFY_ASSOCS
			|| txn->action == DBD_REMOVE_ASSOCS)) {
			KCIResult *result2 = NULL;

			if (txn->clusters) {
				query = xstrdup_printf(
					"select "
					"group_concat(distinct `user` "
					"order by `user`), "
					"group_concat(distinct acct "
					"order by acct) "
					"from `%s_%s` where %s",
					txn->clusters, assoc_table,
					KCIResultGetColumnValue(result, j, TXN_REQ_NAME));
				debug4("%d(%s:%d) query\n%s", kingbase_conn->conn,
				       THIS_FILE, __LINE__, query);
				//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);	   
				result2 = kingbase_db_query_ret(kingbase_conn, query, 0);
				xfree(query);
				if (KCIResultGetStatusCode(result2) != EXECUTE_TUPLES_OK) {
					KCIResultDealloc(result2);
					continue;
				}

				if (KCIResultGetRowCount(result2) > 0) {
					char *temp = KCIResultGetColumnValue(result2, 0, 0);
					if (*temp != '\0')
						txn->users = xstrdup(KCIResultGetColumnValue(result2, 0, 0));
					temp = KCIResultGetColumnValue(result2, 0, 1);
					if (*temp != '\0')
						txn->accts = xstrdup(KCIResultGetColumnValue(result2, 0, 1));
				}
				KCIResultDealloc(result2);
			} else {
				error("We can't handle associations "
				      "from action %s yet.",
				      slurmdbd_msg_type_2_str(txn->action, 1));
			}
		}
	}
	KCIResultDealloc(result);

end_it:
	if (locked) {
		FREE_NULL_LIST(use_cluster_list);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	}

	return txn_list;
}
