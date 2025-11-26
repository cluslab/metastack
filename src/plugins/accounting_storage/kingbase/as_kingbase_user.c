/*****************************************************************************\
 *  as_kingbase_user.c - functions dealing with users and coordinators.
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

#include "as_kingbase_assoc.h"
#include "as_kingbase_user.h"
#include "as_kingbase_wckey.h"

typedef struct {
	list_t *acct_list; /* for coords, a list of just char * instead of
			    * slurmdb_coord_rec_t */
	char *coord_query;
	char *coord_query_pos;
	kingbase_conn_t *kingbase_conn;
	time_t now;
	int rc;
	bool ret_str_err;
	char *ret_str;
	char *ret_str_pos;
	char *txn_query;
	char *txn_query_pos;
	slurmdb_user_rec_t *user_in;
	char *user_name;
} add_user_cond_t;

#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
#include "as_kingbase_usage.h"
#endif

#ifdef __METASTACK_ASSOC_HASH
#include "src/common/assoc_mgr.h"
#endif

static int _change_user_name(kingbase_conn_t *kingbase_conn, slurmdb_user_rec_t *user)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL;
	list_itr_t *itr = NULL;
	char *cluster_name = NULL;
	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t* data_rt = NULL;

	xassert(user->old_name);
	xassert(user->name);

	slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
	itr = list_iterator_create(as_kingbase_cluster_list);
	while ((cluster_name = list_next(itr))) {
		// Change assoc_tables
		xstrfmtcat(query, "update `%s_%s` set `user`='%s' "
			   "where `user`='%s';", cluster_name, assoc_table,
			   user->name, user->old_name);
		// Change wckey_tables
		xstrfmtcat(query, "update `%s_%s` set `user`='%s' "
			   "where `user`='%s';", cluster_name, wckey_table,
			   user->name, user->old_name);
	}
	list_iterator_destroy(itr);
	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	// Change coord_tables
	xstrfmtcat(query, "update %s set `user`='%s' where `user`='%s';",
		   acct_coord_table, user->name, user->old_name);

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	
	xfree(query);

	// KCIResult *res = NULL;
	// res = KCIConnectionFetchResult(kingbase_conn->db_conn);
	// int err = KCIResultGetStatusCode(res);
	if (rc != SLURM_SUCCESS) {
		rc = SLURM_ERROR;
		reset_kingbase_conn(kingbase_conn);
	}
	free_res_data(data_rt, fetch_flag);
	// KCIResultDealloc(res);
	return rc;
}

static List _get_other_user_names_to_mod(kingbase_conn_t *kingbase_conn, uint32_t uid,
					 slurmdb_user_cond_t *user_cond)
{
	List tmp_list = NULL;
	List ret_list = NULL;
	list_itr_t *itr = NULL;

	slurmdb_assoc_cond_t assoc_cond;
	slurmdb_wckey_cond_t wckey_cond;

	if (!user_cond->def_acct_list || !list_count(user_cond->def_acct_list))
		goto no_assocs;

	/* We have to use a different association_cond here because
	   other things could be set here we don't care about in the
	   user's. (So to be safe just move over the info we care about) */
	memset(&assoc_cond, 0, sizeof(slurmdb_assoc_cond_t));
	assoc_cond.acct_list = user_cond->def_acct_list;
	if (user_cond->assoc_cond) {
		if (user_cond->assoc_cond->cluster_list)
			assoc_cond.cluster_list =
				user_cond->assoc_cond->cluster_list;
		if (user_cond->assoc_cond->user_list)
			assoc_cond.user_list = user_cond->assoc_cond->user_list;
	}
	assoc_cond.only_defs = 1;
#ifdef __METASTACK_OPT_LIST_USER
	tmp_list = as_kingbase_get_assocs(kingbase_conn, uid, &assoc_cond, false);
#else
	tmp_list = as_kingbase_get_assocs(kingbase_conn, uid, &assoc_cond);
#endif 
	if (tmp_list) {
		slurmdb_assoc_rec_t *object = NULL;
		itr = list_iterator_create(tmp_list);
		while ((object = list_next(itr))) {
			if (!ret_list)
				ret_list = list_create(xfree_ptr);
			slurm_addto_char_list(ret_list, object->user);
		}
		list_iterator_destroy(itr);
		FREE_NULL_LIST(tmp_list);
	}

no_assocs:
	if (!user_cond->def_wckey_list
	    || !list_count(user_cond->def_wckey_list))
		goto no_wckeys;

	memset(&wckey_cond, 0, sizeof(slurmdb_wckey_cond_t));
	if (user_cond->assoc_cond) {
		if (user_cond->assoc_cond->cluster_list)
			wckey_cond.cluster_list =
				user_cond->assoc_cond->cluster_list;
		if (user_cond->assoc_cond->user_list)
			wckey_cond.user_list = user_cond->assoc_cond->user_list;
	}
	wckey_cond.name_list = user_cond->def_wckey_list;
	wckey_cond.only_defs = 1;

	tmp_list = as_kingbase_get_wckeys(kingbase_conn, uid, &wckey_cond);
	if (tmp_list) {
		slurmdb_wckey_rec_t *object = NULL;
		itr = list_iterator_create(tmp_list);
		while ((object = list_next(itr))) {
			if (!ret_list)
				ret_list = list_create(xfree_ptr);
			slurm_addto_char_list(ret_list, object->user);
		}
		list_iterator_destroy(itr);
		FREE_NULL_LIST(tmp_list);
	}

no_wckeys:

	return ret_list;
}


#ifdef __METASTACK_ASSOC_HASH
/** build a all_sub_acct_hash
 * key - cluster name
 * value - assoc_hash table, key is the parent account, value is the list of sub accounts 
*/
static void get_all_accts(kingbase_conn_t *kingbase_conn, slurmdb_assoc_cond_t *assoc_cond, str_key_hash_t **all_sub_acct_hash)
{
	char *query = NULL;
	list_itr_t *itr = NULL;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	slurmdb_assoc_rec_t *assoc;
	List use_cluster_list = NULL;
	bool locked = false;

	if (all_sub_acct_hash) {
		destroy_hash_value_hash(all_sub_acct_hash);
	}

	if (assoc_cond && assoc_cond->cluster_list && list_count(assoc_cond->cluster_list)) {
		use_cluster_list = assoc_cond->cluster_list;
	} else {
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		use_cluster_list = list_shallow_copy(as_kingbase_cluster_list);
		locked = true;
	}
	
	if (use_cluster_list) {
		itr = list_iterator_create(use_cluster_list);
		char *cluster_name = NULL;
		while ((cluster_name = list_next(itr))){
			assoc_hash_t *sub_acct_hash = NULL;
			query = xstrdup_printf("select distinct acct,`user`,parent_acct,deleted from `%s_%s` "
								"where `user`='' && deleted=0", 
								cluster_name, assoc_table);
								
			DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
			if (!(result = kingbase_db_query_ret(
						kingbase_conn, query, 0))) {
				xfree(query);
				return;
			}
			xfree(query);

			cnt = KCIResultGetRowCount(result);
			for (int i = 0; i < cnt; i++) {
				assoc = xmalloc(sizeof(slurmdb_assoc_rec_t));

				if (KCIResultGetColumnValue(result,i,2)) {
					assoc->acct = xstrdup(KCIResultGetColumnValue(result,i,0));
					assoc->parent_acct = xstrdup(KCIResultGetColumnValue(result,i,2));
				}
				
				insert_assoc_hash(&sub_acct_hash, assoc, assoc->parent_acct, slurmdb_destroy_assoc_rec);
			}
			KCIResultDealloc(result);
			if (sub_acct_hash) {
				insert_str_key_hash(all_sub_acct_hash, sub_acct_hash, cluster_name);
			}
		}
		list_iterator_destroy(itr);
	}

	if (locked) {
		FREE_NULL_LIST(use_cluster_list);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	}
}

/** get all its sub_accounts by account name */
static void get_sub_accts(char *parent_acct, assoc_hash_t *sub_acct_hash, 
	slurmdb_user_rec_t *user, str_key_hash_t *coord_accts_hash)
{
	List sub_acct_list = NULL;
	slurmdb_assoc_rec_t *sub_acct = NULL;
	assoc_hash_t *entry = NULL;
	slurmdb_coord_rec_t *coord = NULL;

	if (!user) {
		error("We need a user to fill in.");
	}

	entry = find_assoc_entry(&sub_acct_hash, parent_acct);

	if (entry) {
		sub_acct_list = entry->value_assoc_list;
	}

	if (sub_acct_list) {
		list_itr_t *itr = list_iterator_create(sub_acct_list);
		while ((sub_acct = list_next(itr))) {
			if (sub_acct && !(find_str_key_hash(&coord_accts_hash, sub_acct->acct))) {
				coord = xmalloc(sizeof(slurmdb_coord_rec_t));
				list_append(user->coord_accts, coord);
				coord->name = xstrdup(sub_acct->acct);
				coord->direct = 0;
				insert_str_key_hash(&coord_accts_hash, coord, coord->name);

				get_sub_accts(sub_acct->acct, sub_acct_hash, user, coord_accts_hash);
			}
		}
		list_iterator_destroy(itr);
	}
}

static void get_assoc_flag_user_coord(kingbase_conn_t *kingbase_conn, 
	assoc_hash_t **all_flag_user_coord_hash, bool *has_implicit)
{
	char *query = NULL, *query_pos = NULL, *implicit_coord_query = NULL, *implicit_query_pos = NULL;
	char *cluster_name = NULL;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	list_itr_t *itr = NULL;
	slurmdb_assoc_rec_t *implicit_acct_coord = NULL;

	slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
	itr = list_iterator_create(as_kingbase_cluster_list);
	while ((cluster_name = list_next(itr))) {
		xstrfmtcatat(query, &query_pos,
			"%sselect distinct t2.acct,t2.`user` from `%s_%s` as t1, "
			"`%s_%s` as t2 where t1.deleted=0 and "
			"t2.deleted=0 and t2.`user`!='' and "
			"(t1.flags & %u) && t2.lineage like "
			"concat('%%/', t1.acct, '/%%')",
			query ? " union " : "", cluster_name, assoc_table,
			cluster_name, assoc_table,
			ASSOC_FLAG_USER_COORD);

		xstrfmtcatat(implicit_coord_query, &implicit_query_pos,
			"%sselect distinct acct,flags from `%s_%s` "
			"where deleted=0 and (flags & %u) ",
			implicit_coord_query ? " union " : "", cluster_name, assoc_table,
			ASSOC_FLAG_USER_COORD);
	}
	list_iterator_destroy(itr);
	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

	if (implicit_coord_query) {
		implicit_query_pos = NULL;

		DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", implicit_coord_query);

		if ((result = kingbase_db_query_ret(kingbase_conn, implicit_coord_query, 0)) && 
		      (KCIResultGetRowCount(result) == 0)) {
				*has_implicit = false;
				xfree(implicit_coord_query);
				query_pos = NULL;
				xfree(query);
				KCIResultDealloc(result);
				debug2("No implicit account coordinators.");
				return;
		}
		xfree(implicit_coord_query);
		if (result) {
			KCIResultDealloc(result);
		}
	}

	if (query) {
		query_pos = NULL;

		DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);

		if (!(result =
		      kingbase_db_query_ret(kingbase_conn, query, 0))) {
			xfree(query);
			error("Failed to get all implicit account coordinators.");
			return;
		}
		xfree(query);

		if (KCIResultGetRowCount(result) == 0) {
			*has_implicit = false;
			KCIResultDealloc(result);
			debug2("No implicit account coordinators.");
			return;
		}

    	cnt = KCIResultGetRowCount(result);
		for (int i = 0; i < cnt; i++) {
			implicit_acct_coord = xmalloc(sizeof(slurmdb_assoc_rec_t));
			implicit_acct_coord->acct = xstrdup(KCIResultGetColumnValue(result,i,0));
			insert_assoc_hash(all_flag_user_coord_hash, implicit_acct_coord, KCIResultGetColumnValue(result,i,1), slurmdb_destroy_assoc_rec);
		}

		KCIResultDealloc(result);
	}
}
#endif

/* Fill in all the accounts this user is coordinator over.  This
 * will fill in all the sub accounts they are coordinator over also.
 */
#ifdef __METASTACK_ASSOC_HASH
static int _get_user_coords(kingbase_conn_t *kingbase_conn, slurmdb_user_rec_t *user, str_key_hash_t *all_sub_acct_hash, 
	assoc_hash_t *all_flag_user_coord_hash, bool has_implicit)
#else
static int _get_user_coords(kingbase_conn_t *kingbase_conn, slurmdb_user_rec_t *user)
#endif
{
	char *query = NULL, *query_pos = NULL;
	char *meat_query = NULL, *meat_query_pos = NULL;
	slurmdb_coord_rec_t *coord = NULL;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	list_itr_t *itr = NULL;
	char *cluster_name = NULL;
#ifdef __METASTACK_ASSOC_HASH
	str_key_hash_t *coord_accts_hash = NULL;
#endif

	if (!user) {
		error("We need a user to fill in.");
		return SLURM_ERROR;
	}

	if (!user->coord_accts)
		user->coord_accts = list_create(slurmdb_destroy_coord_rec);
#ifdef __METASTACK_ASSOC_HASH
	else {
		itr = list_iterator_create(user->coord_accts);
		while ((coord = list_next(itr))) {
			insert_str_key_hash(&coord_accts_hash, coord, coord->name);
		}
		list_iterator_destroy(itr);
	}
#endif
	/*
	 * Get explicit account coordinators
	 */
	query = xstrdup_printf(
		"select acct from %s where `user`='%s' and deleted=0",
		acct_coord_table, user->name);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
#ifdef __METASTACK_ASSOC_HASH
		destroy_str_key_hash(&coord_accts_hash);
#endif
		return SLURM_ERROR;
	}
    cnt = KCIResultGetRowCount(result);
	for (int i = 0; i < cnt; i++) {
#ifdef __METASTACK_ASSOC_HASH
		if (find_str_key_hash(&coord_accts_hash, KCIResultGetColumnValue(result,i,0)))
#else
		if (assoc_mgr_is_user_acct_coord_user_rec(user, KCIResultGetColumnValue(result,i,0)))
#endif
			continue;

		coord = xmalloc(sizeof(slurmdb_coord_rec_t));
		list_append(user->coord_accts, coord);
		coord->name = xstrdup(KCIResultGetColumnValue(result,i,0));
		coord->direct = 1;
#ifdef __METASTACK_ASSOC_HASH
		insert_str_key_hash(&coord_accts_hash, coord, coord->name);
#endif
	}
	KCIResultDealloc(result);

	/*
	 * Get implicit account coordinators
	 */

#ifdef __METASTACK_ASSOC_HASH
	if (all_flag_user_coord_hash) {
		assoc_hash_t *entry = find_assoc_entry(&all_flag_user_coord_hash, user->name);
		if (entry) {
			itr = list_iterator_create(entry->value_assoc_list);
			slurmdb_assoc_rec_t * coord_acct = NULL;
			while ((coord_acct = list_next(itr))) {
				if (find_str_key_hash(&coord_accts_hash, coord_acct->acct)) {
					continue;
				}

				coord = xmalloc(sizeof(slurmdb_coord_rec_t));
				debug2("adding %s to coord_accts for user %s",
					coord_acct->acct, user->name);
				list_append(user->coord_accts, coord);
				coord->name = xstrdup(coord_acct->acct);
				insert_str_key_hash(&coord_accts_hash, coord, coord->name);
			}
		}
	} else if (has_implicit) {
		query_pos = NULL;
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		itr = list_iterator_create(as_kingbase_cluster_list);
		while ((cluster_name = list_next(itr))) {
			xstrfmtcatat(query, &query_pos,
					"%sselect distinct t2.acct from `%s_%s` as t1, "
					"`%s_%s` as t2 where t1.deleted=0 and "
					"t2.deleted=0 and "
					"(t1.flags & %u) and t2.lineage like "
					"concat('%%/', t1.acct, '/%%0-%s/%%')",
					query ? " union " : "", cluster_name, assoc_table,
					cluster_name, assoc_table,
					ASSOC_FLAG_USER_COORD, user->name);
		}
		list_iterator_destroy(itr);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

		if (query) {
			query_pos = NULL;
			DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);

			if (!(result =
				kingbase_db_query_ret(kingbase_conn, query, 0))) {
				xfree(query);
				destroy_str_key_hash(&coord_accts_hash);
				return SLURM_ERROR;
			}
			xfree(query);

			cnt = KCIResultGetRowCount(result);
			for (int i = 0; i < cnt; i++) {
				if (find_str_key_hash(&coord_accts_hash, KCIResultGetColumnValue(result,i,0)))
					continue;

				coord = xmalloc(sizeof(slurmdb_coord_rec_t));
				debug2("adding %s to coord_accts for user %s",
					KCIResultGetColumnValue(result,i,0), user->name);
				list_append(user->coord_accts, coord);
				coord->name = xstrdup(KCIResultGetColumnValue(result,i,0));
				insert_str_key_hash(&coord_accts_hash, coord, coord->name);
			}
			KCIResultDealloc(result);
		}
	}
#else
	query_pos = NULL;
	slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
	itr = list_iterator_create(as_kingbase_cluster_list);
	while ((cluster_name = list_next(itr))) {
		xstrfmtcatat(query, &query_pos,
			     "%sselect distinct t2.acct from `%s_%s` as t1, "
			     "`%s_%s` as t2 where t1.deleted=0 and "
			     "t2.deleted=0 and "
			     "(t1.flags & %u) and t2.lineage like "
			     "concat('%%/', t1.acct, '/%%0-%s/%%')",
			     query ? " union " : "", cluster_name, assoc_table,
			     cluster_name, assoc_table,
			     ASSOC_FLAG_USER_COORD, user->name);
	}
	list_iterator_destroy(itr);
	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

	if (query) {
		query_pos = NULL;

		DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);

		result = kingbase_db_query_ret(kingbase_conn, query, 0);
		xfree(query);
		if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
			KCIResultDealloc(result);
			return SLURM_ERROR;
		}
        cnt = KCIResultGetRowCount(result);
		for (int i = 0; i < cnt; i++) {
			if (assoc_mgr_is_user_acct_coord_user_rec(user, KCIResultGetColumnValue(result,i,0)))
				continue;

			coord = xmalloc(sizeof(slurmdb_coord_rec_t));
			debug2("adding %s to coord_accts for user %s",
			       KCIResultGetColumnValue(result,i,0), user->name);
			list_append(user->coord_accts, coord);
			coord->name = xstrdup(KCIResultGetColumnValue(result,i,0));
		}
		KCIResultDealloc(result);
	}
#endif

	if (!list_count(user->coord_accts)) {
#ifdef __METASTACK_ASSOC_HASH
		destroy_str_key_hash(&coord_accts_hash);
#endif
		return SLURM_SUCCESS;
	}

	itr = list_iterator_create(user->coord_accts);
	while ((coord = list_next(itr))) {
		/*
		 * Make sure we don't get the same account back since we want to
		 * keep track of the sub-accounts.
		 */
		xstrfmtcatat(meat_query, &meat_query_pos,
			     "%s(lineage like '%%/%s/%%' and `user`='' and acct!='%s')",
			     meat_query ? " or " : "",
			     coord->name, coord->name);

	}
	list_iterator_destroy(itr);

	if (!meat_query) {
#ifdef __METASTACK_ASSOC_HASH
		destroy_str_key_hash(&coord_accts_hash);
#endif
		return SLURM_SUCCESS;
	}

#ifdef __METASTACK_ASSOC_HASH
	slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
	itr = list_iterator_create(as_kingbase_cluster_list); 
	assoc_hash_t *sub_acct_hash = NULL;
	List tmp_coord_accts = list_shallow_copy(user->coord_accts);
	list_itr_t *itr2 = list_iterator_create(tmp_coord_accts);
	while ((cluster_name = list_next(itr))) {
		sub_acct_hash = find_str_key_hash(&all_sub_acct_hash, cluster_name);
		if (sub_acct_hash) {
			while ((coord = list_next(itr2))) {
				get_sub_accts(coord->name, sub_acct_hash, user, coord_accts_hash);
			}
			list_iterator_reset(itr2);
		} else 
			xstrfmtcatat(query, &query_pos,
					"%sselect distinct acct from `%s_%s` where deleted=0 && (%s)",
					query ? " union " : "",
					cluster_name, assoc_table, meat_query);
	}
	list_iterator_destroy(itr2);
	sub_acct_hash = NULL;
	FREE_NULL_LIST(tmp_coord_accts);
#else
	slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
	itr = list_iterator_create(as_kingbase_cluster_list);
	while ((cluster_name = list_next(itr))) {
		xstrfmtcatat(query, &query_pos,
			     "%sselect distinct acct from `%s_%s` where deleted=0 and (%s)",
			     query ? " union " : "",
			     cluster_name, assoc_table, meat_query);
	}
#endif 
	xfree(meat_query);
	list_iterator_destroy(itr);
	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

	if (!query) {
#ifdef __METASTACK_ASSOC_HASH
		destroy_str_key_hash(&coord_accts_hash);
#endif
		return SLURM_SUCCESS;
	}

	debug4("%d(%s:%d) query\n%s",
	       kingbase_conn->conn, THIS_FILE, __LINE__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);

	if (!result) {
#ifdef __METASTACK_ASSOC_HASH
		destroy_str_key_hash(&coord_accts_hash);
#endif
		return SLURM_ERROR;
	}
	
    cnt = KCIResultGetRowCount(result);
	for (int j = 0; j < cnt; j++) {
#ifdef __METASTACK_ASSOC_HASH
		if (find_str_key_hash(&coord_accts_hash, KCIResultGetColumnValue(result,j,0)))
#else
		if (assoc_mgr_is_user_acct_coord_user_rec(user, KCIResultGetColumnValue(result,j,0)))
#endif
			continue;

		coord = xmalloc(sizeof(slurmdb_coord_rec_t));
		list_append(user->coord_accts, coord);
		coord->name = xstrdup(KCIResultGetColumnValue(result,j,0));
		coord->direct = 0;
#ifdef __METASTACK_ASSOC_HASH
		insert_str_key_hash(&coord_accts_hash, coord, coord->name);
#endif
	}
	KCIResultDealloc(result);
#ifdef __METASTACK_ASSOC_HASH
	if (coord_accts_hash) {
		destroy_str_key_hash(&coord_accts_hash);
	}
#endif
	return SLURM_SUCCESS;
}

static int _foreach_add_coord(void *x, void *arg)
{
	slurmdb_coord_rec_t *coord = x;
	add_user_cond_t *add_user_cond = arg;

	if (!add_user_cond->coord_query)
		xstrfmtcatat(add_user_cond->coord_query,
			     &add_user_cond->coord_query_pos,
			     "insert into %s (creation_time, mod_time, acct, `user`) values ",
			     acct_coord_table);
	else
		xstrcatat(add_user_cond->coord_query,
			  &add_user_cond->coord_query_pos,
			  ", ");

	xstrfmtcatat(add_user_cond->coord_query,
		     &add_user_cond->coord_query_pos,
		     "(%ld, %ld, '%s', '%s')",
		     add_user_cond->now, add_user_cond->now, coord->name,
		     add_user_cond->user_in->name);

	if (!add_user_cond->txn_query)
		xstrfmtcatat(add_user_cond->txn_query,
			     &add_user_cond->txn_query_pos,
			     "insert into %s (timestamp, action, name, actor, info) values ",
			     txn_table);
	else
		xstrcatat(add_user_cond->txn_query,
			  &add_user_cond->txn_query_pos,
			  ", ");

	xstrfmtcatat(add_user_cond->txn_query,
		     &add_user_cond->txn_query_pos,
		     "(%ld, %u, '%s', '%s', '%s')",
		     add_user_cond->now, DBD_ADD_ACCOUNT_COORDS,
		     add_user_cond->user_in->name,
		     add_user_cond->user_name, coord->name);

	return 0;
}

static int _foreach_add_acct(void *x, void *arg)
{
	char *acct = x;
	list_t *coord_accts = arg;
	slurmdb_coord_rec_t *coord = xmalloc(sizeof(*coord));

	coord->name = xstrdup(acct);
	coord->direct = 1;
	list_append(coord_accts, coord);

	return 0;
}

static int _add_coords(add_user_cond_t *add_user_cond)
{
	xassert(add_user_cond);
	xassert(add_user_cond->kingbase_conn);
	xassert(add_user_cond->user_in);

	if (add_user_cond->acct_list && list_count(add_user_cond->acct_list)) {
		if (add_user_cond->user_in->coord_accts)
			list_flush(add_user_cond->user_in->coord_accts);
		else
			add_user_cond->user_in->coord_accts =
				list_create(slurmdb_destroy_coord_rec);
		(void) list_for_each(add_user_cond->acct_list,
				     _foreach_add_acct,
				     add_user_cond->user_in->coord_accts);
	}

	if (add_user_cond->user_in->coord_accts &&
	    list_count(add_user_cond->user_in->coord_accts))
		(void) list_for_each(add_user_cond->user_in->coord_accts,
				     _foreach_add_coord,
				     add_user_cond);

	if (add_user_cond->coord_query) {
		int rc = SLURM_SUCCESS;
		xstrfmtcat(add_user_cond->coord_query,
			   " on duplicate key update mod_time=%ld, deleted=0, `user`=VALUES(`user`);",
			   add_user_cond->now);
		DB_DEBUG(DB_ASSOC, add_user_cond->kingbase_conn->conn, "query\n%s",
			 add_user_cond->coord_query);
		rc = kingbase_db_query(add_user_cond->kingbase_conn,
				    add_user_cond->coord_query);
		xfree(add_user_cond->coord_query);
		add_user_cond->coord_query_pos = NULL;

		if (rc != SLURM_SUCCESS) {
			error("Couldn't add coords");
			return ESLURM_BAD_SQL;
		}
	}

#ifdef __METASTACK_ASSOC_HASH
	_get_user_coords(add_user_cond->kingbase_conn, add_user_cond->user_in, NULL, NULL, true);
#else
	_get_user_coords(add_user_cond->kingbase_conn, add_user_cond->user_in);
#endif

	return SLURM_SUCCESS;
}

static int _foreach_add_user(void *x, void *arg)
{
	char *name = x;
	add_user_cond_t *add_user_cond = arg;
	slurmdb_user_rec_t *object, check_object;
	char *extra = NULL, *tmp_extra = NULL;
	int rc;
	char *query = NULL;

	/* Check to see if it is already in the assoc_mgr */
	memset(&check_object, 0, sizeof(check_object));
	check_object.name = x;
	check_object.uid = NO_VAL;

	rc = assoc_mgr_fill_in_user(add_user_cond->kingbase_conn,
				    &check_object,
				    ACCOUNTING_ENFORCE_ASSOCS, NULL, false);
	if (rc == SLURM_SUCCESS) {
		debug2("User %s is already here, not adding again.",
		       check_object.name);
		return 0;
	}

	/* Else, add it */
	object = xmalloc(sizeof(*object));
	object->name = xstrdup(x);
	object->admin_level = add_user_cond->user_in->admin_level;
	object->coord_accts = slurmdb_list_copy_coord(
		add_user_cond->user_in->coord_accts);

	query = xstrdup_printf(
		"insert into %s (creation_time, mod_time, name, admin_level) values (%ld, %ld, '%s', %u) on duplicate key update deleted=0, mod_time=VALUES(mod_time), admin_level=VALUES(admin_level);",
		user_table, add_user_cond->now, add_user_cond->now,
		object->name, object->admin_level);

	DB_DEBUG(DB_ASSOC, add_user_cond->kingbase_conn->conn, "query:\n%s",
		 query);
	add_user_cond->rc = kingbase_db_query(add_user_cond->kingbase_conn, query);
	xfree(query);
	if (add_user_cond->rc != SLURM_SUCCESS) {
		add_user_cond->rc = ESLURM_BAD_SQL;
		add_user_cond->ret_str_err = true;
		xfree(add_user_cond->ret_str);
		add_user_cond->ret_str = xstrdup_printf(
			"Couldn't add user %s: %s",
			object->name, slurm_strerror(add_user_cond->rc));
		slurmdb_destroy_user_rec(object);
		error("%s", add_user_cond->ret_str);
		return -1;
	}

	if (object->coord_accts) {
		slurmdb_user_rec_t *user_rec = add_user_cond->user_in;
		add_user_cond->user_in = object;
		add_user_cond->rc = _add_coords(add_user_cond);
		add_user_cond->user_in = user_rec;
	} else {
		add_user_cond->rc =
#ifdef __METASTACK_ASSOC_HASH
			_get_user_coords(add_user_cond->kingbase_conn, object, NULL, NULL, true);
#else
			_get_user_coords(add_user_cond->kingbase_conn, object);
#endif
	}

	if (add_user_cond->rc != SLURM_SUCCESS) {
		slurmdb_destroy_user_rec(object);
		return -1;
	}

	extra = xstrdup_printf("admin_level=%u", object->admin_level);
	tmp_extra = slurm_add_slash_to_quotes2(extra);

	if (!add_user_cond->txn_query)
		xstrfmtcatat(add_user_cond->txn_query,
			     &add_user_cond->txn_query_pos,
			     "insert into %s (timestamp, action, name, actor, info) values ",
			     txn_table);
	else
		xstrcatat(add_user_cond->txn_query,
			  &add_user_cond->txn_query_pos,
			  ", ");

	xstrfmtcatat(add_user_cond->txn_query,
		     &add_user_cond->txn_query_pos,
		     "(%ld, %u, '%s', '%s', '%s')",
		     add_user_cond->now, DBD_ADD_USERS, name,
		     add_user_cond->user_name, tmp_extra);
	xfree(tmp_extra);
	xfree(extra);

	if (addto_update_list(add_user_cond->kingbase_conn->update_list,
			      SLURMDB_ADD_USER,
			      object) == SLURM_SUCCESS) {
		if (!add_user_cond->ret_str)
			xstrcatat(add_user_cond->ret_str,
				  &add_user_cond->ret_str_pos,
				  " Adding User(s)\n");

		xstrfmtcatat(add_user_cond->ret_str,
			     &add_user_cond->ret_str_pos,
			     "  %s\n", object->name);
		object = NULL;
	}

	slurmdb_destroy_user_rec(object);

	return 0;
}

extern int as_kingbase_add_users(kingbase_conn_t *kingbase_conn, uint32_t uid,
			      List user_list)
{
	list_itr_t *itr = NULL;
	int rc = SLURM_SUCCESS;
	slurmdb_user_rec_t *object = NULL;
	char *cols = NULL, *vals = NULL, *query = NULL, *txn_query = NULL;
	char *txn_query_pos = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	char *extra = NULL, *tmp_extra = NULL;
	int affect_rows = 0;
	List assoc_list;
	List wckey_list;
	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t* data_rt = NULL;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;

		if (slurmdbd_conf->flags & DBD_CONF_FLAG_DISABLE_COORD_DBD) {
			error("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can add accounts.");
			return ESLURM_ACCESS_DENIED;
		}

		memset(&user, 0, sizeof(slurmdb_user_rec_t));
		user.uid = uid;

		if (!is_user_any_coord(kingbase_conn, &user)) {
			error("Only admins/operators/coordinators "
			      "can add accounts");
			return ESLURM_ACCESS_DENIED;
		}
		/* If the user is a coord of any acct they can add
		 * accounts they are only able to make associations to
		 * these accounts if they are coordinators of the
		 * parent they are trying to add to
		 */
	}

	if (!user_list || !list_count(user_list)) {
		error("%s: Trying to add empty user list", __func__);
		return ESLURM_EMPTY_LIST;
	}

	assoc_list = list_create(slurmdb_destroy_assoc_rec);
	wckey_list = list_create(slurmdb_destroy_wckey_rec);

	user_name = uid_to_string((uid_t) uid);
	itr = list_iterator_create(user_list);
	while ((object = list_next(itr))) {
		if (!object->name || !object->name[0]) {
			error("We need a user name and "
			      "default acct to add.");
			rc = SLURM_ERROR;
			continue;
		}

		xstrcat(cols, "creation_time, mod_time, name");
		xstrfmtcat(vals, "%ld, %ld, '%s'",
			   (long)now, (long)now, object->name);

		if (object->admin_level != SLURMDB_ADMIN_NOTSET) {
			xstrcat(cols, ", admin_level");
			xstrfmtcat(vals, ", %u", object->admin_level);
			xstrfmtcat(extra, ", admin_level=%u",
				   object->admin_level);
		} else
			xstrfmtcat(extra, ", admin_level=%u",
				   SLURMDB_ADMIN_NONE);

		query = xstrdup_printf(
			"insert into %s (%s) values (%s) "
			"as new on duplicate key update name=new.name, deleted=0, mod_time=%ld %s;",
			user_table, cols, vals,
			(long)now, extra);
		xfree(cols);
		xfree(vals);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		fetch_flag = set_fetch_flag(false, false, true);
		data_rt = xmalloc(sizeof(fetch_result_t));
		
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		xfree(query);
		// KCIResult *res = NULL;
		// res = KCIConnectionFetchResult(kingbase_conn->db_conn);
		// int err = KCIResultGetStatusCode(res);
		if (rc != SLURM_SUCCESS ) {
			rc = SLURM_ERROR;
			error("Couldn't add user %s", object->name);
			xfree(extra);
			free_res_data(data_rt, fetch_flag); 
			// KCIResultDealloc(res);
			continue;
		}
		
		affect_rows = data_rt->rows_affect;
		free_res_data(data_rt, fetch_flag); 
		//KCIResultDealloc(res);

		if (!affect_rows) {
			debug("nothing changed");
			xfree(extra);
			continue;
		}

		if (object->coord_accts) {
			add_user_cond_t add_user_cond;
			memset(&add_user_cond, 0, sizeof(add_user_cond));
			add_user_cond.user_in = object;
			add_user_cond.kingbase_conn = kingbase_conn;
			add_user_cond.user_name = user_name;
			add_user_cond.now = now;
			add_user_cond.txn_query = txn_query;
			add_user_cond.txn_query_pos = txn_query_pos;
			rc = _add_coords(&add_user_cond);
			txn_query = add_user_cond.txn_query;
			txn_query_pos = add_user_cond.txn_query_pos;
		} else {
#ifdef __METASTACK_ASSOC_HASH
			rc = _get_user_coords(kingbase_conn, object, NULL, NULL, true);
#else
			rc = _get_user_coords(kingbase_conn, object);
#endif
		}

		if (rc != SLURM_SUCCESS)
			continue;

		if (addto_update_list(kingbase_conn->update_list, SLURMDB_ADD_USER,
				      object) == SLURM_SUCCESS)
			list_remove(itr);

		/* we always have a ', ' as the first 2 chars */
		tmp_extra = slurm_add_slash_to_quotes2(extra+2);

		if (txn_query)
			xstrfmtcatat(txn_query, &txn_query_pos,
				     ", (%ld, %u, '%s', '%s', '%s')",
				     (long)now, DBD_ADD_USERS, object->name,
				     user_name, tmp_extra);
		else
			xstrfmtcatat(txn_query, &txn_query_pos,
				   "insert into %s "
				   "(timestamp, action, name, actor, info) "
				   "values (%ld, %u, '%s', '%s', '%s')",
				   txn_table,
				   (long)now, DBD_ADD_USERS, object->name,
				   user_name, tmp_extra);
		xfree(tmp_extra);
		xfree(extra);
		if (object->assoc_list)
			list_transfer(assoc_list, object->assoc_list);

		if (object->wckey_list)
			list_transfer(wckey_list, object->wckey_list);
	}
	list_iterator_destroy(itr);
	xfree(user_name);

	if (rc != SLURM_ERROR) {
		if (txn_query) {
			xstrcat(txn_query, ";");
			//info("[query] line %d, %s: txn_query: %s", __LINE__, __func__, txn_query);
			fetch_flag = set_fetch_flag(false, false, false);
			data_rt = xmalloc(sizeof(fetch_result_t));
			rc = kingbase_for_fetch(kingbase_conn, txn_query, fetch_flag, data_rt);
			xfree(txn_query);
			if (rc != SLURM_SUCCESS) {
				error("Couldn't add txn");
				rc = SLURM_SUCCESS;
			}
			free_res_data(data_rt, fetch_flag); 
		}
	} else
		xfree(txn_query);

	if (list_count(assoc_list)) {
		if ((rc = as_kingbase_add_assocs(kingbase_conn, uid, assoc_list))
		     != SLURM_SUCCESS)
			error("Problem adding user associations");
	}
	FREE_NULL_LIST(assoc_list);

	if (rc == SLURM_SUCCESS && list_count(wckey_list)) {
		if ((rc = as_kingbase_add_wckeys(kingbase_conn, uid, wckey_list))
		    != SLURM_SUCCESS)
			error("Problem adding user wckeys");
	}
	FREE_NULL_LIST(wckey_list);
	return rc;
}

extern char *as_kingbase_add_users_cond(kingbase_conn_t *kingbase_conn, uint32_t uid,
				     slurmdb_add_assoc_cond_t *add_assoc,
				     slurmdb_user_rec_t *user)
{
	add_user_cond_t add_user_cond;
	char *ret_str = NULL;
	bool admin_set = false;
	int rc;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS) {
		errno = ESLURM_DB_CONNECTION;
		return NULL;
	}

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;

		if (slurmdbd_conf->flags & DBD_CONF_FLAG_DISABLE_COORD_DBD) {
			ret_str = xstrdup("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can add accounts.");
			error("%s", ret_str);
			errno = ESLURM_ACCESS_DENIED;
			return ret_str;
		}

		memset(&user, 0, sizeof(slurmdb_user_rec_t));
		user.uid = uid;

		if (!is_user_any_coord(kingbase_conn, &user)) {
			ret_str = xstrdup("Only admins/operators/coordinators can add accounts");
			error("%s", ret_str);
			errno = ESLURM_ACCESS_DENIED;
			return ret_str;
		}
		/*
		 * If the user is a coord of any acct they can add
		 * accounts they are only able to make associations to
		 * these accounts if they are coordinators of the
		 * parent they are trying to add to
		 */
	}

	if (user->admin_level == SLURMDB_ADMIN_NOTSET)
		user->admin_level = SLURMDB_ADMIN_NONE;
	else
		admin_set = true;

	memset(&add_user_cond, 0, sizeof(add_user_cond));
	add_user_cond.user_in = user;
	add_user_cond.kingbase_conn = kingbase_conn;
	add_user_cond.now = time(NULL);
	add_user_cond.user_name = uid_to_string((uid_t) uid);

	/* First add the accounts to the user_table. */
	if (list_for_each_ro(add_assoc->user_list, _foreach_add_user,
			     &add_user_cond) < 0) {
		xfree(add_user_cond.ret_str);
		xfree(add_user_cond.txn_query);
		xfree(add_user_cond.user_name);
		errno = add_user_cond.rc;
		return NULL;
	}

	if (add_user_cond.txn_query) {
		/* Success means we add the defaults to the string */
		xstrcatat(add_user_cond.ret_str,
			  &add_user_cond.ret_str_pos,
			  " Settings\n");
		if (user->default_acct)
			xstrfmtcatat(add_user_cond.ret_str,
				     &add_user_cond.ret_str_pos,
				     "  Default Account = %s\n",
				     user->default_acct);
		if (user->default_wckey)
			xstrfmtcatat(add_user_cond.ret_str,
				     &add_user_cond.ret_str_pos,
				     "  Default WCKey   = %s\n",
				     user->default_wckey);
		if (admin_set)
			xstrfmtcatat(add_user_cond.ret_str,
				     &add_user_cond.ret_str_pos,
				     "  Admin Level     = %s\n",
				     slurmdb_admin_level_str(
					     user->admin_level));

		xstrcatat(add_user_cond.txn_query,
			  &add_user_cond.txn_query_pos,
			  ";");
		rc = kingbase_db_query(kingbase_conn, add_user_cond.txn_query);
		xfree(add_user_cond.txn_query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't add txn");
			rc = SLURM_SUCCESS;
		}
	}

	if (add_assoc->acct_list) {
		/* Now add the associations */
		add_assoc->default_acct = user->default_acct;
		ret_str = as_kingbase_add_assocs_cond(kingbase_conn, uid, add_assoc);
		rc = errno;
		add_assoc->default_acct = NULL;

		if (rc != SLURM_SUCCESS) {
			reset_kingbase_conn(kingbase_conn);
			if (!add_user_cond.ret_str_err)
				xfree(add_user_cond.ret_str);
			else
				xfree(ret_str);
			xfree(add_user_cond.txn_query);
			xfree(add_user_cond.user_name);
			errno = rc;
			return add_user_cond.ret_str ?
				add_user_cond.ret_str : ret_str;
		}

		if (ret_str) {
			xstrcatat(add_user_cond.ret_str,
				  &add_user_cond.ret_str_pos,
				  ret_str);
			xfree(ret_str);
		}
	}

	if (add_assoc->wckey_list) {
		ret_str = as_kingbase_add_wckeys_cond(
			kingbase_conn, uid, add_assoc, user);
		rc = errno;

		if (rc != SLURM_SUCCESS) {
			reset_kingbase_conn(kingbase_conn);
			if (!add_user_cond.ret_str_err)
				xfree(add_user_cond.ret_str);
			else
				xfree(ret_str);
			xfree(add_user_cond.txn_query);
			xfree(add_user_cond.user_name);
			errno = rc;
			return add_user_cond.ret_str ?
				add_user_cond.ret_str : ret_str;
		}

		if (ret_str) {
			xstrcatat(add_user_cond.ret_str,
				  &add_user_cond.ret_str_pos,
				  ret_str);
			xfree(ret_str);
		}
	}

	xfree(add_user_cond.txn_query);
	xfree(add_user_cond.user_name);

	if (!add_user_cond.ret_str) {
		DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "didn't affect anything");
		errno = SLURM_NO_CHANGE_IN_DATA;
		return NULL;
	}

	errno = SLURM_SUCCESS;
	return add_user_cond.ret_str;
}

extern int as_kingbase_add_coord(kingbase_conn_t *kingbase_conn, uint32_t uid,
			      List acct_list, slurmdb_user_cond_t *user_cond)
{
	char *user = NULL;
	list_itr_t *itr;
	int rc = SLURM_SUCCESS;
	add_user_cond_t add_user_cond;

	if (!user_cond || !user_cond->assoc_cond
	    || !user_cond->assoc_cond->user_list
	    || !list_count(user_cond->assoc_cond->user_list)
	    || !acct_list || !list_count(acct_list)) {
		error("we need something to add");
		return SLURM_ERROR;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;
		slurmdb_coord_rec_t *coord = NULL;
		char *acct = NULL;
		list_itr_t *itr2;

		if (slurmdbd_conf->flags & DBD_CONF_FLAG_DISABLE_COORD_DBD) {
			error("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can add account coordinators.");
			return ESLURM_ACCESS_DENIED;
		}

		memset(&user, 0, sizeof(slurmdb_user_rec_t));
		user.uid = uid;

		if (!is_user_any_coord(kingbase_conn, &user)) {
			error("Only admins/operators/coordinators "
			      "can add account coordinators");
			return ESLURM_ACCESS_DENIED;
		}

		itr = list_iterator_create(acct_list);
		itr2 = list_iterator_create(user.coord_accts);
		while ((acct = list_next(itr))) {
			while ((coord = list_next(itr2))) {
				if (!xstrcasecmp(coord->name, acct))
					break;
			}
			if (!coord)
				break;
			list_iterator_reset(itr2);
		}
		list_iterator_destroy(itr2);
		list_iterator_destroy(itr);

		if (!coord)  {
			error("Coordinator %s(%d) tried to add another "
			      "coordinator to an account they aren't "
			      "coordinator over.",
			      user.name, user.uid);
			return ESLURM_ACCESS_DENIED;
		}
	}

	memset(&add_user_cond, 0, sizeof(add_user_cond));
	add_user_cond.acct_list = acct_list;
	add_user_cond.kingbase_conn = kingbase_conn;
	add_user_cond.user_name = uid_to_string((uid_t) uid);
	add_user_cond.now = time(NULL);
	itr = list_iterator_create(user_cond->assoc_cond->user_list);
	while ((user = list_next(itr))) {
		if (!user[0])
			continue;
		add_user_cond.user_in = xmalloc(sizeof(slurmdb_user_rec_t));
		add_user_cond.user_in->name = xstrdup(user);

		if ((rc = _add_coords(&add_user_cond)) != SLURM_SUCCESS) {
			slurmdb_destroy_user_rec(add_user_cond.user_in);
			xfree(add_user_cond.txn_query);
			break;
		}

		if ((rc = addto_update_list(kingbase_conn->update_list,
					    SLURMDB_ADD_COORD,
					    add_user_cond.user_in)) !=
		    SLURM_SUCCESS) {
			slurmdb_destroy_user_rec(add_user_cond.user_in);
			xfree(add_user_cond.txn_query);
			break;
		}
		add_user_cond.user_in = NULL;
	}
	list_iterator_destroy(itr);

	xfree(add_user_cond.user_name);

	if (add_user_cond.txn_query) {
		xstrcatat(add_user_cond.txn_query,
			  &add_user_cond.txn_query_pos,
			  ";");
		rc = kingbase_db_query(kingbase_conn, add_user_cond.txn_query);
		xfree(add_user_cond.txn_query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't add txn");
			rc = SLURM_SUCCESS;
		}
	}

	return rc;
}

extern List as_kingbase_modify_users(kingbase_conn_t *kingbase_conn, uint32_t uid,
				  slurmdb_user_cond_t *user_cond,
				  slurmdb_user_rec_t *user)
{
	list_itr_t *itr = NULL;
	List ret_list = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL;
	char *vals = NULL, *extra = NULL, *query = NULL, *name_char = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int set = 0;
	KCIResult *result = NULL;
	uint32_t cnt = 0;

	if (!user_cond || !user) {
		error("we need something to change");
		return NULL;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (user_cond->assoc_cond && user_cond->assoc_cond->user_list
	    && list_count(user_cond->assoc_cond->user_list)) {
		set = 0;
		xstrcat(extra, " and (");
		itr = list_iterator_create(user_cond->assoc_cond->user_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (user_cond->admin_level != SLURMDB_ADMIN_NOTSET)
		xstrfmtcat(extra, " and admin_level=%u",
			   user_cond->admin_level);

	ret_list = _get_other_user_names_to_mod(kingbase_conn, uid, user_cond);

	if (user->name)
		xstrfmtcat(vals, ", name='%s'", user->name);

	if (user->admin_level != SLURMDB_ADMIN_NOTSET)
		xstrfmtcat(vals, ", admin_level=%u", user->admin_level);

	if ((!extra && !ret_list)
	    || (!vals && !user->default_acct && !user->default_wckey)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		error("Nothing to change");
		return NULL;
	}

	if (!extra) {
		/* means we got a ret_list and don't need to look at
		   the user_table. */
		goto no_user_table;
	}

	query = xstrdup_printf(
		"select distinct name from %s where deleted=0 %s;",
		user_table, extra);
	xfree(extra);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		FREE_NULL_LIST(ret_list);
		return NULL;
	}

	if (!ret_list)
		ret_list = list_create(xfree_ptr);
    cnt = KCIResultGetRowCount(result);
	for (int i = 0; i < cnt; i++) {
		slurmdb_user_rec_t *user_rec = NULL;

		object = KCIResultGetColumnValue(result,i,0);
		slurm_addto_char_list(ret_list, object);
		if (!name_char)
			xstrfmtcat(name_char, "(name='%s'", object);
		else
			xstrfmtcat(name_char, " or name='%s'", object);

		user_rec = xmalloc(sizeof(slurmdb_user_rec_t));

		if (!user->name)
			user_rec->name = xstrdup(object);
		else {
			user_rec->name = xstrdup(user->name);
			user_rec->old_name = xstrdup(object);
			if (_change_user_name(kingbase_conn, user_rec)
			    != SLURM_SUCCESS)
				break;
		}

		user_rec->admin_level = user->admin_level;
		if (addto_update_list(kingbase_conn->update_list,
				      SLURMDB_MODIFY_USER, user_rec)
		    != SLURM_SUCCESS)
			slurmdb_destroy_user_rec(user_rec);
	}
	KCIResultDealloc(result);

no_user_table:
	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_ASSOC, kingbase_conn->conn,
		         "didn't affect anything\n%s", query);
		//info("[query] line %d, didn't affect anything\n[query] line %d,: %s", __LINE__, __LINE__, query);				 
		xfree(vals);
		xfree(query);
		return ret_list;
	} else if (user->name && (list_count(ret_list) != 1)) {
		errno = ESLURM_ONE_CHANGE;
		xfree(vals);
		xfree(query);
		FREE_NULL_LIST(ret_list);
		return NULL;
	}

	xfree(query);

	if (name_char && vals) {
		xstrcat(name_char, ")");
		user_name = uid_to_string((uid_t) uid);
		rc = modify_common(kingbase_conn, DBD_MODIFY_USERS, now,
				   user_name, user_table, name_char,
				   vals, NULL);
		xfree(user_name);
	}

	xfree(name_char);
	xfree(vals);
	if (rc == SLURM_ERROR) {
		error("Couldn't modify users");
		FREE_NULL_LIST(ret_list);
	}

	if (user->default_acct && user->default_acct[0]) {
		slurmdb_assoc_cond_t assoc_cond;
		slurmdb_assoc_rec_t assoc;
		List tmp_list = NULL;

		memset(&assoc_cond, 0, sizeof(slurmdb_assoc_cond_t));
		slurmdb_init_assoc_rec(&assoc, 0);
		assoc.is_def = 1;
		assoc_cond.acct_list = list_create(NULL);
		list_append(assoc_cond.acct_list, user->default_acct);
		assoc_cond.user_list = ret_list;
		if (user_cond->assoc_cond
		    && user_cond->assoc_cond->cluster_list)
			assoc_cond.cluster_list =
				user_cond->assoc_cond->cluster_list;
		tmp_list = as_kingbase_modify_assocs(kingbase_conn, uid,
						  &assoc_cond, &assoc);
		FREE_NULL_LIST(assoc_cond.acct_list);

		if (!tmp_list) {
			FREE_NULL_LIST(ret_list);
			goto end_it;
		}
		/* char *names = NULL; */
		/* list_itr_t *itr = list_iterator_create(tmp_list); */
		/* while ((names = list_next(itr))) { */
		/* 	info("%s", names); */
		/* } */
		/* list_iterator_destroy(itr); */
		FREE_NULL_LIST(tmp_list);
	} else if (user->default_acct) {
		List cluster_list = NULL;
		if (user_cond->assoc_cond
		    && user_cond->assoc_cond->cluster_list)
			cluster_list = user_cond->assoc_cond->cluster_list;

		rc = as_kingbase_assoc_remove_default(
			kingbase_conn, ret_list, cluster_list);
		if (rc != SLURM_SUCCESS) {
			FREE_NULL_LIST(ret_list);
			errno = rc;
			goto end_it;
		}
	}

	if (user->default_wckey) {
		slurmdb_wckey_cond_t wckey_cond;
		slurmdb_wckey_rec_t wckey;
		List tmp_list = NULL;

		memset(&wckey_cond, 0, sizeof(slurmdb_wckey_cond_t));
		slurmdb_init_wckey_rec(&wckey, 0);
		wckey.is_def = 1;
		wckey_cond.name_list = list_create(NULL);
		list_append(wckey_cond.name_list, user->default_wckey);
		wckey_cond.user_list = ret_list;
		if (user_cond->assoc_cond
		    && user_cond->assoc_cond->cluster_list)
			wckey_cond.cluster_list =
				user_cond->assoc_cond->cluster_list;
		tmp_list = as_kingbase_modify_wckeys(kingbase_conn, uid,
						  &wckey_cond, &wckey);
		FREE_NULL_LIST(wckey_cond.name_list);

		if (!tmp_list) {
			FREE_NULL_LIST(ret_list);
			goto end_it;
		}
		/* char *names = NULL; */
		/* list_itr_t *itr = list_iterator_create(tmp_list); */
		/* while ((names = list_next(itr))) { */
		/* 	info("%s", names); */
		/* } */
		/* list_iterator_destroy(itr); */
		FREE_NULL_LIST(tmp_list);
	}
end_it:
	errno = rc;
	return ret_list;
}

/*
 * If the coordinator has permissions to modify every account
 * belonging to each user, return true. Otherwise return false.
 */
static bool _is_coord_over_all_accts(kingbase_conn_t *kingbase_conn,
				     char *cluster_name, char *user_char,
				     slurmdb_user_rec_t *coord)
{
	bool has_access;
	char *query = NULL, *sep_str = "";
	KCIResult *result;
	list_itr_t *itr;
	slurmdb_coord_rec_t *coord_acct;

	if (!coord->coord_accts || !list_count(coord->coord_accts)) {
		/* This should never happen */
		error("%s: We are here with no coord accts", __func__);
		return false;
	}

	query = xstrdup_printf("select distinct acct from `%s_%s` where deleted=0 and (%s) and (",
			       cluster_name, assoc_table, user_char);

	/*
	 * Add the accounts we are coordinator of.  If anything is returned
	 * outside of this list we will know there are accounts in the request
	 * that we are not coordinator over.
	 */
	itr = list_iterator_create(coord->coord_accts);
	while ((coord_acct = (list_next(itr)))) {
		xstrfmtcat(query, "%sacct != '%s'", sep_str, coord_acct->name);
		sep_str = " and ";
	}
	list_iterator_destroy(itr);
	xstrcat(query, ");");

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		return false;
	}
	

	/*
	 * If nothing was returned we are coordinator over all these accounts
	 * and users.
	 */
	has_access = !KCIResultGetRowCount(result);

	KCIResultDealloc(result);
	return has_access;
}

extern List as_kingbase_remove_users(kingbase_conn_t *kingbase_conn, uint32_t uid,
				  slurmdb_user_cond_t *user_cond)
{
	list_itr_t *itr = NULL;
	List ret_list = NULL;
	List coord_list = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL;
	char *extra = NULL, *query = NULL,
		*name_char = NULL, *assoc_char = NULL, *user_char = NULL;
	char *name_char_pos = NULL, *assoc_char_pos = NULL,
		*user_char_pos = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int set = 0;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	slurmdb_user_cond_t user_coord_cond;
	slurmdb_user_rec_t user;
	slurmdb_assoc_cond_t assoc_cond;
	slurmdb_wckey_cond_t wckey_cond;
	bool jobs_running = 0;
	bool is_coord = false;
	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t* data_rt = NULL;

	memset(&user, 0, sizeof(slurmdb_user_rec_t));
	user.uid = uid;

	if (!user_cond) {
		error("we need something to remove");
		return NULL;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		if (slurmdbd_conf->flags & DBD_CONF_FLAG_DISABLE_COORD_DBD) {
			error("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can remove users.");
			errno = ESLURM_ACCESS_DENIED;
			return NULL;
		}

		/*
		 * Allow coordinators to delete users from accounts that
		 * they coordinate. After we have gotten every association that
		 * the users belong to, check that the coordinator has access
		 * to modify every affected account.
		 */
		is_coord = is_user_any_coord(kingbase_conn, &user);
		if (!is_coord) {
			error("Only admins/coordinators can remove users");
			errno = ESLURM_ACCESS_DENIED;
			return NULL;
		}
	}

	if (user_cond->assoc_cond && user_cond->assoc_cond->user_list
	    && list_count(user_cond->assoc_cond->user_list)) {
		set = 0;
		itr = list_iterator_create(user_cond->assoc_cond->user_list);
		while ((object = list_next(itr))) {
			if (!object[0])
				continue;
			if (set)
				xstrcat(extra, " or ");
			else
				xstrcat(extra, " and (");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		if (extra)
			xstrcat(extra, ")");
	}

	ret_list = _get_other_user_names_to_mod(kingbase_conn, uid, user_cond);

	if (user_cond->admin_level != SLURMDB_ADMIN_NOTSET) {
		xstrfmtcat(extra, " and admin_level=%u", user_cond->admin_level);
	}

	if (!extra && !ret_list) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		error("Nothing to remove");
		return NULL;
	} else if (!extra) {
		/* means we got a ret_list and don't need to look at
		   the user_table. */
		goto no_user_table;
	}

	/* Only handle this if we need to actually query the
	   user_table.  If a request comes in stating they want to
	   remove all users with default account of whatever then that
	   doesn't deal with the user_table.
	*/
	query = xstrdup_printf("select name from %s where deleted=0 %s;",
			       user_table, extra);
	xfree(extra);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return NULL;
	}

	if (!ret_list)
		ret_list = list_create(xfree_ptr);
    cnt = KCIResultGetRowCount(result);
	for (int i = 0; i < cnt; i++)
		slurm_addto_char_list(ret_list, KCIResultGetColumnValue(result,i,0));
	KCIResultDealloc(result);

no_user_table:

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_ASSOC, kingbase_conn->conn,
		         "didn't affect anything\n%s", query);
		//info("[query] line %d, didn't affect anything\n[query] line %d,: %s", __LINE__, __LINE__, query);				 
		xfree(query);
		return ret_list;
	}
	xfree(query);

	memset(&user_coord_cond, 0, sizeof(slurmdb_user_cond_t));
	memset(&assoc_cond, 0, sizeof(slurmdb_assoc_cond_t));
	/* we do not need to free the objects we put in here since
	   they are also placed in a list that will be freed
	*/
	assoc_cond.user_list = list_create(NULL);
	user_coord_cond.assoc_cond = &assoc_cond;

	itr = list_iterator_create(ret_list);
	while ((object = list_next(itr))) {
		slurmdb_user_rec_t *user_rec;

		/*
		 * Skip empty names or else will select account associations
		 * and remove all associations.
		 */
		if (!object[0]) {
			list_delete_item(itr);
			continue;
		}

		user_rec = xmalloc(sizeof(slurmdb_user_rec_t));
		list_append(assoc_cond.user_list, object);

		if (name_char) {
			xstrfmtcatat(name_char, &name_char_pos, ",'%s'",
				     object);
			xstrfmtcatat(user_char, &user_char_pos, ",'%s'",
				     object);
		} else {
			xstrfmtcatat(name_char, &name_char_pos, "name in('%s'",
				     object);
			xstrfmtcatat(user_char, &user_char_pos, "`user` in('%s'",
				     object);
		}
		xstrfmtcatat(assoc_char, &assoc_char_pos,
			     "%st2.lineage like '%%/0-%s/%%'",
			     assoc_char ? " or " : "", object);

		user_rec->name = xstrdup(object);
		if (addto_update_list(kingbase_conn->update_list,
				      SLURMDB_REMOVE_USER, user_rec)
		    != SLURM_SUCCESS)
			slurmdb_destroy_user_rec(user_rec);
	}
	list_iterator_destroy(itr);
	if (name_char) {
		xstrcatat(name_char, &name_char_pos, ")");
		xstrcatat(user_char, &user_char_pos, ")");
	}
	/* We need to remove these accounts from the coord's that have it */
	coord_list = as_kingbase_remove_coord(
		kingbase_conn, uid, NULL, &user_coord_cond);
	FREE_NULL_LIST(coord_list);

	/* We need to remove these users from the wckey table */
	memset(&wckey_cond, 0, sizeof(slurmdb_wckey_cond_t));
	wckey_cond.user_list = assoc_cond.user_list;
	coord_list = as_kingbase_remove_wckeys(kingbase_conn, uid, &wckey_cond);
	FREE_NULL_LIST(coord_list);

	FREE_NULL_LIST(assoc_cond.user_list);

	user_name = uid_to_string((uid_t) uid);
	slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
	itr = list_iterator_create(as_kingbase_cluster_list);
	while ((object = list_next(itr))) {

		if (is_coord) {
			if (!_is_coord_over_all_accts(kingbase_conn, object,
						      user_char, &user)) {
				errno = ESLURM_ACCESS_DENIED;
				rc = SLURM_ERROR;
				break;
			}
		}
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
		uint32_t rpc_version = get_cluster_version(kingbase_conn, object);
		if (rpc_version < SLURM_23_11_PROTOCOL_VERSION) {
			slurm_mutex_lock(&assoc_lock);
		}

		rc = remove_common(kingbase_conn, DBD_REMOVE_USERS, now,
					user_name, user_table, name_char,
					assoc_char, object, ret_list,
					&jobs_running, NULL);

		if (rpc_version < SLURM_23_11_PROTOCOL_VERSION) {
			slurm_mutex_unlock(&assoc_lock);
		}

		if (rc != SLURM_SUCCESS)
			break;
#else
		if ((rc = remove_common(kingbase_conn, DBD_REMOVE_USERS, now,
					user_name, user_table, name_char,
					assoc_char, object, ret_list,
					&jobs_running, NULL))
		    != SLURM_SUCCESS)
			break;
#endif
	}
	list_iterator_destroy(itr);
	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

	xfree(user_name);
	xfree(name_char);
	xfree(user_char);
	if (rc == SLURM_ERROR) {
		FREE_NULL_LIST(ret_list);
		xfree(assoc_char);
		return NULL;
	}

	query = xstrdup_printf(
		"update %s set deleted=1, mod_time=%ld where %s",
		acct_coord_table, (long)now, user_char);
	xfree(assoc_char);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	fetch_flag = set_fetch_flag(false, false, false);
	data_rt = xmalloc(sizeof(fetch_result_t));
	rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
	
	xfree(query);
	// KCIResult *res = NULL;
	// res = KCIConnectionFetchResult(kingbase_conn->db_conn);
	// int err = KCIResultGetStatusCode(res);
	if (rc != SLURM_SUCCESS) {
		rc = SLURM_ERROR;
		error("Couldn't remove user coordinators");
		FREE_NULL_LIST(ret_list);
		free_res_data(data_rt, fetch_flag);
		// KCIResultDealloc(res);
		return NULL;
	}
	free_res_data(data_rt, fetch_flag);
	// KCIResultDealloc(res);

	if (jobs_running)
		errno = ESLURM_JOBS_RUNNING_ON_ASSOC;
	else
		errno = SLURM_SUCCESS;
	return ret_list;
}

extern List as_kingbase_remove_coord(kingbase_conn_t *kingbase_conn, uint32_t uid,
				  List acct_list,
				  slurmdb_user_cond_t *user_cond)
{
	char *query = NULL, *object = NULL, *extra = NULL, *last_user = NULL;
	char *user_name = NULL;
	time_t now = time(NULL);
	int set = 0, is_admin=0, rc = SLURM_SUCCESS;
	list_itr_t *itr = NULL;
	slurmdb_user_rec_t *user_rec = NULL;
	List ret_list = NULL;
	List user_list = NULL;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	slurmdb_user_rec_t user;

	if (!user_cond && !acct_list) {
		error("we need something to remove");
		return NULL;
	} else if (user_cond && user_cond->assoc_cond)
		user_list = user_cond->assoc_cond->user_list;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	memset(&user, 0, sizeof(slurmdb_user_rec_t));
	user.uid = uid;

	if (!(is_admin = is_user_min_admin_level(
		      kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR))) {
		if (slurmdbd_conf->flags & DBD_CONF_FLAG_DISABLE_COORD_DBD) {
			error("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can remove coordinators.");
			errno = ESLURM_ACCESS_DENIED;
			return NULL;
		}
		if (!is_user_any_coord(kingbase_conn, &user)) {
			error("Only admins/coordinators can "
			      "remove coordinators");
			errno = ESLURM_ACCESS_DENIED;
			return NULL;
		}
	}

	/* Leave it this way since we are using extra below */

	if (user_list && list_count(user_list)) {
		set = 0;
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, "(");

		itr = list_iterator_create(user_list);
		while ((object = list_next(itr))) {
			if (!object[0])
				continue;
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "`user`='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (acct_list && list_count(acct_list)) {
		set = 0;
		if (extra)
			xstrcat(extra, " and (");
		else
			xstrcat(extra, "(");

		itr = list_iterator_create(acct_list);
		while ((object = list_next(itr))) {
			if (!object[0])
				continue;
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "acct='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (!extra) {
		errno = SLURM_ERROR;
		DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "No conditions given");
		return NULL;
	}

	query = xstrdup_printf(
		"select `user`, acct from %s where deleted=0 and %s order by `user`",
		acct_coord_table, extra);

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(extra);
		errno = SLURM_ERROR;
		return NULL;
	}
	
	ret_list = list_create(xfree_ptr);
	user_list = list_create(xfree_ptr);
    cnt = KCIResultGetRowCount(result);
	for (int i = 0; i < cnt; i++) {
		if (!is_admin) {
			slurmdb_coord_rec_t *coord = NULL;
			if (!user.coord_accts) { // This should never
				// happen
				error("We are here with no coord accts");
				errno = ESLURM_ACCESS_DENIED;
				FREE_NULL_LIST(ret_list);
				FREE_NULL_LIST(user_list);
				xfree(extra);
				KCIResultDealloc(result);
				return NULL;
			}
			itr = list_iterator_create(user.coord_accts);
			while ((coord = list_next(itr))) {
				if (!xstrcasecmp(coord->name, KCIResultGetColumnValue(result,i,1)))
					break;
			}
			list_iterator_destroy(itr);

			if (!coord) {
				error("User %s(%d) does not have the "
				      "ability to change this account (%s)",
				      user.name, user.uid, KCIResultGetColumnValue(result,i,1));
				errno = ESLURM_ACCESS_DENIED;
				FREE_NULL_LIST(ret_list);
				FREE_NULL_LIST(user_list);
				xfree(extra);
				KCIResultDealloc(result);
				return NULL;
			}
		}
		if (!last_user || xstrcasecmp(last_user, KCIResultGetColumnValue(result,i,0))) {
			list_append(user_list, xstrdup(KCIResultGetColumnValue(result,i,0)));
			last_user = KCIResultGetColumnValue(result,i,0);
		}
		list_append(ret_list, xstrdup_printf("U = %-9s A = %-10s",
						     KCIResultGetColumnValue(result,i,0), KCIResultGetColumnValue(result,i,1)));
	}
	KCIResultDealloc(result);

	user_name = uid_to_string((uid_t) uid);
	rc = remove_common(kingbase_conn, DBD_REMOVE_ACCOUNT_COORDS,
			   now, user_name, acct_coord_table,
			   extra, NULL, NULL, NULL, NULL, NULL);
	xfree(user_name);
	xfree(extra);
	if (rc == SLURM_ERROR) {
		FREE_NULL_LIST(ret_list);
		FREE_NULL_LIST(user_list);
		errno = SLURM_ERROR;
		return NULL;
	}

	/* get the update list set */
	itr = list_iterator_create(user_list);
	while ((last_user = list_next(itr))) {
		user_rec = xmalloc(sizeof(slurmdb_user_rec_t));
		user_rec->name = xstrdup(last_user);
#ifdef __METASTACK_ASSOC_HASH
		_get_user_coords(kingbase_conn, user_rec, NULL, NULL, true);
#else
		_get_user_coords(kingbase_conn, user_rec);
#endif
		if (addto_update_list(kingbase_conn->update_list,
				      SLURMDB_REMOVE_COORD, user_rec)
		    != SLURM_SUCCESS)
			slurmdb_destroy_user_rec(user_rec);
	}
	list_iterator_destroy(itr);
	FREE_NULL_LIST(user_list);

	return ret_list;
}

extern List as_kingbase_get_users(kingbase_conn_t *kingbase_conn, uid_t uid,
			       slurmdb_user_cond_t *user_cond)
{
	char *query = NULL;
	char *extra = NULL;
	char *tmp = NULL;
	List user_list = NULL;
	list_itr_t *itr = NULL;
	char *object = NULL;
	int set = 0;
	int i=0, is_admin=1;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	slurmdb_user_rec_t user;

	/* if this changes you will need to edit the corresponding enum */
	char *user_req_inx[] = {
		"name",
		"admin_level",
		"deleted",
	};
	enum {
		USER_REQ_NAME,
		USER_REQ_AL,
		USER_REQ_DELETED,
		USER_REQ_COUNT
	};

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	memset(&user, 0, sizeof(slurmdb_user_rec_t));
	user.uid = uid;

	if (slurm_conf.private_data & PRIVATE_DATA_USERS) {
		if (!(is_admin = is_user_min_admin_level(
			      kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR))) {
			assoc_mgr_fill_in_user(
				kingbase_conn, &user, 1, NULL, false);
		}
		if (!is_admin && !user.name) {
			debug("User %u has no associations, and is not admin, "
			      "so not returning any users.", user.uid);
			return NULL;
		}
	}

	if (!user_cond) {
		xstrcat(extra, "where deleted=0");
		goto empty;
	}

	if (user_cond->with_deleted)
		xstrcat(extra, "where (deleted=0 or deleted=1)");
	else
		xstrcat(extra, "where deleted=0");


	user_list = _get_other_user_names_to_mod(kingbase_conn, uid, user_cond);
	if (user_list) {
		if (!user_cond->assoc_cond)
			user_cond->assoc_cond =
				xmalloc(sizeof(slurmdb_assoc_rec_t));

		if (!user_cond->assoc_cond->user_list)
			user_cond->assoc_cond->user_list = user_list;
		else {
			list_transfer(user_cond->assoc_cond->user_list,
				      user_list);
			FREE_NULL_LIST(user_list);
		}
		user_list = NULL;
	} else if ((user_cond->def_acct_list
		    && list_count(user_cond->def_acct_list))
		   || (user_cond->def_wckey_list
		       && list_count(user_cond->def_wckey_list)))
		return NULL;

	if (user_cond->assoc_cond &&
	    user_cond->assoc_cond->user_list
	    && list_count(user_cond->assoc_cond->user_list)) {
		set = 0;
		xstrcat(extra, " and (");
		itr = list_iterator_create(user_cond->assoc_cond->user_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (user_cond->admin_level != SLURMDB_ADMIN_NOTSET) {
		xstrfmtcat(extra, " and admin_level=%u",
			   user_cond->admin_level);
	}

empty:
	/* This is here to make sure we are looking at only this user
	 * if this flag is set.
	 */
	if (!is_admin && (slurm_conf.private_data & PRIVATE_DATA_USERS)) {
		xstrfmtcat(extra, " and name='%s'", user.name);
	}

	xfree(tmp);
	xstrfmtcat(tmp, "%s", user_req_inx[0]);
	for(i=1; i<USER_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", user_req_inx[i]);
	}

	query = xstrdup_printf("select %s from %s %s", tmp, user_table, extra);
	xfree(tmp);
	xfree(extra);

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		return NULL;
	}

	user_list = list_create(slurmdb_destroy_user_rec);


#ifdef __METASTACK_ASSOC_HASH
	str_key_hash_t *all_sub_acct_hash = NULL;
	assoc_hash_t *all_flag_user_coord_hash = NULL;
	bool has_implicit = true;
	if (user_cond && user_cond->with_coords && !assoc_mgr_user_hash ) {
		get_all_accts(kingbase_conn, user_cond->assoc_cond, &all_sub_acct_hash);
		get_assoc_flag_user_coord(kingbase_conn, &all_flag_user_coord_hash, &has_implicit);
	}
#endif

    cnt = KCIResultGetRowCount(result);
	for (int i = 0; i < cnt; i++) {
		slurmdb_user_rec_t *user = xmalloc(sizeof(slurmdb_user_rec_t));

		list_append(user_list, user);

		user->name =  xstrdup(KCIResultGetColumnValue(result,i,USER_REQ_NAME));
		user->admin_level = slurm_atoul(KCIResultGetColumnValue(result,i,USER_REQ_AL));

		if (slurm_atoul(KCIResultGetColumnValue(result,i,USER_REQ_DELETED)))
			user->flags |= SLURMDB_USER_FLAG_DELETED;

		if (user_cond && user_cond->with_coords) {
			/*
			 * On start up the coord list doesn't exist so get it
			 * the SQL way.
			 */
#ifdef __METASTACK_ASSOC_HASH
			slurmdb_user_rec_t *value = NULL;
			if (assoc_mgr_user_hash && user->name) {
				value = find_str_key_hash(&assoc_mgr_user_hash, user->name);
				if (value && value->coord_accts && list_count(value->coord_accts)) {
					user->coord_accts = slurmdb_list_copy_coord(value->coord_accts);
				}
			}
			if (!value) {
				if (!assoc_mgr_coord_list)
					_get_user_coords(kingbase_conn, user, all_sub_acct_hash, all_flag_user_coord_hash, has_implicit);
				else
					user->coord_accts =
						assoc_mgr_user_acct_coords(
							kingbase_conn, user->name);
			}
#else
			if (!assoc_mgr_coord_list)
				_get_user_coords(kingbase_conn, user);
			else
				user->coord_accts =
					assoc_mgr_user_acct_coords(
						kingbase_conn, user->name);
#endif
		}
	}
	KCIResultDealloc(result);


#ifdef __METASTACK_ASSOC_HASH
	if (all_flag_user_coord_hash) {
		destroy_assoc_hash(&all_flag_user_coord_hash);
	}
	if (all_sub_acct_hash) {
		destroy_hash_value_hash(&all_sub_acct_hash);
	}
#endif

	if (user_cond && (user_cond->with_assocs
			  || (user_cond->assoc_cond
			      && user_cond->assoc_cond->only_defs))) {
		list_itr_t *assoc_itr = NULL;
		slurmdb_user_rec_t *user = NULL;
		slurmdb_assoc_rec_t *assoc = NULL;
		List assoc_list = NULL;

		/* Make sure we don't get any non-user associations
		 * this is done by at least having a user_list
		 * defined */
		if (!user_cond->assoc_cond)
			user_cond->assoc_cond =
				xmalloc(sizeof(slurmdb_assoc_cond_t));

		if (!user_cond->assoc_cond->user_list)
			user_cond->assoc_cond->user_list = list_create(NULL);

		user_cond->assoc_cond->with_deleted = user_cond->with_deleted;

#ifdef __METASTACK_OPT_LIST_USER
		assoc_list = as_kingbase_get_assocs(
			kingbase_conn, uid, user_cond->assoc_cond, false);
#else
		assoc_list = as_kingbase_get_assocs(
			kingbase_conn, uid, user_cond->assoc_cond);
#endif

		if (!assoc_list) {
			error("no associations");
			goto get_wckeys;
		}

#ifdef __METASTACK_ASSOC_HASH
        assoc_hash_t *assoc_hash = NULL;

        assoc_itr = list_iterator_create(assoc_list);
        while ((assoc = list_next(assoc_itr))) {
            if (!assoc->user)
                continue;
            
            insert_assoc_hash(&assoc_hash, assoc, assoc->user, slurmdb_destroy_assoc_rec);
            list_remove(assoc_itr);
        }
        list_iterator_destroy(assoc_itr);

        itr = list_iterator_create(user_list);
        while ((user = list_next(itr))) {
            assoc_hash_t *tmp_entry = NULL;
            bool find_flag = false;

            if (!user->default_acct){
                slurmdb_assoc_rec_t *tmp_assoc = NULL;
                List tmp_list = NULL;
                
                tmp_entry = find_assoc_entry(&assoc_hash, user->name);
                find_flag = true;

                if (tmp_entry)
                    tmp_list = tmp_entry->value_assoc_list;

                if (tmp_list) {
                    list_itr_t *tmp_itr = list_iterator_create(tmp_list);
                    while ((tmp_assoc = list_next(tmp_itr))) {
                        if (tmp_assoc->is_def == 1){
                            user->default_acct = xstrdup(tmp_assoc->acct);
                            break;
                        }
                    }
                    list_iterator_destroy(tmp_itr);
				}
                // } else 
                //     error("not find reference assoc for user: %s", user->name);
                
                tmp_list = NULL;
            }

            if (!user_cond->with_assocs) {
                continue;
            }

            if (!find_flag)
                tmp_entry = find_assoc_entry(&assoc_hash, user->name);

            if (tmp_entry) {
                user->assoc_list = tmp_entry->value_assoc_list;
                tmp_entry->value_assoc_list = NULL;
            }
        }
        list_iterator_destroy(itr);
        
        destroy_assoc_hash(&assoc_hash);
#else
		itr = list_iterator_create(user_list);
		assoc_itr = list_iterator_create(assoc_list);
		while ((user = list_next(itr))) {
			while ((assoc = list_next(assoc_itr))) {
				if (xstrcmp(assoc->user, user->name))
					continue;
				/* Set up the default.  This is needed
				 * for older versions primarily that
				 * don't have the notion of default
				 * account per cluster. */
				if (!user->default_acct && (assoc->is_def == 1))
					user->default_acct =
						xstrdup(assoc->acct);

				if (!user_cond->with_assocs) {
					/* We just got the default so no
					   reason to hang around if we aren't
					   getting the associations.
					*/
					if (user->default_acct)
						break;
					else
						continue;
				}

				if (!user->assoc_list)
					user->assoc_list = list_create(
						slurmdb_destroy_assoc_rec);
				list_append(user->assoc_list, assoc);
				list_remove(assoc_itr);
			}
			list_iterator_reset(assoc_itr);
		}
		list_iterator_destroy(itr);
		list_iterator_destroy(assoc_itr);
#endif
		FREE_NULL_LIST(assoc_list);
	}

get_wckeys:
	if (user_cond && (user_cond->with_wckeys
			  || (user_cond->assoc_cond
			      && user_cond->assoc_cond->only_defs))) {
		list_itr_t *wckey_itr = NULL;
		slurmdb_user_rec_t *user = NULL;
		slurmdb_wckey_rec_t *wckey = NULL;
		List wckey_list = NULL;
		slurmdb_wckey_cond_t wckey_cond;

		memset(&wckey_cond, 0, sizeof(slurmdb_wckey_cond_t));
		if (user_cond->assoc_cond) {
			wckey_cond.user_list =
				user_cond->assoc_cond->user_list;
			wckey_cond.cluster_list =
				user_cond->assoc_cond->cluster_list;
			wckey_cond.only_defs =
				user_cond->assoc_cond->only_defs;
		}
		wckey_list = as_kingbase_get_wckeys(kingbase_conn, uid, &wckey_cond);

		if (!wckey_list)
			return user_list;

		itr = list_iterator_create(user_list);
		wckey_itr = list_iterator_create(wckey_list);
		while ((user = list_next(itr))) {
			while ((wckey = list_next(wckey_itr))) {
				if (xstrcmp(wckey->user, user->name))
					continue;

				/* Set up the default.  This is needed
				 * for older versions primarily that
				 * don't have the notion of default
				 * wckey per cluster. */
				if (!user->default_wckey
				    && (wckey->is_def == 1))
					user->default_wckey =
						xstrdup(wckey->name);

				/* We just got the default so no
				   reason to hang around if we aren't
				   getting the wckeys.
				*/
				if (!user_cond->with_wckeys) {
					/* We just got the default so no
					   reason to hang around if we aren't
					   getting the wckeys.
					*/
					if (user->default_wckey)
						break;
					else
						continue;
				}

				if (!user->wckey_list)
					user->wckey_list = list_create(
						slurmdb_destroy_wckey_rec);
				list_append(user->wckey_list, wckey);
				list_remove(wckey_itr);
			}
			list_iterator_reset(wckey_itr);
			/* If a user doesn't have a default wckey (they
			   might not of had track_wckeys on), set it now.
			*/
			if (!user->default_wckey)
				user->default_wckey = xstrdup("");
		}
		list_iterator_destroy(itr);
		list_iterator_destroy(wckey_itr);

		FREE_NULL_LIST(wckey_list);
	}

	return user_list;
}

static int _find_user(void *x, void *arg)
{
	slurmdb_user_rec_t *user_rec = x;
	char *name = arg;

	return slurm_find_char_exact_in_list(user_rec->name, name);
}

static slurmdb_user_rec_t *_make_user_rec_with_coords(
	kingbase_conn_t *kingbase_conn, char *user, bool locked)
{
	slurmdb_user_rec_t *user_rec = NULL;
	/*
	 * We can't use user_rec just yet since we get that filled up
	 * with variables that we don't own. We will eventually free it
	 * later which causes issues memory wise.
	 */
	slurmdb_user_rec_t user_tmp = {
		.name = user,
		.uid = NO_VAL,
	};

	assoc_mgr_lock_t locks = {
		.user = READ_LOCK,
#ifdef __METASTACK_ASSOC_HASH
		.uid = READ_LOCK,
#endif
	};

	if (!locked)
		assoc_mgr_lock(&locks);

	xassert(verify_assoc_lock(USER_LOCK, READ_LOCK));
#ifdef __METASTACK_ASSOC_HASH
	xassert(verify_assoc_lock(UID_LOCK, READ_LOCK));
#endif

	/* Grab the current coord_accts if user exists already */
	(void) assoc_mgr_fill_in_user(kingbase_conn, &user_tmp,
				      ACCOUNTING_ENFORCE_ASSOCS,
				      NULL, true);

	/*
	 * The association manager expects the dbd to do all the lifting
	 * here, so we get a full list and then remove from it.
	 */
	user_rec = xmalloc(sizeof(slurmdb_user_rec_t));
	user_rec->name = xstrdup(user_tmp.name);
	user_rec->uid = NO_VAL;
	user_rec->coord_accts = slurmdb_list_copy_coord(
		user_tmp.coord_accts);

	/*
	 * This is needed if the user is being added for the first time right
	 * now as they will not be in the assoc mgr just yet.
	 */
	if (!user_rec->coord_accts)
		user_rec->coord_accts =
			list_create(slurmdb_destroy_coord_rec);

	if (!locked)
		assoc_mgr_unlock(&locks);
	return user_rec;
}

extern slurmdb_user_rec_t *as_kingbase_user_add_coord_update(
	kingbase_conn_t *kingbase_conn, list_t **user_list, char *user, bool locked)
{
	slurmdb_user_rec_t *user_rec;

	xassert(user_list);
	xassert(user);

	if (!*user_list) {
		/* the kingbase_conn->update_list will free the contents */
		*user_list = list_create(NULL);
	}

	/* See if we have already added it. */
	if ((user_rec = list_find_first(*user_list, _find_user, user)))
		return user_rec;

	user_rec = _make_user_rec_with_coords(kingbase_conn, user, locked);

	if (!user_rec)
		return NULL;

	list_append(*user_list, user_rec);

	/*
	 * NOTE: REMOVE|ADD do the same thing, they both expect the full list so
	 * we can use either one to do the same thing.
	 */
	if (addto_update_list(kingbase_conn->update_list,
			      SLURMDB_REMOVE_COORD, user_rec) !=
	    SLURM_SUCCESS) {
		error("Couldn't add removal of coord, this should never happen.");
		slurmdb_destroy_user_rec(user_rec);
		return NULL;
	}

	return user_rec;
}

extern void as_kingbase_user_handle_user_coord_flag(slurmdb_user_rec_t *user_rec,
						 slurmdb_assoc_flags_t flags,
						 char *acct)
{
	xassert(user_rec);
	xassert(user_rec->coord_accts);
	xassert(acct);

	if (flags & ASSOC_FLAG_USER_COORD_NO) {
		(void) list_delete_first(user_rec->coord_accts,
					 assoc_mgr_find_nondirect_coord_by_name,
					 acct);
		debug2("Removing user %s from being a coordinator of account %s",
		       user_rec->name, acct);
	} else if ((flags & ASSOC_FLAG_USER_COORD) &&
		 !list_find_first(user_rec->coord_accts,
				  assoc_mgr_find_coord_in_user,
				  acct)) {
		slurmdb_coord_rec_t *coord = xmalloc(sizeof(*coord));

		coord->name = xstrdup(acct);
		list_append(user_rec->coord_accts, coord);
		debug2("Adding user %s as a coordinator of account %s",
		       user_rec->name, acct);
	}
}