/*****************************************************************************\
 *  as_kingbase_acct.c - functions dealing with accounts.
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
#include "as_kingbase_acct.h"
#include "as_kingbase_user.h"

#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
#include "as_kingbase_usage.h"
#endif


/* Fill in all the users that are coordinator for this account.  This
 * will fill in if there are coordinators from a parent account also.
 */

static int _get_account_coords(kingbase_conn_t *kingbase_conn,
			       slurmdb_account_rec_t *acct)
{
	char *query = NULL, *cluster_name = NULL;
	slurmdb_coord_rec_t *coord = NULL;
	KCIResult *result = NULL;
	ListIterator itr;

	if (!acct) {
		error("We need a account to fill in.");
		return SLURM_ERROR;
	}

	if (!acct->coordinators)
		acct->coordinators = list_create(slurmdb_destroy_coord_rec);

	query = xstrdup_printf(
		"select `user` from %s where acct='%s' && deleted=0",
		acct_coord_table, acct->name);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return SLURM_ERROR;
	}
	xfree(query);
	int row = KCIResultGetRowCount(result); // 获取结果集行数
	int i = 0;
	for(i = 0 ; i < row ; i++){
		coord = xmalloc(sizeof(slurmdb_coord_rec_t));
		list_append(acct->coordinators, coord);
		coord->name = xstrdup(KCIResultGetColumnValue(result, i, 0));
		coord->direct = 1;
	}
	KCIResultDealloc(result);

	slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
	itr = list_iterator_create(as_kingbase_cluster_list);
	while ((cluster_name = list_next(itr))) {
		if (query)
			xstrcat(query, " union ");
		xstrfmtcat(query,
			   "select distinct t0.user from %s as t0, "
			   "`%s_%s` as t1, `%s_%s` as t2 "
			   "where t0.acct=t1.acct && "
			   "t1.lft<t2.lft && t1.rgt>t2.lft && "
			   "t1.user='' && t2.acct='%s' "
			   "&& t1.acct!='%s' && !t0.deleted",
			   acct_coord_table, cluster_name, assoc_table,
			   cluster_name, assoc_table,
			   acct->name, acct->name);
	}
	list_iterator_destroy(itr);
	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

	if (!query) {
		error("No clusters defined?  How could there be accts?");
		return SLURM_SUCCESS;
	}
	xstrcat(query, ";");
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return SLURM_ERROR;
	}
	xfree(query);

	row = KCIResultGetRowCount(result); // 获取结果集行数
	for(i = 0 ; i < row ; i++){
		coord = xmalloc(sizeof(slurmdb_coord_rec_t));
		list_append(acct->coordinators, coord);
		coord->name = xstrdup(KCIResultGetColumnValue(result, i, 0));
		coord->direct = 0;
	}
	i = 0;
	
	KCIResultDealloc(result);
	return SLURM_SUCCESS;
}


extern int as_kingbase_add_accts(kingbase_conn_t *kingbase_conn, uint32_t uid,
			      List acct_list)
{
	ListIterator itr = NULL;
	int rc = SLURM_SUCCESS;
	slurmdb_account_rec_t *object = NULL;
	char *cols = NULL, *vals = NULL, *query = NULL, *txn_query = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	char *extra = NULL, *tmp_extra = NULL;

	int affect_rows = 0;
	List assoc_list;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;

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

	assoc_list = list_create(slurmdb_destroy_assoc_rec);
	user_name = uid_to_string((uid_t) uid);
	itr = list_iterator_create(acct_list);
	while ((object = list_next(itr))) {
		if (!object->name || !object->name[0]
		    || !object->description || !object->description[0]
		    || !object->organization || !object->organization[0]) {
			error("We need an account name, description, and "
			      "organization to add. %s %s %s",
			      object->name, object->description,
			      object->organization);
			rc = SLURM_ERROR;
			continue;
		}
		xstrcat(cols, "creation_time, mod_time, name, "
			"description, organization");
		xstrfmtcat(vals, "%ld, %ld, '%s', '%s', '%s'",
			   now, now, object->name,
			   object->description, object->organization);
		xstrfmtcat(extra, ", description='%s', organization='%s'",
			   object->description, object->organization);

		query = xstrdup_printf(
			"insert into %s (%s) values (%s) "
			"on duplicate key update deleted=0, mod_time=%ld %s;",
			acct_table, cols, vals,
			now, extra);
		DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		fetch_flag_t *fetch_flag = NULL;
		fetch_result_t *data_rt = NULL;
		fetch_flag = set_fetch_flag(false, false, true);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		xfree(cols);
		xfree(vals);
		xfree(query);
		if (rc == SLURM_ERROR) {
			error("Couldn't add acct");
			xfree(extra);
			free_res_data(data_rt, fetch_flag);
			continue;
		}
		affect_rows = data_rt->rows_affect;
		free_res_data(data_rt, fetch_flag);
		//KCIResultDealloc(res);
		//DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "affected %d",
		//         affect_rows);
		//info("[query] line %d, %s: affected %d",__LINE__,  __func__, affect_rows);
		if (!affect_rows) {
			DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "nothing changed");
			//info("[query] line %d, nothing changed", __LINE__);
			xfree(extra);
			continue;
		}

		/* we always have a ', ' as the first 2 chars */
		tmp_extra = slurm_add_slash_to_quotes2(extra+2);

		if (txn_query)
			xstrfmtcat(txn_query,
				   ", (%ld, %u, '%s', '%s', '%s')",
				   now, DBD_ADD_ACCOUNTS, object->name,
				   user_name, tmp_extra);
		else
			xstrfmtcat(txn_query,
				   "insert into %s "
				   "(timestamp, action, name, actor, info) "
				   "values (%ld, %u, '%s', '%s', '%s')",
				   txn_table,
				   now, DBD_ADD_ACCOUNTS, object->name,
				   user_name, tmp_extra);
		xfree(tmp_extra);
		xfree(extra);

		if (!object->assoc_list)
			continue;

		if (!assoc_list)
			assoc_list =
				list_create(slurmdb_destroy_assoc_rec);
		list_transfer(assoc_list, object->assoc_list);
	}
	list_iterator_destroy(itr);
	xfree(user_name);

	if (rc != SLURM_ERROR) {
		if (txn_query) {
			xstrcat(txn_query, ";");
			//info("[query] line %d, %s: txn_query: %s", __LINE__, __func__, txn_query);
			fetch_flag_t *fetch_flag = NULL;
			fetch_result_t *data_rt = NULL;
			fetch_flag = set_fetch_flag(false, false, false);
			data_rt = xmalloc(sizeof(fetch_result_t));
			rc = kingbase_for_fetch(kingbase_conn, txn_query, fetch_flag, data_rt);
			free_res_data(data_rt, fetch_flag);			
			xfree(txn_query);
			if (rc == SLURM_ERROR) {
				error("Couldn't add txn");
				rc = SLURM_SUCCESS;
			}
		}
	} else
		xfree(txn_query);

	if (assoc_list && list_count(assoc_list)) {
		if ((rc = as_kingbase_add_assocs(kingbase_conn, uid, assoc_list))
		    != SLURM_SUCCESS)
			error("Problem adding accounts associations");
	}
	FREE_NULL_LIST(assoc_list);

	return rc;
}

extern List as_kingbase_modify_accts(kingbase_conn_t *kingbase_conn, uint32_t uid,
				  slurmdb_account_cond_t *acct_cond,
				  slurmdb_account_rec_t *acct)
{
	ListIterator itr = NULL;
	List ret_list = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL;
	char *vals = NULL, *extra = NULL, *query = NULL, *name_char = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int set = 0;
	KCIResult *result = NULL;

	if (!acct_cond || !acct) {
		error("we need something to change");
		return NULL;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	xstrcat(extra, "where deleted=0");
	if (acct_cond->assoc_cond
	    && acct_cond->assoc_cond->acct_list
	    && list_count(acct_cond->assoc_cond->acct_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(acct_cond->assoc_cond->acct_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (acct_cond->description_list
	    && list_count(acct_cond->description_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(acct_cond->description_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "description='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (acct_cond->organization_list
	    && list_count(acct_cond->organization_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(acct_cond->organization_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "organization='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (acct->description)
		xstrfmtcat(vals, ", description='%s'", acct->description);
	if (acct->organization)
		xstrfmtcat(vals, ", organization='%s'", acct->organization);

	if (!extra || !vals) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		error("Nothing to change");
		return NULL;
	}

	query = xstrdup_printf("select name from %s %s;", acct_table, extra);
	xfree(extra);
	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		xfree(vals);
		return NULL;
	}

	rc = 0;
	ret_list = list_create(xfree_ptr);

	int row = KCIResultGetRowCount(result); // 获取结果集行数
	int i = 0;
	for(i = 0 ; i < row ; i++){
		object = xstrdup(KCIResultGetColumnValue(result, i, 0));
		list_append(ret_list, object);
		if (!rc) {
			xstrfmtcat(name_char, "(name='%s'", object);
			rc = 1;
		} else  {
			xstrfmtcat(name_char, " || name='%s'", object);
		}
	}

	KCIResultDealloc(result);

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_ASSOC, kingbase_conn->conn,
		         "didn't affect anything\n%s", query);
		//error("[query] line %d, %s: didn't affect anything\nquery: %s", __LINE__, __func__, query);		 
		xfree(query);
		xfree(vals);
		return ret_list;
	}
	xfree(query);
	xstrcat(name_char, ")");

	user_name = uid_to_string((uid_t) uid);
	rc = modify_common(kingbase_conn, DBD_MODIFY_ACCOUNTS, now,
			   user_name, acct_table, name_char, vals, NULL);
	xfree(user_name);
	if (rc == SLURM_ERROR) {
		error("Couldn't modify accounts");
		FREE_NULL_LIST(ret_list);
		errno = SLURM_ERROR;
		ret_list = NULL;
	}

	xfree(name_char);
	xfree(vals);

	return ret_list;
}

extern List as_kingbase_remove_accts(kingbase_conn_t *kingbase_conn, uint32_t uid,
				  slurmdb_account_cond_t *acct_cond)
{
	ListIterator itr = NULL;
	List ret_list = NULL;
	List coord_list = NULL;
	List cluster_list_tmp = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL;
	char *extra = NULL, *query = NULL,
		*name_char = NULL, *assoc_char = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int set = 0;
	KCIResult *result = NULL;
	bool jobs_running = 0;
	bool default_account = 0;

	if (!acct_cond) {
		error("we need something to change");
		return NULL;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	xstrcat(extra, "where deleted=0");
	if (acct_cond->assoc_cond
	    && acct_cond->assoc_cond->acct_list
	    && list_count(acct_cond->assoc_cond->acct_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(acct_cond->assoc_cond->acct_list);
		while ((object = list_next(itr))) {
			if (!object[0])
				continue;
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (acct_cond->description_list
	    && list_count(acct_cond->description_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(acct_cond->description_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "description='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (acct_cond->organization_list
	    && list_count(acct_cond->organization_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(acct_cond->organization_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "organization='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (!extra) {
		error("Nothing to remove");
		return NULL;
	}

	query = xstrdup_printf("select name from %s %s;", acct_table, extra);
	xfree(extra);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return NULL;
	}

	rc = 0;
	ret_list = list_create(xfree_ptr);

	int row = KCIResultGetRowCount(result); // 获取结果集行数
	int i = 0;
	for(i = 0 ; i < row ; i++){
		char *object = xstrdup(KCIResultGetColumnValue(result, i, 0));
		list_append(ret_list, object);
		if (!rc) {
			xstrfmtcat(name_char, "name='%s'", object);
			xstrfmtcat(assoc_char, "t2.acct='%s'", object);
			rc = 1;
		} else  {
			xstrfmtcat(name_char, " || name='%s'", object);
			xstrfmtcat(assoc_char, " || t2.acct='%s'", object);
		}
	}

	KCIResultDealloc(result);

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_ASSOC, kingbase_conn->conn,
		         "didn't affect anything\n%s", query);
		//error("[query] line %d, %s: didn't affect anything\nquery: %s", __LINE__, __func__, query);
		xfree(query);
		return ret_list;
	}
	xfree(query);

	/* We need to remove these accounts from the coord's that have it */
	coord_list = as_kingbase_remove_coord(
		kingbase_conn, uid, ret_list, NULL);
	FREE_NULL_LIST(coord_list);

	user_name = uid_to_string((uid_t) uid);

	slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
	cluster_list_tmp = list_shallow_copy(as_kingbase_cluster_list);
	itr = list_iterator_create(cluster_list_tmp);
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
	slurm_mutex_lock(&assoc_lock);
#endif
	while ((object = list_next(itr))) {
		if ((rc = remove_common(kingbase_conn, DBD_REMOVE_ACCOUNTS, now,
					user_name, acct_table, name_char,
					assoc_char, object, ret_list,
					&jobs_running, &default_account))
		    != SLURM_SUCCESS)
			break;
	}
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
    slurm_mutex_unlock(&assoc_lock);
#endif
	list_iterator_destroy(itr);
	FREE_NULL_LIST(cluster_list_tmp);
	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

	xfree(user_name);
	xfree(name_char);
	xfree(assoc_char);
	if (rc == SLURM_ERROR) {
		FREE_NULL_LIST(ret_list);
		return NULL;
	}

	if (default_account)
		errno = ESLURM_NO_REMOVE_DEFAULT_ACCOUNT;
	else if (jobs_running)
		errno = ESLURM_JOBS_RUNNING_ON_ASSOC;
	else
		errno = SLURM_SUCCESS;
	return ret_list;
}


extern List as_kingbase_get_accts(kingbase_conn_t *kingbase_conn, uid_t uid,
			       slurmdb_account_cond_t *acct_cond)
{
	char *query = NULL;
	char *extra = NULL;
	char *tmp = NULL;
	List acct_list = NULL;
	ListIterator itr = NULL;
	char *object = NULL;
	int set = 0;
	int i=0, is_admin=1;
	KCIResult *result = NULL;
	slurmdb_user_rec_t user;

#ifdef __METASTACK_OPT_LIST_USER
	bool list_all = true;
#endif 

	/* if this changes you will need to edit the corresponding enum */
	char *acct_req_inx[] = {
		"name",
		"description",
		"organization",
		"deleted",
	};
	enum {
		SLURMDB_REQ_NAME,
		SLURMDB_REQ_DESC,
		SLURMDB_REQ_ORG,
		SLURMDB_REQ_DELETED,
		SLURMDB_REQ_COUNT
	};

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	memset(&user, 0, sizeof(slurmdb_user_rec_t));
	user.uid = uid;

	if (slurm_conf.private_data & PRIVATE_DATA_ACCOUNTS) {
		if (!(is_admin = is_user_min_admin_level(
			      kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR))) {
			if (!is_user_any_coord(kingbase_conn, &user)) {
				error("Only admins/coordinators "
				      "can look at account usage");
				errno = ESLURM_ACCESS_DENIED;
				return NULL;
			}
		}
	}

	if (!acct_cond) {
		xstrcat(extra, "where deleted=0");
		goto empty;
	}

	if (acct_cond->with_deleted)
		xstrcat(extra, "where (deleted=0 || deleted=1)");
	else
		xstrcat(extra, "where deleted=0");

	if (acct_cond->assoc_cond
	    && acct_cond->assoc_cond->acct_list
	    && list_count(acct_cond->assoc_cond->acct_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(acct_cond->assoc_cond->acct_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

#ifdef __METASTACK_OPT_LIST_USER
	if (list_count(acct_cond->assoc_cond->acct_list)) 
		list_all = false;
#endif

	if (acct_cond->description_list
	    && list_count(acct_cond->description_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(acct_cond->description_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "description='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (acct_cond->organization_list
	    && list_count(acct_cond->organization_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(acct_cond->organization_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "organization='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

empty:

	xfree(tmp);
	xstrfmtcat(tmp, "%s", acct_req_inx[i]);
	for(i=1; i<SLURMDB_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", acct_req_inx[i]);
	}

	/* This is here to make sure we are looking at only this user
	 * if this flag is set.  We also include any accounts they may be
	 * coordinator of.
	 */
	if (!is_admin && (slurm_conf.private_data & PRIVATE_DATA_ACCOUNTS)) {
		slurmdb_coord_rec_t *coord = NULL;
		set = 0;
		itr = list_iterator_create(user.coord_accts);
		while ((coord = list_next(itr))) {
			if (set) {
				xstrfmtcat(extra, " || name='%s'",
					   coord->name);
			} else {
				set = 1;
				xstrfmtcat(extra, " && (name='%s'",
					   coord->name);
			}
		}
		list_iterator_destroy(itr);
		if (set)
			xstrcat(extra,")");
	}

	query = xstrdup_printf("select %s from %s %s", tmp, acct_table, extra);
	xfree(tmp);
	xfree(extra);

	DB_DEBUG(DB_ASSOC, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return NULL;
	}
	xfree(query);

	acct_list = list_create(slurmdb_destroy_account_rec);

	if (acct_cond && acct_cond->assoc_cond && acct_cond->with_assocs) {
		/* We are going to be freeing the inners of
		   this list in the acct->name so we don't
		   free it here
		*/
		FREE_NULL_LIST(acct_cond->assoc_cond->acct_list);
		acct_cond->assoc_cond->acct_list = list_create(NULL);
		acct_cond->assoc_cond->with_deleted = acct_cond->with_deleted;
	}

	int row = KCIResultGetRowCount(result); // 获取结果集行数
	for(i = 0 ; i < row ; i++){
		slurmdb_account_rec_t *acct =
			xmalloc(sizeof(slurmdb_account_rec_t));
		list_append(acct_list, acct);

		acct->name =  xstrdup(KCIResultGetColumnValue(result, i, SLURMDB_REQ_NAME));
		acct->description = xstrdup(KCIResultGetColumnValue(result, i, SLURMDB_REQ_DESC));
		acct->organization = xstrdup(KCIResultGetColumnValue(result, i, SLURMDB_REQ_ORG));

		if (slurm_atoul(KCIResultGetColumnValue(result, i, SLURMDB_REQ_DELETED)))
			acct->flags |= SLURMDB_ACCT_FLAG_DELETED;

		if (acct_cond && acct_cond->with_coords) {
			_get_account_coords(kingbase_conn, acct);
		}

		if (acct_cond && acct_cond->with_assocs) {
			if (!acct_cond->assoc_cond) {
				acct_cond->assoc_cond = xmalloc(
					sizeof(slurmdb_assoc_cond_t));
			}

			list_append(acct_cond->assoc_cond->acct_list,
				    acct->name);
		}
	}
	i = 0;
	KCIResultDealloc(result);

	if (acct_cond && acct_cond->with_assocs && acct_cond->assoc_cond
	    && list_count(acct_cond->assoc_cond->acct_list)) {
		ListIterator assoc_itr = NULL;
		slurmdb_account_rec_t *acct = NULL;
		slurmdb_assoc_rec_t *assoc = NULL;

#ifdef __METASTACK_OPT_LIST_USER
		List assoc_list = as_kingbase_get_assocs(
			kingbase_conn, uid, acct_cond->assoc_cond, list_all);
#else
		List assoc_list = as_kingbase_get_assocs(
			kingbase_conn, uid, acct_cond->assoc_cond);
#endif

		if (!assoc_list) {
			error("no associations");
			return acct_list;
		}

#ifdef __METASTACK_ASSOC_HASH
        assoc_hash_t *assoc_hash = NULL;

        /** build hash table */
        assoc_itr = list_iterator_create(assoc_list);
        while ((assoc = list_next(assoc_itr))) {
            if (!assoc->acct)
                continue;
            
            insert_assoc_hash(&assoc_hash, assoc, assoc->acct);
            list_remove(assoc_itr);
        }
        list_iterator_destroy(assoc_itr);

        itr = list_iterator_create(acct_list);
		while ((acct = list_next(itr))) {
            assoc_hash_t *tmp_entry = NULL;
            tmp_entry = find_assoc_entry(&assoc_hash, acct->name);

            if (tmp_entry) {
                acct->assoc_list = tmp_entry->value_assoc_list;
                tmp_entry->value_assoc_list = NULL;
            }
		}
        list_iterator_destroy(itr);
        
        destroy_assoc_hash(&assoc_hash);
#else
		itr = list_iterator_create(acct_list);
		assoc_itr = list_iterator_create(assoc_list);
		while ((acct = list_next(itr))) {
			while ((assoc = list_next(assoc_itr))) {
				if (xstrcmp(assoc->acct, acct->name))
					continue;

				if (!acct->assoc_list)
					acct->assoc_list = list_create(
						slurmdb_destroy_assoc_rec);
				list_append(acct->assoc_list, assoc);
				list_remove(assoc_itr);
			}
			list_iterator_reset(assoc_itr);
		}
		list_iterator_destroy(itr);
		list_iterator_destroy(assoc_itr);

#endif
		FREE_NULL_LIST(assoc_list);
	}

	return acct_list;
}
