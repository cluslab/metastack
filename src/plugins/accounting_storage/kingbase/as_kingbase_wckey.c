/*****************************************************************************\
 *  as_kingbase_wckey.c - functions dealing with the wckey.
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

#include "as_kingbase_wckey.h"
#include "as_kingbase_usage.h"

/* if this changes you will need to edit the corresponding enum */
char *wckey_req_inx[] = {
	"id_wckey",
	"is_def",
	"wckey_name",
	"user",
	"deleted",
};

enum {
	WCKEY_REQ_ID,
	WCKEY_REQ_DEFAULT,
	WCKEY_REQ_NAME,
	WCKEY_REQ_USER,
	WCKEY_REQ_DELETED,
	WCKEY_REQ_COUNT
};

static int _reset_default_wckey(kingbase_conn_t *kingbase_conn,
				slurmdb_wckey_rec_t *wckey)
{
	time_t now = time(NULL);
	int rc = SLURM_SUCCESS;
	char *query = NULL;
	KCIResult *result = NULL;
	uint32_t cnt = 0;

	if ((wckey->is_def != 1)
	    || !wckey->cluster || !wckey->user || !wckey->name)
		return SLURM_ERROR;

	xstrfmtcat(query, "update `%s_%s` set is_def=0, mod_time=%ld "
		   "where (`user`='%s' and wckey_name!='%s' and is_def=1);"
		   "select id_wckey from `%s_%s` "
		   "where (`user`='%s' and wckey_name!='%s' and is_def=1);",
		   wckey->cluster, wckey_table, (long)now,
		   wckey->user, wckey->name,
		   wckey->cluster, wckey_table,
		   wckey->user, wckey->name);
	DB_DEBUG(DB_WCKEY, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 1);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		rc = SLURM_ERROR;
		goto end_it;
	}
	
    cnt = KCIResultGetRowCount(result);
	for (int i = 0; i < cnt; i++)  {
		slurmdb_wckey_rec_t *mod_wckey =
			xmalloc(sizeof(slurmdb_wckey_rec_t));
		slurmdb_init_wckey_rec(mod_wckey, 0);

		mod_wckey->id = slurm_atoul(KCIResultGetColumnValue(result,i,0));
		mod_wckey->is_def = 0;

		if (addto_update_list(kingbase_conn->update_list,
				      SLURMDB_MODIFY_WCKEY,
				      mod_wckey)
		    != SLURM_SUCCESS) {
			slurmdb_destroy_wckey_rec(mod_wckey);
			error("couldn't add to the update list");
			rc = SLURM_ERROR;
			break;
		}
	}
	KCIResultDealloc(result);
end_it:
	return rc;
}

/* This needs to happen to make since 2.1 code doesn't have enough
 * smarts to figure out it isn't adding a default wckey if just
 * adding a new wckey for a user that has never been on the cluster before.
 */
static int _make_sure_users_have_default(
	kingbase_conn_t *kingbase_conn, List user_list, List cluster_list)
{
	char *query = NULL, *cluster = NULL, *user = NULL;
	ListIterator itr = NULL, clus_itr = NULL;
	int rc = SLURM_SUCCESS;
	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t* data_rt = NULL;

	if (!user_list)
		return SLURM_SUCCESS;

	clus_itr = list_iterator_create(cluster_list);
	itr = list_iterator_create(user_list);

	while ((user = list_next(itr))) {
		while ((cluster = list_next(clus_itr))) {
			KCIResult *result = NULL;


			/* only look at non * and non deleted ones */
			query = xstrdup_printf(
				"select distinct is_def, wckey_name from "
				"(select * from `%s_%s` where `user`='%s' and wckey_name "
				//"`%s_%s` where `user`='%s' and wckey_name "
				"not like '*%%' and deleted=0 ORDER BY "
				"is_def desc, creation_time desc) LIMIT 1;",
				cluster, wckey_table, user);
			debug4("%d(%s:%d) query\n%s",
			       kingbase_conn->conn, THIS_FILE, __LINE__, query);
			//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
			result = kingbase_db_query_ret(kingbase_conn, query, 0);
			xfree(query);
			if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
				KCIResultDealloc(result);
				error("couldn't query the database");
				rc = SLURM_ERROR;
				break;
			}
			/* Check to see if the user is even added to
			   the cluster.
			*/
			if (!KCIResultGetRowCount(result)) {
				KCIResultDealloc(result);
				continue;
			}

			/* check if row is default */
			char *temp = KCIResultGetColumnValue(result,0,0);
			if (*temp != '\0') {
				/* default found, continue */
				KCIResultDealloc(result);
				continue;
			}

			/* if we made it here, there is no default */
			query = xstrdup_printf(
				"update `%s_%s` set is_def=1 where "
				"`user`='%s' and wckey_name='%s';",
				cluster, wckey_table, user, KCIResultGetColumnValue(result,0,1));
			KCIResultDealloc(result);

			DB_DEBUG(DB_WCKEY, kingbase_conn->conn, "query\n%s",
			         query);
			//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);		
			//info("[query]set_fetch_flag(false, false, false);
			data_rt = xmalloc(sizeof(fetch_result_t));
			fetch_flag = set_fetch_flag(false, false, false);
			rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
			xfree(query);
			// KCIResult *res = NULL;
			// res = KCIConnectionFetchResult(kingbase_conn->db_conn);
			// int err = KCIResultGetStatusCode(res);
			if (rc != SLURM_SUCCESS) {
				error("problem with update query");
				rc = SLURM_ERROR;
				free_res_data(data_rt, fetch_flag); 
				break;
			}
			free_res_data(data_rt, fetch_flag); 
			// KCIResultDealloc(res);
		}
		if (rc != SLURM_SUCCESS)
			break;
		list_iterator_reset(clus_itr);
	}
	list_iterator_destroy(itr);
	list_iterator_destroy(clus_itr);

	return rc;
}

/* when doing a select on this all the select should have a prefix of
 * t1. */
static int _setup_wckey_cond_limits(slurmdb_wckey_cond_t *wckey_cond,
				    char **extra)
{
	int set = 0;
	ListIterator itr = NULL;
	char *object = NULL;
	char *prefix = "t1";
	if (!wckey_cond)
		return 0;

	if (wckey_cond->with_deleted){
		xstrfmtcat(*extra, " where (%s.deleted=0 or %s.deleted=1)",
			   prefix, prefix);
	} else {
		xstrfmtcat(*extra, " where %s.deleted=0", prefix);
	}

	if (wckey_cond->only_defs) {
		set = 1;
		xstrfmtcat(*extra, " and (%s.is_def=1)", prefix);
	}

	if (wckey_cond->name_list && list_count(wckey_cond->name_list)) {
		set = 0;
		xstrcat(*extra, " and (");
		itr = list_iterator_create(wckey_cond->name_list);
		while ((object = list_next(itr))) {
			if (set){
				xstrcat(*extra, " or ");
			}
			xstrfmtcat(*extra, "%s.wckey_name='%s'",
				   prefix, object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (wckey_cond->id_list && list_count(wckey_cond->id_list)) {
		set = 0;
		xstrcat(*extra, " and (");
		itr = list_iterator_create(wckey_cond->id_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "%s.id_wckey=%s", prefix, object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (wckey_cond->user_list && list_count(wckey_cond->user_list)) {
		set = 0;
		xstrcat(*extra, " and (");
		itr = list_iterator_create(wckey_cond->user_list);
		while ((object = list_next(itr))) {
			if (set){
				xstrcat(*extra, " or ");
			}
			xstrfmtcat(*extra, "%s.user='%s'", prefix, object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	return set;
}

static int _cluster_remove_wckeys(kingbase_conn_t *kingbase_conn,
				  char *extra,
				  char *cluster_name,
				  char *user_name,
				  List ret_list)
{
	int rc = SLURM_SUCCESS;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	char *assoc_char = NULL;
	time_t now = time(NULL);
	char *query = xstrdup_printf("select t1.id_wckey, t1.wckey_name, "
				     "t1.user from `%s_%s` as t1%s;",
				     cluster_name, wckey_table, extra);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);		
	result = kingbase_db_query_ret(kingbase_conn, query, 0);		 
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return SLURM_ERROR;
	}

	if (!KCIResultGetRowCount(result)) {
		KCIResultDealloc(result);
		xfree(query);
		return SLURM_SUCCESS;
	}
    cnt = KCIResultGetRowCount(result);
	for (int i = 0; i < cnt; i++) {
		slurmdb_wckey_rec_t *wckey_rec = NULL;
		char *object = xstrdup_printf("C = %-10s W = %-20s U = %-9s",
					      cluster_name, KCIResultGetColumnValue(result,i,1), KCIResultGetColumnValue(result,i,2));
		list_append(ret_list, object);

		if (!assoc_char)
			xstrfmtcat(assoc_char, "id_wckey='%s'", KCIResultGetColumnValue(result,i,0));
		else
			xstrfmtcat(assoc_char, " or id_wckey='%s'", KCIResultGetColumnValue(result,i,0));

		wckey_rec = xmalloc(sizeof(slurmdb_wckey_rec_t));
		/* we only need id and cluster when removing
		   no real need to init */
		wckey_rec->id = slurm_atoul(KCIResultGetColumnValue(result,i,0));
		wckey_rec->cluster = xstrdup(cluster_name);
		if (addto_update_list(kingbase_conn->update_list,
				      SLURMDB_REMOVE_WCKEY, wckey_rec)
		    != SLURM_SUCCESS){
			slurmdb_destroy_wckey_rec(wckey_rec);
		}
	}
	KCIResultDealloc(result);

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_WCKEY, kingbase_conn->conn,
		         "didn't affect anything\n%s", query);
		xfree(query);
		xfree(assoc_char);
		return SLURM_SUCCESS;
	}

	xfree(query);
	rc = remove_common(kingbase_conn, DBD_REMOVE_WCKEYS, now,
			   user_name, wckey_table, assoc_char, assoc_char,
			   cluster_name, NULL, NULL, NULL);
	xfree(assoc_char);

	if (rc == SLURM_ERROR) {
		FREE_NULL_LIST(ret_list);
		return SLURM_ERROR;
	}

	return SLURM_SUCCESS;
}

static int _cluster_modify_wckeys(kingbase_conn_t *kingbase_conn,
				  slurmdb_wckey_rec_t *wckey,
				  char *cluster_name, char *extra,
				  char *vals, char *user_name,
				  List ret_list)
{
	int rc = SLURM_SUCCESS;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	char *wckey_char = NULL;
	time_t now = time(NULL);
	char *query = NULL;

	query = xstrdup_printf("select t1.id_wckey, t1.wckey_name, t1.user "
			       "from `%s_%s` as t1%s;",
			       cluster_name, wckey_table, extra);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);		
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
		   
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return SLURM_ERROR;
	}

	/* This key doesn't exist on this cluster, that is ok. */
	if ((!KCIResultGetRowCount(result))) {
		KCIResultDealloc(result);
		xfree(query);
		return SLURM_SUCCESS;
	}
    cnt = KCIResultGetRowCount(result);
	for (int i = 0; i < cnt; i++) {
		slurmdb_wckey_rec_t *wckey_rec = NULL;
		char *object = xstrdup_printf(
			"C = %-10s W = %-20s U = %-9s",
			cluster_name, KCIResultGetColumnValue(result,i,1), KCIResultGetColumnValue(result,i,2));
		list_append(ret_list, object);
		if (!wckey_char)
			xstrfmtcat(wckey_char, "id_wckey='%s'", KCIResultGetColumnValue(result,i,0));
		else
			xstrfmtcat(wckey_char, " or id_wckey='%s'", KCIResultGetColumnValue(result,i,0));

		wckey_rec = xmalloc(sizeof(slurmdb_wckey_rec_t));
		/* we only need id and cluster when removing
		   no real need to init */
		wckey_rec->id = slurm_atoul(KCIResultGetColumnValue(result,i,0));
		wckey_rec->cluster = xstrdup(cluster_name);
		wckey_rec->is_def = wckey->is_def;
		if (addto_update_list(kingbase_conn->update_list,
				      SLURMDB_MODIFY_WCKEY, wckey_rec)
		    != SLURM_SUCCESS){
			slurmdb_destroy_wckey_rec(wckey_rec);
		}

		if (wckey->is_def == 1) {
			/* Use fresh one here so we don't have to
			   worry about dealing with bad values.
			*/
			slurmdb_wckey_rec_t tmp_wckey;
			slurmdb_init_wckey_rec(&tmp_wckey, 0);
			tmp_wckey.is_def = 1;
			tmp_wckey.cluster = cluster_name;
			tmp_wckey.name = KCIResultGetColumnValue(result,i,1);
			tmp_wckey.user = KCIResultGetColumnValue(result,i,2);
			if ((rc = _reset_default_wckey(kingbase_conn, &tmp_wckey))
			    != SLURM_SUCCESS){
				break;
			}
		}
	}
	KCIResultDealloc(result);

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_WCKEY, kingbase_conn->conn,
		         "didn't affect anything\n%s", query);
		xfree(query);
		xfree(wckey_char);
		return SLURM_SUCCESS;
	}

	xfree(query);
	rc = modify_common(kingbase_conn, DBD_MODIFY_WCKEYS, now,
			   user_name, wckey_table, wckey_char,
			   vals, cluster_name);
	xfree(wckey_char);

	return rc;
}

static int _cluster_get_wckeys(kingbase_conn_t *kingbase_conn,
			       slurmdb_wckey_cond_t *wckey_cond,
			       char *fields,
			       char *extra,
			       char *cluster_name,
			       List sent_list)
{
	List wckey_list = NULL;
	KCIResult *result = NULL;
	uint32_t cnt = 0;
	char *query = NULL;
	bool with_usage = 0;

	if (wckey_cond)
		with_usage = wckey_cond->with_usage;

	xstrfmtcat(query, "select distinct %s from `%s_%s` as t1%s "
		   "order by wckey_name, `user`;",
		   fields, cluster_name, wckey_table, extra);

	DB_DEBUG(DB_WCKEY, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		if (strstr(KCIConnectionGetLastError(kingbase_conn->db_conn),"不存在") 
			|| strstr(KCIConnectionGetLastError(kingbase_conn->db_conn),"not exist"))
			return SLURM_SUCCESS;
		else
			return SLURM_ERROR;
	}
	xfree(query);

	if (!KCIResultGetRowCount(result)) {
		KCIResultDealloc(result);
		return SLURM_SUCCESS;
	}

	wckey_list = list_create(slurmdb_destroy_wckey_rec);
    cnt = KCIResultGetRowCount(result);
	for (int i = 0; i < cnt; i++) {
		slurmdb_wckey_rec_t *wckey =
			xmalloc(sizeof(slurmdb_wckey_rec_t));
		list_append(wckey_list, wckey);

		wckey->id = slurm_atoul(KCIResultGetColumnValue(result,i,WCKEY_REQ_ID));
		wckey->is_def = slurm_atoul(KCIResultGetColumnValue(result,i,WCKEY_REQ_DEFAULT));
		wckey->user = xstrdup(KCIResultGetColumnValue(result,i,WCKEY_REQ_USER));

		if (slurm_atoul(KCIResultGetColumnValue(result,i,WCKEY_REQ_DELETED)))
			wckey->flags |= SLURMDB_WCKEY_FLAG_DELETED;

		/* we want a blank wckey if the name is null */
		char *temp = KCIResultGetColumnValue(result,i,WCKEY_REQ_NAME);
		if (*temp != '\0')
			wckey->name = xstrdup(KCIResultGetColumnValue(result,i,WCKEY_REQ_NAME));
		else
			wckey->name = xstrdup("");

		wckey->cluster = xstrdup(cluster_name);
	}
	KCIResultDealloc(result);

	if (with_usage && wckey_list && list_count(wckey_list)){
		get_usage_for_list(kingbase_conn, DBD_GET_WCKEY_USAGE,
				   wckey_list, cluster_name,
				   wckey_cond->usage_start,
				   wckey_cond->usage_end);
	}
	list_transfer(sent_list, wckey_list);
	FREE_NULL_LIST(wckey_list);
	return SLURM_SUCCESS;
}

/* extern functions */

extern int as_kingbase_add_wckeys(kingbase_conn_t *kingbase_conn, uint32_t uid,
			       List wckey_list)
{
	ListIterator itr = NULL;
	int rc = SLURM_SUCCESS;
	slurmdb_wckey_rec_t *object = NULL;
	char *cols = NULL, *extra = NULL, *vals = NULL, *query = NULL,
		*tmp_extra = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int affect_rows = 0;
	int added = 0;
	List local_cluster_list = NULL;
	List added_user_list = NULL;
	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t* data_rt = NULL;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR))
		return ESLURM_ACCESS_DENIED;

	local_cluster_list = list_create(NULL);

	user_name = uid_to_string((uid_t) uid);
	itr = list_iterator_create(wckey_list);
	while ((object = list_next(itr))) {
		if (!object->cluster || !object->cluster[0]
		    || !object->user || !object->user[0]
		    || !object->name) {
			error("We need a wckey name (%s), cluster (%s), "
			      "and user (%s) to add.",
			      object->name, object->cluster, object->user);
			rc = SLURM_ERROR;
			continue;
		}

		if (!added_user_list){
			added_user_list = list_create(NULL);
		}
		if (!list_find_first(added_user_list,
				     slurm_find_char_in_list,
				     object->user)){
			list_append(added_user_list, object->user);
		}
		xstrcat(cols, "creation_time, mod_time, `user`");
		xstrfmtcat(vals, "%ld, %ld, '%s'",
			   now, now, object->user);
		xstrfmtcat(extra, ", mod_time=%ld, `user`='%s'",
			   now, object->user);

		if (object->name) {
			xstrcat(cols, ", wckey_name");
			xstrfmtcat(vals, ", '%s'", object->name);
			xstrfmtcat(extra, ", wckey_name='%s'", object->name);
		}

		/* When adding, if this isn't a default might as well
		   force it to be 0 to avoid confusion since
		   uninitialized it is NO_VAL.
		*/
		if (object->is_def == 1) {
			xstrcat(cols, ", is_def");
			xstrcat(vals, ", 1");
			xstrcat(extra, ", is_def=1");
		} else {
			object->is_def = 0;
			xstrcat(cols, ", is_def");
			xstrcat(vals, ", 0");
			xstrcat(extra, ", is_def=0");
		}

		xstrfmtcat(query,
			   "insert into `%s_%s` (%s) values (%s) "
			   //"on duplicate key update deleted=0, "
			   //"id_wckey=nextval(id_wckey)%s;",
               "on duplicate key update deleted=0 "
			   "%s;",
			   object->cluster, wckey_table, cols, vals, extra);

		char *query2 = NULL;
		xstrfmtcat(query2,
			   "insert into `%s_%s` (%s) values (%s) "
			   //"on duplicate key update deleted=0, "
			   //"id_wckey=nextval(id_wckey)%s;",
               "on duplicate key update deleted=0 "
			   "%s returning id_wckey;",
			   object->cluster, wckey_table, cols, vals, extra);
		DB_DEBUG(DB_WCKEY, kingbase_conn->conn, "query\n%s", query);
		// object->id = (uint32_t)kingbase_db_insert_ret_id(
		// 	kingbase_conn, query);
		fetch_flag = set_fetch_flag(true, false, true);
		data_rt = xmalloc(sizeof(fetch_result_t));
		kingbase_for_fetch2(kingbase_conn, query, fetch_flag, data_rt, query2);
		object->id = (uint32_t)data_rt->insert_ret_id;
		xfree(query);
		xfree(query2);
		if (!object->id) {
			error("Couldn't add wckey %s", object->name);
			added=0;
			xfree(cols);
			xfree(extra);
			xfree(vals);
			free_res_data(data_rt, fetch_flag); 
			break;
		}

		// KCIResult *res = NULL;
		// res = KCIConnectionFetchResult(kingbase_conn->db_conn);
		// int err = KCIResultGetStatusCode(res);
		// affect_rows = last_affected_rows(res);
		affect_rows = data_rt->rows_affect;
		//KCIResultDealloc(res);
		free_res_data(data_rt, fetch_flag); 
		if (!affect_rows) {
			debug2("nothing changed %d", affect_rows);
			xfree(cols);
			xfree(extra);
			xfree(vals);
			continue;
		}

		if (!list_find_first(local_cluster_list,
				     slurm_find_char_in_list,
				     object->cluster)){
			list_append(local_cluster_list, object->cluster);
		}

		/* we always have a ', ' as the first 2 chars */
		tmp_extra = slurm_add_slash_to_quotes2(extra+2);

		xstrfmtcat(query,
			   "insert into %s "
			   "(timestamp, action, name, actor, info, cluster) "
			   "values (%ld, %u, 'id_wckey=%d', '%s', '%s', '%s');",
			   txn_table,
			   now, DBD_ADD_WCKEYS, object->id, user_name,
			   tmp_extra, object->cluster);

		xfree(tmp_extra);
		xfree(cols);
		xfree(extra);
		xfree(vals);
		debug4("query\n%s",query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		xfree(query);
		// res = KCIConnectionFetchResult(kingbase_conn->db_conn);
		// err = KCIResultGetStatusCode(res);
		if (rc != SLURM_SUCCESS) {
			rc = SLURM_ERROR;
			error("Couldn't add txn");
		} else {
			if (addto_update_list(kingbase_conn->update_list,
					      SLURMDB_ADD_WCKEY,
					      object) == SLURM_SUCCESS){
				list_remove(itr);
			}
			added++;
		}
		free_res_data(data_rt, fetch_flag); 
		// KCIResultDealloc(res);

	}
	list_iterator_destroy(itr);
	xfree(user_name);

	if (!added) {
		reset_kingbase_conn(kingbase_conn);
		goto end_it;
	}

	/* now reset all the other defaults accordingly. (if needed) */
	itr = list_iterator_create(wckey_list);
	while ((object = list_next(itr))) {
		if ((object->is_def != 1) || !object->cluster
		    || !object->user || !object->name)
			continue;
		if ((rc = _reset_default_wckey(kingbase_conn, object)
		     != SLURM_SUCCESS))
			break;
	}
	list_iterator_destroy(itr);
end_it:
	if (rc == SLURM_SUCCESS)
		_make_sure_users_have_default(kingbase_conn, added_user_list,
					      local_cluster_list);
	FREE_NULL_LIST(added_user_list);
	FREE_NULL_LIST(local_cluster_list);

	return rc;
}

extern List as_kingbase_modify_wckeys(kingbase_conn_t *kingbase_conn,
				   uint32_t uid,
				   slurmdb_wckey_cond_t *wckey_cond,
				   slurmdb_wckey_rec_t *wckey)
{
	List ret_list = NULL;
	int rc = SLURM_SUCCESS;
	char *extra = NULL, *object = NULL, *vals = NULL;
	char *user_name = NULL;
	List use_cluster_list = NULL;
	ListIterator itr;
	bool locked = false;

	if (!wckey_cond || !wckey) {
		error("we need something to change");
		return NULL;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		if (wckey_cond->user_list
		    && (list_count(wckey_cond->user_list) == 1)) {
			uid_t pw_uid;
			char *name;
			name = list_peek(wckey_cond->user_list);
		        if ((uid_from_string (name, &pw_uid) >= 0)
			    && (pw_uid == uid)) {
				/* Make sure they aren't trying to
				   change something else and then set
				   this association as a default.
				*/
				slurmdb_init_wckey_rec(wckey, 1);
				wckey->is_def = 1;
				goto is_same_user;
			}
		}

		error("Only admins can modify wckeys");
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}
is_same_user:

	(void) _setup_wckey_cond_limits(wckey_cond, &extra);

	if (wckey->is_def == 1)
		xstrcat(vals, ", is_def=1");

	if (!extra || !vals) {
		error("Nothing to modify '%s' '%s'", extra, vals);
		return NULL;
	}

	user_name = uid_to_string((uid_t) uid);

	if (wckey_cond->cluster_list && list_count(wckey_cond->cluster_list))
		use_cluster_list = wckey_cond->cluster_list;
	else {
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		use_cluster_list = list_shallow_copy(as_kingbase_cluster_list);
		locked = true;
	}

	ret_list = list_create(xfree_ptr);
	itr = list_iterator_create(use_cluster_list);
	while ((object = list_next(itr))) {
		if ((rc = _cluster_modify_wckeys(
			     kingbase_conn, wckey, object,
			     extra, vals, user_name, ret_list))
		    != SLURM_SUCCESS)
			break;
	}
	list_iterator_destroy(itr);
	xfree(extra);
	xfree(user_name);

	if (locked) {
		FREE_NULL_LIST(use_cluster_list);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	}

	if (rc == SLURM_ERROR) {
		FREE_NULL_LIST(ret_list);
		ret_list = NULL;
	}

	return ret_list;
}

extern List as_kingbase_remove_wckeys(kingbase_conn_t *kingbase_conn,
				   uint32_t uid,
				   slurmdb_wckey_cond_t *wckey_cond)
{
	List ret_list = NULL;
	int rc = SLURM_SUCCESS;
	char *extra = NULL, *object = NULL;
	char *user_name = NULL;
	List use_cluster_list = NULL;
	ListIterator itr;
	bool locked = false;

	if (!wckey_cond) {
		xstrcat(extra, " where deleted=0");
		goto empty;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	(void) _setup_wckey_cond_limits(wckey_cond, &extra);

empty:
	if (!extra) {
		error("Nothing to remove");
		return NULL;
	}

	user_name = uid_to_string((uid_t) uid);

	if (wckey_cond && wckey_cond->cluster_list &&
	    list_count(wckey_cond->cluster_list)) {
		use_cluster_list = wckey_cond->cluster_list;
	} else {
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		use_cluster_list = list_shallow_copy(as_kingbase_cluster_list);
		locked = true;
	}
	ret_list = list_create(xfree_ptr);
	itr = list_iterator_create(use_cluster_list);
	while ((object = list_next(itr))) {
		if ((rc = _cluster_remove_wckeys(
			     kingbase_conn, extra, object, user_name, ret_list))
		    != SLURM_SUCCESS)
			break;
	}
	list_iterator_destroy(itr);
	xfree(extra);
	xfree(user_name);

	if (locked) {
		FREE_NULL_LIST(use_cluster_list);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	}

	if (rc == SLURM_ERROR) {
		FREE_NULL_LIST(ret_list);
		return NULL;
	}

	return ret_list;
}

extern List as_kingbase_get_wckeys(kingbase_conn_t *kingbase_conn, uid_t uid,
				slurmdb_wckey_cond_t *wckey_cond)
{
	//DEF_TIMERS;
	char *extra = NULL;
	char *tmp = NULL;
	char *cluster_name = NULL;
	List wckey_list = NULL;
	int i=0, is_admin=1;
	slurmdb_user_rec_t user;
	List use_cluster_list = NULL;
	ListIterator itr;
	bool locked = false;

	if (!wckey_cond) {
		xstrcat(extra, " where deleted=0");
		goto empty;
	}

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
			      "so not returning any wckeys.", user.uid);
			return NULL;
		}
	}

	(void) _setup_wckey_cond_limits(wckey_cond, &extra);

empty:
	xfree(tmp);
	xstrfmtcat(tmp, "t1.%s", wckey_req_inx[i]);
	for(i=1; i<WCKEY_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", t1.%s", wckey_req_inx[i]);
	}

	/* this is here to make sure we are looking at only this user
	 * if this flag is set.  We also include any accounts they may be
	 * coordinator of.
	 */
	if (!is_admin && (slurm_conf.private_data & PRIVATE_DATA_USERS))
		xstrfmtcat(extra, " and t1.user='%s'", user.name);

	wckey_list = list_create(slurmdb_destroy_wckey_rec);

	if (wckey_cond && wckey_cond->cluster_list &&
	    list_count(wckey_cond->cluster_list)) {
		use_cluster_list = wckey_cond->cluster_list;
	} else {
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		use_cluster_list = list_shallow_copy(as_kingbase_cluster_list);
		locked = true;
	}
	//START_TIMER;
	itr = list_iterator_create(use_cluster_list);
	while ((cluster_name = list_next(itr))) {
		if (_cluster_get_wckeys(kingbase_conn, wckey_cond, tmp, extra,
					cluster_name, wckey_list)
		    != SLURM_SUCCESS) {
			FREE_NULL_LIST(wckey_list);
			wckey_list = NULL;
			break;
		}
	}
	list_iterator_destroy(itr);

	if (locked) {
		FREE_NULL_LIST(use_cluster_list);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	}
	xfree(tmp);
	xfree(extra);

	//END_TIMER2("get_wckeys");
	return wckey_list;
}
