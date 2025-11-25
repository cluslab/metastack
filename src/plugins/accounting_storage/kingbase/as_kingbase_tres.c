/*****************************************************************************\
 *  as_kingbase_tres.c - functions dealing with accounts.
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

#include "as_kingbase_tres.h"
#include "as_kingbase_usage.h"
#include "src/common/xstring.h"

extern int as_kingbase_add_tres(kingbase_conn_t *kingbase_conn,
			     uint32_t uid, List tres_list_in)
{
	ListIterator itr = NULL;
	int rc = SLURM_SUCCESS;
	slurmdb_tres_rec_t *object = NULL;
	char *cols = NULL, *extra = NULL, *vals = NULL, *query = NULL,
		*tmp_extra = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int affect_rows = 0;
	fetch_flag_t* fetch_flag = NULL;
	fetch_result_t* data_rt = NULL;

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_OPERATOR))
		return ESLURM_ACCESS_DENIED;

	if (!tres_list_in) {
		error("as_kingbase_add_tres: Trying to add a blank list");
		return SLURM_ERROR;
	}

	user_name = uid_to_string((uid_t) uid);
	itr = list_iterator_create(tres_list_in);
	while ((object = list_next(itr))) {
		if (!object->type || !object->type[0]) {
			error("We need a tres type.");
			rc = SLURM_ERROR;
			continue;
		} else if ((!xstrcasecmp(object->type, "gres") ||
			    !xstrcasecmp(object->type, "bb") ||
			    !xstrcasecmp(object->type, "license") ||
			    !xstrcasecmp(object->type, "fs") ||
			    !xstrcasecmp(object->type, "ic"))) {
			if (!object->name) {
				error("%s type tres "
				      "need to have a name, "
				      "(i.e. Gres/GPU).  You gave none",
				      object->type);
				rc = SLURM_ERROR;
				continue;
			}
		} else /* only the above have a name */
			xfree(object->name);

		xstrcat(cols, "creation_time, type");
		xstrfmtcat(vals, "%ld, '%s'", now, object->type);
		xstrfmtcat(extra, "type='%s'", object->type);
		if (object->name) {
			xstrcat(cols, ", name");
			xstrfmtcat(vals, ", '%s'", object->name);
			xstrfmtcat(extra, ", name='%s'", object->name);
		}

		xstrfmtcat(query,
			   "insert into %s (%s) values (%s) "
			   //"on duplicate key update deleted=0, "
			   //"id=nextval(id);",
			   "on duplicate key update deleted=0; ",
			   tres_table, cols, vals);
        char *query2 = NULL;
		xstrfmtcat(query2,
			   "insert into %s (%s) values (%s) "
			   //"on duplicate key update deleted=0, "
			   //"id=nextval(id);",
			   "on duplicate key update deleted=0 returning id; ",
			   tres_table, cols, vals);

		DB_DEBUG(DB_TRES, kingbase_conn->conn, "query\n%s", query);
		fetch_flag = set_fetch_flag(true, false, true);
		data_rt = xmalloc(sizeof(fetch_result_t));
		int rc = kingbase_for_fetch2(kingbase_conn, query,fetch_flag, data_rt, query2);
		object->id = (uint32_t)data_rt->insert_ret_id;
		xfree(query);
		xfree(query2);
		if (!object->id) {
			error("Couldn't add tres %s%s%s", object->type,
			      object->name ? "/" : "",
			      object->name ? object->name : "");
			xfree(cols);
			xfree(extra);
			xfree(vals);
			free_res_data(data_rt, fetch_flag);
			break;
		}

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

		tmp_extra = slurm_add_slash_to_quotes2(extra);

		xstrfmtcat(query,
			   "insert into %s "
			   "(timestamp, action, name, actor, info, cluster) "
			   "values (%ld, %u, 'id=%d', '%s', '%s', '%s');",
			   txn_table,
			   now, DBD_ADD_TRES, object->id, user_name,
			   tmp_extra, kingbase_conn->cluster_name);

		xfree(tmp_extra);
		xfree(cols);
		xfree(extra);
		xfree(vals);
		debug4("query\n%s", query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query,fetch_flag, data_rt); 
		xfree(query);
		if (rc != SLURM_SUCCESS) {
			rc = SLURM_ERROR;
			error("Couldn't add txn");
		} else {
			if (addto_update_list(kingbase_conn->update_list,
					      SLURMDB_ADD_TRES,
					      object) == SLURM_SUCCESS)
				list_remove(itr);
		}
		free_res_data(data_rt, fetch_flag);

	}
	list_iterator_destroy(itr);
	xfree(user_name);

	if (list_count(kingbase_conn->update_list)) {
		/* We only want to update the local cache DBD or ctld */
		assoc_mgr_update(kingbase_conn->update_list, 0);
		list_flush(kingbase_conn->update_list);
	}

	return rc;
}

extern List as_kingbase_get_tres(kingbase_conn_t *kingbase_conn, uid_t uid,
				slurmdb_tres_cond_t *tres_cond)
{
	char *query = NULL;
	char *extra = NULL;
	char *tmp = NULL;
	List my_tres_list = NULL;
	ListIterator itr = NULL;
	char *object = NULL;
	int set = 0;
	int i=0;
	KCIResult *result = NULL;

	/* if this changes you will need to edit the corresponding enum */
	char *tres_req_inx[] = {
		"id",
		"type",
		"name"
	};
	enum {
		SLURMDB_REQ_ID,
		SLURMDB_REQ_TYPE,
		SLURMDB_REQ_NAME,
		SLURMDB_REQ_COUNT
	};

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!tres_cond) {
		xstrcat(extra, "where deleted=0");
		goto empty;
	}

	if (tres_cond->with_deleted)
		xstrcat(extra, "where (deleted=0 or deleted=1)");
	else
		xstrcat(extra, "where deleted=0");

	if (tres_cond->id_list
	    && list_count(tres_cond->id_list)) {
		set = 0;
		xstrcat(extra, " and (");
		itr = list_iterator_create(tres_cond->id_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "id='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (tres_cond->type_list
	    && list_count(tres_cond->type_list)) {
		set = 0;
		xstrcat(extra, " and (");
		itr = list_iterator_create(tres_cond->type_list);
		while ((object = list_next(itr))) {
			char *slash;
			if (set)
				xstrcat(extra, " or ");
			if (!(slash = strchr(object, '/')))
				xstrfmtcat(extra, "type='%s'", object);
			else {
				/* This means we have the name
				 * attached, so split the string and
				 * handle it this way, only on this type.
				 */
				char *name = slash;
				*slash = '\0';
				name++;
				xstrfmtcat(extra, "(type='%s' and name='%s')",
					   object, name);
			}
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (tres_cond->name_list
	    && list_count(tres_cond->name_list)) {
		set = 0;
		xstrcat(extra, " and (");
		itr = list_iterator_create(tres_cond->name_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " or ");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

empty:

	xfree(tmp);
	xstrfmtcat(tmp, "%s", tres_req_inx[i]);
	for(i=1; i<SLURMDB_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", tres_req_inx[i]);
	}

	query = xstrdup_printf("select %s from %s %s order by id",
			       tmp, tres_table, extra);
	xfree(tmp);
	xfree(extra);

	DB_DEBUG(DB_TRES, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return NULL;
	}
	xfree(query);

	my_tres_list = list_create(slurmdb_destroy_tres_rec);
    int rows = KCIResultGetRowCount(result);
    i = 0;
    for(i = 0 ; i < rows ; ++i){
		slurmdb_tres_rec_t *tres =
			xmalloc(sizeof(slurmdb_tres_rec_t));
		list_append(my_tres_list, tres);

		tres->id =  slurm_atoul(KCIResultGetColumnValue(result,i,SLURMDB_REQ_ID));
		char *temp = KCIResultGetColumnValue(result,i,SLURMDB_REQ_TYPE);
		if (*temp != '\0')
			tres->type = xstrdup(KCIResultGetColumnValue(result,i,SLURMDB_REQ_TYPE));
		temp = KCIResultGetColumnValue(result,i,SLURMDB_REQ_NAME);
		if (*temp != '\0')
			tres->name = xstrdup(KCIResultGetColumnValue(result,i,SLURMDB_REQ_NAME));
	}
	KCIResultDealloc(result);
	return my_tres_list;
}
