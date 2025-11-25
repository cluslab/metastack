/*****************************************************************************\
 *  as_kingbase_problems.c - functions for finding out problems in the
 *                     associations and other places in the database.
 *****************************************************************************
 *  Copyright (C) 2009 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
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

#include "as_kingbase_problems.h"
#include "src/common/uid.h"

static int _setup_assoc_cond_limits(
	slurmdb_assoc_cond_t *assoc_cond,
	char **extra, bool user_query)
{
	int set = 0;
	ListIterator itr = NULL;
	char *object = NULL;

	xstrfmtcat(*extra, "where deleted=0");

	if (!assoc_cond)
		return 0;

	if (assoc_cond->acct_list && list_count(assoc_cond->acct_list)) {
		set = 0;
		xstrcat(*extra, " and (");
		itr = list_iterator_create(assoc_cond->acct_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "acct='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (assoc_cond->user_list && list_count(assoc_cond->user_list)) {
		set = 0;
		xstrcat(*extra, " and (");
		itr = list_iterator_create(assoc_cond->user_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "`user`='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	} else if (user_query) {
		/* we want all the users, but no non-user associations */
		set = 1;
		xstrcat(*extra, " and (`user`!='')");
	}

	if (assoc_cond->partition_list
	    && list_count(assoc_cond->partition_list)) {
		set = 0;
		xstrcat(*extra, " and (");
		itr = list_iterator_create(assoc_cond->partition_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "partition='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	return set;
}


extern int as_kingbase_acct_no_assocs(kingbase_conn_t *kingbase_conn,
				   slurmdb_assoc_cond_t *assoc_cond,
				   List ret_list)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL;
	KCIResult *result = NULL;
	List use_cluster_list = NULL;
	ListIterator itr = NULL;
	char *cluster_name = NULL;
	bool locked = false;

	xassert(ret_list);

	query = xstrdup_printf("select name from %s where deleted=0",
			       acct_table);
	if (assoc_cond &&
	    assoc_cond->acct_list && list_count(assoc_cond->acct_list)) {
		int set = 0;
		ListIterator itr = NULL;
		char *object = NULL;
		xstrcat(query, " and (");
		itr = list_iterator_create(assoc_cond->acct_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(query, " or ");
			xstrfmtcat(query, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(query, ")");
	}
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		return SLURM_ERROR;
	}

	if (assoc_cond &&
	    assoc_cond->cluster_list && list_count(assoc_cond->cluster_list))
		use_cluster_list = assoc_cond->cluster_list;
	else {
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		use_cluster_list = list_shallow_copy(as_kingbase_cluster_list);
		locked = true;
	}

	itr = list_iterator_create(use_cluster_list);
	for(int i = 0; i < KCIResultGetRowCount(result); i++) {
		KCIResult *result2 = NULL;
		int cnt = 0;
		slurmdb_assoc_rec_t *assoc = NULL;

		/* See if we have at least 1 association in the system */
		while ((cluster_name = list_next(itr))) {
			if (query)
				xstrcat(query, " union ");
			xstrfmtcat(query,
				   "select distinct id_assoc from `%s_%s` "
				   "where deleted=0 and "
				   "acct='%s'",
				   cluster_name, assoc_table, KCIResultGetColumnValue(result, i, 0));
		}
		list_iterator_reset(itr);
		if (query)
			xstrcat(query, " limit 1");
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		result2 = kingbase_db_query_ret(kingbase_conn, query, 0);
		xfree(query);
		if (KCIResultGetStatusCode(result2) != EXECUTE_TUPLES_OK) {
			KCIResultDealloc(result2);
			rc = SLURM_ERROR;
			break;
		}

		cnt = KCIResultGetRowCount(result2);
		KCIResultDealloc(result2);

		if (cnt)
			continue;

		assoc =	xmalloc(sizeof(slurmdb_assoc_rec_t));
		list_append(ret_list, assoc);

		assoc->id = SLURMDB_PROBLEM_ACCT_NO_ASSOC;
		assoc->acct = xstrdup(KCIResultGetColumnValue(result, i, 0));
	}
	KCIResultDealloc(result);

	list_iterator_destroy(itr);
	if (locked) {
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
		FREE_NULL_LIST(use_cluster_list);
	}

	return rc;
}

extern int as_kingbase_acct_no_users(kingbase_conn_t *kingbase_conn,
				  slurmdb_assoc_cond_t *assoc_cond,
				  List ret_list)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL, *tmp = NULL;
	char *extra = NULL;
	int i = 0;
	KCIResult *result = NULL;
	List use_cluster_list = NULL;
	ListIterator itr = NULL;
	char *cluster_name;
	bool locked = false;

	xassert(ret_list);

	_setup_assoc_cond_limits(assoc_cond, &extra, 0);

	/* if this changes you will need to edit the corresponding enum */
	char *assoc_req_inx[] = {
		"id_assoc",
		"`user`",
		"acct",
		"partition",
		"parent_acct",
	};
	enum {
		ASSOC_REQ_ID,
		ASSOC_REQ_USER,
		ASSOC_REQ_ACCT,
		ASSOC_REQ_PART,
		ASSOC_REQ_PARENT,
		ASSOC_REQ_COUNT
	};

	xfree(tmp);
	xstrfmtcat(tmp, "%s", assoc_req_inx[i]);
	for(i=1; i<ASSOC_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", assoc_req_inx[i]);
	}

	if (assoc_cond &&
	    assoc_cond->cluster_list && list_count(assoc_cond->cluster_list))
		use_cluster_list = assoc_cond->cluster_list;
	else {
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		use_cluster_list = as_kingbase_cluster_list;
		locked = true;
	}

	itr = list_iterator_create(use_cluster_list);
	while ((cluster_name = list_next(itr))) {
		/* only get the account associations */
		if (query)
			xstrcat(query, " union ");
		xstrfmtcat(query, "select distinct %s, '%s' as cluster "
			   "from `%s_%s` %s and `user`='' and lft=(rgt-1) ",
			   tmp, cluster_name, cluster_name,
			   assoc_table, extra);
	}
	list_iterator_destroy(itr);
	if (locked)
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

	if (query)
		xstrcat(query, " order by cluster, acct;");

	xfree(tmp);
	xfree(extra);
	DB_DEBUG(DB_QUERY, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		return SLURM_ERROR;
	}

	for(int i = 0; i < KCIResultGetRowCount(result); i++) {
		slurmdb_assoc_rec_t *assoc =
			xmalloc(sizeof(slurmdb_assoc_rec_t));

		list_append(ret_list, assoc);

		assoc->id = SLURMDB_PROBLEM_ACCT_NO_USERS;
		char* temp = KCIResultGetColumnValue(result, i, ASSOC_REQ_USER);
		if (*temp != '\0')
			assoc->user = xstrdup(temp);
		assoc->acct = xstrdup(KCIResultGetColumnValue(result, i, ASSOC_REQ_ACCT));
		assoc->cluster = xstrdup(KCIResultGetColumnValue(result, i, ASSOC_REQ_COUNT));
		temp = KCIResultGetColumnValue(result, i, ASSOC_REQ_PARENT);
		if (*temp != '\0')
			assoc->parent_acct = xstrdup(temp);
		temp = KCIResultGetColumnValue(result, i, ASSOC_REQ_PART);
		if (*temp != '\0')
			assoc->partition = xstrdup(temp);
	}
	KCIResultDealloc(result);

	return rc;
}

extern int as_kingbase_user_no_assocs_or_no_uid(
	kingbase_conn_t *kingbase_conn,
	slurmdb_assoc_cond_t *assoc_cond,
	List ret_list)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL;
	KCIResult *result = NULL;
	List use_cluster_list = NULL;
	ListIterator itr = NULL;
	char *cluster_name = NULL;
	bool locked = false;

	xassert(ret_list);

	query = xstrdup_printf("select name from %s where deleted=0",
			       user_table);
	if (assoc_cond &&
	    assoc_cond->user_list && list_count(assoc_cond->user_list)) {
		int set = 0;
		char *object = NULL;
		xstrcat(query, " and (");
		itr = list_iterator_create(assoc_cond->user_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(query, " or ");
			xstrfmtcat(query, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(query, ")");
	}
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	xfree(query);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		return SLURM_ERROR;
	}

	if (assoc_cond &&
	    assoc_cond->cluster_list && list_count(assoc_cond->cluster_list))
		use_cluster_list = assoc_cond->cluster_list;
	else {
		slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
		use_cluster_list = list_shallow_copy(as_kingbase_cluster_list);
		locked = true;
	}

	itr = list_iterator_create(use_cluster_list);
	for(int i = 0; i < KCIResultGetRowCount(result); i++) {
		KCIResult *result2 = NULL;
		int cnt = 0;
		slurmdb_assoc_rec_t *assoc = NULL;
		uid_t pw_uid;

		if (uid_from_string (KCIResultGetColumnValue(result, i, 0), &pw_uid) < 0) {
			assoc =	xmalloc(sizeof(slurmdb_assoc_rec_t));
			list_append(ret_list, assoc);

			assoc->id = SLURMDB_PROBLEM_USER_NO_UID;
			assoc->user = xstrdup(KCIResultGetColumnValue(result, i, 0));

			continue;
		}

		/* See if we have at least 1 association in the system */
		while ((cluster_name = list_next(itr))) {
			if (query)
				xstrcat(query, " union ");
			xstrfmtcat(query,
				   "select distinct id_assoc from `%s_%s` "
				   "where deleted=0 and "
				   "`user`='%s'",
				   cluster_name, assoc_table, KCIResultGetColumnValue(result, i, 0));
		}
		list_iterator_reset(itr);

		if (query)
			xstrcat(query, " limit 1");
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
		result2 = kingbase_db_query_ret(kingbase_conn, query, 0);
		xfree(query);
		if (KCIResultGetStatusCode(result2) != EXECUTE_TUPLES_OK) {
			KCIResultDealloc(result2);
			rc = SLURM_ERROR;
			break;
		}


		cnt = KCIResultGetRowCount(result2);
		KCIResultDealloc(result2);

		if (cnt)
			continue;

		assoc =	xmalloc(sizeof(slurmdb_assoc_rec_t));
		list_append(ret_list, assoc);

		assoc->id = SLURMDB_PROBLEM_USER_NO_ASSOC;
		assoc->user = xstrdup(KCIResultGetColumnValue(result, i, 0));
	}
	KCIResultDealloc(result);

	list_iterator_destroy(itr);
	if (locked) {
		FREE_NULL_LIST(use_cluster_list);
		slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);
	}

	return rc;
}
