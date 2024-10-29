/*****************************************************************************\
 *  as_mysql_user.c - functions dealing with users and coordinators.
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

#include "as_mysql_assoc.h"
#include "as_mysql_user.h"
#include "as_mysql_wckey.h"

#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
#include "as_mysql_usage.h"
#endif

#ifdef __METASTACK_ASSOC_HASH

#include "src/common/assoc_mgr.h"

static assoc_hash_t *sub_acct_hash = NULL;

/** build hash table, assign assoc to entry according to key. 
 * IN:  hash table, assoc
 * OUT: assoc_hash
 */
extern void insert_assoc_hash(assoc_hash_t **assoc_hash, slurmdb_assoc_rec_t *assoc, char *str_key) {
    assoc_hash_t *entry = NULL;

    if (!str_key)
        return;

    HASH_FIND_STR(*assoc_hash, str_key, entry);

    if (entry == NULL) {
        entry = xmalloc(sizeof(assoc_hash_t));
        entry->key = xstrdup(str_key);
        entry->value_assoc_list = list_create(slurmdb_destroy_assoc_rec);
        HASH_ADD_KEYPTR(hh, *assoc_hash, entry->key, strlen(entry->key), entry);
    }

    // add assoc to entry->value_assoc_list
    list_append(entry->value_assoc_list, assoc);
}

extern assoc_hash_t *find_assoc_entry(assoc_hash_t **assoc_hash, char *key) {
    assoc_hash_t *entry = NULL;

    if(!key)
        return entry;
    
    HASH_FIND_STR(*assoc_hash, key, entry);

    return entry;
}

/** delete hash */
extern void destroy_assoc_hash(assoc_hash_t **assoc_hash) {
    assoc_hash_t *current_entry, *tmp;

    HASH_ITER(hh, *assoc_hash, current_entry, tmp) {
        HASH_DEL(*assoc_hash, current_entry);
        xfree(current_entry->key);
        if (current_entry->value_assoc_list)
            FREE_NULL_LIST(current_entry->value_assoc_list);
        
        xfree(current_entry);
    }
}
#endif

static int _change_user_name(mysql_conn_t *mysql_conn, slurmdb_user_rec_t *user)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL;
	ListIterator itr = NULL;
	char *cluster_name = NULL;

	xassert(user->old_name);
	xassert(user->name);

	slurm_rwlock_rdlock(&as_mysql_cluster_list_lock);
	itr = list_iterator_create(as_mysql_cluster_list);
	while ((cluster_name = list_next(itr))) {
		// Change assoc_tables
		xstrfmtcat(query, "update \"%s_%s\" set user='%s' "
			   "where user='%s';", cluster_name, assoc_table,
			   user->name, user->old_name);
		// Change wckey_tables
		xstrfmtcat(query, "update \"%s_%s\" set user='%s' "
			   "where user='%s';", cluster_name, wckey_table,
			   user->name, user->old_name);
	}
	list_iterator_destroy(itr);
	slurm_rwlock_unlock(&as_mysql_cluster_list_lock);
	// Change coord_tables
	xstrfmtcat(query, "update %s set user='%s' where user='%s';",
		   acct_coord_table, user->name, user->old_name);

	DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
	rc = mysql_db_query(mysql_conn, query);
	xfree(query);

	if (rc != SLURM_SUCCESS)
		reset_mysql_conn(mysql_conn);

	return rc;
}

static List _get_other_user_names_to_mod(mysql_conn_t *mysql_conn, uint32_t uid,
					 slurmdb_user_cond_t *user_cond)
{
	List tmp_list = NULL;
	List ret_list = NULL;
	ListIterator itr = NULL;

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
	tmp_list = as_mysql_get_assocs(mysql_conn, uid, &assoc_cond, false);
#else
	tmp_list = as_mysql_get_assocs(mysql_conn, uid, &assoc_cond);
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

	tmp_list = as_mysql_get_wckeys(mysql_conn, uid, &wckey_cond);
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
/** build a sub_acct_hash: key is the name of parent account, value is the list of sub accounts */
static void get_all_accts(mysql_conn_t *mysql_conn)
{
	char *query = NULL;
	ListIterator itr = NULL;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
	slurmdb_assoc_rec_t *assoc;
	
	if (as_mysql_cluster_list && list_count(as_mysql_cluster_list)) {
		slurm_rwlock_rdlock(&as_mysql_cluster_list_lock);
		itr = list_iterator_create(as_mysql_cluster_list);
		char *cluster_name = NULL;
		while ((cluster_name = list_next(itr))){
			if (query)
				xstrcat(query, " union ");
			query = xstrdup_printf("select distinct acct,user,parent_acct,deleted from \"%s_%s\" "
								"where user='' && deleted=0", 
								cluster_name, assoc_table);
		}
		list_iterator_destroy(itr);
		slurm_rwlock_unlock(&as_mysql_cluster_list_lock);

		DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
		if (!(result = mysql_db_query_ret(
					mysql_conn, query, 0))) {
			xfree(query);
			return;
		}
		xfree(query);

		if (sub_acct_hash) 
			destroy_assoc_hash(&sub_acct_hash);

		while ((row = mysql_fetch_row(result))) {
			assoc = xmalloc(sizeof(slurmdb_assoc_rec_t));

			if (row[2]) {
				assoc->acct = xstrdup(row[0]);
				assoc->parent_acct = xstrdup(row[2]);
			}
			
			insert_assoc_hash(&sub_acct_hash, assoc, assoc->parent_acct);
		}
		mysql_free_result(result);
	}
}

/** get all its subaccounts by account name */
static void _get_sub_accts(char *parent_acct, assoc_hash_t *sub_acct_hash, slurmdb_user_rec_t *user)
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
		ListIterator itr = list_iterator_create(sub_acct_list);
		while ((sub_acct = list_next(itr))) {
			if (sub_acct) {
				coord = xmalloc(sizeof(slurmdb_coord_rec_t));
				list_append(user->coord_accts, coord);
				coord->name = xstrdup(sub_acct->acct);
				coord->direct = 0;

				_get_sub_accts(sub_acct->acct, sub_acct_hash, user);
			}
		}
		list_iterator_destroy(itr);
	}
}
#endif

/* Fill in all the accounts this user is coordinator over.  This
 * will fill in all the sub accounts they are coordinator over also.
 */
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
extern int _get_user_coords(mysql_conn_t *mysql_conn, slurmdb_user_rec_t *user)
#else
static int _get_user_coords(mysql_conn_t *mysql_conn, slurmdb_user_rec_t *user)
#endif
{
	char *query = NULL;
	slurmdb_coord_rec_t *coord = NULL;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
	ListIterator itr = NULL, itr2 = NULL;
	char *cluster_name = NULL;

	if (!user) {
		error("We need a user to fill in.");
		return SLURM_ERROR;
	}

	if (!user->coord_accts)
		user->coord_accts = list_create(slurmdb_destroy_coord_rec);

	query = xstrdup_printf(
		"select acct from %s where user='%s' && deleted=0",
		acct_coord_table, user->name);

	if (!(result =
	      mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		return SLURM_ERROR;
	}
	xfree(query);

	while ((row = mysql_fetch_row(result))) {
		coord = xmalloc(sizeof(slurmdb_coord_rec_t));
		list_append(user->coord_accts, coord);
		coord->name = xstrdup(row[0]);
		coord->direct = 1;
	}
	mysql_free_result(result);

	if (!list_count(user->coord_accts))
		return SLURM_SUCCESS;

#ifdef __METASTACK_ASSOC_HASH
	if (sub_acct_hash) {
		itr = list_iterator_create(user->coord_accts);
		while ((coord = list_next(itr))) {
			_get_sub_accts(coord->name, sub_acct_hash, user);
		}
	} else {
		slurm_rwlock_rdlock(&as_mysql_cluster_list_lock);
		itr2 = list_iterator_create(as_mysql_cluster_list);
		itr = list_iterator_create(user->coord_accts);
		while ((cluster_name = list_next(itr2))) {
			int set = 0;
			if (query)
				xstrcat(query, " union ");

			while ((coord = list_next(itr))) {
				if (set)
					xstrcat(query, " || ");
				else
					xstrfmtcat(query,
						"select distinct t1.acct from "
						"\"%s_%s\" as t1, \"%s_%s\" "
						"as t2 where t1.deleted=0 && (",
						cluster_name, assoc_table,
						cluster_name, assoc_table);
				/* Make sure we don't get the same
				* account back since we want to keep
				* track of the sub-accounts.
				*/
				xstrfmtcat(query, "(t2.acct='%s' "
					"&& t1.lft between t2.lft "
					"and t2.rgt && t1.user='' "
					"&& t1.acct!='%s')",
					coord->name, coord->name);
				set = 1;
			}
			list_iterator_reset(itr);
			if (set)
				xstrcat(query, ")");

		}
		list_iterator_destroy(itr2);
		slurm_rwlock_unlock(&as_mysql_cluster_list_lock);

		if (query) {
			debug4("%d(%s:%d) query\n%s",
				mysql_conn->conn, THIS_FILE, __LINE__, query);
			if (!(result = mysql_db_query_ret(
					mysql_conn, query, 0))) {
				xfree(query);
				return SLURM_ERROR;
			}
			xfree(query);

			while ((row = mysql_fetch_row(result))) {
				list_iterator_reset(itr);
				while ((coord = list_next(itr))) {
					if (!xstrcmp(coord->name, row[0]))
						break;
				}

				if (coord)
					continue;

				coord = xmalloc(sizeof(slurmdb_coord_rec_t));
				list_append(user->coord_accts, coord);
				coord->name = xstrdup(row[0]);
				coord->direct = 0;
			}
			mysql_free_result(result);
		}
		list_iterator_destroy(itr);
	}
#endif

	return SLURM_SUCCESS;
}

extern int as_mysql_add_users(mysql_conn_t *mysql_conn, uint32_t uid,
			      List user_list)
{
	ListIterator itr = NULL;
	int rc = SLURM_SUCCESS;
	slurmdb_user_rec_t *object = NULL;
	char *cols = NULL, *vals = NULL, *query = NULL, *txn_query = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	char *extra = NULL, *tmp_extra = NULL;
	int affect_rows = 0;
	List assoc_list;
	List wckey_list;

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;

		memset(&user, 0, sizeof(slurmdb_user_rec_t));
		user.uid = uid;

		if (!is_user_any_coord(mysql_conn, &user)) {
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

		if (object->coord_accts)
			debug("Adding coordinators with users is not supported, ignored, use as_mysql_add_coord() separately instead.");

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
			"on duplicate key update name=VALUES(name), deleted=0, mod_time=%ld %s;",
			user_table, cols, vals,
			(long)now, extra);
		xfree(cols);
		xfree(vals);

		rc = mysql_db_query(mysql_conn, query);
		xfree(query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't add user %s", object->name);
			xfree(extra);
			continue;
		}

		affect_rows = last_affected_rows(mysql_conn);
		if (!affect_rows) {
			debug("nothing changed");
			xfree(extra);
			continue;
		}

		if (addto_update_list(mysql_conn->update_list, SLURMDB_ADD_USER,
				      object) == SLURM_SUCCESS)
			list_remove(itr);

		/* we always have a ', ' as the first 2 chars */
		tmp_extra = slurm_add_slash_to_quotes(extra+2);

		if (txn_query)
			xstrfmtcat(txn_query,
				   ", (%ld, %u, '%s', '%s', '%s')",
				   (long)now, DBD_ADD_USERS, object->name,
				   user_name, tmp_extra);
		else
			xstrfmtcat(txn_query,
				   "insert into %s "
				   "(timestamp, action, name, actor, info) "
				   "values (%ld, %u, '%s', '%s', '%s')",
				   txn_table,
				   (long)now, DBD_ADD_USERS, object->name,
				   user_name, tmp_extra);
		xfree(tmp_extra);
		xfree(extra);

		/* For < 2.2 systems we need to set the is_def flag in
		   the default association/wckey so as to make sure we get
		   it set correctly.
		*/
		if (object->assoc_list) {
			slurmdb_assoc_rec_t *assoc = NULL;
			ListIterator assoc_itr =
				list_iterator_create(object->assoc_list);
			while ((assoc = list_next(assoc_itr))) {
				/* We need to mark all of the
				   associations with this account
				   since there could be multiple
				   clusters here.
				*/
				if (!xstrcmp(assoc->acct, object->default_acct))
					assoc->is_def = 1;
			}
			list_iterator_destroy(assoc_itr);
			list_transfer(assoc_list, object->assoc_list);
		}

		if (object->wckey_list) {
			if (object->default_wckey) {
				slurmdb_wckey_rec_t *wckey = NULL;
				ListIterator wckey_itr = list_iterator_create(
					object->wckey_list);
				while ((wckey = list_next(wckey_itr))) {
					/* We need to mark all of the
					   wckeys with this account
					   since there could be multiple
					   clusters here.
					*/
					if (!xstrcmp(wckey->name,
						     object->default_wckey))
						wckey->is_def = 1;
				}
				list_iterator_destroy(wckey_itr);
			}
			list_transfer(wckey_list, object->wckey_list);
		}
	}
	list_iterator_destroy(itr);
	xfree(user_name);

	if (rc != SLURM_ERROR) {
		if (txn_query) {
			xstrcat(txn_query, ";");
			rc = mysql_db_query(mysql_conn,
					    txn_query);
			xfree(txn_query);
			if (rc != SLURM_SUCCESS) {
				error("Couldn't add txn");
				rc = SLURM_SUCCESS;
			}
		}
	} else
		xfree(txn_query);

	if (list_count(assoc_list)) {
		if ((rc = as_mysql_add_assocs(mysql_conn, uid, assoc_list))
		     != SLURM_SUCCESS)
			error("Problem adding user associations");
	}
	FREE_NULL_LIST(assoc_list);

	if (rc == SLURM_SUCCESS && list_count(wckey_list)) {
		if ((rc = as_mysql_add_wckeys(mysql_conn, uid, wckey_list))
		    != SLURM_SUCCESS)
			error("Problem adding user wckeys");
	}
	FREE_NULL_LIST(wckey_list);
	return rc;
}

extern int as_mysql_add_coord(mysql_conn_t *mysql_conn, uint32_t uid,
			      List acct_list, slurmdb_user_cond_t *user_cond)
{
	char *query = NULL, *user = NULL, *acct = NULL;
	char *user_name = NULL, *txn_query = NULL;
	ListIterator itr, itr2;
	time_t now = time(NULL);
	int rc = SLURM_SUCCESS;
	slurmdb_user_rec_t *user_rec = NULL;

	if (!user_cond || !user_cond->assoc_cond
	    || !user_cond->assoc_cond->user_list
	    || !list_count(user_cond->assoc_cond->user_list)
	    || !acct_list || !list_count(acct_list)) {
		error("we need something to add");
		return SLURM_ERROR;
	}

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;
		slurmdb_coord_rec_t *coord = NULL;
		char *acct = NULL;

		memset(&user, 0, sizeof(slurmdb_user_rec_t));
		user.uid = uid;

		if (!is_user_any_coord(mysql_conn, &user)) {
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

	user_name = uid_to_string((uid_t) uid);
	itr = list_iterator_create(user_cond->assoc_cond->user_list);
	itr2 = list_iterator_create(acct_list);
	while ((user = list_next(itr))) {
		if (!user[0])
			continue;
		while ((acct = list_next(itr2))) {
			if (!acct[0])
				continue;
			if (query)
				xstrfmtcat(query, ", (%ld, %ld, '%s', '%s')",
					   (long)now, (long)now, acct, user);
			else
				query = xstrdup_printf(
					"insert into %s (creation_time, "
					"mod_time, acct, user) values "
					"(%ld, %ld, '%s', '%s')",
					acct_coord_table,
					(long)now, (long)now, acct, user);

			if (txn_query)
				xstrfmtcat(txn_query,
					   ", (%ld, %u, '%s', '%s', '%s')",
					   (long)now, DBD_ADD_ACCOUNT_COORDS,
					   user, user_name, acct);
			else
				xstrfmtcat(txn_query,
					   "insert into %s "
					   "(timestamp, action, name, "
					   "actor, info) "
					   "values (%ld, %u, '%s', "
					   "'%s', '%s')",
					   txn_table,
					   (long)now, DBD_ADD_ACCOUNT_COORDS,
					   user,
					   user_name, acct);
		}
		list_iterator_reset(itr2);
	}
	xfree(user_name);
	list_iterator_destroy(itr);
	list_iterator_destroy(itr2);

	if (query) {
		xstrfmtcat(query,
			   " on duplicate key update mod_time=%ld, "
			   "deleted=0, user=VALUES(user);%s",
			   (long)now, txn_query);
		DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
		rc = mysql_db_query(mysql_conn, query);
		xfree(query);
		xfree(txn_query);

		if (rc != SLURM_SUCCESS) {
			error("Couldn't add cluster hour rollup");
			return rc;
		}

#ifdef __METASTACK_ASSOC_HASH
		/* get all accounts to build a sub_acct_hash*/
		if (sub_acct_hash) 
			destroy_assoc_hash(&sub_acct_hash);
		if (user_cond->assoc_cond->user_list && list_count(user_cond->assoc_cond->user_list))
			get_all_accts(mysql_conn);
#endif

		/* get the update list set */
		itr = list_iterator_create(user_cond->assoc_cond->user_list);
		while ((user = list_next(itr))) {
			user_rec = xmalloc(sizeof(slurmdb_user_rec_t));
			user_rec->name = xstrdup(user);
			_get_user_coords(mysql_conn, user_rec);
			if (addto_update_list(mysql_conn->update_list,
					      SLURMDB_ADD_COORD, user_rec)
			    != SLURM_SUCCESS)
				slurmdb_destroy_user_rec(user_rec);
		}
		list_iterator_destroy(itr);
#ifdef __METASTACK_ASSOC_HASH
		if (sub_acct_hash)
			destroy_assoc_hash(&sub_acct_hash);
#endif
	}

	return SLURM_SUCCESS;
}

extern List as_mysql_modify_users(mysql_conn_t *mysql_conn, uint32_t uid,
				  slurmdb_user_cond_t *user_cond,
				  slurmdb_user_rec_t *user)
{
	ListIterator itr = NULL;
	List ret_list = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL;
	char *vals = NULL, *extra = NULL, *query = NULL, *name_char = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int set = 0;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;

	if (!user_cond || !user) {
		error("we need something to change");
		return NULL;
	}

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	if (user_cond->assoc_cond && user_cond->assoc_cond->user_list
	    && list_count(user_cond->assoc_cond->user_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(user_cond->assoc_cond->user_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (user_cond->admin_level != SLURMDB_ADMIN_NOTSET)
		xstrfmtcat(extra, " && admin_level=%u",
			   user_cond->admin_level);

	ret_list = _get_other_user_names_to_mod(mysql_conn, uid, user_cond);

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
	if (!(result = mysql_db_query_ret(
		      mysql_conn, query, 0))) {
		xfree(query);
		FREE_NULL_LIST(ret_list);
		return NULL;
	}

	if (!ret_list)
		ret_list = list_create(xfree_ptr);
	while ((row = mysql_fetch_row(result))) {
		slurmdb_user_rec_t *user_rec = NULL;

		object = row[0];
		slurm_addto_char_list(ret_list, object);
		if (!name_char)
			xstrfmtcat(name_char, "(name='%s'", object);
		else
			xstrfmtcat(name_char, " || name='%s'", object);

		user_rec = xmalloc(sizeof(slurmdb_user_rec_t));

		if (!user->name)
			user_rec->name = xstrdup(object);
		else {
			user_rec->name = xstrdup(user->name);
			user_rec->old_name = xstrdup(object);
			if (_change_user_name(mysql_conn, user_rec)
			    != SLURM_SUCCESS)
				break;
		}

		user_rec->admin_level = user->admin_level;
		if (addto_update_list(mysql_conn->update_list,
				      SLURMDB_MODIFY_USER, user_rec)
		    != SLURM_SUCCESS)
			slurmdb_destroy_user_rec(user_rec);
	}
	mysql_free_result(result);

no_user_table:
	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_ASSOC, mysql_conn->conn,
		         "didn't affect anything\n%s", query);
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
		rc = modify_common(mysql_conn, DBD_MODIFY_USERS, now,
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
		tmp_list = as_mysql_modify_assocs(mysql_conn, uid,
						  &assoc_cond, &assoc);
		FREE_NULL_LIST(assoc_cond.acct_list);

		if (!tmp_list) {
			FREE_NULL_LIST(ret_list);
			goto end_it;
		}
		/* char *names = NULL; */
		/* ListIterator itr = list_iterator_create(tmp_list); */
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

		rc = as_mysql_assoc_remove_default(
			mysql_conn, ret_list, cluster_list);
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
		tmp_list = as_mysql_modify_wckeys(mysql_conn, uid,
						  &wckey_cond, &wckey);
		FREE_NULL_LIST(wckey_cond.name_list);

		if (!tmp_list) {
			FREE_NULL_LIST(ret_list);
			goto end_it;
		}
		/* char *names = NULL; */
		/* ListIterator itr = list_iterator_create(tmp_list); */
		/* while ((names = list_next(itr))) { */
		/* 	info("%s", names); */
		/* } */
		/* list_iterator_destroy(itr); */
		FREE_NULL_LIST(tmp_list);
	}
end_it:
	return ret_list;
}

/*
 * If the coordinator has permissions to modify every account
 * belonging to each user, return true. Otherwise return false.
 */
static bool _is_coord_over_all_accts(mysql_conn_t *mysql_conn,
				     char *cluster_name, char *user_char,
				     slurmdb_user_rec_t *coord)
{
	bool has_access;
	char *query = NULL, *sep_str = "";
	MYSQL_RES *result;
	ListIterator itr;
	slurmdb_coord_rec_t *coord_acct;

	if (!coord->coord_accts || !list_count(coord->coord_accts)) {
		/* This should never happen */
		error("%s: We are here with no coord accts", __func__);
		return false;
	}

	query = xstrdup_printf("select distinct acct from \"%s_%s\" where deleted=0 && (%s) && (",
			       cluster_name, assoc_table, user_char);

	/*
	 * Add the accounts we are coordinator of.  If anything is returned
	 * outside of this list we will know there are accounts in the request
	 * that we are not coordinator over.
	 */
	itr = list_iterator_create(coord->coord_accts);
	while ((coord_acct = (list_next(itr)))) {
		xstrfmtcat(query, "%sacct != '%s'", sep_str, coord_acct->name);
		sep_str = " && ";
	}
	list_iterator_destroy(itr);
	xstrcat(query, ");");

	DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
	if (!(result = mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		return false;
	}
	xfree(query);

	/*
	 * If nothing was returned we are coordinator over all these accounts
	 * and users.
	 */
	has_access = !mysql_num_rows(result);

	mysql_free_result(result);
	return has_access;
}

extern List as_mysql_remove_users(mysql_conn_t *mysql_conn, uint32_t uid,
				  slurmdb_user_cond_t *user_cond)
{
	ListIterator itr = NULL;
	List ret_list = NULL;
	List coord_list = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL;
	char *extra = NULL, *query = NULL,
		*name_char = NULL, *assoc_char = NULL, *user_char = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int set = 0;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
	slurmdb_user_cond_t user_coord_cond;
	slurmdb_user_rec_t user;
	slurmdb_assoc_cond_t assoc_cond;
	slurmdb_wckey_cond_t wckey_cond;
	bool jobs_running = 0;
	bool is_coord = false;

	memset(&user, 0, sizeof(slurmdb_user_rec_t));
	user.uid = uid;

	if (!user_cond) {
		error("we need something to remove");
		return NULL;
	}

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		/*
		 * Allow coordinators to delete users from accounts that
		 * they coordinate. After we have gotten every association that
		 * the users belong to, check that the coordinator has access
		 * to modify every affected account.
		 */
		is_coord = is_user_any_coord(mysql_conn, &user);
		if (!is_coord) {
			error("Only admins/coordinators can remove users");
			errno = ESLURM_ACCESS_DENIED;
			return NULL;
		}
	}

	if (user_cond->assoc_cond && user_cond->assoc_cond->user_list
	    && list_count(user_cond->assoc_cond->user_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(user_cond->assoc_cond->user_list);
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

	ret_list = _get_other_user_names_to_mod(mysql_conn, uid, user_cond);

	if (user_cond->admin_level != SLURMDB_ADMIN_NOTSET) {
		xstrfmtcat(extra, " && admin_level=%u", user_cond->admin_level);
	}

	if (!extra && !ret_list) {
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
	if (!(result = mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		return NULL;
	}

	if (!ret_list)
		ret_list = list_create(xfree_ptr);
	while ((row = mysql_fetch_row(result)))
		slurm_addto_char_list(ret_list, row[0]);
	mysql_free_result(result);

no_user_table:

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_ASSOC, mysql_conn->conn,
		         "didn't affect anything\n%s", query);
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
			xstrfmtcat(name_char, " || name='%s'", object);
			xstrfmtcat(user_char, " || user='%s'", object);
			xstrfmtcat(assoc_char, " || t2.user='%s'", object);
		} else {
			xstrfmtcat(name_char, "name='%s'", object);
			xstrfmtcat(user_char, "user='%s'", object);
			xstrfmtcat(assoc_char, "t2.user='%s'", object);
		}

		user_rec->name = xstrdup(object);
		if (addto_update_list(mysql_conn->update_list,
				      SLURMDB_REMOVE_USER, user_rec)
		    != SLURM_SUCCESS)
			slurmdb_destroy_user_rec(user_rec);
	}
	list_iterator_destroy(itr);

	/* We need to remove these accounts from the coord's that have it */
	coord_list = as_mysql_remove_coord(
		mysql_conn, uid, NULL, &user_coord_cond);
	FREE_NULL_LIST(coord_list);

	/* We need to remove these users from the wckey table */
	memset(&wckey_cond, 0, sizeof(slurmdb_wckey_cond_t));
	wckey_cond.user_list = assoc_cond.user_list;
	coord_list = as_mysql_remove_wckeys(mysql_conn, uid, &wckey_cond);
	FREE_NULL_LIST(coord_list);

	FREE_NULL_LIST(assoc_cond.user_list);

	user_name = uid_to_string((uid_t) uid);
	slurm_rwlock_rdlock(&as_mysql_cluster_list_lock);
	itr = list_iterator_create(as_mysql_cluster_list);
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
	slurm_mutex_lock(&assoc_lock);
#endif
	while ((object = list_next(itr))) {

		if (is_coord) {
			if (!_is_coord_over_all_accts(mysql_conn, object,
						      user_char, &user)) {
				errno = ESLURM_ACCESS_DENIED;
				rc = SLURM_ERROR;
				break;
			}
		}

		if ((rc = remove_common(mysql_conn, DBD_REMOVE_USERS, now,
					user_name, user_table, name_char,
					assoc_char, object, ret_list,
					&jobs_running, NULL))
		    != SLURM_SUCCESS)
			break;
	}
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
	slurm_mutex_unlock(&assoc_lock);
#endif
	list_iterator_destroy(itr);
	slurm_rwlock_unlock(&as_mysql_cluster_list_lock);

	xfree(user_name);
	xfree(name_char);
	xfree(user_char);
	if (rc == SLURM_ERROR) {
		FREE_NULL_LIST(ret_list);
		xfree(assoc_char);
		return NULL;
	}

	query = xstrdup_printf(
		"update %s as t2 set deleted=1, mod_time=%ld where %s",
		acct_coord_table, (long)now, assoc_char);
	xfree(assoc_char);

	rc = mysql_db_query(mysql_conn, query);
	xfree(query);
	if (rc != SLURM_SUCCESS) {
		error("Couldn't remove user coordinators");
		FREE_NULL_LIST(ret_list);
		return NULL;
	}

	if (jobs_running)
		errno = ESLURM_JOBS_RUNNING_ON_ASSOC;
	else
		errno = SLURM_SUCCESS;
	return ret_list;
}

extern List as_mysql_remove_coord(mysql_conn_t *mysql_conn, uint32_t uid,
				  List acct_list,
				  slurmdb_user_cond_t *user_cond)
{
	char *query = NULL, *object = NULL, *extra = NULL, *last_user = NULL;
	char *user_name = NULL;
	time_t now = time(NULL);
	int set = 0, is_admin=0, rc = SLURM_SUCCESS;
	ListIterator itr = NULL;
	slurmdb_user_rec_t *user_rec = NULL;
	List ret_list = NULL;
	List user_list = NULL;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
	slurmdb_user_rec_t user;

	if (!user_cond && !acct_list) {
		error("we need something to remove");
		return NULL;
	} else if (user_cond && user_cond->assoc_cond)
		user_list = user_cond->assoc_cond->user_list;

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	memset(&user, 0, sizeof(slurmdb_user_rec_t));
	user.uid = uid;

	if (!(is_admin = is_user_min_admin_level(
		      mysql_conn, uid, SLURMDB_ADMIN_OPERATOR))) {
		if (!is_user_any_coord(mysql_conn, &user)) {
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
			xstrcat(extra, " && (");
		else
			xstrcat(extra, "(");

		itr = list_iterator_create(user_list);
		while ((object = list_next(itr))) {
			if (!object[0])
				continue;
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "user='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (acct_list && list_count(acct_list)) {
		set = 0;
		if (extra)
			xstrcat(extra, " && (");
		else
			xstrcat(extra, "(");

		itr = list_iterator_create(acct_list);
		while ((object = list_next(itr))) {
			if (!object[0])
				continue;
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "acct='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (!extra) {
		errno = SLURM_ERROR;
		DB_DEBUG(DB_ASSOC, mysql_conn->conn, "No conditions given");
		return NULL;
	}

	query = xstrdup_printf(
		"select user, acct from %s where deleted=0 && %s order by user",
		acct_coord_table, extra);

	DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
	if (!(result =
	      mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		xfree(extra);
		errno = SLURM_ERROR;
		return NULL;
	}
	xfree(query);
	ret_list = list_create(xfree_ptr);
	user_list = list_create(xfree_ptr);
	while ((row = mysql_fetch_row(result))) {
		if (!is_admin) {
			slurmdb_coord_rec_t *coord = NULL;
			if (!user.coord_accts) { // This should never
				// happen
				error("We are here with no coord accts");
				errno = ESLURM_ACCESS_DENIED;
				FREE_NULL_LIST(ret_list);
				FREE_NULL_LIST(user_list);
				xfree(extra);
				mysql_free_result(result);
				return NULL;
			}
			itr = list_iterator_create(user.coord_accts);
			while ((coord = list_next(itr))) {
				if (!xstrcasecmp(coord->name, row[1]))
					break;
			}
			list_iterator_destroy(itr);

			if (!coord) {
				error("User %s(%d) does not have the "
				      "ability to change this account (%s)",
				      user.name, user.uid, row[1]);
				errno = ESLURM_ACCESS_DENIED;
				FREE_NULL_LIST(ret_list);
				FREE_NULL_LIST(user_list);
				xfree(extra);
				mysql_free_result(result);
				return NULL;
			}
		}
		if (!last_user || xstrcasecmp(last_user, row[0])) {
			list_append(user_list, xstrdup(row[0]));
			last_user = row[0];
		}
		list_append(ret_list, xstrdup_printf("U = %-9s A = %-10s",
						     row[0], row[1]));
	}
	mysql_free_result(result);

	user_name = uid_to_string((uid_t) uid);
	rc = remove_common(mysql_conn, DBD_REMOVE_ACCOUNT_COORDS,
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
#ifdef __METASTACK_ASSOC_HASH
		/* get all accounts to build a sub_acct_hash*/
		if (sub_acct_hash) 
			destroy_assoc_hash(&sub_acct_hash);
		if (user_list && list_count(user_list))
			get_all_accts(mysql_conn);
#endif
	/* get the update list set */
	itr = list_iterator_create(user_list);
	while ((last_user = list_next(itr))) {
		user_rec = xmalloc(sizeof(slurmdb_user_rec_t));
		user_rec->name = xstrdup(last_user);
		_get_user_coords(mysql_conn, user_rec);
		if (addto_update_list(mysql_conn->update_list,
				      SLURMDB_REMOVE_COORD, user_rec)
		    != SLURM_SUCCESS)
			slurmdb_destroy_user_rec(user_rec);
	}
	list_iterator_destroy(itr);
#ifdef __METASTACK_ASSOC_HASH
	if (sub_acct_hash)
		destroy_assoc_hash(&sub_acct_hash);
#endif
	FREE_NULL_LIST(user_list);

	return ret_list;
}

extern List as_mysql_get_users(mysql_conn_t *mysql_conn, uid_t uid,
			       slurmdb_user_cond_t *user_cond)
{
	char *query = NULL;
	char *extra = NULL;
	char *tmp = NULL;
	List user_list = NULL;
	ListIterator itr = NULL;
	char *object = NULL;
	int set = 0;
	int i=0, is_admin=1;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
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

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	memset(&user, 0, sizeof(slurmdb_user_rec_t));
	user.uid = uid;

	if (slurm_conf.private_data & PRIVATE_DATA_USERS) {
		if (!(is_admin = is_user_min_admin_level(
			      mysql_conn, uid, SLURMDB_ADMIN_OPERATOR))) {
			assoc_mgr_fill_in_user(
				mysql_conn, &user, 1, NULL, false);
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
		xstrcat(extra, "where (deleted=0 || deleted=1)");
	else
		xstrcat(extra, "where deleted=0");


	user_list = _get_other_user_names_to_mod(mysql_conn, uid, user_cond);
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
		xstrcat(extra, " && (");
		itr = list_iterator_create(user_cond->assoc_cond->user_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (user_cond->admin_level != SLURMDB_ADMIN_NOTSET) {
		xstrfmtcat(extra, " && admin_level=%u",
			   user_cond->admin_level);
	}

empty:
	/* This is here to make sure we are looking at only this user
	 * if this flag is set.
	 */
	if (!is_admin && (slurm_conf.private_data & PRIVATE_DATA_USERS)) {
		xstrfmtcat(extra, " && name='%s'", user.name);
	}

	xfree(tmp);
	xstrfmtcat(tmp, "%s", user_req_inx[0]);
	for(i=1; i<USER_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", user_req_inx[i]);
	}

	query = xstrdup_printf("select %s from %s %s", tmp, user_table, extra);
	xfree(tmp);
	xfree(extra);

	DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
	if (!(result = mysql_db_query_ret(
		      mysql_conn, query, 0))) {
		xfree(query);
		return NULL;
	}
	xfree(query);

	user_list = list_create(slurmdb_destroy_user_rec);

#ifdef __METASTACK_ASSOC_HASH
	if (sub_acct_hash) 
		destroy_assoc_hash(&sub_acct_hash);
	if (user_cond && user_cond->with_coords && !user_hash) {
		get_all_accts(mysql_conn);
	}
#endif

	while ((row = mysql_fetch_row(result))) {
		slurmdb_user_rec_t *user = xmalloc(sizeof(slurmdb_user_rec_t));

		list_append(user_list, user);

		user->name =  xstrdup(row[USER_REQ_NAME]);
		user->admin_level = slurm_atoul(row[USER_REQ_AL]);

		if (slurm_atoul(row[USER_REQ_DELETED]))
			user->flags |= SLURMDB_USER_FLAG_DELETED;

#ifdef __METASTACK_ASSOC_HASH
		if (user_cond && user_cond->with_coords) {
			slurmdb_user_rec_t *value = NULL;
			if (user_hash && user->name) {
				value = find_str_key_hash(&user_hash, user->name);
				if (value && value->coord_accts && list_count(value->coord_accts)) {
					user->coord_accts = list_shallow_copy(value->coord_accts);
				}
			} 
			if (!value) {
				_get_user_coords(mysql_conn, user);
			}
		}
#endif
	}
	mysql_free_result(result);
#ifdef __METASTACK_ASSOC_HASH
	if (sub_acct_hash)
		destroy_assoc_hash(&sub_acct_hash);	
#endif

	if (user_cond && (user_cond->with_assocs
			  || (user_cond->assoc_cond
			      && user_cond->assoc_cond->only_defs))) {
		ListIterator assoc_itr = NULL;
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
		assoc_list = as_mysql_get_assocs(
			mysql_conn, uid, user_cond->assoc_cond, false);
#else
		assoc_list = as_mysql_get_assocs(
			mysql_conn, uid, user_cond->assoc_cond);
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
            
            insert_assoc_hash(&assoc_hash, assoc, assoc->user);
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
                    ListIterator tmp_itr = list_iterator_create(tmp_list);
                    while ((tmp_assoc = list_next(tmp_itr))) {
                        if (tmp_assoc->is_def == 1){
                            user->default_acct = xstrdup(tmp_assoc->acct);
                            break;
                        }
                    }
                    list_iterator_destroy(tmp_itr);
                } else 
                    error("not find reference assoc for user: %s", user->name);
                
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
		ListIterator wckey_itr = NULL;
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
		wckey_list = as_mysql_get_wckeys(mysql_conn, uid, &wckey_cond);

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
