/*****************************************************************************\
 *  assoc_mgr.h - keep track of local cache of accounting data.
 *****************************************************************************
 *  Copyright (C) 2004-2007 The Regents of the University of California.
 *  Copyright (C) 2008 Lawrence Livermore National Security.
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
 *****************************************************************************
 * NOTE: When using lock_slurmctld() and assoc_mgr_lock(), always call
 * lock_slurmctld() before calling assoc_mgr_lock() and then call
 * assoc_mgr_unlock() before calling unlock_slurmctld().
\*****************************************************************************/

#ifndef _SLURM_ASSOC_MGR_H
#define _SLURM_ASSOC_MGR_H

#include "src/common/list.h"
#include "src/common/slurm_accounting_storage.h"
#include "src/common/slurmdbd_defs.h"
#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/locks.h"
#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#define ASSOC_MGR_CACHE_ASSOC 0x0001
#define ASSOC_MGR_CACHE_QOS   0x0002
#define ASSOC_MGR_CACHE_USER  0x0004
#define ASSOC_MGR_CACHE_WCKEY 0x0008
#define ASSOC_MGR_CACHE_RES   0x0010
#define ASSOC_MGR_CACHE_TRES  0x0020
#define ASSOC_MGR_CACHE_ALL   0xffff

enum {
	RUNNING_CACHE_STATE_NOTRUNNING = 0,
	RUNNING_CACHE_STATE_RUNNING,
	RUNNING_CACHE_STATE_EXITING,
	RUNNING_CACHE_STATE_LISTS_REFRESHED,
};

/* to lock or not */
typedef struct {
	lock_level_t assoc;
	lock_level_t file;
	lock_level_t qos;
	lock_level_t res;
	lock_level_t tres;
	lock_level_t user;
	lock_level_t wckey;
#ifdef __METASTACK_ASSOC_HASH
	lock_level_t uid;
#endif
} assoc_mgr_lock_t;

typedef enum {
	ASSOC_LOCK,
	FILE_LOCK,
	QOS_LOCK,
	RES_LOCK,
	TRES_LOCK,
	USER_LOCK,
	WCKEY_LOCK,
#ifdef __METASTACK_ASSOC_HASH
	UID_LOCK,
#endif
	ASSOC_MGR_ENTITY_COUNT
} assoc_mgr_lock_datatype_t;

typedef struct {
 	uint16_t cache_level;
	uint16_t enforce;
	uint16_t *running_cache;
	void (*add_license_notify) (slurmdb_res_rec_t *rec);
	void (*resize_qos_notify) (void);
	void (*remove_assoc_notify) (slurmdb_assoc_rec_t *rec);
	void (*remove_license_notify) (slurmdb_res_rec_t *rec);
	void (*remove_qos_notify) (slurmdb_qos_rec_t *rec);
	char **state_save_location;
	void (*sync_license_notify) (List clus_res_list);
	void (*update_assoc_notify) (slurmdb_assoc_rec_t *rec);
	void (*update_cluster_tres) (void);
	void (*update_license_notify) (slurmdb_res_rec_t *rec);
	void (*update_qos_notify) (slurmdb_qos_rec_t *rec);
	void (*update_resvs) ();
} assoc_init_args_t;

#ifdef __METASTACK_QOS_HASH
extern qos_hash_t *qos_hash;
#endif

#ifdef __METASTACK_ASSOC_HASH
#include "uthash.h"

typedef struct struct_assoc_hash {
    char *key;
    List value_assoc_list;
    UT_hash_handle hh;
} assoc_hash_t;

/** build hash table, assign assoc to entry according to key. 
 * IN:  hash table, assoc, hash_key, list del function
 * OUT: assoc_hash
 */
extern void insert_assoc_hash(assoc_hash_t **assoc_hash, slurmdb_assoc_rec_t *assoc, char *str_key, ListDelF del_function);

/** find entry by key */ 
extern assoc_hash_t *find_assoc_entry(assoc_hash_t **assoc_hash, char *key);

/** delete assoc rec by key */
extern void remove_assoc_rec(assoc_hash_t **assoc_hash, slurmdb_assoc_rec_t *assoc, char *key);

/** delete entry by key */
extern void remove_assoc_entry(assoc_hash_t **assoc_hash, char *key);

/** delete hash */
extern void destroy_assoc_hash(assoc_hash_t **assoc_hash);

typedef struct struct_str_key_hash {
	char *key;
	void *value;
	UT_hash_handle hh;
} str_key_hash_t;

extern void insert_str_key_hash(str_key_hash_t **str_key_hash, void *insert_value, char *str_key);
extern void *find_str_key_hash(str_key_hash_t **str_key_hash, char *str_key);
extern void remove_str_key_hash(str_key_hash_t **str_key_hash, char *str_key);
extern void destroy_str_key_hash(str_key_hash_t **str_key_hash);

/** destroy the hash table whose value is the assoc_hash table */
extern void destroy_hash_value_hash(str_key_hash_t **str_key_hash);

typedef struct struct_user_uid_hash {
	char *username;
	uid_t  uid;
	UT_hash_handle hh;
} user_uid_hash_t;

extern void insert_user_uid_hash(user_uid_hash_t **user_uid_hash, uid_t value, char *str_key);
extern uid_t find_user_uid_hash(user_uid_hash_t **user_uid_hash, char *str_key);
extern void remove_user_uid_hash(user_uid_hash_t **user_uid_hash, char *str_key);
extern void destroy_user_uid_hash(user_uid_hash_t **user_uid_hash);

typedef struct struct_uid_user_hash {
	uid_t  uid;
	char *username;
	UT_hash_handle hh;
} uid_user_hash_t;

extern void insert_uid_user_hash(uid_user_hash_t **uid_user_hash, char *value, uid_t key);
extern char *find_uid_user_hash(uid_user_hash_t **uid_user_hash, uid_t key);
extern void remove_uid_user_hash(uid_user_hash_t **uid_user_hash, uid_t key);
extern void destroy_uid_user_hash(uid_user_hash_t **uid_user_hash);

/*
 * update user_hash, user_uid_hash and uid_user_hash
 * IN:  user - slurmdb_user_rec_t of user to update
 * IN:  rec_user_entry - NULL or the key pair of the user to be updated in user_hash
 * IN:  rec_user_uid_entry - NULL or the key pair of the user to be updated in user_uid_hash
 */
extern void update_user_hash(slurmdb_user_rec_t *user,
				     str_key_hash_t *rec_user_entry, 
				     user_uid_hash_t *rec_user_uid_entry);

extern str_key_hash_t *user_hash;
extern user_uid_hash_t *user_uid_hash;
extern uid_user_hash_t *uid_user_hash;

extern pthread_mutex_t uid_save_lock;
extern pthread_cond_t  uid_save_cond;
extern int save_uids;

extern assoc_hash_t *assoc_mgr_user_assoc_hash;
#endif

extern List assoc_mgr_tres_list;
extern slurmdb_tres_rec_t **assoc_mgr_tres_array;
extern char **assoc_mgr_tres_name_array;
extern List assoc_mgr_assoc_list;
extern List assoc_mgr_res_list;
extern List assoc_mgr_qos_list;
extern List assoc_mgr_user_list;
extern List assoc_mgr_wckey_list;

extern slurmdb_assoc_rec_t *assoc_mgr_root_assoc;

extern uint32_t g_qos_max_priority; /* max priority in all qos's */
extern uint32_t g_qos_count; /* count used for generating qos bitstr's */
extern uint32_t g_user_assoc_count; /* Number of associations which are users */
extern uint32_t g_tres_count; /* Number of TRES from the database
			       * which also is the number of elements
			       * in the assoc_mgr_tres_array */

extern int assoc_mgr_init(void *db_conn, assoc_init_args_t *args,
			  int db_conn_errno);
extern int assoc_mgr_fini(bool save_state);
extern void assoc_mgr_lock(assoc_mgr_lock_t *locks);
extern void assoc_mgr_unlock(assoc_mgr_lock_t *locks);

#ifdef __METASTACK_NEW_PART_PARA_SCHED
extern void assoc_mgr_lock_init();
extern void assoc_mgr_lock_destory();
#endif

#ifndef NDEBUG
extern bool verify_assoc_lock(assoc_mgr_lock_datatype_t datatype, lock_level_t level);
#endif

/* ran after a new tres_list is given */
extern int assoc_mgr_post_tres_list(List new_list);

/*
 * get info from the storage
 * IN:  assoc - slurmdb_assoc_rec_t with at least cluster and
 *		    account set for account association.  To get user
 *		    association set user, and optional partition.
 *		    Sets "id" field with the association ID.
 * IN: enforce - return an error if no such association exists
 * IN/OUT: assoc_list - contains a list of assoc_rec ptrs to
 *                      associations this user has in the list.  This
 *                      list should be created with list_create(NULL)
 *                      since we are putting pointers to memory used elsewhere.
 * RET: SLURM_SUCCESS on success, else SLURM_ERROR
 *
 * NOTE: Since the returned assoc_list is full of pointers from the
 *       assoc_mgr_assoc_list assoc_mgr_lock_t READ_LOCK on
 *       associations must be set before calling this function and while
 *       handling it after a return.
 */
extern int assoc_mgr_get_user_assocs(void *db_conn,
				     slurmdb_assoc_rec_t *assoc,
				     int enforce,
				     List assoc_list);

/*
 * get info from the storage
 * IN/OUT:  tres - slurmdb_tres_rec_t with at least id or type and
 *                  optional name set.
 * IN: enforce - return an error if no such tres exists
 * IN/OUT: tres_pptr - if non-NULL then return a pointer to the
 *			slurmdb_tres record in cache on success
 *                      DO NOT FREE.
 * IN: locked - If you plan on using tres_pptr after this function
 *              you need to have an assoc_mgr_lock_t READ_LOCK for
 *              tres while you use it before and after the
 *              return.  This is not required if using the assoc for
 *              non-pointer portions.
 * RET: SLURM_SUCCESS on success, else SLURM_ERROR
 */
extern int assoc_mgr_fill_in_tres(void *db_conn,
				   slurmdb_tres_rec_t *tres,
				   int enforce,
				   slurmdb_tres_rec_t **tres_pptr,
				   bool locked);

/*
 * get info from the storage
 * IN/OUT:  assoc - slurmdb_assoc_rec_t with at least cluster and
 *		    account set for account association.  To get user
 *		    association set user, and optional partition.
 *		    Sets "id" field with the association ID.
 * IN: enforce - return an error if no such association exists
 * IN/OUT: assoc_pptr - if non-NULL then return a pointer to the
 *			slurmdb_assoc record in cache on success
 *                      DO NOT FREE.
 * IN: locked - If you plan on using assoc_pptr after this function
 *              you need to have an assoc_mgr_lock_t READ_LOCK for
 *              associations and users while you use it before and after the
 *              return.  This is not required if using the assoc for
 *              non-pointer portions.
 * RET: SLURM_SUCCESS on success, else SLURM_ERROR
 */
extern int assoc_mgr_fill_in_assoc(void *db_conn,
				   slurmdb_assoc_rec_t *assoc,
				   int enforce,
				   slurmdb_assoc_rec_t **assoc_pptr,
				   bool locked);

/*
 * get info from the storage
 * IN/OUT:  user - slurmdb_user_rec_t with the name set of the user.
 *                 "default_account" will be filled in on
 *                 successful return DO NOT FREE.
 * IN/OUT: user_pptr - if non-NULL then return a pointer to the
 *		       slurmdb_user record in cache on success
 *                     DO NOT FREE.
 * IN: locked - If you plan on using user_pptr outside
 *              this function you need to have an assoc_mgr_lock_t
 *              READ_LOCK for User while you use it before and after the
 *              return.  This is not required if using the assoc for
 *              non-pointer portions.
 * RET: SLURM_SUCCESS on success SLURM_ERROR else
 */
extern int assoc_mgr_fill_in_user(void *db_conn, slurmdb_user_rec_t *user,
				  int enforce,
				  slurmdb_user_rec_t **user_pptr, bool locked);

/*
 * get info from the storage
 * IN/OUT:  qos - slurmdb_qos_rec_t with the id set of the qos.
 * IN/OUT:  qos_pptr - if non-NULL then return a pointer to the
 *		       slurmdb_qos record in cache on success
 *                     DO NOT FREE.
 * IN: locked - If you plan on using qos_pptr, or g_qos_count outside
 *              this function you need to have an assoc_mgr_lock_t
 *              READ_LOCK for QOS while you use it before and after the
 *              return.  This is not required if using the assoc for
 *              non-pointer portions.
 * RET: SLURM_SUCCESS on success SLURM_ERROR else
 */
extern int assoc_mgr_fill_in_qos(void *db_conn, slurmdb_qos_rec_t *qos,
				 int enforce,
				 slurmdb_qos_rec_t **qos_pptr, bool locked);
/*
 * get info from the storage
 * IN/OUT:  wckey - slurmdb_wckey_rec_t with the name, cluster and user
 *		    for the wckey association.
 *		    Sets "id" field with the wckey ID.
 * IN: enforce - return an error if no such wckey exists
 * IN/OUT: wckey_pptr - if non-NULL then return a pointer to the
 *			slurmdb_wckey record in cache on success
 * IN: locked - If you plan on using wckey_pptr outside
 *              this function you need to have an assoc_mgr_lock_t
 *              READ_LOCK for WCKey and Users while you use it before and after
 *              the return.  This is not required if using the assoc for
 *              non-pointer portions.
 * RET: SLURM_SUCCESS on success, else SLURM_ERROR
 */
extern int assoc_mgr_fill_in_wckey(void *db_conn,
				   slurmdb_wckey_rec_t *wckey,
				   int enforce,
				   slurmdb_wckey_rec_t **wckey_pptr,
				   bool locked);

/*
 * get admin_level of uid
 * IN: uid - uid of user to check admin_level of.
 * RET: admin level SLURMDB_ADMIN_NOTSET on error
 */
extern slurmdb_admin_level_t assoc_mgr_get_admin_level(void *db_conn,
						       uint32_t uid);

/*
 * see if user is coordinator of given acct
 * IN: uid - uid of user to check.
 * IN: acct - name of account
 * RET: true or false
 */
extern bool assoc_mgr_is_user_acct_coord(void *db_conn, uint32_t uid,
					char *acct);

/*
 * see if user is coordinator of given acct
 * IN: user - slurmdb_user_rec_t of user to check.
 * IN: acct - name of account
 * RET: true or false
 */
extern bool assoc_mgr_is_user_acct_coord_user_rec(void *db_conn,
                                                 slurmdb_user_rec_t *user,
                                                 char *acct_name);

/*
 * get the share information from the association list
 * IN: uid: uid_t of user issuing the request
 * IN: req_msg: info about request
 * IN/OUT: resp_msg: message filled in with assoc_mgr info
 */
extern void assoc_mgr_get_shares(void *db_conn,
				 uid_t uid, shares_request_msg_t *req_msg,
				 shares_response_msg_t *resp_msg);

/*
 * get the state of the association manager and pack it up in buffer
 * OUT buffer_ptr - the pointer is set to the allocated buffer.
 * OUT buffer_size - set to size of the buffer in bytes
 * IN: msg: request for various states
 * IN: uid: uid_t of user issuing the request
 * IN: db_conn: needed if not already connected to the database or DBD
 * IN: protocol_version: version of Slurm we are sending to.
 */
extern void assoc_mgr_info_get_pack_msg(
	char **buffer_ptr, int *buffer_size,
	assoc_mgr_info_request_msg_t *msg, uid_t uid,
	void *db_conn, uint16_t protocol_version);

/*
 * unpack the packing of the above assoc_mgr_get_pack_state_msg function.
 * OUT: object - what to unpack into
 * IN: buffer - buffer to unpack
 * IN: version of Slurm this is packed in
 * RET: SLURM_SUCCESS on SUCCESS, SLURM_ERROR else
 */
extern int assoc_mgr_info_unpack_msg(assoc_mgr_info_msg_t **object,
				     buf_t *buffer, uint16_t protocol_version);

/*
 * assoc_mgr_update - update the association manager
 * IN update_list: updates to perform
 * IN locked: if appropriate write locks are locked before calling or not
 * RET: error code
 * NOTE: the items in update_list are not deleted
 */
extern int assoc_mgr_update(List update_list, bool locked);

/*
 * update associations in cache
 * IN:  slurmdb_update_object_t *object
 * IN   locked: if appropriate write locks are locked before calling or not
 * RET: SLURM_SUCCESS on success (or not found) SLURM_ERROR else
 */
extern int assoc_mgr_update_assocs(slurmdb_update_object_t *update,
				   bool locked);

/*
 * update wckeys in cache
 * IN:  slurmdb_update_object_t *object
 * IN   locked: if appropriate write locks are locked before calling or not
 * RET: SLURM_SUCCESS on success (or not found) SLURM_ERROR else
 */
extern int assoc_mgr_update_wckeys(slurmdb_update_object_t *update,
				   bool locked);

/*
 * update qos in cache
 * IN:  slurmdb_update_object_t *object
 * IN   locked: if appropriate write locks are locked before calling or not
 * RET: SLURM_SUCCESS on success (or not found) SLURM_ERROR else
 */
extern int assoc_mgr_update_qos(slurmdb_update_object_t *update,
				bool locked);

/*
 * update cluster resources in cache
 * IN:  slurmdb_update_object_t *object
 * IN   locked: if appropriate write locks are locked before calling or not
 * RET: SLURM_SUCCESS on success (or not found) SLURM_ERROR else
 */
extern int assoc_mgr_update_res(slurmdb_update_object_t *update,
				bool locked);

/*
 * update cluster tres in cache
 * IN:  slurmdb_update_object_t *object
 * IN   locked: if appropriate write locks are locked before calling or not
 * RET: SLURM_SUCCESS on success (or not found) SLURM_ERROR else
 */
extern int assoc_mgr_update_tres(slurmdb_update_object_t *update,
				 bool locked);

/*
 * update users in cache
 * IN:  slurmdb_update_object_t *object
 * IN   locked: if appropriate write locks are locked before calling or not
 * RET: SLURM_SUCCESS on success (or not found) SLURM_ERROR else
 */
extern int assoc_mgr_update_users(slurmdb_update_object_t *update,
				  bool locked);

/*
 * validate that an association ID is still valid
 * IN:  assoc_id - association ID previously returned by
 *		get_assoc_id(void *db_conn,
 )
 * RET: SLURM_SUCCESS on success SLURM_ERROR else
 */
extern int assoc_mgr_validate_assoc_id(void *db_conn,
				       uint32_t assoc_id,
				       int enforce);

/*
 * clear the used_* fields from every association,
 *	used on reconfiguration
 */
extern void assoc_mgr_clear_used_info(void);

/*
 * Remove the association's accumulated usage
 * IN:  slurmdb_assoc_rec_t *assoc
 * RET: SLURM_SUCCESS on success or else SLURM_ERROR
 */
extern void assoc_mgr_remove_assoc_usage(slurmdb_assoc_rec_t *assoc);

/*
 * Remove the QOS's accumulated usage
 * IN:  slurmdb_qos_rec_t *qos
 * RET: SLURM_SUCCESS on success or else SLURM_ERROR
 */
extern void assoc_mgr_remove_qos_usage(slurmdb_qos_rec_t *qos);

/*
 * Dump the state information of the association mgr just in case the
 * database isn't up next time we run.
 */
extern int dump_assoc_mgr_state(void);

#ifdef __METASTACK_ASSOC_HASH
/*
 * Dump the uid information of all users for quick use next time we run.
 */
extern int dump_uid_state(void);

/*
 * Read in the uids of the users.
 */
extern int load_uid_state(bool uid_write_locked);

extern void uid_save(void);
#endif

/*
 * Read in the past usage for associations.
 */
extern int load_assoc_usage(void);

/*
 * Read in the past usage for qos.
 */
extern int load_qos_usage(void);

/*
 * Read in the past tres list.
 */
extern int load_assoc_mgr_last_tres(void);

/*
 * Read in the information of the association mgr if the database
 * isn't up when starting.
 */
extern int load_assoc_mgr_state(bool only_tres);

/*
 * Refresh the lists if when running_cache is set this will load new
 * information from the database (if any) and update the cached list.
 */
extern int assoc_mgr_refresh_lists(void *db_conn, uint16_t cache_level);

/*
 * Sets the uids of users added to the system after the start of the
 * calling program.
 */
extern int assoc_mgr_set_missing_uids();

/* Normalize shares for an association. External so a priority plugin
 * can call it if needed.
 */
extern void assoc_mgr_normalize_assoc_shares(slurmdb_assoc_rec_t *assoc);

/*
 * Find the position of the given TRES ID or type/name in the
 * assoc_mgr_tres_array. If the TRES name or ID isn't found -1 is returned.
 */
extern int assoc_mgr_find_tres_pos(slurmdb_tres_rec_t *tres_rec, bool locked);

/*
 * Find the position of the given TRES name in the
 * assoc_mgr_tres_array. Ignore anything after ":" in the TRES name.
 * So tres_rec->name of "gpu" can match accounting TRES name of "gpu:tesla".
 * If the TRES name isn't found -1 is returned.
 */
extern int assoc_mgr_find_tres_pos2(slurmdb_tres_rec_t *tres_rec, bool locked);

/*
 * Calls assoc_mgr_find_tres_pos and returns the pointer in the
 * assoc_mgr_tres_array.
 * NOTE: The assoc_mgr tres read lock needs to be locked before calling this
 * function and while using the returned record.
 */
extern slurmdb_tres_rec_t *assoc_mgr_find_tres_rec(
	slurmdb_tres_rec_t *tres_rec);

/* fills in allocates and sets tres_cnt based off tres_str
 * OUT tres_cnt - array to be filled in g_tres_cnt in length
 * IN tres_str - simple format of tres used with id and count set
 * IN init_val - what the initial value is going to be set to
 * IN locked - if the assoc_mgr tres read lock is locked or not.
 * RET if positions changed in array from string 1 if nothing changed 0
 */
extern int assoc_mgr_set_tres_cnt_array(uint64_t **tres_cnt, char *tres_str,
					uint64_t init_val, bool locked);

/* Creates all the tres arrays for an association.
 * NOTE: The assoc_mgr tres read lock needs to be locked before this
 * is called. */
extern void assoc_mgr_set_assoc_tres_cnt(slurmdb_assoc_rec_t *assoc);

/* Creates all the tres arrays for a QOS.
 * NOTE: The assoc_mgr tres read lock needs to be locked before this
 * is called. */
extern void assoc_mgr_set_qos_tres_cnt(slurmdb_qos_rec_t *qos);

/* Make a simple tres string from a tres count array.
 * IN tres_cnt - counts of each tres used
 * IN flags - TRES_STR_FLAG_SIMPLE or 0 for formatted string
 * IN locked - if the assoc_mgr tres read lock is locked or not.
 * RET char * of simple tres string
 */
extern char *assoc_mgr_make_tres_str_from_array(
	uint64_t *tres_cnt, uint32_t flags, bool locked);

/* Fill in the default qos id or name given an association record.  If
 * none is given it gives the default qos for the system.
 * IN/OUT: qos_rec - fills in the name or id of the default qos
 *
 * NOTE: READ lock needs to be set on associations and QOS before
 * calling this. */
extern void assoc_mgr_get_default_qos_info(
	slurmdb_assoc_rec_t *assoc_ptr, slurmdb_qos_rec_t *qos_rec);

/*
 * Calculate a weighted tres value.
 * IN: tres_cnt - array of tres values of size g_tres_count.
 * IN: weights - weights to apply to tres values of size g_tres_count.
 * IN: flags - priority flags (toogle between MAX or SUM of tres).
 * IN: locked - whether the tres read assoc mgr lock is locked or not.
 * RET: returns the calculated tres weight.
 */
extern double assoc_mgr_tres_weighted(uint64_t *tres_cnt, double *weights,
				      uint16_t flags, bool locked);

/* Get TRES's old position.
 * IN: cur_pos - the current position in the tres array.
 */
extern int assoc_mgr_get_old_tres_pos(int cur_pos);

/* Test whether the tres positions have changed since last reading the tres
 * list.
 */
extern int assoc_mgr_tres_pos_changed();

#endif /* _SLURM_ASSOC_MGR_H */
