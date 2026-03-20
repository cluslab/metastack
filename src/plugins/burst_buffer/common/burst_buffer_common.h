/*****************************************************************************\
 *  burst_buffer_common.h - Common header for managing burst_buffers
 *
 *  NOTE: These functions are designed so they can be used by multiple burst
 *  buffer plugins at the same time, so the state information is largely in the
 *  individual plugin and passed as a pointer argument to these functions.
 *****************************************************************************
 *  Copyright (C) SchedMD LLC.
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

#ifndef __BURST_BUFFER_COMMON_H__
#define __BURST_BUFFER_COMMON_H__

#include "src/common/list.h"
#include "src/common/pack.h"
#include "slurm/slurm.h"
#include "slurm/slurmdb.h"
#ifdef __METASTACK_NEW_BURSTBUFFER
#include "src/interfaces/cred.h" 
#endif
/* Interval, in seconds, for purging orphan bb_alloc_t records and timing out
 * staging */
#define AGENT_INTERVAL	30

/* Hash tables are used for both job burst buffer and user limit records */
#define BB_HASH_SIZE	100

#define DEFAULT_BB_POLL_INTERVAL 15 /* 15 seconds */

/* Default operation timeouts */
#define DEFAULT_OTHER_TIMEOUT		300	/* 5 minutes */
#define DEFAULT_STATE_IN_TIMEOUT	86400	/* 1 day */
#define DEFAULT_STATE_OUT_TIMEOUT	86400	/* 1 day */
#define DEFAULT_VALIDATE_TIMEOUT	5	/* 5 seconds */

#ifdef __METASTACK_NEW_BURSTBUFFER
#define DEFAULT_RETRY_COUNT		3	/* 3 retries */
#define DEFAULT_MAX_GROUPS 2048
#define DEFAULT_MAX_DATASETS 8192
#define DEFAULT_MAX_GROUPS_PER_CLIENTS 4
#define DEFAULT_MAX_CLIENTS_PER_JOB 4
#define DEFAULT_POLL_INTERVAL 5  /* 5 second */
#define DEFAULT_MAX_ACC_DIRS_PER_JOB 8
#define DEFAULT_MAX_ACC_DIR_LEN 512
#endif

/* Burst buffer configuration parameters */
typedef struct bb_config {
	uid_t   *allow_users;
	char    *allow_users_str;
	char    *create_buffer;
	char	*default_pool;
	uid_t   *deny_users;
	char    *deny_users_str;
	char    *destroy_buffer;
	char *directive_str;
	uint32_t flags;			/* See BB_FLAG_* in slurm.h */
	char    *get_sys_state;
	char    *get_sys_status;
	uint64_t granularity;		/* space allocation granularity,
					 * units are GB */
	uint32_t pool_cnt;		/* Count of records in pool_ptr */
	burst_buffer_pool_t *pool_ptr;	/* Type is defined in slurm.h */
	uint32_t poll_interval;
	uint32_t other_timeout;
	uint32_t stage_in_timeout;
	uint32_t stage_out_timeout;
	char    *start_stage_in;
	char    *start_stage_out;
	char    *stop_stage_in;
	char    *stop_stage_out;
	uint32_t validate_timeout;
#ifdef __METASTACK_NEW_BURSTBUFFER
	uint32_t max_groups;
	uint32_t max_datasets;
	uint32_t max_clients_join;
	uint32_t max_clients_per_job;	/* Cache group sizing: clients per group */
	uint32_t max_acc_dir_len;	/* Max path length for one access (stage) directory */
	uint32_t max_acc_dirs_per_job;	/* Max access directories to stage per job */
	char    *file_system;		/* Storage backend / filesystem name */
	char    *file_system_mount;	/* Mount point for the filesystem */

	char    *para_stor_addr;	/* IP address */
	uint32_t para_stor_port;	/* port number */
	char    *para_stor_user_name;	/* user name */
	char    *para_stor_password;	/* password */
	char    *token;			/* API or session authentication token */
	uint32_t retry_count;		/* Retries after timeout on backend calls */
#endif
} bb_config_t;



/* Current burst buffer allocations (instances). Some of these will be job
 * specific (job_id != 0) and others persistent */
#define BB_ALLOC_MAGIC		0xDEAD3448
typedef struct bb_alloc {
	char *account;		/* Associated account (for limits) */
	slurmdb_assoc_rec_t *assoc_ptr;
	char *assocs;		/* Association string, used for accounting */
	uint32_t array_job_id;
	uint32_t array_task_id;
	bool cancelled;
	time_t create_time;	/* Time of creation */
	time_t end_time;	/* Expected time when use will end */
	uint32_t group_id;
	uint32_t id;		/* ID for reservation/accounting */
	uint32_t job_id;
	uint32_t magic;
	char *name;		/* For persistent burst buffers */
	struct bb_alloc *next;
	bool orphaned;		/* Job is purged, could not stage-out data */
	char *partition;	/* Associated partition (for limits) */
	char *pool;		/* Resource (pool) used */
	char *qos;		/* Associated QOS (for limits) */
	slurmdb_qos_rec_t *qos_ptr;
	time_t seen_time;	/* Time buffer last seen */
	uint64_t size;
	uint16_t state;
	time_t state_time;	/* Time of last state change */
	time_t use_time;	/* Expected time when use will begin */
	uint32_t user_id;
#ifdef __METASTACK_NEW_BURSTBUFFER
	uint32_t type;			/* 0=temporary, 1=persistent cache group */
	bool  enforce_bb_flag;		/* Require burst buffer (enforce acceleration) */
	uint32_t bb_task;		/* Max concurrent burst buffer tasks on this group */
	uint32_t groups_nodes;		/* Nodes participating in this cache group */
	uint64_t req_space;		/* Burst buffer space requested for the job */
	uint32_t access_mode;		/* 0=shared/striped, 1=private/local */
	char *pfs;			/* Backend PFS path list */
	uint32_t pfs_cnt;		/* Number of entries in pfs */
	bool metadata_acceleration;	/* Enable metadata acceleration */
	uint32_t *bb_group_ids;		/* Cache group IDs for this job */
	uint32_t *bb_dataset_ids;	/* Dataset IDs for this job */
	uint32_t *bb_task_ids;		/* Backend task IDs for this job */
	uint32_t index_groups;		/* Count of cache groups (bb_group_ids) */
	uint32_t index_datasets;	/* Count of datasets (bb_dataset_ids) */
	uint32_t index_tasks;		/* Count of tasks (bb_task_ids) */
	bool bb_create_finished;	/* Cache group provisioning completed */
#endif
} bb_alloc_t;

/* User's storage use, needed to enforce per-user limits without TRES */
#define BB_USER_MAGIC		0xDEAD3493
typedef struct bb_user {
	uint32_t magic;
	struct bb_user *next;
	uint64_t size;
	uint32_t user_id;
} bb_user_t;

#define BB_FLAG_BB_OP		1	/* Requested using #BB prefix */
#define BB_FLAG_DW_OP		2	/* Requested using #DW prefix */
#ifdef __METASTACK_NEW_BURSTBUFFER
#define BB_FLAG_PB_OP		3	/* Requested using #DW prefix */
#endif

/* Burst buffer creation records with state */
typedef struct {
	char    *access;	/* Buffer access */
	uint32_t flags;		/* See BB_FLAG_* above */
	bool     create;	/* Set if buffer create requested */
	bool     destroy;	/* Set if buffer destroy requested */
	bool     hurry;		/* Fast buffer destroy */
	char    *name;		/* Buffer name, non-numeric for persistent */
	char    *pool;		/* Pool in which to create buffer */
	uint64_t size;		/* Buffer size in bytes */
	uint16_t state;		/* Buffer state, see BB_STATE_* in slurm.h.in */
	char    *type;		/* Buffer type */
	bool     use;		/* Set if persistent buffer use requested */
} bb_buf_t;

/* Burst buffer resources required for a job, based upon a job record's
 * burst_buffer string field */
#define BB_JOB_MAGIC		0xDEAD3412
typedef struct bb_job {
	char      *account;	/* Associated account (for limits) */
	uint32_t   buf_cnt;	/* Number of records in buf_ptr */
	bb_buf_t  *buf_ptr;	/* Buffer creation records */
	uint32_t   job_id;
	char      *job_pool;	/* Pool in which to create job buffers */
	uint32_t   magic;
	int memfd;		/* memfd descriptor for symbol-replaced
				 * burst-buffer script */
	char *memfd_path;	/* path to memfd file */
	bool need_symbol_replacement; /* '%' characters found in script */
	struct bb_job *next;
	char      *partition;	/* Associated partition (for limits) */
	uint64_t   persist_add;	/* Persistent buffer space job adds, bytes */
	char      *qos;	 	/* Associated QOS (for limits) */
	int        retry_cnt;	/* Count of attempted retries */
	uint64_t   req_size;	/* Bytes requested by job (excludes
				 * persistent buffers) */
	int        state;	/* job state with respect to burst buffers,
				 * See BB_STATE_* in slurm.h.in */
	uint32_t   swap_size;	/* swap space required per node in GB */
	uint32_t   swap_nodes;	/* Number of nodes needed */
	uint64_t   total_size;	/* Total bytes required for job (excludes
				 * persistent buffers, rounded up from
				 * req_size) */
	bool       use_job_buf;	/* True if uses job buffer,
				 * false if uses persistent buffer only */
	uint32_t   user_id;	/* user the job runs as */
#ifdef __METASTACK_NEW_BURSTBUFFER
	uint32_t type;			/* 0=temporary, 1=persistent cache group */
	bool  enforce_bb_flag;		/* Require burst buffer (enforce acceleration) */
	uint32_t bb_task;		/* Max concurrent burst buffer tasks on this group */
	uint32_t groups_nodes;		/* Nodes participating in this cache group */
	uint64_t req_space;		/* Burst buffer space requested for the job */
	uint32_t access_mode;		/* 0=shared/striped, 1=private/local */
	char *pfs;			/* Backend PFS path list */
	uint32_t pfs_cnt;		/* Number of entries in pfs */
	bool metadata_acceleration;	/* Enable metadata acceleration */
	/* char *default_workdir;	 default work directory (reserved) */
	uint32_t *bb_group_ids;		/* Cache group IDs for this job */
	uint32_t *bb_dataset_ids;	/* Dataset IDs for this job */
	uint32_t *bb_task_ids;		/* Backend task IDs for this job */
	uint32_t index_groups;		/* Count of cache groups (bb_group_ids) */
	uint32_t index_datasets;	/* Count of datasets (bb_dataset_ids) */
	uint32_t index_tasks;		/* Count of tasks (bb_task_ids) */
	bool bb_create_finished;	/* Cache group provisioning completed */
	uint32_t parastor_inx;		/* Backend storage index (1=first system, 2=second, ...) */
#endif
} bb_job_t;

/* Used for building queue of jobs records for various purposes */
typedef struct bb_job_queue_rec {
	bb_job_t *bb_job;
	job_record_t *job_ptr;
} bb_job_queue_rec_t;

/* Used for building queue of job preemption candidates */
struct preempt_bb_recs {
	bb_alloc_t *bb_ptr;
	uint32_t job_id;
	char *pool;
	uint64_t size;
	time_t   use_time;
	uint32_t user_id;
};

#ifdef __METASTACK_NEW_BURSTBUFFER

/* Subset of burst buffer config for lightweight RPC / client use */
typedef struct bb_minimal_config {
	char    *para_stor_addr;	/* IP address */
	uint32_t para_stor_port;	/* port number */
	char    *para_stor_user_name;	/* user name */
	char    *para_stor_password;	/* password */
	char    *token;			/* API or session authentication token */
	uint32_t	other_timeout;		/* General operation timeout (seconds) */
	uint32_t	stagein_timeout;	/* Stage-in timeout (seconds) */
	uint32_t	stageout_timeout;	/* Stage-out timeout (seconds) */
	uint32_t	poll_interval;		/* Poll interval (seconds) */
	uint32_t	retry_count;		/* Retries after timeout on backend calls */
} bb_minimal_config_t;

typedef enum {
	BURST_BUFFER_TASK_TYPE_NULL = 0,   /* Unused; omit task type */
	BURST_BUFFER_TASK_TYPE_PREFETCH,   /* Warm-up / prefetch */
	BURST_BUFFER_TASK_TYPE_RECYCLE	/* Reclaim / recycle */
} bb_task_type;

typedef enum {
	BB_TASK_STATE_NULL = 0,	    /* Unused; omit task state */
	BB_TASK_STATE_SUBMITTING,
	BB_TASK_STATE_RUNNING,
	BB_TASK_STATE_COMPLETED,
	BB_TASK_STATE_FAILED,
	BB_TASK_STATE_CANCELED
} bb_task_state_type;

/* Cache group attributes (backend / Parastor API shape) */
typedef struct {
	uint32_t id;			/* Cache group ID */
	uint32_t client_num;		/* Number of clients */
	uint32_t *client_ids;		/* Client ID list */
	time_t del_delay_time;		/* Deletion deferral time */
	time_t fault_delay_time;	/* Fault-handling delay */
	double hit_bytes_rate;		/* Byte hit ratio */
	double hit_io_num_rate;		/* I/O operation hit ratio */
	double meta_hit_io_num_rate;	/* Metadata I/O hit ratio */
	char *group_sn;			/* Cache group serial / unique name */
} bb_attribute_group;

/* Dataset fields in backend JSON response */
typedef struct {
    char *burstBufferDataSetCacheMode;
    char *burstBufferDataSetCacheType;
    char *burstBufferMetaDataSetCacheMode;
    time_t create_time;
    bool dataOpen;
    char *data_cache_mode;
    char *data_cache_type;
    uint32_t fs_id;
    uint32_t group_id;
    uint32_t id;
    char *idesc;
    int key;
    time_t last_submit_task_time;
    char *last_submit_task_type;
    bool lock_flag;
    char *meta_data_cache_mode;
    char *path;
    int path_version;
    char *state;
    bool use_data;
    bool use_meta_data;
    int version;
} bb_attribute_dataset;

/* Client (node) attributes from backend */
typedef struct {
	uint32_t id;			/* Client ID */
	char *hostname;
	char *ip;
	uint32_t total_size;		/* bytes */
	uint32_t used_size;		/* bytes */
	int version;
	int ips_count;
	uint32_t devices_count;
	uint32_t groups_count;		/* Cache groups this node has joined */
	uint32_t *groups_ids;
	char *cap_dev_name;
} bb_attribute_client;

/* Task record from backend (prefetch / recycle, etc.) */
typedef struct {
	uint32_t task_id;
	uint32_t dataset_id;
	uint32_t group_id;
	bb_task_type task_type;
	bb_task_state_type task_state;
	time_t begin_time;
	time_t end_time;
	time_t completed_bytes;
	uint32_t total_node_num;
	uint32_t completed_node_num;
	uint32_t canceled_node_num;
	char *error_action_type;
	uint32_t failed_node_num;
	char **failed_node_infos;
	int exit_code;
} bb_attribute_task;

/*
 * Aggregated backend response (bb_response)
 *
 * Common envelope:
 *   err_no           0 = success, non-zero = failure
 *   sync             Synchronization flag (reserved / unused)
 *   time_stamp       Timestamp (milliseconds)
 *   time_zone_offset Offset from UTC (minutes)
 *   trace_id         Request correlation ID
 *   err_msg          Short error summary
 *   detail_err_msg   Verbose error text
 *
 * Create cache group:
 *   group_id         Assigned cache group ID on success
 *
 * Create dataset:
 *   dataset_id       Assigned dataset ID on success
 */

typedef struct {
    int err_no;
    int sync;
    time_t time_stamp;
    time_t time_zone_offset;
    char* trace_id;
    char* err_msg;
    char* detail_err_msg;

    uint32_t group_count;      
    uint32_t dataset_count;   
    uint32_t client_count;
    uint32_t task_count;

	uint32_t client_join_groups_counts;

    uint32_t group_id;
    uint32_t dataset_id; 
    uint32_t task_id;
	uint32_t last_client_id;	/* Client ID for this node in the response */
	bb_attribute_group   *bb_group;
	bb_attribute_client  *bb_client;
	bb_attribute_dataset *bb_dataset;
	bb_attribute_task    *bb_task;
} bb_response;
#endif

/* Current plugin state information */
typedef struct bb_state {
	bb_config_t	bb_config;
	bb_alloc_t **	bb_ahash;	/* Allocation buffers, hash by job_id */
	bb_job_t **	bb_jhash;	/* Job state, hash by job_id */
	bb_user_t **	bb_uhash;	/* User limit, hash by user_id */
	pthread_mutex_t	bb_mutex;
	pthread_t	bb_thread;
	time_t		last_load_time;
	char *		name;		/* Plugin name */
	time_t		next_end_time;
	time_t		last_update_time;
	uint64_t	persist_resv_sz; /* Space reserved for persistent buffers */
	List		persist_resv_rec;/* List of bb_pend_persist_t records */
	pthread_cond_t	term_cond;
	bool		term_flag;
	pthread_mutex_t	term_mutex;
	uint64_t	total_space;	/* units are bytes */
	int		tres_id;	/* TRES ID, for limits */
	int		tres_pos;	/* TRES index, for limits */
	uint64_t	used_space;	/* Allocated space, in bytes */
	uint64_t	unfree_space;	/* Includes alloc_space (above) plus
					 * drained, units are bytes */
#ifdef __METASTACK_NEW_BURSTBUFFER	
	uint32_t used_groups_cnt;
	uint32_t free_groups_cnt;
	uint32_t used_datasets_cnt;
	uint32_t free_datasets_cnt;
#endif
} bb_state_t;

/* Return codes for bb_test_size_limit */
enum {
	BB_CAN_START_NOW = 0,
	BB_EXCEEDS_LIMITS,
	BB_NOT_ENOUGH_RESOURCES,
};

/* Allocate burst buffer hash tables */
extern void bb_alloc_cache(bb_state_t *state_ptr);

/* Allocate a per-job burst buffer record for a specific job.
 * Return a pointer to that record.
 * Use bb_free_alloc_buf() to purge the returned record. */
extern bb_alloc_t *bb_alloc_job_rec(bb_state_t *state_ptr,
				    job_record_t *job_ptr,
				    bb_job_t *bb_job);

#ifdef __METASTACK_NEW_BURSTBUFFER
/* Update a per-job burst buffer record for a specific job.
 * Return a pointer to that record.
 * Use bb_free_alloc_buf() to purge the returned record. */	 
extern void alter_bb_alloc_job_rec(bb_alloc_t *bb_alloc, bb_job_t *bb_job, bool update);
#endif
/* Allocate a burst buffer record for a job and increase the job priority
 * if so configured.
 * Use bb_free_alloc_buf() to purge the returned record. */
extern bb_alloc_t *bb_alloc_job(bb_state_t *state_ptr, job_record_t *job_ptr,
				bb_job_t *bb_job);

/* Allocate a named burst buffer record for a specific user.
 * Return a pointer to that record.
 * Use bb_free_alloc_buf() to purge the returned record. */
extern bb_alloc_t *bb_alloc_name_rec(bb_state_t *state_ptr, char *name,
				     uint32_t user_id);

/*
 * For interactive jobs, build a script containing the burst buffer commands.
 *
 * Return SLURM_SUCCESS if it succeeded or SLURM_ERROR if it failed.
 */
extern int bb_build_bb_script(job_record_t *job_ptr, char *script_file);

/*
 * Create job script based on het job offsets
 *
 * Offset 0 - prepend burst buffer directives w/#EXCLUDED for offsets > 0
 * Offset > 0 - remove all directives that are not current component
 */
extern char *bb_common_build_het_job_script(char *script,
					    uint32_t het_job_offset,
					    bool (*is_directive) (char *tok));

/* Clear all cached burst buffer records, freeing all memory. */
extern void bb_clear_cache(bb_state_t *state_ptr);

/* Clear configuration parameters, free memory
 * config_ptr IN - Initial configuration to be cleared
 * fini IN - True if shutting down, do more complete clean-up */
extern void bb_clear_config(bb_config_t *config_ptr, bool fini);

/* Find a per-job burst buffer record for a specific job.
 * If not found, return NULL. */
extern bb_alloc_t *bb_find_alloc_rec(bb_state_t *state_ptr,
				     job_record_t *job_ptr);

/* Find a burst buffer record by name
 * bb_name IN - Buffer's name
 * user_id IN - Possible user ID, advisory use only
 * RET the buffer or NULL if not found */
extern bb_alloc_t *bb_find_name_rec(char *bb_name, uint32_t user_id,
				    bb_state_t *state_ptr);

/* Find a per-user burst buffer record for a specific user ID */
extern bb_user_t *bb_find_user_rec(uint32_t user_id, bb_state_t *state_ptr);

/* Remove a specific bb_alloc_t from global records.
 * RET true if found, false otherwise */
extern bool bb_free_alloc_rec(bb_state_t *state_ptr, bb_alloc_t *bb_ptr);

/* Free memory associated with allocated bb record, caller is responsible for
 * maintaining linked list */
extern void bb_free_alloc_buf(bb_alloc_t *bb_alloc);

/* Translate a burst buffer size specification in string form to numeric form,
 * recognizing various sufficies (MB, GB, TB, PB, and Nodes). Default units
 * are bytes. */
extern uint64_t bb_get_size_num(char *tok, uint64_t granularity);

/* Translate a burst buffer size specification in numeric form to string form,
 * recognizing various sufficies (KB, MB, GB, TB, PB, and Nodes). */
extern char *bb_get_size_str(uint64_t size);

/* Round up a number based upon some granularity */
extern uint64_t bb_granularity(uint64_t start_size, uint64_t granularity);

/* Allocate a bb_job_t record, hashed by job_id, delete with bb_job_del() */
extern bb_job_t *bb_job_alloc(bb_state_t *state_ptr, uint32_t job_id);

/* Delete a bb_job_t record, hashed by job_id */
extern void bb_job_del(bb_state_t *state_ptr, uint32_t job_id);

/* Return a pointer to the existing bb_job_t record for a given job_id or
 * NULL if not found */
extern bb_job_t *bb_job_find(bb_state_t *state_ptr, uint32_t job_id);

/* Log the contents of a bb_job_t record using "info()" */
extern void bb_job_log(bb_state_t *state_ptr, bb_job_t *bb_job);

extern void bb_job_queue_del(void *x);

/* Sort job queue by expected start time */
extern int bb_job_queue_sort(void *x, void *y);

/*
 * Returns the script, or a symbol-replaced version of the script,
 * that can be used as an argument to exec().
 */
char *bb_handle_job_script(job_record_t *job_ptr, bb_job_t *bb_job);

/* Load and process configuration parameters */
extern void bb_load_config(bb_state_t *state_ptr, char *plugin_type);

#ifdef __METASTACK_NEW_BURSTBUFFER
/* Load and process configuration parameters */
extern void bb_load_config2(bb_state_t *state_ptr, char *plugin_type);
#endif
/*
 * Open the state save file, or the backup if necessary.
 * IN file_name - the name of the state save file to open
 * OUT state_file - the name (including path) of the state save file
 * RET the file description to read from or error code
 */
extern int bb_open_state_file(const char *file_name, char **state_file);

/* Pack individual burst buffer records into a buffer */
extern int bb_pack_bufs(uid_t uid, bb_state_t *state_ptr, buf_t *buffer,
			uint16_t protocol_version);

/* Pack state and configuration parameters into a buffer */
extern void bb_pack_state(bb_state_t *state_ptr, buf_t *buffer,
			  uint16_t protocol_version);
#ifdef __METASTACK_NEW_BURSTBUFFER

/* Pack individual burst buffer job records into a buffer */
extern int bb_pack_job_bufs(uid_t uid, bb_state_t *state_ptr, buf_t *buffer,
			uint16_t protocol_version);
/* Pack state and configuration parameters into a buffer */
extern void bb_pack_state_parastor(bb_state_t *state_ptr, buf_t *buffer,
			  uint16_t protocol_version);
#endif
/* Pack individual burst buffer usage records into a buffer (used for limits) */
extern int bb_pack_usage(uid_t uid, bb_state_t *state_ptr, buf_t *buffer,
			 uint16_t protocol_version);

/* Sort preempt_bb_recs in order of DECREASING use_time */
extern int bb_preempt_queue_sort(void *x, void *y);

/*
 * Set state (integer) in bb_job and set the state (string) in job_ptr.
 * bb_job is used in burst buffer plugins. The string is used to display to the
 * user and to save the job's burst buffer state in StateSaveLocation.
 */
extern void bb_set_job_bb_state(job_record_t *job_ptr, bb_job_t *bb_job,
				int new_state);

/* Set the bb_state's tres_pos for limit enforcement.
 * Value is set to -1 if not found. */
extern void bb_set_tres_pos(bb_state_t *state_ptr);

/* For each burst buffer record, set the use_time to the time at which its
 * use is expected to begin (i.e. each job's expected start time) */
extern void bb_set_use_time(bb_state_t *state_ptr);

/* Sleep function, also handles termination signal */
extern void bb_sleep(bb_state_t *state_ptr, int add_secs);

/* Make claim against resource limit for a user
 * user_id IN - Owner of burst buffer
 * bb_size IN - Size of burst buffer
 * pool IN - Pool containing the burst buffer
 * state_ptr IN - Global state to update
 * update_pool_unfree IN - If true, update the pool's unfree space */
extern void bb_limit_add(uint32_t user_id, uint64_t bb_size, char *pool,
			 bb_state_t *state_ptr, bool update_pool_unfree);

/* Release claim against resource limit for a user */
extern void bb_limit_rem(uint32_t user_id, uint64_t bb_size, char *pool,
			 bb_state_t *state_ptr);

/* Log creation of a persistent burst buffer in the database
 * job_ptr IN - Point to job that created, could be NULL at startup
 * bb_alloc IN - Pointer to persistent burst buffer state info
 * state_ptr IN - Pointer to burst_buffer plugin state info
 */
extern int bb_post_persist_create(job_record_t *job_ptr, bb_alloc_t *bb_alloc,
				  bb_state_t *state_ptr);

/* Log deletion of a persistent burst buffer in the database */
extern int bb_post_persist_delete(bb_alloc_t *bb_alloc, bb_state_t *state_ptr);

/*
 * Test if a job can be allocated a burst buffer.
 * This may preempt currently active stage-in for higher priority jobs.
 *
 * RET BB_CAN_START_NOW: Job can be started now
 *     BB_EXCEEDS_LIMITS: Job exceeds configured limits, continue testing with
 *                        next job
 *     BB_NOT_ENOUGH_RESOURCES: Job needs more resources than currently
 *                              available can not start, skip all remaining jobs
 */
extern int bb_test_size_limit(job_record_t *job_ptr, bb_job_t *bb_job,
			      bb_state_t *bb_state_ptr,
			      void (*preempt_func) (uint32_t job_id,
						    uint32_t user_id,
						    bool hurry) );

/* Update "system_comment" in a job record. */
extern void bb_update_system_comment(job_record_t *job_ptr, char *operation,
				     char *resp_msg, bool update_database);

/* Determine if the specified pool name is valid on this system */
extern bool bb_valid_pool_test(bb_state_t *state_ptr, char *pool_name);

#ifdef __METASTACK_NEW_BURSTBUFFER
/* Determine if the specified pool name is valid on this system */
extern bool bb_valid_groups_test(uint64_t tmp_cnt);

extern bool bb_valid_groups_test_2(bb_job_t *bb_job, bb_state_t *state_ptr);
#endif

/* Write an arbitrary string to an arbitrary file name */
extern int bb_write_file(char *file_name, char *buf);

/*
 * Write a string representing the node IDs of a job's nodes to an arbitrary
 * file location.
 * RET 0 or Slurm error code
 */
extern int bb_write_nid_file(char *file_name, char *node_list,
			     job_record_t *job_ptr);
/*
 * Save buffer to state file
 * IN old_file - state file name with ".old" appended
 * IN reg_file - state file name
 * IN new_file - state file name with ".new" appended
 * IN plugin - name of plugin, just used for debugging
 * IN buffer - write this data to the file
 * IN buffer_size - size of buffer to create
 * IN save_time - timestamp when state saving began
 * OUT last_save_time - set to save_time if writing to the state save file
 *                      succeeds
 */
extern void bb_write_state_file(char* old_file, char *reg_file, char *new_file,
				const char *plugin, buf_t *buffer,
				int buffer_size, time_t save_time,
				time_t *last_save_time);

#endif	/* __BURST_BUFFER_COMMON_H__ */
