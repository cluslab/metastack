/*****************************************************************************\
 *  burst_buffer_parastorbb.c - Plugin for managing burst buffers with parastor
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

#define _GNU_SOURCE

#include <ctype.h>
#include <curl/curl.h>
#include <stdlib.h>
#include <unistd.h>

#include "slurm/slurm.h"

#include "src/common/assoc_mgr.h"
#include "src/common/data.h"
#include "src/common/fd.h"
#include "src/common/run_command.h"
#include "src/common/slurm_protocol_pack.h"
#include "src/common/xsignal.h"
#include "src/common/xstring.h"
#include "src/interfaces/serializer.h"
// #include "src/parastor/slurm_parastor.h"
#include "src/slurmctld/agent.h"
#include "src/slurmctld/job_scheduler.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/node_scheduler.h"
#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/slurmscriptd.h"
#include "src/slurmctld/trigger_mgr.h"
#include "src/plugins/burst_buffer/common/burst_buffer_common.h"

#include "bb_curl_wrapper.h"
#include "bb_api.h"
/* Script directive */
#define DEFAULT_DIRECTIVE_STR "PB"
/* Script line types */
#define LINE_OTHER 0
#define LINE_BB    1
#define LINE_DW    2
#define LINE_PB    3
//define GROUP_SIZE 100 //设置缓存组包含的最大节点数量
/* Hold job if pre_run fails more times than MAX_RETRY_CNT */
#define MAX_RETRY_CNT 2
/* Used for the polling hooks "test_data_{in|out}" */
#define SLURM_BB_BUSY "BUSY"
#define GROUP_TYPE_TEMPORARY 0  /* 作业申请缓存组类型，0：临时 */
#define GROUP_TYPE_PERSISTENT 1 /* 作业申请缓存组类型，1：持久 */
#define DATASET_TYPE_STRIPED 0 /* 数据集加速类型，0:共享方式 */
#define DATASET_TYPE_PRIVATE 1 /* 数据集加速类型，1:本地方式 */
/*
 * Limit the number of burst buffers APIs allowed to run in parallel so that we
 * don't exceed process or system resource limits (such as number of processes
 * or max open files) when we run scripts through slurmscriptd. We limit this
 * per "stage" (stage in, pre run, stage out, teardown) so that if we hit the
 * maximum in stage in (for example) we won't block all jobs from completing.
 * We also do this so that if 1000+ jobs complete or get cancelled all at
 * once they won't all run teardown at the same time.
 */
#define MAX_BURST_BUFFERS_PER_STAGE 128

/*
 * These variables are required by the burst buffer plugin interface.  If they
 * are not found in the plugin, the plugin loader will ignore it.
 *
 * plugin_name - a string giving a human-readable description of the
 * plugin.  There is no maximum length, but the symbol must refer to
 * a valid string.
 *
 * plugin_type - a string suggesting the type of the plugin or its
 * applicability to a particular form of data or method of data handling.
 * If the low-level plugin API is used, the contents of this string are
 * unimportant and may be anything.  Slurm uses the higher-level plugin
 * interface which requires this string to be of the form
 *
 *      <application>/<method>
 *
 * where <application> is a description of the intended application of
 * the plugin (e.g., "burst_buffer" for Slurm burst_buffer) and <method> is a
 * description of how this plugin satisfies that application.  Slurm will only
 * load a burst_buffer plugin if the plugin_type string has a prefix of
 * "burst_buffer/".
 *
 * plugin_version - an unsigned 32-bit integer containing the Slurm version
 * (major.minor.micro combined into a single number).
 */
const char plugin_name[]        = "burst_buffer parastor plugin";
const char plugin_type[]        = "burst_buffer/parastor";
const uint32_t plugin_version   = SLURM_VERSION_NUMBER;

/*
 * Most state information is in a common structure so that we can more
 * easily use common functions from multiple burst buffer plugins.
 */
static bb_state_t bb_state;
static bool bb_state_init = false;
static char *directive_str;
static int directive_len = 0;

static char *parastor_script_path;
static const char *req_fxns[] = {
	"slurm_bb_job_process",
	"slurm_bb_pools",
	"slurm_bb_job_teardown",
	"slurm_bb_setup",
	"slurm_bb_data_in",
	"slurm_bb_test_data_in",
	"slurm_bb_real_size",
	"slurm_bb_paths",
	"slurm_bb_pre_run",
	"slurm_bb_post_run",
	"slurm_bb_data_out",
	"slurm_bb_test_data_out",
	"slurm_bb_get_status",
	NULL
};

/* Keep this in sync with req_fxns */
typedef enum {
	SLURM_BB_JOB_PROCESS = 0,
	SLURM_BB_POOLS,
	SLURM_BB_JOB_TEARDOWN,
	SLURM_BB_SETUP,
	SLURM_BB_DATA_IN,
	SLURM_BB_TEST_DATA_IN,
	SLURM_BB_REAL_SIZE,
	SLURM_BB_PATHS,
	SLURM_BB_PRE_RUN,
	SLURM_BB_POST_RUN,
	SLURM_BB_DATA_OUT,
	SLURM_BB_TEST_DATA_OUT,
	SLURM_BB_GET_STATUS,
	SLURM_BB_OP_MAX
} bb_op_e;

typedef struct {
	int i;
	int groups_pools;
	bb_groups_job_t *pools;
} data_pools_arg_t;

typedef struct {
	uint64_t bb_size;
	uint32_t gid;
	bool hurry;
	uint32_t job_id;
	char *job_script;
	char *pool;
	uint32_t uid;
} stage_args_t;

typedef struct {
	uint32_t argc;
	char **argv;
} status_args_t;

typedef struct {
	uint32_t argc;
	char **argv;
	bool get_job_ptr;
	bool have_job_lock;
	uint32_t job_id;
	job_record_t *job_ptr;
	const char *lua_func;
	char **resp_msg;
	uint32_t timeout;
	bool *track_script_signal;
	bool with_scriptd;
} run_lua_args_t;

typedef struct {
	char   **args;
	uint32_t job_id;
	uint32_t timeout;
	uint32_t user_id;
	int bb_node_cnt;
} pre_run_bb_args_t;

typedef void (*init_argv_f_t)(stage_args_t *stage_args, int *argc_p,
			      char ***argv_p);
typedef struct {
	init_argv_f_t init_argv;
	bb_op_e op_type;
	int (*run_func)(stage_args_t *stage_args,
			init_argv_f_t init_argv,
			const char *op, uint32_t job_id, uint32_t timeout,
			char **resp_msg);
	uint32_t timeout;
} bb_func_t;

static uint32_t		last_persistent_id = 1;

static int parastor_thread_cnt = 0;
pthread_mutex_t parastor_thread_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Function prototypes */
static bb_job_t *_get_bb_job(job_record_t *job_ptr);
static void _queue_teardown(bb_job_t *bb_job);
// static void _fail_stage(stage_args_t *stage_args, const char *op, int rc, char *resp_msg);
static void _init_data_in_argv(stage_args_t *stage_args, int *argc_p, char ***argv_p);
static int _bb_get_parastors_state(void);
static void *_start_stage_out(void *x);
static void *_start_teardown(void *x);
static int _calibrate_task_state(uint32_t job_id, int *bb_task_ids, int index_tasks, bb_minimal_config_t *bb_min_config);
static bb_minimal_config_t *_create_bb_min_config(bb_config_t *bb_config);
static void _bb_min_config_free(bb_minimal_config_t * config);
static void _cleanup_bb_resources_from_alloc(void *x);
static void _clean_by_bb_alloc(bb_alloc_t *bb_alloc);
static int _get_parastor_thread_cnt(void)
{
	int cnt;

	slurm_mutex_lock(&parastor_thread_mutex);
	cnt = parastor_thread_cnt;
	slurm_mutex_unlock(&parastor_thread_mutex);

	return cnt;
}

static void _incr_parastor_thread_cnt(void)
{
	slurm_mutex_lock(&parastor_thread_mutex);
	parastor_thread_cnt++;
	slurm_mutex_unlock(&parastor_thread_mutex);
}

static void _decr_parastor_thread_cnt(void)
{
	slurm_mutex_lock(&parastor_thread_mutex);
	parastor_thread_cnt--;
	slurm_mutex_unlock(&parastor_thread_mutex);
}

/* Write current burst buffer state to a file so that we can preserve account,
 * partition, and QOS information of persistent burst buffers as there is no
 * place to store that information within the DataWarp data structures */
static void _save_bb_state(void)
{
	static time_t last_save_time = 0;
	static int high_buffer_size = 16 * 1024;
	time_t save_time = time(NULL);
	bb_alloc_t *bb_alloc;
	uint32_t rec_count = 0;
	buf_t *buffer;
	char *old_file = NULL, *new_file = NULL, *reg_file = NULL;
	int i, count_offset, offset;
	uint16_t protocol_version = SLURM_PROTOCOL_VERSION;

	if ((bb_state.last_update_time <= last_save_time) &&
	    !bb_state.term_flag)
		return;

	/* Build buffer with name/account/partition/qos information for all
	 * named burst buffers so we can preserve limits across restarts */
	buffer = init_buf(high_buffer_size);
	pack16(protocol_version, buffer);
	count_offset = get_buf_offset(buffer);
	pack32(rec_count, buffer);
	if (bb_state.bb_ahash) {
		slurm_mutex_lock(&bb_state.bb_mutex);
		for (i = 0; i < BB_HASH_SIZE; i++) {
			bb_alloc = bb_state.bb_ahash[i];
			while (bb_alloc) {
				if (bb_alloc->name) {
					packstr(bb_alloc->account,	buffer);
					pack_time(bb_alloc->create_time,buffer);
					pack32(bb_alloc->id,		buffer);
					packstr(bb_alloc->name,		buffer);
					packstr(bb_alloc->partition,	buffer);
					packstr(bb_alloc->pool,		buffer);
					packstr(bb_alloc->qos,		buffer);
					pack32(bb_alloc->user_id,	buffer);
#ifdef __METASTACK_NEW_BURSTBUFFER
					debug("BB-----save parastor bb state");
					pack16(bb_alloc->state,		buffer);
					//pack64(bb_alloc->bb_group_id, buffer);
					pack32(bb_alloc->type, buffer);
					packbool(bb_alloc->cache_tmp, buffer);
					pack32(bb_alloc->bb_task, buffer);
					pack32(bb_alloc->groups_nodes, buffer);
					pack64(bb_alloc->req_space, buffer);
					pack32(bb_alloc->access_mode, buffer);
					packstr(bb_alloc->pfs, buffer);
					pack32(bb_alloc->pfs_cnt, buffer);
					packbool(bb_alloc->metadata_acceleration, buffer);
					/* Save bb_group_ids array */
					pack32(bb_alloc->index_groups, buffer);
					if (bb_alloc->bb_group_ids && bb_alloc->index_groups > 0) {
						for (int j = 0; j < bb_alloc->index_groups; j++) {
							pack32(bb_alloc->bb_group_ids[j], buffer);
						}
					}
					/* Save bb_dataset_ids array */
					pack32(bb_alloc->index_datasets, buffer);
					if (bb_alloc->bb_dataset_ids && bb_alloc->index_datasets > 0) {
						for (int j = 0; j < bb_alloc->index_datasets; j++) {
							pack32(bb_alloc->bb_dataset_ids[j], buffer);
						}
					}
					/* Save bb_task_ids array */
					pack32(bb_alloc->index_tasks, buffer);
					if (bb_alloc->bb_task_ids && bb_alloc->index_tasks > 0) {
						for (int j = 0; j < bb_alloc->index_tasks; j++) {
							pack32(bb_alloc->bb_task_ids[j], buffer);
						}
					}
#endif
					rec_count++;
				}
				bb_alloc = bb_alloc->next;
			}
		}
		save_time = time(NULL);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		offset = get_buf_offset(buffer);
		set_buf_offset(buffer, count_offset);
		pack32(rec_count, buffer);
		set_buf_offset(buffer, offset);
	}

	xstrfmtcat(old_file, "%s/%s", slurm_conf.state_save_location,
	           "burst_buffer_parastor_state.old");
	xstrfmtcat(reg_file, "%s/%s", slurm_conf.state_save_location,
	           "burst_buffer_parastor_state");
	xstrfmtcat(new_file, "%s/%s", slurm_conf.state_save_location,
	           "burst_buffer_parastor_state.new");

	bb_write_state_file(old_file, reg_file, new_file, "burst_buffer_parastor",
			    buffer, high_buffer_size, save_time,
			    &last_save_time);

	xfree(old_file);
	xfree(reg_file);
	xfree(new_file);
	FREE_NULL_BUFFER(buffer);
}

static void _recover_bb_state(void)
{
	char *state_file = NULL, *data = NULL;
	int data_allocated, data_read = 0;
	uint16_t protocol_version = NO_VAL16;
	uint32_t data_size = 0, rec_count = 0, name_len = 0;
	uint32_t id = 0, user_id = 0;
	int i, state_fd;
	char *account = NULL, *name = NULL;
	char *partition = NULL, *pool = NULL, *qos = NULL;
	char *end_ptr = NULL;
	time_t create_time = 0;
	bb_alloc_t *bb_alloc;
	buf_t *buffer;

	state_fd = bb_open_state_file("burst_buffer_parastor_state", &state_file);
	if (state_fd < 0) {
		info("No burst buffer state file (%s) to recover",
		     state_file);
		xfree(state_file);
		return;
	}
	data_allocated = BUF_SIZE;
	data = xmalloc(data_allocated);
	while (1) {
		data_read = read(state_fd, &data[data_size], BUF_SIZE);
		if (data_read < 0) {
			if  (errno == EINTR)
				continue;
			else {
				error("Read error on %s: %m", state_file);
				break;
			}
		} else if (data_read == 0)     /* eof */
			break;
		data_size      += data_read;
		data_allocated += data_read;
		xrealloc(data, data_allocated);
	}
	close(state_fd);
	xfree(state_file);

	buffer = create_buf(data, data_size);
	safe_unpack16(&protocol_version, buffer);
	if (protocol_version == NO_VAL16) {
		if (!ignore_state_errors)
			fatal("Can not recover burst_buffer/parastor state, data version incompatible, start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.");
		error("**********************************************************************");
		error("Can not recover burst_buffer/parastor state, data version incompatible");
		error("**********************************************************************");
		return;
	}

	safe_unpack32(&rec_count, buffer);
	for (i = 0; i < rec_count; i++) {
		if (protocol_version >= SLURM_MIN_PROTOCOL_VERSION) {
			safe_unpackstr_xmalloc(&account,   &name_len, buffer);
			safe_unpack_time(&create_time, buffer);
			safe_unpack32(&id, buffer);
			safe_unpackstr_xmalloc(&name,      &name_len, buffer);
			safe_unpackstr_xmalloc(&partition, &name_len, buffer);
			safe_unpackstr_xmalloc(&pool,      &name_len, buffer);
			safe_unpackstr_xmalloc(&qos,       &name_len, buffer);
			safe_unpack32(&user_id, buffer);
		}

		slurm_mutex_lock(&bb_state.bb_mutex);
		/* Always create a new record for parastorbb */
		bb_alloc = bb_alloc_name_rec(&bb_state, name, user_id);
		bb_alloc->id = id;
		last_persistent_id = MAX(last_persistent_id, id);
		if (name && (name[0] >='0') && (name[0] <='9')) {
			bb_alloc->job_id = strtol(name, &end_ptr, 10);
			bb_alloc->array_job_id = bb_alloc->job_id;
			bb_alloc->array_task_id = NO_VAL;
		}
		bb_alloc->seen_time = time(NULL);
		log_flag(BURST_BUF, "Recovered burst buffer %s from user %u",
			 bb_alloc->name, bb_alloc->user_id);
		bb_alloc->account = account;
		account = NULL;
		bb_alloc->create_time = create_time;
		bb_alloc->partition = partition;
		partition = NULL;
		bb_alloc->pool = pool;
		pool = NULL;
		bb_alloc->qos = qos;
		qos = NULL;
#ifdef __METASTACK_NEW_BURSTBUFFER
		debug("BB-----recover BB state");
		/* Recover parastorbb specific fields */
		uint16_t state = 0;
		//uint64_t bb_group_id = 0;
		uint32_t type = 0;
		bool cache_tmp = false;
		uint32_t bb_task = 0;
		uint32_t groups_nodes = 0;
		uint64_t req_space = 0;
		uint32_t access_mode = 0;
		char *pfs = NULL;
		uint32_t pfs_cnt = 0;
		// char *bb_state_str = NULL;
		bool metadata_acceleration = false;
		uint32_t index_groups = 0;
		uint32_t index_datasets = 0;
		uint32_t index_tasks = 0;
		int j;

		safe_unpack16(&state, buffer);
		//safe_unpack64(&bb_group_id, buffer);
		safe_unpack32(&type, buffer);
		safe_unpackbool(&cache_tmp, buffer);
		safe_unpack32(&bb_task, buffer);
		safe_unpack32(&groups_nodes, buffer);
		safe_unpack64(&req_space, buffer);
		safe_unpack32(&access_mode, buffer);
		safe_unpackstr_xmalloc(&pfs, &name_len, buffer);
		safe_unpack32(&pfs_cnt, buffer);
		safe_unpackbool(&metadata_acceleration, buffer);

		/* Recover bb_group_ids array */
		safe_unpack32(&index_groups, buffer);
		xfree(bb_alloc->bb_group_ids);
		if (index_groups > 0) {
			bb_alloc->bb_group_ids = xmalloc(sizeof(int) * index_groups);
			for (j = 0; j < index_groups; j++) {
				uint32_t tmp_val;
				safe_unpack32(&tmp_val, buffer);
				bb_alloc->bb_group_ids[j] = (int)tmp_val;
			}
		} else {
			bb_alloc->bb_group_ids = NULL;
		}
		bb_alloc->index_groups = index_groups;

		/* Recover bb_dataset_ids array */
		safe_unpack32(&index_datasets, buffer);
		xfree(bb_alloc->bb_dataset_ids);
		if (index_datasets > 0) {
			bb_alloc->bb_dataset_ids = xmalloc(sizeof(int) * index_datasets);
			for (j = 0; j < index_datasets; j++) {
				uint32_t tmp_val;
				safe_unpack32(&tmp_val, buffer);
				bb_alloc->bb_dataset_ids[j] = (int)tmp_val;
			}
		} else {
			bb_alloc->bb_dataset_ids = NULL;
		}
		bb_alloc->index_datasets = index_datasets;

		/* Recover bb_task_ids array */
		safe_unpack32(&index_tasks, buffer);
		xfree(bb_alloc->bb_task_ids);
		if (index_tasks > 0) {
			bb_alloc->bb_task_ids = xmalloc(sizeof(int) * index_tasks);
			for (j = 0; j < index_tasks; j++) {
				uint32_t tmp_val;
				safe_unpack32(&tmp_val, buffer);
				bb_alloc->bb_task_ids[j] = (int)tmp_val;
			}
		} else {
			bb_alloc->bb_task_ids = NULL;
		}
		bb_alloc->index_tasks = index_tasks;

		/* Assign recovered values to bb_alloc */
		bb_alloc->state = state; 
		//bb_alloc->bb_group_id = bb_group_id;
		bb_alloc->type = type;
		bb_alloc->cache_tmp = cache_tmp;
		bb_alloc->bb_task = bb_task;
		bb_alloc->groups_nodes = groups_nodes;
		bb_alloc->req_space = req_space;
		bb_alloc->access_mode = access_mode;
		xfree(bb_alloc->pfs);
		bb_alloc->pfs = pfs;
		pfs = NULL;
		bb_alloc->pfs_cnt = pfs_cnt;
		bb_alloc->metadata_acceleration = metadata_acceleration;
#endif
		slurm_mutex_unlock(&bb_state.bb_mutex);
		xfree(name);
	}

	info("Recovered state of %d burst buffers", rec_count);
	
	FREE_NULL_BUFFER(buffer);
	return;

unpack_error:
	if (!ignore_state_errors)
		fatal("Incomplete burst buffer data checkpoint file, start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.");
	error("Incomplete burst buffer data checkpoint file");
	xfree(account);
	xfree(name);
	xfree(partition);
	xfree(qos);
	FREE_NULL_BUFFER(buffer);
	return;
}

/* For a given user/partition/account, set it's assoc_ptr */
static void _set_assoc_mgr_ptrs(bb_alloc_t *bb_alloc)
{
	/* read locks on assoc */
	assoc_mgr_lock_t assoc_locks =
		{ .assoc = READ_LOCK, .qos = READ_LOCK, .user = READ_LOCK };
#ifdef __METASTACK_ASSOC_HASH
	assoc_locks.uid = READ_LOCK;
#endif
	slurmdb_assoc_rec_t assoc_rec;
	slurmdb_qos_rec_t qos_rec;

	memset(&assoc_rec, 0, sizeof(slurmdb_assoc_rec_t));
	assoc_rec.acct      = bb_alloc->account;
	assoc_rec.partition = bb_alloc->partition;
	assoc_rec.uid       = bb_alloc->user_id;
	assoc_mgr_lock(&assoc_locks);
	if (assoc_mgr_fill_in_assoc(acct_db_conn, &assoc_rec,
				    accounting_enforce,
				    &bb_alloc->assoc_ptr,
				    true) == SLURM_SUCCESS) {
		xfree(bb_alloc->assocs);
		if (bb_alloc->assoc_ptr) {
			bb_alloc->assocs =
				xstrdup_printf(",%u,", bb_alloc->assoc_ptr->id);
		}
	}

	memset(&qos_rec, 0, sizeof(slurmdb_qos_rec_t));
	qos_rec.name = bb_alloc->qos;
	if (assoc_mgr_fill_in_qos(acct_db_conn, &qos_rec, accounting_enforce,
				  &bb_alloc->qos_ptr, true) != SLURM_SUCCESS)
		verbose("Invalid QOS name: %s",
			bb_alloc->qos);

	assoc_mgr_unlock(&assoc_locks);

}


static int _run_post_run(stage_args_t *stage_args, init_argv_f_t init_argv,
			 const char *op, uint32_t job_id, uint32_t timeout,
			 char **resp_msg)
{


	return SLURM_SUCCESS;
}

static int _run_test_data_inout(stage_args_t *stage_args,
				init_argv_f_t init_argv, const char *op,
				uint32_t job_id, uint32_t timeout,
				char **resp_msg)
{

	return SLURM_SUCCESS;
}
// TODO:待命中率进一步处理
/*  打印缓存组命中率（注意：必须在删除缓存组前(teardown)打印，否则查询不到） */
static void *_start_stage_out(void *x)
{
	bb_job_t *bb_job = (bb_job_t *)x;
	int rc = SLURM_SUCCESS;
	slurmctld_lock_t job_write_lock = { NO_LOCK, WRITE_LOCK, NO_LOCK, NO_LOCK, NO_LOCK };
	job_record_t *job_ptr = NULL;
	bb_alloc_t *bb_alloc = NULL;
	
	/* 拿min_config，并把需要查询到缓存组给临时变量以提前释放锁 */
	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_minimal_config_t *bb_min_config = _create_bb_min_config(&bb_state.bb_config);
	if (!bb_job->bb_group_ids || bb_job->index_groups < 1) {
		error("BB-----Job %u has no cache groups to query hit rate ", bb_job->job_id);
		slurm_mutex_lock(&bb_state.bb_mutex);
		return SLURM_ERROR;
	}
	uint32_t tmp_job_id = bb_job->job_id;
	int tmp_index_groups = bb_job->index_groups;
	int *tmp_groups_arr = xmalloc(sizeof(int) * tmp_index_groups);
	if (bb_job->bb_group_ids)
		memcpy(tmp_groups_arr, bb_job->bb_group_ids, sizeof(int) * tmp_index_groups);
	slurm_mutex_unlock(&bb_state.bb_mutex);

	DEF_TIMERS
	_incr_parastor_thread_cnt();
	track_script_rec_add(tmp_job_id, 0, pthread_self());
	START_TIMER;

	/* 设置查询参数，拼接缓存组ID */
	query_params_request query_params = { 0 };
	query_params.start = 0;
	query_params.limit = 100; /* FIXME:确定分页数后更改 */
	for (int i = 0; i < tmp_index_groups; i++) {
		if (query_params.ids) {
			xstrfmtcat(query_params.ids, ",%d", tmp_groups_arr[i]);
		} else {
			xstrfmtcat(query_params.ids, "%d", tmp_groups_arr[i]);
		}
	}
	bb_response *resp_out = xmalloc(sizeof(bb_response));
	/* 获取作业运行完成后的group状态 */
	List tmp_groups_list = get_groups_burst_buffer(&query_params, bb_min_config, resp_out);
	if ( !tmp_groups_list || !resp_out || resp_out->err_no != 0) {
		error("BB-----query cache group ids %s for JobId=%u failed, the retail message \" %s \" ",
			query_params.ids ? query_params.ids : "NULL",
			tmp_job_id,
			(resp_out && resp_out->detail_err_msg) ? resp_out->detail_err_msg : "unknown");
		rc = SLURM_ERROR;
	} else {
		/* 打印命中率 */
		for (int i = 0; i < tmp_index_groups; i++) {
			bb_attribute_group *bb_group = { 0 };
			bb_group = (bb_attribute_group *)list_find_first_ro(tmp_groups_list, _find_group_key, &tmp_groups_arr[i]);
			if (bb_group) {
				info("BB-----Job %u cache hit rate in cache group %d: hit_bytes_rate=%f, hit_io_num_rate=%f, meta_hit_io_num_rate=%f",
					tmp_job_id, bb_group->id,
					bb_group->hit_bytes_rate,
					bb_group->hit_io_num_rate,
					bb_group->meta_hit_io_num_rate);
				slurm_free_group(bb_group);
			} else {
				error("BB-----query cache group %d of job job %u is NULL ",
					tmp_groups_arr[i], tmp_job_id);
			}
		}
	}
	xfree(resp_out);
	xfree(query_params.ids);
	xfree(tmp_groups_arr);
	_bb_min_config_free(bb_min_config);
	END_TIMER;


	/* 更新作业与分配状态，并触发 teardown */
	lock_slurmctld(job_write_lock);
	job_ptr = find_job_record(bb_job->job_id);
	if (job_ptr) {
		if (rc != SLURM_SUCCESS) {
			job_ptr->state_reason = FAIL_BURST_BUFFER_OP;
			xfree(job_ptr->state_desc);
			xstrfmtcat(job_ptr->state_desc, "%s: stage-out cleanup failed", plugin_type);
#ifdef __METASTACK_OPT_CACHE_QUERY
			_add_job_state_to_queue(job_ptr);
#endif
		} else {
			job_state_unset_flag(job_ptr, JOB_STAGE_OUT);
			xfree(job_ptr->state_desc);
			last_job_update = time(NULL);
		}

		slurm_mutex_lock(&bb_state.bb_mutex);
		bb_alloc = bb_find_alloc_rec(&bb_state, job_ptr);
		if (bb_alloc) {
			if (rc == SLURM_SUCCESS) {
				bb_alloc->state = BB_STATE_TEARDOWN;
				bb_alloc->state_time = time(NULL);
			} else {
				bb_alloc->state = BB_STATE_STAGED_IN;
			}
		}
		_queue_teardown(bb_job);
		slurm_mutex_unlock(&bb_state.bb_mutex);
	}
	unlock_slurmctld(job_write_lock);

	track_script_remove(pthread_self());
	_decr_parastor_thread_cnt();
	return NULL;
}

static void _queue_stage_out(job_record_t *job_ptr, bb_job_t *bb_job)
{
	if (!bb_job) {
		debug("bb_job为空");
			return SLURM_ERROR;
	}
	slurm_thread_create_detached(_start_stage_out, bb_job);
	return SLURM_SUCCESS;
}

static void _pre_queue_stage_out(job_record_t *job_ptr, bb_job_t *bb_job)
{
	bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_POST_RUN);
	job_state_set_flag(job_ptr, JOB_STAGE_OUT);
	xfree(job_ptr->state_desc);
	xstrfmtcat(job_ptr->state_desc, "%s: Stage-out in progress",
		plugin_type);
	_queue_stage_out(job_ptr, bb_job);
#ifdef __METASTACK_OPT_CACHE_QUERY
	_add_job_state_to_queue(job_ptr);
#endif
}

static void _load_state(bool init_config)
{
	uint32_t timeout;

	timeout = bb_state.bb_config.other_timeout;

	// 不涉及pool概念
	// if (_load_pools(timeout) != SLURM_SUCCESS)
	// 	return;
	bb_state.last_load_time = time(NULL);
	
	if (!init_config)
		return;

	/* Load allocated burst buffers from state files. */
	_recover_bb_state();
	//_apply_limits();
	bb_state.last_update_time = time(NULL);

	return;
}

/* Perform periodic background activities */
static void *_bb_agent(void *args)
{
	log_flag(BURST_BUF, "start 	bb_agent");
	while (!bb_state.term_flag) {
		bb_sleep(&bb_state, AGENT_INTERVAL+30);
		if (!bb_state.term_flag) {
			_load_state(false);	/* Has own locking */
		}
		_save_bb_state();	/* Has own locks excluding file write */
	}

	/* Wait for lua threads to finish, then save state once more. */
	while (_get_parastor_thread_cnt())
		usleep(100000); /* 100 ms */
	////后续再打开
	_save_bb_state();

	return NULL;
}

/* Parse simple interactive burst_buffer options into an format identical to
 * burst_buffer options in a batch script file */
static int _xlate_interactive(job_desc_msg_t *job_desc)
{
	char *bb_copy = NULL, *capacity = NULL, *pfs = NULL, *type = NULL, *enforce_bb = NULL;
	char *end_ptr = NULL, *sep, *tok;
	uint64_t buf_size = 0;
	int i, rc = SLURM_SUCCESS, tok_len;

	if (!job_desc->burst_buffer || (job_desc->burst_buffer[0] == '#'))
		return rc;

	// if (strstr(job_desc->burst_buffer, "create_persistent") ||
	//     strstr(job_desc->burst_buffer, "destroy_persistent")) {
	// 	/* Create or destroy of persistent burst buffers NOT supported
	// 	 * via --bb option. Use --bbf or a batch script instead. */
	// 	return ESLURM_INVALID_BURST_BUFFER_REQUEST;
	// }


	if ((tok = strstr(bb_copy, "capacity="))) {
		buf_size = bb_get_size_num(tok + 9, 1);
		if (buf_size == 0) {
			rc = ESLURM_INVALID_BURST_BUFFER_REQUEST;
			goto fini;
		}
		capacity = xstrdup(tok + 9);
		sep = strchr(capacity, ',');
		if (sep)
			sep[0] = '\0';
		sep = strchr(capacity, ' ');
		if (sep)
			sep[0] = '\0';
		tok_len = strlen(capacity) + 9;
		memset(tok, ' ', tok_len);
	}


	if ((tok = strstr(bb_copy, "pfs="))) {
		pfs = xstrdup(tok + 4);
		sep = strchr(pfs, ',');
		if (sep)
			sep[0] = '\0';
		sep = strchr(pfs, ' ');
		if (sep)
			sep[0] = '\0';
		tok_len = strlen(pfs) + 4;
		memset(tok, ' ', tok_len);
	}

	if ((tok = strstr(bb_copy, "type="))) {
		type = xstrdup(tok + 5);
		sep = strchr(type, ',');
		if (sep)
			sep[0] = '\0';
		sep = strchr(type, ' ');
		if (sep)
			sep[0] = '\0';
		tok_len = strlen(type) + 5;
		memset(tok, ' ', tok_len);
	}

	if ((tok = strstr(bb_copy, "enforce_bb="))) {
		enforce_bb = xstrdup(tok + 11);
		sep = strchr(enforce_bb, ',');
		if (sep)
			sep[0] = '\0';
		sep = strchr(enforce_bb, ' ');
		if (sep)
			sep[0] = '\0';
		tok_len = strlen(enforce_bb) + 11;
		memset(tok, ' ', tok_len);
	}	

	if (rc == SLURM_SUCCESS) {
		/* Look for vestigial content. Treating this as an error would
		 * prevent backward compatibility. Just log it for now. */
		for (i = 0; bb_copy[i]; i++) {
			if (isspace(bb_copy[i]))
				continue;
			verbose("Unrecognized --bb content: %s",
				bb_copy + i);
//			rc = ESLURM_INVALID_BURST_BUFFER_REQUEST;
//			goto fini;
		}
	}

	if (rc == SLURM_SUCCESS)
		xfree(job_desc->burst_buffer);
	if ((rc == SLURM_SUCCESS) && (buf_size)) {

		if (buf_size) {
			if (job_desc->burst_buffer)
				xstrfmtcat(job_desc->burst_buffer, "\n");
			xstrfmtcat(job_desc->burst_buffer,
				   "#PB jobpara capacity=%s",
				   bb_get_size_str(buf_size));

			if (pfs) {
				xstrfmtcat(job_desc->burst_buffer,
					   " pfs=%s", pfs);
			}
			if (type) {
				xstrfmtcat(job_desc->burst_buffer,
					   " type=%s", type);
			}
			if (enforce_bb) {
				xstrfmtcat(job_desc->burst_buffer,
					   " enforce_bb=%s", enforce_bb);
			}
		}
	}

fini:	xfree(access);
	xfree(bb_copy);
	xfree(capacity);
	xfree(pfs);
	xfree(type);
	xfree(enforce_bb);
	return rc;
}

/*
 * Copy a batch job's burst_buffer options into a separate buffer.
 * Merge continued lines into a single line.
 */
static int _xlate_batch(job_desc_msg_t *job_desc)
{
	char *script, *save_ptr = NULL, *tok;
	int line_type, prev_type = LINE_OTHER;
	bool is_cont = false, has_space = false;
	int len, rc = SLURM_SUCCESS;

	/*
	 * Any command line --bb options get added to the script
	 */
	if (job_desc->burst_buffer) {
		rc = _xlate_interactive(job_desc);
		if (rc != SLURM_SUCCESS)
			return rc;
		run_command_add_to_script(&job_desc->script,
					  job_desc->burst_buffer);
		xfree(job_desc->burst_buffer);
	}

	script = xstrdup(job_desc->script);
	tok = strtok_r(script, "\n", &save_ptr);
	while (tok) {
		if (tok[0] != '#')
			break;	/* Quit at first non-comment */

	 if ((tok[1] == 'P') && (tok[2] == 'B'))
			line_type = LINE_PB;
		else
			line_type = LINE_OTHER;

		if (line_type == LINE_OTHER) {
			is_cont = false;
		} else {
			if (is_cont) {
				if (line_type != prev_type) {
					/*
					 * Mixing "#DW" with "#BB" on same
					 * (continued) line, error
					 */
					rc =ESLURM_INVALID_BURST_BUFFER_REQUEST;
					break;
				}
				tok += 3; 	/* Skip "#DW" or "#BB" */
				while (has_space && isspace(tok[0]))
					tok++;	/* Skip duplicate spaces */
			} else if (job_desc->burst_buffer) {
				xstrcat(job_desc->burst_buffer, "\n");
			}
			prev_type = line_type;

			len = strlen(tok);
			if (tok[len - 1] == '\\') {
				has_space = isspace(tok[len - 2]);
				tok[strlen(tok) - 1] = '\0';
				is_cont = true;
			} else {
				is_cont = false;
			}
			xstrcat(job_desc->burst_buffer, tok);
		}
		tok = strtok_r(NULL, "\n", &save_ptr);
	}
	xfree(script);
	if (rc != SLURM_SUCCESS)
		xfree(job_desc->burst_buffer);
	return rc;
}


/*
 * IN tok - a line in a burst buffer specification containing "capacity="
 * IN capacity_ptr - pointer to the first character after "capacity=" within tok
 * OUT pool - return a malloc'd string of the pool name, caller is responsible
 *            to free
 * OUT size - return the number specified after "capacity="
 */
static int _parse_capacity(char *tok, char *capacity_ptr, char **pool,
			   uint64_t *size)
{
	return SLURM_SUCCESS;
}

/* Perform basic burst_buffer option validation */
static int _parse_bb_opts(job_desc_msg_t *job_desc, uint64_t *bb_size,
			  uid_t submit_uid)
{

	int rc = SLURM_SUCCESS;
	char *bb_script, *save_ptr = NULL;
	char *bb_name = NULL, *bb_pool, *capacity;
	char *end_ptr = NULL, *sub_tok, *tok;
	uint64_t tmp_cnt, swap_cnt = 0;
	bool enable_persist = false, have_bb = false, have_stage_out = false;

	xassert(bb_size);
	*bb_size = 0;

	if (validate_operator(submit_uid) ||
	    (bb_state.bb_config.flags & BB_FLAG_ENABLE_PERSISTENT))
		enable_persist = true;

	if (job_desc->script)
		rc = _xlate_batch(job_desc);
	else
		rc = _xlate_interactive(job_desc);
	if ((rc != SLURM_SUCCESS) || (!job_desc->burst_buffer))
		return rc;

	bb_script = xstrdup(job_desc->burst_buffer);
	tok = strtok_r(bb_script, "\n", &save_ptr);
	while (tok) {
		uint32_t bb_flag = 0;
		tmp_cnt = 0;
		if (tok[0] != '#')
			break;	/* Quit at first non-comment */

		if ((tok[1] == 'B') && (tok[2] == 'B'))
			bb_flag = BB_FLAG_BB_OP;
		else if ((tok[1] == 'D') && (tok[2] == 'W'))
			bb_flag = BB_FLAG_DW_OP;
		else if ((tok[1] == 'P') && (tok[2] == 'B'))
			bb_flag = BB_FLAG_PB_OP;
		/*
		 * Effective Slurm v18.08 and CLE6.0UP06 the create_persistent
		 * and destroy_persistent functions are directly supported by
		 * dw_wlm_cli. Support "#BB" format for backward compatibility.
		 */

		if (bb_flag == BB_FLAG_PB_OP) {
			tok += 3;
			while (isspace(tok[0]))
				tok++;
			if (!xstrncmp(tok, "jobpara", 7)) {
				bb_pool = NULL;
				have_bb = true;
				if ((sub_tok = strstr(tok, "capacity="))) {
					tmp_cnt = bb_get_size_num(sub_tok + 9, 1);
				}
				//slurm_mutex_lock(&bb_state.bb_mutex);
				if (!bb_valid_groups_test(&tmp_cnt))
					rc = ESLURM_INVALID_BURST_BUFFER_REQUEST;
				//slurm_mutex_unlock(&bb_state.bb_mutex);
			   

				xfree(bb_pool);
			} 
		}
		tok = strtok_r(NULL, "\n", &save_ptr);
	}
	xfree(bb_script);

	if (!have_bb)
		rc = ESLURM_INVALID_BURST_BUFFER_REQUEST;

	// if (!have_stage_out) {
	// 	/* prevent sending stage out email */
	// 	job_desc->mail_type &= (~MAIL_JOB_STAGE_OUT);
	// }

	return rc;
}

/* Note: bb_mutex is locked on entry */
static bb_job_t *_get_bb_job(job_record_t *job_ptr)
{
	char *bb_specs, *bb_hurry, *pfs_list, *bb_type, *bb_access, *bb_pool;
	char *end_ptr = NULL, *save_ptr = NULL, *sub_tok, *tok;
	bool have_bb = false;
	uint64_t tmp_cnt;
	int inx;
	bb_job_t *bb_job;
	uint16_t new_bb_state;

	if ((job_ptr->burst_buffer == NULL) ||
	    (job_ptr->burst_buffer[0] == '\0'))
		return NULL;

	if ((bb_job = bb_job_find(&bb_state, job_ptr->job_id)))
		return bb_job;	/* Cached data */

	bb_job = bb_job_alloc(&bb_state, job_ptr->job_id);
	bb_job->account = xstrdup(job_ptr->account);

	if (job_ptr->part_ptr)
		bb_job->partition = xstrdup(job_ptr->part_ptr->name);
	if (job_ptr->qos_ptr)
		bb_job->qos = xstrdup(job_ptr->qos_ptr->name);
	new_bb_state = job_ptr->burst_buffer_state ?
		bb_state_num(job_ptr->burst_buffer_state) : BB_STATE_PENDING;
	bb_set_job_bb_state(job_ptr, bb_job, new_bb_state);
	bb_job->user_id = job_ptr->user_id;
	bb_specs = xstrdup(job_ptr->burst_buffer);
	tok = strtok_r(bb_specs, "\n", &save_ptr);
	while (tok) {
		uint32_t bb_flag = 0;
		if (tok[0] != '#') {
			tok = strtok_r(NULL, "\n", &save_ptr);
			continue;
		}
		if ((tok[1] == 'B') && (tok[2] == 'B'))
			bb_flag = BB_FLAG_BB_OP;
		else if ((tok[1] == 'D') && (tok[2] == 'W'))
			bb_flag = BB_FLAG_DW_OP;
		else if ((tok[1] == 'P') && (tok[2] == 'B'))
			bb_flag = BB_FLAG_PB_OP;
		/*
		 * Effective Slurm v18.08 and CLE6.0UP06 the create_persistent
		 * and destroy_persistent functions are directly supported by
		 * dw_wlm_cli. Support "#BB" format for backward compatibility.
		 */
		if (bb_flag != 0) {
			tok += 3;
			while (isspace(tok[0]))
				tok++;
		}

		if (bb_flag == BB_FLAG_PB_OP) {
			if (!xstrncmp(tok, "jobpara", 7)) {

				if (sub_tok = strstr(tok, "capacity=")) {
					bb_job->req_space = bb_get_size_num(sub_tok + 9, 1);
				} 

				if ((sub_tok = strstr(tok, "pfslist="))) {
					bb_job->pfs = xstrdup(sub_tok + 8);
					if ((sub_tok = strchr(bb_job->pfs, ' ')))
						sub_tok[0] = '\0';
				} else {
					/* 需要获取作业的默认目录 */
					bb_job->pfs = xstrdup(job_ptr->command);
				}

				if ((sub_tok = strstr(tok, "type="))) {
					char *tmp_type = xstrdup(sub_tok + 5);
					sub_tok = xstrchr(tmp_type, ' ');
					if (sub_tok)
						sub_tok[0] = '\0';
					if (xstrcmp(tmp_type, "persistent") == 0)
						bb_job->type = GROUP_TYPE_PERSISTENT;
					else {
						/* 默认类型为temporary临时类型 */
						bb_job->type = GROUP_TYPE_TEMPORARY;
					}
					xfree(tmp_type);
				} 
			
				if ((sub_tok = strstr(tok, "enforce_bb=no"))) {
					bb_job->enforce_bb_flag = false;
				} else {
					/* 默认强制加速（资源不足等待资源） */
					bb_job->enforce_bb_flag = true;
				}

				inx = bb_job->buf_cnt++;
				bb_job->buf_ptr = xrealloc(bb_job->buf_ptr,
							   sizeof(bb_buf_t) *
							   bb_job->buf_cnt);
				//bb_job->buf_ptr[inx].access = bb_access;
				//bb_job->buf_ptr[inx].create = true;
				bb_job->buf_ptr[inx].flags = bb_flag;
				//bb_job->buf_ptr[inx].hurry = false;
				// bb_job->buf_ptr[inx].name = bb_name;
				// bb_job->buf_ptr[inx].pool = bb_pool;
				//tmp_cnt = _set_granularity(tmp_cnt, bb_pool);
				bb_job->buf_ptr[inx].size = bb_job->req_space;
				bb_job->buf_ptr[inx].state = BB_STATE_PENDING;
				bb_job->buf_ptr[inx].type = bb_type;
				//bb_job->buf_ptr[inx].use = false;
				//bb_job->persist_add += tmp_cnt;
				/* 校验请求的条件是否满足，如果满足，先将缓存组和数据集进行减减操作 */
				have_bb = bb_valid_groups_test_2(bb_job, &bb_state);
			}
			if (!xstrncmp(tok, "pstage_in", 9)) {
				/* 设置数据集缓存加速类型，如果没有匹配到字符串则默认使用共享方式 */
				if (strstr(tok, "access_mode=private") || strstr(tok, "access_mode=PRIVATE")) {
					bb_job->access_mode = DATASET_TYPE_PRIVATE;
				} else if (strstr(tok, "access_mode=striped") || strstr(tok, "access_mode=striped")) {
					bb_job->access_mode = DATASET_TYPE_STRIPED;
				} else {
					bb_job->access_mode = DATASET_TYPE_STRIPED;
				}
				if (strstr(tok, "metadata_acceleration=enable") || strstr(tok, "metadata_acceleration=yes")) {
					bb_job->metadata_acceleration = true;
				} else {
					bb_job->metadata_acceleration = false;
				}
			} 
		}
		tok = strtok_r(NULL, "\n", &save_ptr);
	}
	xfree(bb_specs);

	if (!have_bb) {
		xfree(job_ptr->state_desc);
		job_ptr->state_reason = FAIL_BURST_BUFFER_OP;
		xstrfmtcat(job_ptr->state_desc,
			   "%s: Invalid burst buffer spec (%s)",
			   plugin_type, job_ptr->burst_buffer);
		job_ptr->priority = 0;
		info("Invalid burst buffer spec for %pJ (%s)",
		     job_ptr, job_ptr->burst_buffer);
#ifdef __METASTACK_OPT_CACHE_QUERY
		_add_job_state_to_queue(job_ptr);
#endif
		bb_job_del(&bb_state, job_ptr->job_id);
		return NULL;
	}
	if (!bb_job->job_pool)
		bb_job->job_pool = xstrdup(bb_state.bb_config.default_pool);
	if (slurm_conf.debug_flags & DEBUG_FLAG_BURST_BUF)
		bb_job_log(&bb_state, bb_job);
	return bb_job;
}

/* Validate burst buffer configuration */
static void _test_config()
{
		/* 24-day max time limit. (2073600 seconds) */
	static uint32_t max_timeout = (60 * 60 * 24 * 24);
	uint32_t max_groups = 1024;
 	uint32_t max_datasets = 1024;
	uint32_t max_node_per_groups = 1024;
	if (bb_state.bb_config.get_sys_state) {
		error("%s: found get_sys_state which is unused in this plugin, unsetting",
		      plugin_type);
		xfree(bb_state.bb_config.get_sys_state);
	}
	if (bb_state.bb_config.get_sys_status) {
		error("%s: found get_sys_status which is unused in this plugin, unsetting",
		      plugin_type);
		xfree(bb_state.bb_config.get_sys_status);
	}
	if (bb_state.bb_config.flags & BB_FLAG_ENABLE_PERSISTENT) {
		error("%s: found flags=EnablePersistent: persistent burst buffers don't exist in this plugin, setting DisablePersistent",
		      plugin_type);
		bb_state.bb_config.flags &= (~BB_FLAG_ENABLE_PERSISTENT);
		bb_state.bb_config.flags |= BB_FLAG_DISABLE_PERSISTENT;
	}
	if (bb_state.bb_config.flags & BB_FLAG_EMULATE_CRAY) {
		error("%s: found flags=EmulateCray which is invalid for this plugin, unsetting",
		      plugin_type);
		bb_state.bb_config.flags &= (~BB_FLAG_EMULATE_CRAY);
	}
	if (bb_state.bb_config.directive_str)
		directive_str = bb_state.bb_config.directive_str;
	else
	directive_len = strlen(directive_str);

	if (bb_state.bb_config.default_pool) {
		error("%s: found DefaultPool=%s, but DefaultPool is unused for this plugin, unsetting",
		      plugin_type, bb_state.bb_config.default_pool);
		xfree(bb_state.bb_config.default_pool);
	}

	/*
	 * Burst buffer APIs that would use ValidateTimeout
	 * (slurm_bb_job_process and slurm_bb_paths) are actually called
	 * directly from slurmctld, not through SlurmScriptd. Because of this,
	 * they cannot be killed, so there is no timeout for them. Therefore,
	 * ValidateTimeout doesn't matter in this plugin.
	 */
	if (bb_state.bb_config.validate_timeout &&
	    (bb_state.bb_config.validate_timeout != DEFAULT_VALIDATE_TIMEOUT))
		info("%s: ValidateTimeout is not used in this plugin, ignoring",
		     plugin_type);

	/*
	 * Test time limits. In order to prevent overflow when converting
	 * the time limits in seconds to milliseconds (multiply by 1000),
	 * the maximum value for time limits is 2073600 seconds (24 days).
	 * 2073600 * 1000 is still less than the maximum 32-bit signed integer.
	 */
	if (bb_state.bb_config.other_timeout > max_timeout) {
		error("%s: OtherTimeout=%u exceeds maximum allowed timeout=%u, setting OtherTimeout to maximum",
		      plugin_type, bb_state.bb_config.other_timeout,
		      max_timeout);
		bb_state.bb_config.other_timeout = max_timeout;
	}
	if (bb_state.bb_config.stage_in_timeout > max_timeout) {
		error("%s: StageInTimeout=%u exceeds maximum allowed timeout=%u, setting StageInTimeout to maximum",
		      plugin_type, bb_state.bb_config.stage_in_timeout,
		      max_timeout);
		bb_state.bb_config.stage_in_timeout = max_timeout;
	}
	if (bb_state.bb_config.stage_out_timeout > max_timeout) {
		error("%s: StageOutTimeout=%u exceeds maximum allowed timeout=%u, setting StageOutTimeout to maximum",
		      plugin_type, bb_state.bb_config.stage_out_timeout,
		      max_timeout);
		bb_state.bb_config.stage_out_timeout = max_timeout;
	}
	if (bb_state.bb_config.max_groups > max_groups) {
		error("%s: MaxGroups=%u exceeds maximum allowed %u, setting MaxGroups to maximum",
		      plugin_type, bb_state.bb_config.max_groups,
		      max_groups);
		bb_state.bb_config.max_groups = max_groups;
	}	
	if (bb_state.bb_config.max_datasets > max_datasets) {
		error("%s: MaxDatasets=%u exceeds maximum allowed %u, setting MaxDatasets to maximum",
		      plugin_type, bb_state.bb_config.max_datasets,
		      max_datasets);
		bb_state.bb_config.max_datasets = max_datasets;
	}	
}

	
static int _bb_get_parastors_state(void) {

	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_minimal_config_t *bb_min_config = _create_bb_min_config(&bb_state.bb_config);
	slurm_mutex_unlock(&bb_state.bb_mutex);
	int rc = SLURM_SUCCESS;

	//slurm_mutex_unlock(&bb_state.bb_mutex);
	query_params_request *params    = xmalloc(sizeof(query_params_request));
	bb_response *resp_out           =  xmalloc(sizeof(bb_response));
	bb_response *resp_out1          =  xmalloc(sizeof(bb_response));
	memset(params, 0, sizeof(query_params_request));
	params->start =   0;
	params->limit = 100;

	//缓存组
	List tmp_list_groups = get_groups_burst_buffer(params, bb_min_config, resp_out);
	if (!resp_out || resp_out->err_no != 0) { //不能用tmp_list_groups为NULL判断，因为可能没有缓存组
		error("get groups returned error, the detail message is %s",
			resp_out->detail_err_msg ? resp_out->detail_err_msg : "unknown");
		return SLURM_ERROR;
	}
	debug("burst group_count=%d , list_count(tmp_list_groups)=%d", resp_out->group_count, list_count(tmp_list_groups));

	// 数据集
	List tmp_list_datasets = get_datasets_burst_buffer(params, bb_min_config, resp_out1);
	if (!resp_out || resp_out->err_no != 0) { //不能用tmp_list_datasets为NULL判断，因为可能没有数据集
		error("get datasets returned error, the detail message is %s",
			resp_out->detail_err_msg ? resp_out->detail_err_msg : "unknown");
		return SLURM_ERROR;
	}
	debug("burst datasets=%d , list_count(datasets)=%d", resp_out->dataset_count, list_count(tmp_list_datasets));

	xfree(params);

	slurm_mutex_lock(&bb_state.bb_mutex);
	/* load the bb information from parastor resrful*/
	if(!bb_state.list_clients)
		bb_state.list_clients       = list_create(slurm_free_client);//需要释放
	if(!bb_state.list_tasks)
		bb_state.list_tasks         = list_create(slurm_free_task);
	if(!tmp_list_groups)
		bb_state.list_groups        = list_create(slurm_free_group);//需要释放
	else
		bb_state.list_groups        = tmp_list_groups;
		
	if(!tmp_list_datasets)
		bb_state.list_datasets      = list_create(slurm_free_dataset);//需要释放
	else
		bb_state.list_datasets      = tmp_list_datasets;

	
	bb_state.bb_config.used_groups = list_count(bb_state.list_groups);
	bb_state.bb_config.free_groups = bb_state.bb_config.max_groups - resp_out->group_count;

	bb_state.bb_config.used_datasets = resp_out1->dataset_count;
	bb_state.bb_config.free_datasets = bb_state.bb_config.max_datasets - resp_out1->dataset_count;
	debug("the current system has total groups count is %d, %d in use, and %d remaining.",bb_state.bb_config.max_groups,
			bb_state.bb_config.used_groups, bb_state.bb_config.free_groups );
	debug("the current system has total datasets count is %d, %d in use, and %d remaining.",bb_state.bb_config.max_datasets,
			bb_state.bb_config.used_datasets, bb_state.bb_config.free_datasets );
	bb_response_free(resp_out);
	bb_response_free(resp_out1);
	_bb_min_config_free(bb_min_config);
	slurm_mutex_unlock(&bb_state.bb_mutex);

	return rc;
}
/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */
extern int init(void)
{
	int rc = SLURM_SUCCESS;
	int count = 3;
	//parastor_script_path = get_extra_conf_path("burst_buffer.parastor");
	//time_t parastor_script_last_loaded = (time_t) 0;
	/*
	 * slurmscriptd calls bb_g_init() and then bb_g_run_script(). We only
	 * need to initialize lua to run the script. We don't want
	 * slurmscriptd to read from or write to the state save location, nor
	 * do we need slurmscriptd to load the configuration file.
	 */
	if (!running_in_slurmctld()) {
		return SLURM_SUCCESS;
	}
	slurm_mutex_init(&parastor_thread_mutex);
	slurm_mutex_init(&bb_state.bb_mutex);
	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_load_config2(&bb_state, (char *)plugin_type); /* removes "const" */
	_test_config();
	for (size_t i = 0; i < count; i++) {
		rc = get_permanent_token(&bb_state.bb_config);
		if(rc == SLURM_SUCCESS) {
			break;
		}
		unsigned int delay = (1 << i);  /* delay time = 2^i seconds */
		if (delay > 30)                 /* set a maximum wait limit to avoid excessive delays */ 
			delay = 30;
		sleep(delay);
	}
	if(rc == SLURM_ERROR) {
		error("dailed to get permanent token");
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return rc;
	}
	bb_alloc_cache(&bb_state);
	slurm_mutex_unlock(&bb_state.bb_mutex);
	
    if( _bb_get_parastors_state() != SLURM_SUCCESS) {
		error("failed to get parastor burst buffer state");
		return SLURM_ERROR;
	}
	slurm_thread_create(&bb_state.bb_thread, _bb_agent, NULL); 
	return SLURM_SUCCESS;
}

/*
 * fini() is called when the plugin is unloaded. Free all memory.
 */
extern int fini(void)
{
	int pc, last_pc = 0;

	while ((pc = run_command_count()) > 0) {
		if ((last_pc != 0) && (last_pc != pc)) {
			info("waiting for %d running processes",
			     pc);
		}
		last_pc = pc;
		usleep(100000);
	}

	slurm_mutex_lock(&bb_state.bb_mutex);
	log_flag(BURST_BUF, "");

	slurm_mutex_lock(&bb_state.term_mutex);
	bb_state.term_flag = true;
	slurm_cond_signal(&bb_state.term_cond);
	slurm_mutex_unlock(&bb_state.term_mutex);

	if (bb_state.bb_thread) {
		slurm_mutex_unlock(&bb_state.bb_mutex);
		slurm_thread_join(bb_state.bb_thread);
		slurm_mutex_lock(&bb_state.bb_mutex);
	}
	bb_clear_config(&bb_state.bb_config, true);
	bb_clear_cache(&bb_state);
	slurm_mutex_unlock(&bb_state.bb_mutex);
	return SLURM_SUCCESS;
}

static void _free_orphan_alloc_rec(void *x)
{
	bb_alloc_t *rec = (bb_alloc_t *)x;
	(void) bb_free_alloc_rec(&bb_state, rec);

}
static void _cleanup_bb_resources_from_alloc(void *x)
{
	bb_alloc_t *bb_alloc = (bb_alloc_t *)x;

	/* 拿锁，把数据赋值给临时变量，减少持锁时间 */
	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_minimal_config_t *bb_min_config = _create_bb_min_config(&bb_state.bb_config);
	uint32_t tmp_job_id = bb_alloc->job_id;
	int tmp_index_tasks = bb_alloc->index_tasks;
	int *tmp_task_arr = xmalloc(sizeof(int) * tmp_index_tasks);
	if (bb_alloc->bb_task_ids)
		memcpy(tmp_task_arr, bb_alloc->bb_task_ids, sizeof(int) * tmp_index_tasks);
	int tmp_index_datasets = bb_alloc->index_datasets;
	int *tmp_dataset_arr = xmalloc(sizeof(int) * tmp_index_datasets);
	if (bb_alloc->bb_dataset_ids)
		memcpy(tmp_dataset_arr, bb_alloc->bb_dataset_ids, sizeof(int) * tmp_index_datasets);
	int tmp_index_groups = bb_alloc->index_groups;
	int *tmp_group_arr = xmalloc(sizeof(int) * tmp_index_groups);
	if (bb_alloc->bb_group_ids)
		memcpy(tmp_group_arr, bb_alloc->bb_group_ids, sizeof(int) * tmp_index_groups);
	slurm_mutex_unlock(&bb_state.bb_mutex);


	if (bb_min_config == NULL) {
		error("failed to get bb minimal config for JobId=%u", tmp_job_id);
		return NULL;
	}

	/* 1) 删除数据集 */
	if (tmp_dataset_arr && tmp_index_datasets > 0) {
		for (int i = 0; i < tmp_index_datasets; i++) {
			delete_params_request delete_params = { 0 };
			delete_params.dataset_id = tmp_dataset_arr[i];
			bb_response *resp_out = xmalloc(sizeof(bb_response));
			int ret = delete_burst_buffer_dataset(&delete_params, bb_min_config, resp_out);
			if (ret != SLURM_SUCCESS || (resp_out && resp_out->err_no != 0)) {
				error("delete dataset %d for JobId=%u failed",
					delete_params.dataset_id, tmp_job_id);
			} else {
				bb_attribute_dataset *bb_dataset_tmp = NULL;
				slurm_mutex_lock(&bb_state.bb_mutex);
				if (bb_state.list_datasets)
					bb_dataset_tmp = list_remove_first(bb_state.list_datasets, _find_dataset_key, &delete_params.dataset_id);
				if (bb_dataset_tmp)
					slurm_free_dataset(bb_dataset_tmp);
				if (bb_state.bb_config.free_datasets < bb_state.bb_config.max_datasets)
					bb_state.bb_config.free_datasets++;
				if (bb_state.bb_config.used_datasets > 0)
					bb_state.bb_config.used_datasets--;
				slurm_mutex_unlock(&bb_state.bb_mutex);
			}
			bb_response_free(resp_out);
		}
	} else {
		info("no dataset to delete for JobId=%u", tmp_job_id);
	}

	/* 2) 删除缓存组（必须在数据集清理完成后） */
	if (tmp_group_arr && tmp_index_groups > 0) {
		for (int i = 0; i < tmp_index_groups; i++) {
			delete_params_request delete_params = { 0 };
			delete_params.group_id = tmp_group_arr[i];
			bb_response *resp_out = xmalloc(sizeof(bb_response));
			int ret = delete_burst_buffer_group(&delete_params, bb_min_config, resp_out);
			if (ret != SLURM_SUCCESS || (resp_out && resp_out->err_no != 0)) {
				error("delete group %d for JobId=%u failed",
					delete_params.group_id, tmp_job_id);
			} else {
				bb_attribute_group *bb_group_tmp = NULL;
				slurm_mutex_lock(&bb_state.bb_mutex);
				if (bb_state.list_groups)
					bb_group_tmp = list_remove_first(bb_state.list_groups, _find_group_key, &delete_params.group_id);
				if (bb_group_tmp)
					slurm_free_group(bb_group_tmp);
				if (bb_state.bb_config.free_groups < bb_state.bb_config.max_groups)
					bb_state.bb_config.free_groups++;
				if (bb_state.bb_config.used_groups > 0)
					bb_state.bb_config.used_groups--;
				slurm_mutex_unlock(&bb_state.bb_mutex);
			}
			bb_response_free(resp_out);
		}
	} else {
		info("no group to delete for JobId=%u", tmp_job_id);
	}

	_bb_min_config_free(bb_min_config);
	xfree(tmp_task_arr);
	xfree(tmp_dataset_arr);
	xfree(tmp_group_arr);
	_bb_min_config_free(bb_min_config);
}

static void _clean_by_bb_alloc(bb_alloc_t *bb_alloc)
{
	slurm_thread_create_detached(_cleanup_bb_resources_from_alloc, bb_alloc);
}

/*
 * Restore resource information from bb_alloc to bb_job.
 * Only restores resources if job_bb_state indicates resources were created
 * (i.e., BB_STATE_RUNNING and later states).
 * For parastorbb, resources (groups, datasets, tasks) are created in
 * bb_p_job_begin() during BB_STATE_PRE_RUN stage.
 */
static void _restore_bb_job_from_alloc(bb_job_t *bb_job, bb_alloc_t *bb_alloc,
				       uint16_t job_bb_state)
{
	/* Only restore resources if they were already created */
	/* Resources are created in PRE_RUN stage, so only restore for RUNNING and later */
	if (job_bb_state < BB_STATE_RUNNING) {
		/* Resources not created yet, no need to restore */
		return;
	}

	/* Restore resource arrays (groups, datasets, tasks) */
	if (bb_alloc->bb_group_ids && bb_alloc->index_groups > 0) {
		xfree(bb_job->bb_group_ids);
		bb_job->bb_group_ids = xmalloc(sizeof(int) * bb_alloc->index_groups);
		memcpy(bb_job->bb_group_ids, bb_alloc->bb_group_ids,
		       sizeof(int) * bb_alloc->index_groups);
		bb_job->index_groups = bb_alloc->index_groups;
	}
	
	if (bb_alloc->bb_dataset_ids && bb_alloc->index_datasets > 0) {
		xfree(bb_job->bb_dataset_ids);
		bb_job->bb_dataset_ids = xmalloc(sizeof(int) * bb_alloc->index_datasets);
		memcpy(bb_job->bb_dataset_ids, bb_alloc->bb_dataset_ids,
		       sizeof(int) * bb_alloc->index_datasets);
		bb_job->index_datasets = bb_alloc->index_datasets;
	}
	
	if (bb_alloc->bb_task_ids && bb_alloc->index_tasks > 0) {
		xfree(bb_job->bb_task_ids);
		bb_job->bb_task_ids = xmalloc(sizeof(int) * bb_alloc->index_tasks);
		memcpy(bb_job->bb_task_ids, bb_alloc->bb_task_ids,
		       sizeof(int) * bb_alloc->index_tasks);
		bb_job->index_tasks = bb_alloc->index_tasks;
	}
	
	/* Restore other fields */
	bb_job->req_space = bb_alloc->req_space;
	bb_job->type = bb_alloc->type;
	bb_job->cache_tmp = bb_alloc->cache_tmp;
	bb_job->bb_task = bb_alloc->bb_task;
	bb_job->groups_nodes = bb_alloc->groups_nodes;
	bb_job->access_mode = bb_alloc->access_mode;
	if (bb_alloc->pfs) {
		xfree(bb_job->pfs);
		bb_job->pfs = xstrdup(bb_alloc->pfs);
	}
	bb_job->pfs_cnt = bb_alloc->pfs_cnt;
	bb_job->state = (int)bb_alloc->state;  // 从 bb_alloc 恢复 state 到 bb_job
	bb_job->metadata_acceleration = bb_alloc->metadata_acceleration;
}

/*
 * This function should only be called from _purge_vestigial_bufs().
 * We need to reset the burst buffer state and restart any threads that may
 * have been running before slurmctld was shutdown, depending on the state
 * that the burst buffer is in.
 */
static void _recover_job_bb(job_record_t *job_ptr, bb_alloc_t *bb_alloc,
	time_t defer_time, List orphan_rec_list)
{
	bb_job_t *bb_job;
	uint16_t job_bb_state = bb_state_num(job_ptr->burst_buffer_state);

	/*
	 * Call _get_bb_job() to create a cache of the job's burst buffer info,
	 * including the state. Lots of functions will call this so do it now to
	 * create the cache, and we may need to change the burst buffer state.
	 * The job burst buffer state is set in job_ptr and in bb_job.
	 */
	bb_job = _get_bb_job(job_ptr);
	if (!bb_job) {
		/* This shouldn't happen. */
		error("%s: %pJ does not have a burst buffer specification, tearing down vestigial burst buffer.",
			__func__, job_ptr);
		/* For parastorbb, we need bb_job to call _queue_teardown */
		/* If bb_job doesn't exist, we should clean up bb_alloc directly */
		bb_limit_rem(bb_alloc->user_id, bb_alloc->req_space,
			bb_alloc->pool, &bb_state);
		(void)bb_free_alloc_rec(&bb_state, bb_alloc);
		return;
	}
	/*
	 * Restore resource information from bb_alloc to bb_job.
	 * This function will check the state and only restore if resources
	 * were already created (RUNNING and later states).
	 */
	_restore_bb_job_from_alloc(bb_job, bb_alloc, job_bb_state);

	switch (job_bb_state) {
		/*
		 * First 4 states are specific to persistent burst buffers,
		 * which aren't used in burst_buffer/lua.
		 */
	case BB_STATE_ALLOCATING:
	case BB_STATE_ALLOCATED:
	case BB_STATE_DELETING:
	case BB_STATE_DELETED:
		error("%s: Unexpected burst buffer state %s for %pJ",
			__func__, job_ptr->burst_buffer_state, job_ptr);
		break;
		/* Pending states for jobs: */
	case BB_STATE_STAGING_IN:
	case BB_STATE_STAGED_IN:
		/* parastor中stage_in不做暂存操作 */
		break;
	case BB_STATE_ALLOC_REVOKE:
		/*
		 * We do not know the state of staging,
		 * so teardown the buffer and defer the job
		 * for at least 60 seconds (for the teardown).
		 * Also set the burst buffer state back to PENDING.
		 */
		log_flag(BURST_BUF, "Purging buffer for pending %pJ",
			job_ptr);
		bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_TEARDOWN);
		_queue_teardown(bb_job);
		if (job_ptr->details &&
			(job_ptr->details->begin_time < defer_time)) {
			job_ptr->details->begin_time = defer_time;
		}
		break;
		/* Running states for jobs: */
	case BB_STATE_PRE_RUN:
		break;
	case BB_STATE_RUNNING:
	case BB_STATE_SUSPEND:
		/* Nothing to do here. */
		break;
	case BB_STATE_POST_RUN:
	case BB_STATE_STAGING_OUT:
	case BB_STATE_STAGED_OUT:
		log_flag(BURST_BUF, "Restarting burst buffer stage out for %pJ",
			job_ptr);
		/*
		 * _pre_queue_stage_out() sets the burst buffer state
		 * correctly and restarts the needed thread.
		 */
		_pre_queue_stage_out(job_ptr, bb_job);
		break;
	case BB_STATE_TEARDOWN:
	case BB_STATE_TEARDOWN_FAIL:
		log_flag(BURST_BUF, "Restarting burst buffer teardown for %pJ",
			job_ptr);
		_queue_teardown(bb_job);
		break;
	case BB_STATE_COMPLETE:
		/*
		 * We shouldn't get here since the bb_alloc record is
		 * removed when the job's bb state is set to
		 * BB_STATE_COMPLETE during teardown.
		 */
		log_flag(BURST_BUF, "Clearing burst buffer for completed job %pJ",
			job_ptr);
		list_append(orphan_rec_list, bb_alloc);
		break;
	default:
		error("%s: Invalid job burst buffer state %s for %pJ",
			__func__, job_ptr->burst_buffer_state, job_ptr);
		break;
	}
}

/*
 * Identify and purge any vestigial buffers (i.e. we have a job buffer, but
 * the matching job is either gone or completed OR we have a job buffer and a
 * pending job, but don't know the status of stage-in)
 */
static void _purge_vestigial_bufs(void)
{
	List orphan_rec_list = list_create(_free_orphan_alloc_rec);
	bb_alloc_t *bb_alloc = NULL;
	time_t defer_time = time(NULL) + 60;
	int i;

	for (i = 0; i < BB_HASH_SIZE; i++) {
		bb_alloc = bb_state.bb_ahash[i];
		while (bb_alloc) {
			bb_alloc_t *next_alloc = bb_alloc->next;
			job_record_t *job_ptr = NULL;
			if (bb_alloc->job_id == 0) {
				/* This should not happen */
				error("Burst buffer without a job found, removing buffer.");
				list_append(orphan_rec_list, bb_alloc);
			} else if (!(job_ptr = find_job_record(bb_alloc->job_id))) {
				info("Purging vestigial buffer for JobId=%u", bb_alloc->job_id);
				/* For parastorbb, try to find bb_job first */
				bb_job_t *bb_job = bb_job_find(&bb_state, bb_alloc->job_id);
				if (bb_job) {
					_queue_teardown(bb_job);
				} else {
					/* No bb_job found, use bb_alloc to clean up resources */
					info("Job and bb_job not found for JobId=%u, cleaning up resources from bb_alloc",
						bb_alloc->job_id);
					_clean_by_bb_alloc(bb_alloc);
					bb_limit_rem(bb_alloc->user_id, bb_alloc->req_space,
						bb_alloc->pool, &bb_state);
					(void)bb_free_alloc_rec(&bb_state, bb_alloc);
				}
			} else {
				/* job_ptr found, recover job bb */
				_recover_job_bb(job_ptr, bb_alloc, defer_time,
					orphan_rec_list);
			}
			bb_alloc = next_alloc;
		}
	}
	FREE_NULL_LIST(orphan_rec_list);
}

static bool _is_directive(char *tok)
{
	if ((tok[0] == '#') &&
	     ((tok[1] == 'P') && (tok[2] == 'B'))) {
		return true;
	}
	return false;
}

extern char *bb_p_build_het_job_script(char *script, uint32_t het_job_offset)
{
	return bb_common_build_het_job_script(script, het_job_offset,
					      _is_directive);
}

/*
 * Return the total burst buffer size in MB
 */
extern uint64_t bb_p_get_system_size(void)
{
	debug("The current plugin used is the parastor burst buffer plugin, do nothing");
    return 0;

}

/*
 * Load the current burst buffer state (e.g. how much space is available now).
 * Run at the beginning of each scheduling cycle in order to recognize external
 * changes to the burst buffer state (e.g. capacity is added, removed, fails,
 * etc.)
 *
 * init_config IN - true if called as part of slurmctld initialization
 * Returns a Slurm errno.
 */
extern int bb_p_load_state(bool init_config)
{
	if (!init_config)
		return SLURM_SUCCESS;

	log_flag(BURST_BUF, "");
	_load_state(init_config);	/* Has own locking */
	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_set_tres_pos(&bb_state);
	_purge_vestigial_bufs();
	slurm_mutex_unlock(&bb_state.bb_mutex);

	_save_bb_state();	/* Has own locks excluding file write */

	return SLURM_SUCCESS;
}

/*
 * Return string containing current burst buffer status
 * argc IN - count of status command arguments
 * argv IN - status command arguments
 * uid - authenticated UID
 * gid - authenticated GID
 * RET status string, release memory using xfree()
 */
extern char *bb_p_get_status(uint32_t argc, char **argv, uint32_t uid,
			     uint32_t gid)
{
    char *status_resp = NULL;
    debug("The current plugin used is the parastor burst buffer plugin, cray commands are not supported. ");
    return status_resp;
}

/*
 * Note configuration may have changed. Handle changes in BurstBufferParameters.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_reconfig(void)
{
	/* NOTE:not deal default pool */
	int i;
	slurm_mutex_lock(&bb_state.bb_mutex);
	log_flag(BURST_BUF, "");
	bb_load_config2(&bb_state, (char *)plugin_type); /* Remove "const" */
	_test_config();
	slurm_mutex_unlock(&bb_state.bb_mutex);
	/* reconfig is the place we make sure the pointers are correct */
	for (i = 0; i < BB_HASH_SIZE; i++) {
		bb_alloc_t *bb_alloc = bb_state.bb_ahash[i];
		while (bb_alloc) {
			_set_assoc_mgr_ptrs(bb_alloc);
			bb_alloc = bb_alloc->next;
		}
	}

	return SLURM_SUCCESS;
}

/*
 * Pack current burst buffer state information for network transmission to
 * user (e.g. "scontrol show burst")
 *
 * Returns a Slurm errno.
 */
extern int bb_p_state_pack(uid_t uid, buf_t *buffer, uint16_t protocol_version, bool parastor)
{
	uint32_t rec_count = 0;
	if(!parastor)
		return SLURM_SUCCESS;
	slurm_mutex_lock(&bb_state.bb_mutex);
	packstr(bb_state.name, buffer); //插件名称
	bb_pack_state_parastor(&bb_state, buffer, protocol_version);
	if (((bb_state.bb_config.flags & BB_FLAG_PRIVATE_DATA) == 0) ||
	    validate_operator(uid))
		uid = 0;	/* User can see all data */

	rec_count = bb_pack_job_bufs(uid, &bb_state, buffer, protocol_version);
	// (void) bb_pack_usage(uid, &bb_state, buffer, protocol_version);
	log_flag(BURST_BUF, "record_count:%u", rec_count);
	slurm_mutex_unlock(&bb_state.bb_mutex);
	
	return SLURM_SUCCESS;
}

/*
 * Preliminary validation of a job submit request with respect to burst buffer
 * options. Performed after setting default account + qos, but prior to
 * establishing job ID or creating script file.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_validate(job_desc_msg_t *job_desc, uid_t submit_uid,
			     char **err_msg)
{

	uint64_t bb_size = 0;
	int i, rc;

	xassert(job_desc);
	xassert(job_desc->tres_req_cnt);
	xassert(err_msg);

	rc = _parse_bb_opts(job_desc, &bb_size, submit_uid);
	if (rc != SLURM_SUCCESS)
		return rc;

	//debug("burst_buffer:%s", job_desc->burst_buffer);
	if ((job_desc->burst_buffer == NULL) ||
	    (job_desc->burst_buffer[0] == '\0'))
		return rc;

	log_flag(BURST_BUF, "job_user_id:%u, submit_uid:%u",
		 job_desc->user_id, submit_uid);
	log_flag(BURST_BUF, "burst_buffer:%s",
		 job_desc->burst_buffer);

	if (job_desc->user_id == 0) {
		info("User root can not allocate burst buffers");
		*err_msg = xstrdup("User root can not allocate burst buffers");
		return ESLURM_BURST_BUFFER_PERMISSION;
	}

	slurm_mutex_lock(&bb_state.bb_mutex);
	if (bb_state.bb_config.allow_users) {
		bool found_user = false;
		for (i = 0; bb_state.bb_config.allow_users[i]; i++) {
			if (job_desc->user_id ==
			    bb_state.bb_config.allow_users[i]) {
				found_user = true;
				break;
			}
		}
		if (!found_user) {
			*err_msg = xstrdup("User not found in AllowUsers");
			rc = ESLURM_BURST_BUFFER_PERMISSION;
			goto fini;
		}
	}

	if (bb_state.bb_config.deny_users) {
		bool found_user = false;
		for (i = 0; bb_state.bb_config.deny_users[i]; i++) {
			if (job_desc->user_id ==
			    bb_state.bb_config.deny_users[i]) {
				found_user = true;
				break;
			}
		}
		if (found_user) {
			*err_msg = xstrdup("User found in DenyUsers");
			rc = ESLURM_BURST_BUFFER_PERMISSION;
			goto fini;
		}
	}

	if (bb_state.tres_pos > 0) {
		job_desc->tres_req_cnt[bb_state.tres_pos] =
			bb_size / (1024 * 1024);
	}

fini:	slurm_mutex_unlock(&bb_state.bb_mutex);

	return rc;
}


/*
 * Secondary validation of a job submit request with respect to burst buffer
 * options. Performed after establishing job ID and creating script file.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_validate2(job_record_t *job_ptr, char **err_msg)
{

	char *hash_dir = NULL, *job_dir = NULL, *script_file = NULL;
	char *task_script_file = NULL;
	//char *resp_msg = NULL, **script_argv;
	char *dw_cli_path;
	int fd = -1, hash_inx, rc = SLURM_SUCCESS, status = 0;
	bb_job_t *bb_job;
	uint32_t timeout;
	stage_args_t *pre_run_args;
	bool using_master_script = false;
	DEF_TIMERS;
	run_command_args_t run_command_args = {
		.script_path = bb_state.bb_config.get_sys_state,
		.script_type = "job_process",
		.status = &status,
	};

	if ((job_ptr->burst_buffer == NULL) ||
	    (job_ptr->burst_buffer[0] == '\0')) {
		if (job_ptr->details->min_nodes == 0)
			rc = ESLURM_INVALID_NODE_COUNT;
		return rc;
	}

	/* Initialization */
	slurm_mutex_lock(&bb_state.bb_mutex);
	if (bb_state.last_load_time == 0) {
		/* Assume request is valid for now, can't test it anyway */
		info("Burst buffer down, skip tests for %pJ",
		      job_ptr);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return rc;
	}
	bb_job = _get_bb_job(job_ptr);
	if (bb_job == NULL) {
		slurm_mutex_unlock(&bb_state.bb_mutex);
		if (job_ptr->details->min_nodes == 0)
			rc = ESLURM_INVALID_NODE_COUNT;
		return rc;
	}
	if ((job_ptr->details->min_nodes == 0) && bb_job->use_job_buf) {
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return ESLURM_INVALID_BURST_BUFFER_REQUEST;
	}

	log_flag(BURST_BUF, "%pJ", job_ptr);
	timeout = bb_state.bb_config.validate_timeout * 1000;
	slurm_mutex_unlock(&bb_state.bb_mutex);

	/* Standard file location for job arrays */
	if ((job_ptr->array_task_id != NO_VAL) &&
	    (job_ptr->array_job_id != job_ptr->job_id)) {
		hash_inx = job_ptr->array_job_id % 10;
		xstrfmtcat(hash_dir, "%s/hash.%d",
			   slurm_conf.state_save_location, hash_inx);
		(void) mkdir(hash_dir, 0700);
		xstrfmtcat(job_dir, "%s/job.%u", hash_dir,
			   job_ptr->array_job_id);
		(void) mkdir(job_dir, 0700);
		xstrfmtcat(script_file, "%s/script", job_dir);
		fd = open(script_file, 0);
		if (fd >= 0) {	/* found the script */
			close(fd);
			using_master_script = true;
		} else {
			xfree(hash_dir);
		}
	} else {
		hash_inx = job_ptr->job_id % 10;
		xstrfmtcat(hash_dir, "%s/hash.%d",
			   slurm_conf.state_save_location, hash_inx);
		(void) mkdir(hash_dir, 0700);
		xstrfmtcat(job_dir, "%s/job.%u", hash_dir, job_ptr->job_id);
		(void) mkdir(job_dir, 0700);
		xstrfmtcat(script_file, "%s/script", job_dir);
		if (job_ptr->batch_flag == 0)
			rc = bb_build_bb_script(job_ptr, script_file);
	}


	//xfree(dw_cli_path);
	if (rc != SLURM_SUCCESS) {
		slurm_mutex_lock(&bb_state.bb_mutex);
		bb_job_del(&bb_state, job_ptr->job_id);
		slurm_mutex_unlock(&bb_state.bb_mutex);
	} else if (using_master_script) {
		/* Job array's need to have script file in the "standard"
		 * location for the remaining logic, make hard link */
		hash_inx = job_ptr->job_id % 10;
		xstrfmtcat(hash_dir, "%s/hash.%d",
			   slurm_conf.state_save_location, hash_inx);
		(void) mkdir(hash_dir, 0700);
		xstrfmtcat(job_dir, "%s/job.%u", hash_dir, job_ptr->job_id);
		xfree(hash_dir);
		(void) mkdir(job_dir, 0700);
		xstrfmtcat(task_script_file, "%s/script", job_dir);
		xfree(job_dir);
		if ((link(script_file, task_script_file) != 0) &&
		    (errno != EEXIST)) {
			error("link(%s,%s): %m",
			      script_file,
			      task_script_file);
		}
	}
	/* Clean-up */
	xfree(hash_dir);
	xfree(job_dir);
	xfree(task_script_file);
	xfree(script_file);

		return rc;
}

/*
 * Fill in the tres_cnt (in MB) based off the job record
 * NOTE: Based upon job-specific burst buffers, excludes persistent buffers
 * IN job_ptr - job record
 * IN/OUT tres_cnt - fill in this already allocated array with tres_cnts
 * IN locked - if the assoc_mgr tres read locked is locked or not
 */
extern void bb_p_job_set_tres_cnt(job_record_t *job_ptr, uint64_t *tres_cnt,
				  bool locked)
{
    bb_job_t *bb_job;
    if (!tres_cnt) {
        error("No tres_cnt given when looking at %pJ",
              job_ptr);
    }
    if (bb_state.tres_pos < 0) {
        /* BB not defined in AccountingStorageTRES */
        return;
    }
    slurm_mutex_lock(&bb_state.bb_mutex);
    if ((bb_job = _get_bb_job(job_ptr))) {
        tres_cnt[bb_state.tres_pos] =
            bb_job->total_size / (1024 * 1024);
    }
    slurm_mutex_unlock(&bb_state.bb_mutex);
	
}

/*
 * For a given job, return our best guess if when it might be able to start
 */
extern time_t bb_p_job_get_est_start(job_record_t *job_ptr)
{
	time_t est_start = time(NULL);
	
	return est_start;
}

// /*
//  * If the job (x) should be allocated a burst buffer, add it to the
//  * job_candidates list (arg).
//  */
// static int _identify_bb_candidate(void *x, void *arg)
// {
// 	job_record_t *job_ptr = (job_record_t *) x;
// 	List job_candidates = (List) arg;
// 	bb_job_t *bb_job;
// 	bb_job_queue_rec_t *job_rec;

// 	if (!IS_JOB_PENDING(job_ptr) || (job_ptr->start_time == 0) ||
// 	    (job_ptr->burst_buffer == NULL) ||
// 	    (job_ptr->burst_buffer[0] == '\0'))
// 		return SLURM_SUCCESS;

// 	if (job_ptr->array_recs &&
// 	    ((job_ptr->array_task_id == NO_VAL) ||
// 	     (job_ptr->array_task_id == INFINITE)))
// 		return SLURM_SUCCESS; /* Can't operate on job array struct */

// 	bb_job = _get_bb_job(job_ptr);
// 	if (bb_job == NULL)
// 		return SLURM_SUCCESS;
// 	if (bb_job->state == BB_STATE_COMPLETE) {
// 		/* Job requeued or slurmctld restarted during stage-in */
// 		bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_PENDING);
// 	} else if (bb_job->state >= BB_STATE_POST_RUN) {
// 		/* Requeued job still staging out */
// 		return SLURM_SUCCESS;
// 	}
// 	job_rec = xmalloc(sizeof(bb_job_queue_rec_t));
// 	job_rec->job_ptr = job_ptr;
// 	job_rec->bb_job = bb_job;
// 	list_push(job_candidates, job_rec);
// 	return SLURM_SUCCESS;
// }

/*
 * Purge files we have created for the job.
 * bb_state.bb_mutex is locked on function entry.
 * job_ptr may be NULL if not found
 */
static void _purge_bb_files(uint32_t job_id, job_record_t *job_ptr)
{
	char *hash_dir = NULL, *job_dir = NULL;
	char *script_file = NULL, *path_file = NULL;
	int hash_inx;

	hash_inx = job_id % 10;
	xstrfmtcat(hash_dir, "%s/hash.%d",
		   slurm_conf.state_save_location, hash_inx);
	(void) mkdir(hash_dir, 0700);
	xstrfmtcat(job_dir, "%s/job.%u", hash_dir, job_id);
	(void) mkdir(job_dir, 0700);

	xstrfmtcat(path_file, "%s/pathfile", job_dir);
	(void) unlink(path_file);
	xfree(path_file);

	if (!job_ptr || (job_ptr->batch_flag == 0)) {
		xstrfmtcat(script_file, "%s/script", job_dir);
		(void) unlink(script_file);
		xfree(script_file);
	}

	(void) unlink(job_dir);
	xfree(job_dir);
	xfree(hash_dir);
}

static void *_start_teardown(void *x)
{
	bb_job_t *bb_job = (bb_job_t *)x;
	int rc = SLURM_SUCCESS;
	slurmctld_lock_t job_write_lock = { NO_LOCK, WRITE_LOCK, NO_LOCK, NO_LOCK, NO_LOCK };
	job_record_t *job_ptr = NULL;
	bb_alloc_t *bb_alloc = NULL;

	/* 拿锁，把数据赋值给临时变量，减少持锁时间 */
	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_minimal_config_t *bb_min_config = _create_bb_min_config(&bb_state.bb_config);
	uint32_t tmp_job_id = bb_job->job_id;
	int tmp_index_tasks = bb_job->index_tasks;
	int *tmp_task_arr = xmalloc(sizeof(int) * tmp_index_tasks);
	if (bb_job->bb_task_ids)
		memcpy(tmp_task_arr, bb_job->bb_task_ids, sizeof(int) * tmp_index_tasks);
	int tmp_index_datasets = bb_job->index_datasets;
	int *tmp_dataset_arr = xmalloc(sizeof(int) * tmp_index_datasets);
	if (bb_job->bb_dataset_ids)
		memcpy(tmp_dataset_arr, bb_job->bb_dataset_ids, sizeof(int) * tmp_index_datasets);
	int tmp_index_groups = bb_job->index_groups;
	int *tmp_group_arr = xmalloc(sizeof(int) * tmp_index_groups);
	if (bb_job->bb_group_ids)
		memcpy(tmp_group_arr, bb_job->bb_group_ids, sizeof(int) * tmp_index_groups);
	slurm_mutex_unlock(&bb_state.bb_mutex);

	if (bb_min_config == NULL) {
		error("failed to get bb minimal config for JobId=%u", tmp_job_id);
		return NULL;
	}

	DEF_TIMERS
	_incr_parastor_thread_cnt();
	track_script_rec_add(tmp_job_id, 0, pthread_self());
	_calibrate_task_state(tmp_job_id, tmp_task_arr, tmp_index_tasks, bb_min_config); // 查询作业task状态

	START_TIMER;
	/* 1) 删除数据集 */
	if (tmp_dataset_arr && tmp_index_datasets > 0) {
		for (int i = 0; i < tmp_index_datasets; i++) {
			delete_params_request delete_params = { 0 };
			delete_params.dataset_id = tmp_dataset_arr[i];
			bb_response *resp_out = xmalloc(sizeof(bb_response));
			int ret = delete_burst_buffer_dataset(&delete_params, bb_min_config, resp_out);
			if (ret != SLURM_SUCCESS || (resp_out && resp_out->err_no != 0)) {
				error("delete dataset %d for JobId=%u failed",
					delete_params.dataset_id, tmp_job_id);
				rc = SLURM_ERROR;
			} else {
				bb_attribute_dataset *bb_dataset_tmp = NULL;
				slurm_mutex_lock(&bb_state.bb_mutex);
				if (bb_state.list_datasets)
					bb_dataset_tmp = list_remove_first(bb_state.list_datasets, _find_dataset_key, &delete_params.dataset_id);
				if (bb_dataset_tmp)
					slurm_free_dataset(bb_dataset_tmp);
				if (bb_state.bb_config.free_datasets < bb_state.bb_config.max_datasets)
					bb_state.bb_config.free_datasets++;
				if (bb_state.bb_config.used_datasets > 0)
					bb_state.bb_config.used_datasets--;
				slurm_mutex_unlock(&bb_state.bb_mutex);
			}
			bb_response_free(resp_out);
		}
	}else {
		info("no dataset to delete for JobId=%u", tmp_job_id);
	}
	/* 2) 删除缓存组（必须在数据集清理完成后） */
	if (tmp_group_arr && tmp_index_groups > 0) {
		for (int i = 0; i < tmp_index_groups; i++) {
			delete_params_request delete_params = { 0 };
			delete_params.group_id = tmp_group_arr[i];
			bb_response *resp_out = xmalloc(sizeof(bb_response));
			int ret = delete_burst_buffer_group(&delete_params, bb_min_config, resp_out);
			if (ret != SLURM_SUCCESS || (resp_out && resp_out->err_no != 0)) {
				error("delete group %d for JobId=%u failed",
					delete_params.group_id, tmp_job_id);
				rc = SLURM_ERROR;
			} else {
				bb_attribute_group *bb_group_tmp = NULL;
				slurm_mutex_lock(&bb_state.bb_mutex);
				if (bb_state.list_groups)
					bb_group_tmp = list_remove_first(bb_state.list_groups, _find_group_key, &delete_params.group_id);
				if (bb_group_tmp)
					slurm_free_group(bb_group_tmp);
				if (bb_state.bb_config.free_groups < bb_state.bb_config.max_groups)
					bb_state.bb_config.free_groups++;
				if (bb_state.bb_config.used_groups > 0)
					bb_state.bb_config.used_groups--;
				slurm_mutex_unlock(&bb_state.bb_mutex);
			}
			bb_response_free(resp_out);
		}
	}else {
		info("no group to delete for JobId=%u", tmp_job_id);
	}
	_bb_min_config_free(bb_min_config);
	xfree(tmp_task_arr);
	xfree(tmp_dataset_arr);
	xfree(tmp_group_arr);
	END_TIMER;
	info("teardown (parastor cleanup) for JobId=%u ran for %s", bb_job->job_id, TIME_STR);

	/* 更新作业与分配状态，释放记录 */
	lock_slurmctld(job_write_lock);
	job_ptr = find_job_record(bb_job->job_id);
	_purge_bb_files(bb_job->job_id, job_ptr);
	if (job_ptr) {
		if (rc != SLURM_SUCCESS) {
			job_ptr->state_reason = FAIL_BURST_BUFFER_OP;
			xfree(job_ptr->state_desc);
			xstrfmtcat(job_ptr->state_desc, "%s: teardown cleanup failed", plugin_type);
#ifdef __METASTACK_OPT_CACHE_QUERY
			_add_job_state_to_queue(job_ptr);
#endif
		} else {
			job_state_unset_flag(job_ptr, JOB_STAGE_OUT);
			xfree(job_ptr->state_desc);
			last_job_update = time(NULL);
		}

		slurm_mutex_lock(&bb_state.bb_mutex);
		bb_alloc = bb_find_alloc_rec(&bb_state, job_ptr);
		if (bb_alloc) {
			bb_limit_rem(bb_alloc->user_id, bb_alloc->size, bb_alloc->pool, &bb_state);
			(void)bb_free_alloc_rec(&bb_state, bb_alloc);
		}
		if (rc == SLURM_SUCCESS) {
			bb_job_t *tmp = _get_bb_job(job_ptr);
			if (tmp) {
				bb_set_job_bb_state(job_ptr, tmp, BB_STATE_COMPLETE);
				bb_job_del(&bb_state, tmp->job_id);
			}
		}
		slurm_mutex_unlock(&bb_state.bb_mutex);
	}
	unlock_slurmctld(job_write_lock);

	track_script_remove(pthread_self());
	_decr_parastor_thread_cnt();
	return NULL;
}

static void _queue_teardown(bb_job_t *bb_job)
{
	slurm_thread_create_detached(_start_teardown, bb_job);
}

static int _run_real_size(stage_args_t *stage_args, init_argv_f_t init_argv,
			  const char *op, uint32_t job_id, uint32_t timeout,
			  char **resp_msg)
{
	
	return SLURM_SUCCESS;
}


static void *_start_stage_in(void *x)
{
	stage_args_t *stage_in_args = x;
	uint64_t real_size = 0;
	uint64_t orig_real_size = stage_in_args->bb_size;
	job_record_t *job_ptr;
	slurmctld_lock_t job_write_lock = { .job = WRITE_LOCK };

	// bb_func_t stage_in_ops[] = {
	// 	{
	// 		.init_argv = _init_setup_argv,
	// 		.op_type = SLURM_BB_SETUP,
	// 		.run_func = _run_lua_stage_script,
	// 		.timeout = bb_state.bb_config.other_timeout,
	// 	},
	// 	{
	// 		.init_argv = _init_data_in_argv,
	// 		.op_type = SLURM_BB_DATA_IN,
	// 		.run_func = _run_lua_stage_script,
	// 		.timeout = bb_state.bb_config.stage_in_timeout,
	// 	},
	// 	{
	// 		.init_argv = _init_data_in_argv, /* Same as data in */
	// 		.op_type = SLURM_BB_TEST_DATA_IN,
	// 		.run_func = _run_test_data_inout,
	// 		.timeout = bb_state.bb_config.stage_in_timeout,
	// 	},
	// 	{
	// 		.init_argv = _init_real_size_argv,
	// 		.op_type = SLURM_BB_REAL_SIZE,
	// 		.run_func = _run_real_size,
	// 		.timeout = bb_state.bb_config.stage_in_timeout,
	// 	},
	// };

	stage_in_args->hurry = true;
	// if (_run_stage_ops(stage_in_ops, ARRAY_SIZE(stage_in_ops),
	// 		   stage_in_args) != SLURM_SUCCESS)
	// 	goto fini;
	real_size = stage_in_args->bb_size; /* Updated by _run_real_size */

	lock_slurmctld(job_write_lock);
	slurm_mutex_lock(&bb_state.bb_mutex);
	job_ptr = find_job_record(stage_in_args->job_id);
	if (!job_ptr) {
		error("unable to find job record for JobId=%u",
		      stage_in_args->job_id);
	} else {
		bb_job_t *bb_job;
		bb_alloc_t *bb_alloc = NULL;

		bb_job = bb_job_find(&bb_state, stage_in_args->job_id);
		if (bb_job)
			bb_set_job_bb_state(job_ptr, bb_job,
					    BB_STATE_STAGING_IN);
		if (bb_job && bb_job->total_size) {
			/*
			 * Adjust total size to real size if real size
			 * returns something bigger.
			 */
			// if (real_size > bb_job->req_size) {
			// 	log_flag(BURST_BUF, "%pJ total_size increased from %"PRIu64" to %"PRIu64,
			// 		 job_ptr, bb_job->req_size, real_size);
			// 	bb_job->total_size = real_size;
			// 	bb_limit_rem(stage_in_args->uid,
			// 		     orig_real_size,
			// 		     stage_in_args->pool, &bb_state);
			// 	/* Restore limit based upon actual size. */
			// 	bb_limit_add(stage_in_args->uid,
			// 		     bb_job->total_size,
			// 		     stage_in_args->pool, &bb_state,
			// 		     true);
			// }
			bb_alloc = bb_find_alloc_rec(&bb_state, job_ptr);
			if (bb_alloc) {
			// 	if (bb_alloc->size != bb_job->total_size) {
			// 		/*
			// 		 * bb_alloc is state saved, so we need
			// 		 * to update bb_alloc in case slurmctld
			// 		 * restarts.
			// 		 */
			// 		// bb_alloc->size = bb_job->total_size;
					bb_state.last_update_time = time(NULL);
			// 	}
			// } else {
			// 	error("unable to find bb_alloc record for %pJ",
			// 	      job_ptr);
			}
		}
		log_flag(BURST_BUF, "Setup/stage-in complete for %pJ", job_ptr);
		queue_job_scheduler();
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	unlock_slurmctld(job_write_lock);

fini:
	xfree(stage_in_args->job_script);
	xfree(stage_in_args->pool);
	xfree(stage_in_args);

	return NULL;
}


/* 使能数据集 */
static int _queue_stage_in(job_record_t *job_ptr, bb_job_t *bb_job)
{
	char *hash_dir = NULL, *job_dir = NULL;
	int hash_inx = job_ptr->job_id % 10;
	stage_args_t *stage_in_args;
	bb_alloc_t *bb_alloc = NULL;
	xstrfmtcat(hash_dir, "%s/hash.%d",
		   slurm_conf.state_save_location, hash_inx);
	(void) mkdir(hash_dir, 0700);
	xstrfmtcat(job_dir, "%s/job.%u", hash_dir, job_ptr->job_id);

	stage_in_args = xmalloc(sizeof *stage_in_args);
	stage_in_args->job_id = job_ptr->job_id;
	stage_in_args->uid = job_ptr->user_id;
	stage_in_args->gid = job_ptr->group_id;
	// if (bb_job->job_pool)
	// 	stage_in_args->pool = xstrdup(bb_job->job_pool);
	// else
	// 	stage_in_args->pool = NULL;
	// stage_in_args->bb_size = bb_job->total_size;
	stage_in_args->job_script = bb_handle_job_script(job_ptr, bb_job);
	/*
	 * Create bb allocation for the job now. Check if it has already been
	 * created (perhaps it was created but then slurmctld restarted).
	 * bb_alloc is the structure that is state saved.
	 * If we wait until the _start_stage_in thread to create bb_alloc,
	 * we introduce a race condition where the thread could be killed
	 * (if slurmctld is shut down) before the thread creates
	 * bb_alloc. That race would mean the burst buffer isn't state saved.
	 */
	if (!(bb_alloc = bb_find_alloc_rec(&bb_state, job_ptr))) {
		bb_alloc = bb_alloc_job(&bb_state, job_ptr, bb_job);
		bb_alloc->create_time = time(NULL);
	}

	// bb_limit_add(job_ptr->user_id, bb_job->total_size, bb_job->job_pool,
	// 	     &bb_state, true);
	slurm_thread_create_detached(_start_stage_in, stage_in_args);

	xfree(hash_dir);
	xfree(job_dir);
	return SLURM_SUCCESS;
}

static int _alloc_job_bb(job_record_t *job_ptr, bb_job_t *bb_job,
			 bool job_ready)
{
	int rc = SLURM_SUCCESS;
	log_flag(BURST_BUF, "start job allocate %pJ", job_ptr);
	if (bb_job->state < BB_STATE_STAGING_IN) {
		//bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_STAGING_IN);
		rc = _queue_stage_in(job_ptr, bb_job);
		if (rc != SLURM_SUCCESS) {
			bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_TEARDOWN);
			_queue_teardown(bb_job);
		}
	}
	return rc;
}

static int _calibrate_task_state(uint32_t job_id, int *bb_task_ids, int index_tasks, bb_minimal_config_t *bb_min_config)
{
	if (bb_min_config == NULL) {
		error("failed to get bb minimal config for JobId=%u", job_id);
		return NULL;
	}
	bb_response *resp_out = xmalloc(sizeof(bb_response));
	for (int i = 0; i < index_tasks; i++) {
		bb_attribute_task *bb_task = xmalloc(sizeof(bb_attribute_task)) ;

		int rc = get_single_burst_buffer_tasks(bb_task_ids[i], bb_task, bb_min_config, resp_out);
		if (rc != 0) {
			error("BB-----can't get task %d", bb_task_ids[i]);
			_bb_min_config_free(bb_min_config);
			bb_response_free(resp_out);
			return SLURM_ERROR;
		}
		/* 任务状态非完成（作业运行完时），则预热失败 */
		debug("BB-----the state of task %d of job %u is %s", bb_task_ids[i], job_id, bb_task->task_state);
		// TODO:预热失败信息需要进一步处理
		if (xstrcmp(bb_task->task_state, "COMPLETED") != 0) {
			debug("BB-----the task %d of job %u haven't finish cache prefetch", bb_task_ids[i], job_id);
			_bb_min_config_free(bb_min_config);
			bb_response_free(resp_out);
			return SLURM_ERROR;
		}
		slurm_free_task(bb_task);
	}
	bb_response_free(resp_out);
	return SLURM_SUCCESS;
}

// static int _try_alloc_job_bb(void *x, void *arg)
// {
// 	bb_job_queue_rec_t *job_rec = (bb_job_queue_rec_t *) x;
// 	job_record_t *job_ptr = job_rec->job_ptr;
// 	bb_job_t *bb_job = job_rec->bb_job;
// 	int rc = 0;

// 	if (bb_job->state >= BB_STATE_STAGING_IN)
// 		return SLURM_SUCCESS; /* Job was already allocated a buffer */

// 	// if (bb_job->job_pool && bb_job->req_size)
// 	// 	rc = bb_test_size_limit(job_ptr, bb_job, &bb_state, NULL);
// 	// else
// 	// 	rc = 0;

// 	if (rc == 0) {
// 		/*
// 		 * Job could start now. Allocate burst buffer and continue to
// 		 * the next job.
// 		 */
// 		_alloc_job_bb(job_ptr, bb_job, true);
// 		rc = SLURM_SUCCESS;
// 	} else if (rc == 1) /* Exceeds configured limits, try next job */
// 		rc = SLURM_SUCCESS;
// 	else /* No space currently available, break out of loop */
// 		rc = SLURM_ERROR;

// 	return rc;
// }
static void _job_queue_del(void *x)
{
	bb_job_queue_rec_t *job_rec = (bb_job_queue_rec_t *) x;
	if (job_rec) {
		xfree(job_rec);
	}
}
/*
 * Attempt to allocate resources and begin file staging for pending jobs.
 */
extern int bb_p_job_try_stage_in(List job_queue)
{
	bb_job_queue_rec_t *job_rec;
	List job_candidates;
	list_itr_t *job_iter;
	job_record_t *job_ptr;
	bb_job_t *bb_job;
	int rc = 0;

	// slurm_mutex_lock(&bb_state.bb_mutex);
	// log_flag(BURST_BUF, "Mutex locked");

	// if (bb_state.last_load_time == 0) {
	// 	slurm_mutex_unlock(&bb_state.bb_mutex);
	// 	return SLURM_SUCCESS;
	// }

	// /* Identify candidates to be allocated burst buffers */
	// job_candidates = list_create(_job_queue_del);
	// job_iter = list_iterator_create(job_queue);
	// while ((job_ptr = list_next(job_iter))) {
	// 	if (!IS_JOB_PENDING(job_ptr) ||
	// 	    (job_ptr->start_time == 0) ||
	// 	    (job_ptr->burst_buffer == NULL) ||
	// 	    (job_ptr->burst_buffer[0] == '\0'))
	// 		continue;
	// 	if (job_ptr->array_recs &&
	// 	    ((job_ptr->array_task_id == NO_VAL) ||
	// 	     (job_ptr->array_task_id == INFINITE)))
	// 		continue;	/* Can't operate on job array struct */
	// 	bb_job = _get_bb_job(job_ptr);
	// 	if (bb_job == NULL)
	// 		continue;
	// 	if (bb_job->state == BB_STATE_COMPLETE)
	// 		bb_set_job_bb_state(job_ptr, bb_job, /* job requeued */
	// 				    BB_STATE_PENDING);
	// 	else if (bb_job->state >= BB_STATE_POST_RUN)
	// 		continue;	/* Requeued job still staging out */
	// 	job_rec = xmalloc(sizeof(bb_job_queue_rec_t));
	// 	job_rec->job_ptr = job_ptr;
	// 	job_rec->bb_job = bb_job;
	// 	list_push(job_candidates, job_rec);
	// }
	// list_iterator_destroy(job_iter);

	// /* Sort in order of expected start time */
	// list_sort(job_candidates, bb_job_queue_sort);

	// bb_set_use_time(&bb_state);
	// job_iter = list_iterator_create(job_candidates);
	// while ((job_rec = list_next(job_iter))) {
	// 	job_ptr = job_rec->job_ptr;
	// 	bb_job = job_rec->bb_job;
	// 	if (bb_job->state >= BB_STATE_STAGING_IN)
	// 		continue;	/* Job was already allocated a buffer */

	// 	// rc = bb_test_size_limit(job_ptr, bb_job, &bb_state,
	// 	// 			_queue_teardown);
	// 	if (rc == 0) {  /* Could start now */
	// 	// 	(void)_alloc_job_bb(job_ptr, bb_job, true);
	// 	// 	//校验预热任务状态
	// 	// 	while ((job_rec = list_next(job_iter))) {
	// 	// 		if (bb_job->state == BB_STATE_STAGING_IN)
	// 	// 			/* 只保留task状态为running、submitting或者completed的任务ID */
	// 	// 			_calibrate_task_state(job_ptr, bb_job);
	// 	// 			bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_RUNNING);
	// 	// 	}
	// 	// }
	// 	} else if (rc == 1)	/* Exceeds configured limits */
	// 		continue;
	// 	else			/* No space currently available */
	// 		break;
	// }
	// list_iterator_destroy(job_iter);



	// slurm_mutex_unlock(&bb_state.bb_mutex);
	FREE_NULL_LIST(job_candidates);

	return SLURM_SUCCESS;
}


/*
 * Determine if a job's burst buffer stage-in is complete
 * job_ptr IN - Job to test
 * test_only IN - If false, then attempt to allocate burst buffer if possible
 *
 * RET: 0 - stage-in is underway
 *      1 - stage-in complete
 *     -1 - stage-in not started or burst buffer in some unexpected state
 */
extern int bb_p_job_test_stage_in(job_record_t *job_ptr, bool test_only)
{
	bb_job_t *bb_job = NULL;
	int rc = 1;
	debug("test ");
	/* 校验作业所在分区是否启用了 burst buffer */
	if (!(job_ptr->bit_flags & JOB_FLAG_PART_BURSTBUFFER)) {
		/* 分区未启用 返回-1 */
		return -1;
	}
	if ((job_ptr->burst_buffer == NULL) ||
		(job_ptr->burst_buffer[0] == '\0'))
		return 1;

	if (job_ptr->array_recs &&
		((job_ptr->array_task_id == NO_VAL) ||
			(job_ptr->array_task_id == INFINITE)))
		return -1;	/* Can't operate on job array structure */

	slurm_mutex_lock(&bb_state.bb_mutex);
	log_flag(BURST_BUF, "%pJ test_only:%d",
		job_ptr, (int)test_only);
	if (bb_state.last_load_time != 0)
		bb_job = _get_bb_job(job_ptr);
	if (bb_job && (bb_job->state == BB_STATE_COMPLETE))
		bb_set_job_bb_state(job_ptr, bb_job,
			BB_STATE_PENDING); /* job requeued */
	if (bb_job == NULL) {
		rc = -1;
	} else if (bb_job->state < BB_STATE_STAGING_IN) {
		/* Job buffer not allocated, create now if space available */
		rc = 1;
		if (test_only)
			goto fini;
		 _alloc_job_bb(job_ptr, bb_job, false);
	// 	if (bb_job->job_pool && bb_job->req_size) {
	// 		if ((bb_test_size_limit(job_ptr, bb_job, &bb_state,
	// 			NULL) == 0)) {
	// 			// _alloc_job_bb(job_ptr, bb_job, false);
	// 			rc = 0; /* Setup/stage-in in progress */
	// 		}
	// 	} else {
	// 		// _alloc_job_bb(job_ptr, bb_job, false);
	// 		rc = 0; /* Setup/stage-in in progress */
	// 	}
	} else if (bb_job->state == BB_STATE_STAGING_IN) {
		rc = 1;
	} else if (bb_job->state == BB_STATE_STAGED_IN) {
		rc = 1;
	} else {
		rc = -1;	/* Requeued job still staging in */
	}

fini:
	slurm_mutex_unlock(&bb_state.bb_mutex);

	return rc;
}

/* Add key=value pairs from file_path to the job's environment */
static void _update_job_env(job_record_t *job_ptr, char *file_path)
{

}

/* Kill job from CONFIGURING state */
static void _kill_job(job_record_t *job_ptr, bool hold_job)
{
	last_job_update = time(NULL);
	job_ptr->end_time = last_job_update;
	if (hold_job)
		job_ptr->priority = 0;
	build_cg_bitmap(job_ptr);
	job_ptr->exit_code = 1;
	job_ptr->state_reason = FAIL_BURST_BUFFER_OP;
	xfree(job_ptr->state_desc);
	job_ptr->state_desc = xstrdup("Burst buffer pre_run error");

	//job_state_set(job_ptr, JOB_REQUEUE);
	job_completion_logger(job_ptr, true);
	job_state_set(job_ptr, (JOB_PENDING | JOB_COMPLETING));

	deallocate_nodes(job_ptr, false, false, false);
#ifdef __METASTACK_OPT_CACHE_QUERY
	_add_job_state_to_queue(job_ptr);
#endif
}

static void *_start_pre_run(void *x)
{
    bb_job_t   *bb_job = NULL;
	bb_alloc_t *bb_alloc = NULL;
    int bb_node_cnt = 0;
    //int dividend = 10; 
	int *last_client_ids  = NULL;
	int index = 0;
	int tmp_groups_count = 0;
	int *group_ids = NULL;
	int index_groups = 0;
	char *pfs_copy = NULL, *save_ptr = NULL;;
	char *str_split = NULL;
	int *dataset_ids = NULL;
	size_t index_datasets = 0;
	int ret = SLURM_SUCCESS;
	bool run_kill_job = false;
	bool hold_job = false;
	create_params_request create_params = {0};
	query_params_request params = {0};
    job_record_t *job_ptr;
	/* Locks: read job */
	slurmctld_lock_t job_read_lock = {
		NO_LOCK, READ_LOCK, NO_LOCK, NO_LOCK };
	/* Locks: write job */
	slurmctld_lock_t job_write_lock = {
		NO_LOCK, WRITE_LOCK, NO_LOCK, NO_LOCK, READ_LOCK };
	int tmp_task_cnt = 0; /* 只记录成功的个数 */

	pre_run_bb_args_t *pre_run_args = (pre_run_bb_args_t *) x;
	pthread_t tid = pthread_self();
	track_script_rec_add(pre_run_args->job_id, 0, pthread_self());	

	lock_slurmctld(job_read_lock);
	slurm_mutex_lock(&bb_state.bb_mutex);
    job_ptr = find_job_record(pre_run_args->job_id);
	if (job_ptr) {
		bb_job   =  _get_bb_job(job_ptr);
		bb_alloc =  bb_find_alloc_rec(&bb_state, job_ptr);
		if(!bb_job || !bb_alloc) {
			error( "Invalid jobid: %d can't start pre_run", pre_run_args->job_id);
			slurm_mutex_unlock(&bb_state.bb_mutex);
			unlock_slurmctld(job_read_lock);
			track_script_remove(pthread_self());
			_decr_parastor_thread_cnt();
			return NULL;
		}
	} else {
	    error( "Invalid jobid: %d", pre_run_args->job_id);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		unlock_slurmctld(job_read_lock);
		track_script_remove(pthread_self());
		_decr_parastor_thread_cnt();
		return NULL;
	}
	
	char *tmp_job_nodes = bitmap2node_name(job_ptr->node_bitmap);
	slurm_mutex_unlock(&bb_state.bb_mutex);
	unlock_slurmctld(job_read_lock);


	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_minimal_config_t *bb_min_config = _create_bb_min_config(&bb_state.bb_config);
	if(!bb_state.list_clients || !bb_state.list_groups ||!bb_state.list_datasets) {
	    error("The bb system may not be initialized.");
		slurm_mutex_unlock(&bb_state.bb_mutex);
		track_script_remove(pthread_self());
		_decr_parastor_thread_cnt();
		return NULL;
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (bb_min_config == NULL) {
		error("failed to get bb minimal config for JobId=%u", bb_job->job_id);
		track_script_remove(pthread_self());
		_decr_parastor_thread_cnt();
		return NULL;
	}

	//创建缓存组
	int GROUP_SIZE =  10;
	if( bb_state.bb_config.max_clients_per_job > 0) {
		GROUP_SIZE = bb_state.bb_config.max_clients_per_job;
	}
	//数据集规则目录
	pfs_copy = xstrdup(bb_job->pfs);
	
	if(tmp_job_nodes) {
		hostlist_t *hl = NULL;
		char *host = NULL;
		hl = hostlist_create_dims(tmp_job_nodes, 0);
		if (!hl) {
			error( "Invalid hostlist: %s", tmp_job_nodes);
			//slurm_mutex_unlock(&bb_state.bb_mutex);
			_bb_min_config_free(bb_min_config);
			track_script_remove(pthread_self());
			_decr_parastor_thread_cnt();
			return NULL;
		}
		last_client_ids= xmalloc(sizeof(int) * hostlist_count(hl));  // 存储每个节点的 last_client_id


		while ((host = hostlist_shift_dims(hl, 0))) {
			bb_response *resp_out = xmalloc(sizeof(bb_response));	
			params.host_name = xstrdup(host);
			params.max_clients_join = bb_state.bb_config.max_clients_join;
			params.limit = 1;
			params.start = 0;
			params.ids = NULL;
			get_set_burst_buffer_clients_and_tasks(&params, bb_min_config, RESULT_CLIENT, resp_out);
			slurm_mutex_lock(&bb_state.bb_mutex);
			bb_attribute_client *bb_client = resp_out->bb_client;
			bb_attribute_client* bb_client_tmp = NULL;
                if(bb_state.list_clients)
                    bb_client_tmp = list_remove_first(bb_state.list_clients, _find_client_key, bb_client->hostname);
                if(bb_client_tmp) {
                    slurm_free_client(bb_client_tmp);
                }
            list_append(bb_state.list_clients, bb_client);
			

			if(bb_state.bb_config.max_clients_join <= 0) {
				bb_state.bb_config.max_clients_join = 32; //默认最大客户端连接数
			}
			debug(" The number of cache groups the %s has joined is %d, and the current maximum limit for this node is %d.",
				host, resp_out->client_join_groups_counts, bb_state.bb_config.max_clients_join);
			if(resp_out->client_join_groups_counts >= bb_state.bb_config.max_clients_join) {
				debug("The node exceeds the configured number of cache groups it can join. ");
				xfree(last_client_ids);
				bb_response_free(resp_out);
				hostlist_destroy(hl);
				slurm_mutex_unlock(&bb_state.bb_mutex);
				_bb_min_config_free(bb_min_config);
				track_script_remove(pthread_self());
				_decr_parastor_thread_cnt();
				return NULL;
			}
			last_client_ids[index++] = resp_out->last_client_id;
			debug(" The host %s clients’ IDs is %d of the current node. index is %d", host, resp_out->last_client_id, index);
			xfree(params.host_name);
			bb_response_free(resp_out);
			slurm_mutex_unlock(&bb_state.bb_mutex);
		}
		hostlist_destroy(hl);
	
	} else{
		error("host list is empty");
	}
	
	tmp_groups_count  = index / (GROUP_SIZE);
	int dvi = index % (GROUP_SIZE);
	if( dvi > 0) {
		tmp_groups_count += 1;
	}
	
	group_ids = xmalloc(sizeof(int) * (tmp_groups_count));


	bb_response *resp_out1 = xmalloc(sizeof(bb_response));	
	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_state.bb_config.free_groups    -= (tmp_groups_count );
	bb_state.bb_config.free_datasets  -= bb_job->pfs_cnt * (tmp_groups_count );
	bb_state.bb_config.used_groups += (tmp_groups_count);
	bb_state.bb_config.used_datasets += bb_job->pfs_cnt * (tmp_groups_count);
	bb_job->index_groups               = (tmp_groups_count);
	bb_job->index_datasets             = bb_job->pfs_cnt * (tmp_groups_count);
	int tmp_datasets_counts = bb_job->index_groups * bb_job->pfs_cnt;
	slurm_mutex_unlock(&bb_state.bb_mutex);

	if(tmp_groups_count > 1) {
		for (size_t i = 0; i < index; i += GROUP_SIZE) {
			int tmp_ids[GROUP_SIZE]; // 创建一个新数组保存这一组 id
			size_t batch_size = (i + GROUP_SIZE <= index) ? GROUP_SIZE : (index - i);  // 计算当前批次的大小（可能最后一组不足 10 个）
			memcpy(tmp_ids, &last_client_ids[i], batch_size * sizeof(int));
			create_params.client_ids = tmp_ids;
			create_params.client_count = batch_size;
			ret = create_burst_buffer_group(&create_params, bb_min_config, resp_out1);
			slurm_mutex_lock(&bb_state.bb_mutex);
			if (ret == SLURM_ERROR) {
				xfree(last_client_ids);
				xfree(tmp_job_nodes);
				bb_job->bb_group_ids  = group_ids;
				slurm_mutex_unlock(&bb_state.bb_mutex);
				_bb_min_config_free(bb_min_config);
				track_script_remove(pthread_self());
				_decr_parastor_thread_cnt();
				return NULL;
			}
			group_ids[index_groups++] = resp_out1->group_id; //记录创建缓存组id
			//group->id = resp_out1->group_id;
			
			bb_attribute_group* bb_group_tmp = list_remove_first(bb_state.list_groups, _find_group_key, &resp_out1->group_id);
			bb_attribute_group* bb_group     = resp_out1->bb_group;
			if(bb_group_tmp)
				slurm_free_dataset(bb_group_tmp);
			list_append(bb_state.list_groups, bb_group);
			slurm_mutex_unlock(&bb_state.bb_mutex);
			debug("Create cache group with ID  %d",  bb_group->id);
		}
	} else {
		create_params.client_ids = last_client_ids;
		create_params.client_count = index;
		ret = create_burst_buffer_group(&create_params, bb_min_config, resp_out1);
		group_ids[index_groups++] = resp_out1->group_id; //记录创建缓存组id
		slurm_mutex_lock(&bb_state.bb_mutex);
		if (ret == SLURM_ERROR) {
				xfree(last_client_ids);
				bb_response_free(resp_out1);
				xfree(tmp_job_nodes);
				bb_job->bb_group_ids = group_ids;
				slurm_mutex_unlock(&bb_state.bb_mutex);
				_bb_min_config_free(bb_min_config);
				track_script_remove(pthread_self());
				_decr_parastor_thread_cnt();
				return NULL;
		}
		bb_attribute_group* bb_group_tmp = list_remove_first(bb_state.list_groups, _find_group_key, &resp_out1->group_id);
		bb_attribute_group* bb_group     = resp_out1->bb_group;
		if(bb_group_tmp)
			slurm_free_dataset(bb_group_tmp);	
		list_append(bb_state.list_groups, bb_group);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		debug("Create cache group with ID  %d", bb_group->id);
	}

	bb_response_free(resp_out1);

	//创建数据集规则
	str_split = strtok_r(pfs_copy,",",&save_ptr);
	dataset_ids = xmalloc(sizeof(int) * tmp_datasets_counts);
	while (str_split) {	
		//create_params.path = xstrdup(str_split);
		for (size_t i = 0; i < index_groups; i++) {
			bb_response *resp_out3 = xmalloc(sizeof(bb_response));	
			create_params.group_id = group_ids[i];
			create_params.path = xstrdup(str_split);

			if ( bb_job->type == DATASET_TYPE_PRIVATE )
				create_params.task_type == LOCAL_CACHE;
			else
				create_params.task_type == SHARE_CACHE;	
			ret = create_burst_buffer_dataset(&create_params, bb_min_config, resp_out3);
			slurm_mutex_lock(&bb_state.bb_mutex);
			if (ret == SLURM_ERROR) {
					xfree(create_params.path);
					xfree(last_client_ids);
					bb_response_free(resp_out3);
					xfree(tmp_job_nodes);
					xfree(pfs_copy);
					//xfree(dataset_ids);
					//bb_job->bb_dataset_ids =  dataset_ids;
					error("Failed create datasets with ID[%ld]  %d and path is %s",  i, dataset_ids[index_datasets], str_split);
					//run_kill_job = true;
					slurm_mutex_unlock(&bb_state.bb_mutex);
					break;
			}	
			dataset_ids[index_datasets++] = resp_out3->dataset_id;
			bb_attribute_dataset* bb_dataset_tmp = list_remove_first(bb_state.list_datasets, _find_dataset_key, &resp_out3->dataset_id);
			bb_attribute_dataset* bb_dataset     = resp_out3->bb_dataset;
			if(bb_dataset_tmp)
				slurm_free_dataset(bb_dataset_tmp);
			list_append(bb_state.list_datasets, bb_dataset);
			debug("Job[%d] create datasets with ID  %d", bb_job->job_id, bb_dataset->id);
			slurm_mutex_unlock(&bb_state.bb_mutex);
			bb_response_free(resp_out3);
		}
		xfree(create_params.path);
		str_split = strtok_r(NULL,",", &save_ptr);
	}
	xfree(pfs_copy);

	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_job->bb_group_ids = group_ids;
	bb_job->bb_dataset_ids = dataset_ids;
	bb_job->index_datasets = index_datasets;
	int tmp_index_datasets = bb_job->index_datasets;
	int *tmp_dataset_arr = xmalloc(sizeof(int) * tmp_index_datasets);
	if (bb_job->bb_dataset_ids)
		memcpy(tmp_dataset_arr, bb_job->bb_dataset_ids, sizeof(int) * tmp_index_datasets);
	int *task_ids = xmalloc(sizeof(int) * tmp_index_datasets); /* 最多每个数据集一个task */
	slurm_mutex_unlock(&bb_state.bb_mutex);

	for (size_t i = 0; i < tmp_index_datasets; i++) {
		create_params_request create_params1 = {0};
		create_params1.dataset_id = tmp_dataset_arr[i];
		create_params1.task_type = BURST_BUFFER_TASK_TYPE_PREFETCH;
		create_params1.error_action_type = 0; //0:单个节点失败后任务中断执行;1:单个节点失败后任务继续执行
		debug(" start the warm-up task ");
		bb_response *resp_out = xmalloc(sizeof(bb_response));
		int rc = submit_burst_buffer_task(&create_params1, bb_min_config, resp_out);
		if (rc != 0 || resp_out->err_no != 0) {
			debug("submit task of the dataset %d failure", create_params1.dataset_id);
			continue; /* 不成功不记录，直接跳过 */
		}
		task_ids[tmp_task_cnt++] = resp_out->task_id;
		bb_response_free(resp_out);
	}
	//作业需要记录缓存组id和数据集id
	slurm_mutex_lock(&bb_state.bb_mutex);

	/* 把实际task个数存储到job_ptr，任务号数组存储到bb_job */
	bb_job->index_tasks = tmp_task_cnt;
	task_ids = xrealloc(task_ids, tmp_task_cnt * sizeof(int));
	bb_job->bb_task_ids = task_ids;

	alter_bb_alloc_job_rec(bb_alloc, bb_job, true);

	debug(" The number of cache groups that have been created is %d," 
			"and the number of datasets that have been created is %d,"	 
			" and the number of taskids that have been created is %d",	
			index_groups, index_datasets, tmp_task_cnt);
	xfree(last_client_ids);
	xfree(tmp_job_nodes);
	xfree(tmp_dataset_arr);
		
    char* new_state_str = bb_state_string(BB_STATE_RUNNING);
    // bb_job->state = BB_STATE_RUNNING;
	slurm_mutex_unlock(&bb_state.bb_mutex);
	_bb_min_config_free(bb_min_config);
	//////////

    // //slurm_mutex_unlock(&bb_state.bb_mutex);
    // lock_slurmctld(job_write_lock);
    // if (!job_ptr) {
    //     /* This should never happen, but handle it just in case. */
	// 	error("%s: Could not find job_ptr for JobId=%u, unable to set new burst buffer state %s in job.",
	// 		__func__, bb_job->job_id, new_state_str);
	// 	_bb_min_config_free(bb_min_config);
	// 	track_script_remove(pthread_self());
	// 	_decr_parastor_thread_cnt();
	// 	return;
    // }
    // log_flag(BURST_BUF, "Modify %pJ burst buffer state from %s to %s",
    //      job_ptr, job_ptr->burst_buffer_state, new_state_str);
    // xfree(job_ptr->burst_buffer_state);

    // job_ptr->burst_buffer_state = xstrdup(new_state_str);
    // unlock_slurmctld(job_write_lock);
    // /* 只保留task状态为running、submitting或者completed的任务ID */
	lock_slurmctld(job_write_lock);
	slurm_mutex_lock(&bb_state.bb_mutex);
	if (ret != SLURM_SUCCESS) {
		/* pre_run failure */
		trigger_burst_buffer();
		error("pre_run failed for JobId=%u", pre_run_args->job_id);
		bb_update_system_comment(job_ptr, "pre_run", resp_msg, 0);
		if (job_ptr && bb_job) {
			if (IS_JOB_RUNNING(job_ptr))
				run_kill_job = true;
			bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_TEARDOWN);
			hold_job = true;
			_queue_teardown(bb_job);
		}
	} else if (bb_job) {
		/* pre_run success and the job's BB record exists */
		/* 状态已经在前面设置为 BB_STATE_RUNNING，这里只需要确保一致性 */
		if (bb_job->state != BB_STATE_RUNNING) {
			bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_RUNNING);
		}
	}

	if (run_kill_job) {
		job_state_unset_flag(job_ptr, JOB_CONFIGURING);
#ifdef __METASTACK_OPT_CACHE_QUERY
		_add_job_state_to_queue(job_ptr);
#endif
	}
	prolog_running_decr(job_ptr);
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (run_kill_job) {
		/* bb_mutex must be unlocked before calling this */
		_kill_job(job_ptr, hold_job);
	}
	unlock_slurmctld(job_write_lock);
	track_script_remove(pthread_self());
	_decr_parastor_thread_cnt();
	return NULL;
	
}

/* Attempt to claim burst buffer resources.
 * At this time, bb_g_job_test_stage_in() should have been run successfully AND
 * the compute nodes selected for the job.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_begin(job_record_t *job_ptr)
{
    bb_job_t *bb_job = NULL;
	pre_run_bb_args_t *pre_run_args;
    int bb_node_cnt = 0;
	int ret = SLURM_SUCCESS;

	if ((job_ptr->burst_buffer == NULL) || (job_ptr->burst_buffer[0] == '\0')){
		debug("BB-----jobid %d no need burst buffer", job_ptr->job_id);
		return SLURM_SUCCESS;
	}

	if (!job_ptr->job_resrcs || !job_ptr->job_resrcs->nodes) {
		error("%pJ lacks node allocation",
		      job_ptr);
		return SLURM_ERROR;
	}

    slurm_mutex_lock(&bb_state.bb_mutex);
    log_flag(BURST_BUF, "%pJ",
         job_ptr);
    if (bb_state.last_load_time == 0) {
        info("Burst buffer down, can not start %pJ",
              job_ptr);
        slurm_mutex_unlock(&bb_state.bb_mutex);
        return SLURM_ERROR;
    }

    bb_job = _get_bb_job(job_ptr);
    if (!bb_job) {
        error("no job record buffer for %pJ",
              job_ptr);
        xfree(job_ptr->state_desc);
        job_ptr->state_desc =
            xstrdup("Could not find burst buffer record");
        job_ptr->state_reason = FAIL_BURST_BUFFER_OP;
        //_queue_teardown(bb_job);
        slurm_mutex_unlock(&bb_state.bb_mutex);
#ifdef __METASTACK_OPT_CACHE_QUERY
        _add_job_state_to_queue(job_ptr);
#endif
        return SLURM_ERROR;
    }

    if(job_ptr->node_bitmap)
        bb_node_cnt = bit_set_count(job_ptr->node_bitmap);//计算分配的节点数量
    else {
        error("no job nodes for %pJ", job_ptr);
        xfree(job_ptr->state_desc);
        job_ptr->state_desc = xstrdup("Could not find job node");
        job_ptr->state_reason = FAIL_BURST_BUFFER_OP;
        _queue_teardown(bb_job);
        slurm_mutex_unlock(&bb_state.bb_mutex);
#ifdef __METASTACK_OPT_CACHE_QUERY
        _add_job_state_to_queue(job_ptr);
#endif
        return SLURM_ERROR;
    }
	int GROUP_SIZE = 10;
	if (bb_state.bb_config.max_clients_per_job > 0) {
		GROUP_SIZE = bb_state.bb_config.max_clients_per_job;
	}
    bb_node_cnt = (GROUP_SIZE + bb_node_cnt - 1)  / GROUP_SIZE; 
    log_flag(BURST_BUF, "required number of cache groups %d", bb_node_cnt);
	log_flag(BURST_BUF, "required number of datasets %ld", (bb_node_cnt * bb_job->pfs_cnt));
	if (bb_node_cnt > bb_state.bb_config.free_groups || ((bb_node_cnt * bb_job->pfs_cnt) > bb_state.bb_config.free_datasets)) {


#ifdef __METASTACK_OPT_CACHE_QUERY
		_add_job_state_to_queue(job_ptr);
#endif
		if (bb_job->enforce_bb_flag == true) {
			/* enforce_bb_flag=true: 资源不足时不运行作业 */
			xfree(job_ptr->state_desc);
			job_ptr->state_desc = xstrdup("insufficient datasets or groups.");
			job_ptr->state_reason = WAIT_BURST_BUFFER_RESOURCE;
			_queue_teardown(bb_job);
			slurm_mutex_unlock(&bb_state.bb_mutex);
#ifdef __METASTACK_OPT_CACHE_QUERY
			_add_job_state_to_queue(job_ptr);
#endif
			return SLURM_ERROR;
		} else {
			/* enforce_bb_flag=false: 资源不足作业不用BB资源直接运行 */
			xfree(job_ptr->state_desc);
			job_ptr->state_desc = xstrdup("enforce_bb_flag=false:The job runs without using BB resources");
			job_ptr->state_reason = WAIT_BURST_BUFFER_RESOURCE;
			_queue_teardown(bb_job);
			slurm_mutex_unlock(&bb_state.bb_mutex);
#ifdef __METASTACK_OPT_CACHE_QUERY
			_add_job_state_to_queue(job_ptr);
#endif
			return SLURM_SUCCESS;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	pre_run_args = xmalloc(sizeof(pre_run_bb_args_t));
	pre_run_args->args    = NULL;
	pre_run_args->job_id  = job_ptr->job_id;
	pre_run_args->timeout = bb_state.bb_config.other_timeout * 1000;
	pre_run_args->user_id = job_ptr->user_id;
	pre_run_args->bb_node_cnt = bb_node_cnt;

	slurm_thread_create_detached(_start_pre_run, pre_run_args);

    return SLURM_SUCCESS;
}

/* Revoke allocation, but do not release resources.
 * Executed after bb_p_job_begin() if there was an allocation failure.
 * Does not release previously allocated resources.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_revoke_alloc(job_record_t *job_ptr)
{

	bb_job_t *bb_job = NULL;
	int rc = SLURM_SUCCESS;

	slurm_mutex_lock(&bb_state.bb_mutex);
	if (job_ptr)
		bb_job = _get_bb_job(job_ptr);
	if (bb_job) {
		if (bb_job->state == BB_STATE_RUNNING)
			bb_set_job_bb_state(job_ptr, bb_job,
					    BB_STATE_STAGED_IN);
		else if (bb_job->state == BB_STATE_PRE_RUN)
			bb_set_job_bb_state(job_ptr, bb_job,
					    BB_STATE_ALLOC_REVOKE);
	} else {
		rc = SLURM_ERROR;
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);

	return rc;
}

/*
 * Trigger a job's burst buffer stage-out to begin
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_start_stage_out(job_record_t *job_ptr)
{
	bb_job_t *bb_job;

	if ((job_ptr->burst_buffer == NULL) ||
		(job_ptr->burst_buffer[0] == '\0'))
		return SLURM_SUCCESS;

	slurm_mutex_lock(&bb_state.bb_mutex);
	log_flag(BURST_BUF, "%pJ", job_ptr);

	if (bb_state.last_load_time == 0) {
		info("Burst buffer down, can not stage out %pJ",
			job_ptr);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return SLURM_ERROR;
	}
	bb_job = _get_bb_job(job_ptr);
	if (!bb_job) {
		/* No job buffers. Assuming use of persistent buffers only */
		verbose("%pJ bb job record not found",
			job_ptr);
	} else if (bb_job->state < BB_STATE_RUNNING) {
		/* Job never started. Just teardown the buffer */
		bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_TEARDOWN);
		_queue_teardown(bb_job);
	} else if (bb_job->state < BB_STATE_POST_RUN) {
		_pre_queue_stage_out(job_ptr, bb_job);
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);

	return SLURM_SUCCESS;
}

/*
 * Determine if a job's burst buffer post_run operation is complete
 *
 * RET: 0 - post_run is underway
 *      1 - post_run complete
 *     -1 - fatal error
 */
extern int bb_p_job_test_post_run(job_record_t *job_ptr)
{
	bb_job_t *bb_job;
	int rc = -1;

	if ((job_ptr->burst_buffer == NULL) ||
	    (job_ptr->burst_buffer[0] == '\0'))
		return 1;

	slurm_mutex_lock(&bb_state.bb_mutex);
	log_flag(BURST_BUF, "%pJ", job_ptr);

	if (bb_state.last_load_time == 0) {
		info("Burst buffer down, can not post_run %pJ",
		      job_ptr);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return -1;
	}
	bb_job = bb_job_find(&bb_state, job_ptr->job_id);
	if (!bb_job) {
		/* No job buffers. Assuming use of persistent buffers only */
		verbose("%pJ bb job record not found",
			job_ptr);
		rc =  1;
	} else {
		if (bb_job->state < BB_STATE_POST_RUN) {
			rc = -1;
		} else if (bb_job->state > BB_STATE_POST_RUN) {
			rc =  1;
		} else {
			rc =  0;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);

	return rc;
}

/*
 * Determine if a job's burst buffer stage-out is complete
 *
 * RET: 0 - stage-out is underway
 *      1 - stage-out complete
 *     -1 - fatal error
 */
extern int bb_p_job_test_stage_out(job_record_t *job_ptr)
{
	bb_job_t *bb_job;
	int rc = -1;

	if ((job_ptr->burst_buffer == NULL) ||
	    (job_ptr->burst_buffer[0] == '\0'))
		return 1;

	slurm_mutex_lock(&bb_state.bb_mutex);
	log_flag(BURST_BUF, "%pJ", job_ptr);

	if (bb_state.last_load_time == 0) {
		info("Burst buffer down, can not stage-out %pJ",
		      job_ptr);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return -1;
	}
	bb_job = bb_job_find(&bb_state, job_ptr->job_id);
	if (!bb_job) {
		/*
		 * This is expected if the burst buffer completed teardown,
		 * or if only persistent burst buffers were used.
		 */
		rc =  1;
	} else {
		if (bb_job->state == BB_STATE_PENDING) {
			/*
			 * No job BB work not started before job was killed.
			 * Alternately slurmctld daemon restarted after the
			 * job's BB work was completed.
			 */
			rc =  1;
		} else if (bb_job->state < BB_STATE_POST_RUN) {
			rc = -1;
		} else if (bb_job->state > BB_STATE_STAGING_OUT) {
			rc =  1;
			if (bb_job->state == BB_STATE_COMPLETE)
				bb_job_del(&bb_state, bb_job->job_id);
		} else {
			rc =  0;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);

	return rc;
}

/*
 * Terminate any file staging and completely release burst buffer resources
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_cancel(job_record_t *job_ptr)
{
	bb_job_t *bb_job;
	bb_alloc_t *bb_alloc;

	slurm_mutex_lock(&bb_state.bb_mutex);
	log_flag(BURST_BUF, "%pJ", job_ptr);

	if (bb_state.last_load_time == 0) {
		info("Burst buffer down, can not cancel %pJ",
			job_ptr);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return SLURM_ERROR;
	}

	bb_job = _get_bb_job(job_ptr);
	if (!bb_job) {
		/* Nothing ever allocated, nothing to clean up */
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return SLURM_SUCCESS;
	}

	if (bb_job->state == BB_STATE_PENDING) {
		/* No resources allocated yet, just mark as complete */
		bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_COMPLETE);
		bb_alloc = bb_find_alloc_rec(&bb_state, job_ptr);
		if (bb_alloc) {
			bb_limit_rem(bb_alloc->user_id, bb_alloc->req_space,
				bb_alloc->pool, &bb_state);
			(void)bb_free_alloc_rec(&bb_state, bb_alloc);
		}
		bb_job_del(&bb_state, bb_job->job_id);
	} else if (bb_job->state == BB_STATE_COMPLETE) {
		/* Teardown already done. */
	} else {
		/* Resources allocated, trigger teardown */
		bb_set_job_bb_state(job_ptr, bb_job, BB_STATE_TEARDOWN);
		bb_alloc = bb_find_alloc_rec(&bb_state, job_ptr);
		if (bb_alloc) {
			bb_alloc->state = BB_STATE_TEARDOWN;
			bb_alloc->state_time = time(NULL);
			bb_state.last_update_time = time(NULL);
		}
		_queue_teardown(bb_job);
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);

	return SLURM_SUCCESS;
}

/*
 * Run a script in the burst buffer plugin
 *
 * func IN - script function to run
 * jobid IN - job id for which we are running the script (0 if not for a job)
 * argc IN - number of arguments to pass to script
 * argv IN - argument list to pass to script
 * resp_msg OUT - string returned by script
 *
 * Returns the status of the script.
 */
extern int bb_p_run_script(char *func, uint32_t job_id, uint32_t argc,
			   char **argv, job_info_msg_t *job_info,
			   char **resp_msg)
{
	return 0;
}

/*
 * Translate a burst buffer string to it's equivalent TRES string
 * For example:
 *   "bb/lua=2M" -> "1004=2"
 * Caller must xfree the return value
 */
extern char *bb_p_xlate_bb_2_tres_str(char *burst_buffer)
{
	char *result = NULL;


	return result;
}


static bb_minimal_config_t *_create_bb_min_config(bb_config_t *bb_config)
{
	bb_minimal_config_t *bb_min_config = NULL;
	if (bb_config) {
		bb_min_config = xmalloc(sizeof(bb_minimal_config_t));
		bb_min_config->para_stor_addr      = xstrdup(bb_config->para_stor_addr);
		bb_min_config->para_stor_port      = bb_config->para_stor_port;
		bb_min_config->para_stor_user_name = xstrdup(bb_config->para_stor_user_name);
		bb_min_config->para_stor_password  = xstrdup(bb_config->para_stor_password);
		bb_min_config->token			   = xstrdup(bb_config->token);
		bb_min_config->other_timeout	   = bb_config->other_timeout;
	} else {
		error("bb_config is NULL, can't get minimal config.");
		return NULL;
	}
	return bb_min_config;
}

static void _bb_min_config_free(bb_minimal_config_t * config)
{
	if (config) {
		xfree(config->para_stor_addr);
		xfree(config->para_stor_user_name);
		xfree(config->para_stor_password);
		xfree(config->token);
		xfree(config);
	}
}