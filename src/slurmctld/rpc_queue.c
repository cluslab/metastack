/*****************************************************************************\
 *  rpc_queue.c
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

#include "config.h"

#include <inttypes.h>

#if HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#include "src/common/list.h"
#include "src/common/macros.h"
#include "src/common/read_config.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "src/interfaces/serializer.h"

#include "src/slurmctld/job_scheduler.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/proc_req.h"
#include "src/slurmctld/state_save.h"

bool enabled = true;

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
/*
 * When dividing the node regions processed by threads handling Epilog Complete messages, 
 * the average number of nodes per region (rounded down).
 * */
static int normal_regional_node_count = 0;
/*
 * The number of regions where the number of nodes exceeds the average when the total 
 * number of nodes in the cluster cannot be evenly divided by the configured number of threads.
 * */
static int widened_regional_num = 0;
/* The number of nodes within regions containing a relatively large number of nodes. */
static int widened_regional_node_count = 0;
/* Index the boundary between the widened region and the normal region. */
static int widened_regional_threshold = 0;

static void *async_worker(void *arg);

void _destroy_async_task(void *object) {
	async_task_t *task = (async_task_t *)object;
	
	slurm_free_msg(task->msg);
	xfree(task);
}

void _create_async_queue(slurmctld_rpc_t *q) {
	q->async_queue = xmalloc(sizeof(async_queue_t));
	q->async_queue->worker_count = rpc_queue_pool_size;
	q->async_queue->next_worker = 0;

	/*
	 * If atomic.h is enabled, this section of the comments needs to be opened.
	 * atomic_init(&q->async_queue->pending_tasks, 0);
	 */
	q->async_queue->pending_tasks = 0;
	slurm_mutex_init(&q->async_queue->poll_mutex);
	slurm_mutex_init(&q->async_queue->pending_mutex);
	slurm_cond_init(&q->async_queue->pending_cond, NULL);

	q->async_queue->workers = xmalloc(sizeof(worker_data_t) * rpc_queue_pool_size);

	for (int i = 0; i < rpc_queue_pool_size; i++) {
		worker_data_t *worker = &q->async_queue->workers[i];
		worker->index = i;
		worker->tasks = list_create(_destroy_async_task);
		worker->msg_type = q->msg_type;
		worker->queue = q->async_queue;
		slurm_mutex_init(&worker->mutex);
		slurm_cond_init(&worker->cond, NULL);
		worker->shutdown = false;

		slurm_thread_create(&worker->thread, async_worker, worker);
		
		info("%s: create worker thread %d/%d for %s", __func__, i + 1,
				rpc_queue_pool_size, rpc_num2string(q->msg_type));
	}
}
#endif

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_SUBMIT_PARALLEL
static void free_para_submit_resource(int worker_count) {
	int i = 0;
	if (para_submit_avail_node_bitmap) {
		for (i=0; i < worker_count; i++) {
			FREE_NULL_BITMAP(para_submit_avail_node_bitmap[i]);
		}
		xfree(para_submit_avail_node_bitmap);
	}

	if (para_submit_share_node_bitmap) {
		for (i=0; i < worker_count; i++) {
			FREE_NULL_BITMAP(para_submit_share_node_bitmap[i]);
		}
		xfree(para_submit_share_node_bitmap);
	}

	if (para_submit_idle_node_bitmap) {
		for (i=0; i < worker_count; i++) {
			FREE_NULL_BITMAP(para_submit_idle_node_bitmap[i]);
		}
		xfree(para_submit_idle_node_bitmap);
	}

	if (para_submit_resv_node_bitmap) {
		for (i=0; i < worker_count; i++) {
			FREE_NULL_BITMAP(para_submit_resv_node_bitmap[i]);
		}
		xfree(para_submit_resv_node_bitmap);
	}
}

static void init_submit_resource(slurmctld_rpc_t *q) {
	free_para_submit_resource(rpc_queue_pool_size);
	para_submit = false;
	if (!xstrcasestr(slurm_conf.slurmctld_params, "para_submit")) 
		return;
	
	if(rpc_queue_pool_enabled) {
		para_submit = true;
		para_submit_avail_node_bitmap = xcalloc(rpc_queue_pool_size, sizeof(bitstr_t *));
		para_submit_share_node_bitmap = xcalloc(rpc_queue_pool_size, sizeof(bitstr_t *));
		para_submit_idle_node_bitmap  = xcalloc(rpc_queue_pool_size, sizeof(bitstr_t *));
		para_submit_resv_node_bitmap = xcalloc(rpc_queue_pool_size, sizeof(bitstr_t *));
		_create_async_queue(q);
	}
}

static void para_submit_bitmap_init(){
	int i = 0;

	if (para_submit && rpc_queue_pool_enabled) {
		if (para_submit_avail_node_bitmap) {
			for (i = 0; i < rpc_queue_pool_size; i++) {
				FREE_NULL_BITMAP(para_submit_avail_node_bitmap[i]);
				para_submit_avail_node_bitmap[i] = bit_copy(avail_node_bitmap);
			}
		} else {
			para_submit = false;
			error("para_submit_avail_node_bitmap is NULL, ignore para_submit");
		}

		if (para_submit_share_node_bitmap) {
			for (i = 0; i < rpc_queue_pool_size; i++) {
				FREE_NULL_BITMAP(para_submit_share_node_bitmap[i]);
				para_submit_share_node_bitmap[i] = bit_copy(share_node_bitmap);
			}
		} else {
			para_submit = false;
			error("para_submit_share_node_bitmap is NULL, ignore para_submit");
		}

		if (para_submit_idle_node_bitmap) {
			for (i = 0; i < rpc_queue_pool_size; i++) {
				FREE_NULL_BITMAP(para_submit_idle_node_bitmap[i]);
				para_submit_idle_node_bitmap[i] = bit_copy(idle_node_bitmap);
			}
		} else {
			para_submit = false;
			error("para_submit_idle_node_bitmap is NULL, ignore para_submit");
		}

		if (para_submit_resv_node_bitmap) {
			for (i = 0; i < rpc_queue_pool_size; i++) {
				FREE_NULL_BITMAP(para_submit_resv_node_bitmap[i]);
				para_submit_resv_node_bitmap[i] = bit_copy(resv_node_bitmap);
			}
		} else {
			para_submit = false;
			error("para_submit_resv_node_bitmap is NULL, ignore para_submit");
		}
	}
}	
#endif

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_EPILOG_PARALLEL

static void free_para_epilog_resource(int worker_count) {
	int i = 0;
	if (para_epilog_cg_node_bitmap) {
		for (i = 0; i < worker_count; i++) {
			if (para_epilog_cg_node_bitmap[i]) {
				FREE_NULL_BITMAP(para_epilog_cg_node_bitmap[i]);
			}
		}
		xfree(para_epilog_cg_node_bitmap);
	}

	if (para_epilog_up_node_bitmap) {
		for (i = 0; i < worker_count; i++) {
			if (para_epilog_up_node_bitmap[i]) {
				FREE_NULL_BITMAP(para_epilog_up_node_bitmap[i]);
			}
		}
		xfree(para_epilog_up_node_bitmap);
	}

	if (para_epilog_avail_node_bitmap) {
		for (i = 0; i < worker_count; i++) {
			if (para_epilog_avail_node_bitmap[i]) {
				FREE_NULL_BITMAP(para_epilog_avail_node_bitmap[i]);
			}
		}
		xfree(para_epilog_avail_node_bitmap);
	}

	if (para_epilog_bf_ignore_node_bitmap) {
		for (i = 0; i < worker_count; i++) {
			if (para_epilog_bf_ignore_node_bitmap[i]) {
				FREE_NULL_BITMAP(para_epilog_bf_ignore_node_bitmap[i]);
			}
		}
		xfree(para_epilog_bf_ignore_node_bitmap);
	}

	if (para_epilog_idle_node_bitmap) {
		for (i = 0; i < worker_count; i++) {
			if (para_epilog_idle_node_bitmap[i]) {
				FREE_NULL_BITMAP(para_epilog_idle_node_bitmap[i]);
			}
		}
		xfree(para_epilog_idle_node_bitmap);
	}
}

static void init_epilog_resource(slurmctld_rpc_t *q) {
	free_para_epilog_resource(rpc_queue_pool_size);
	enable_para_epilog = false;
	if (!xstrcasestr(slurm_conf.slurmctld_params, "para_epilog")) 
		return;
	
	if(rpc_queue_pool_enabled) {
		enable_para_epilog = true;
		para_epilog_cg_node_bitmap = xcalloc(rpc_queue_pool_size, sizeof(bitstr_t *));
		para_epilog_up_node_bitmap = xcalloc(rpc_queue_pool_size, sizeof(bitstr_t *));
		para_epilog_avail_node_bitmap  = xcalloc(rpc_queue_pool_size, sizeof(bitstr_t *));
		para_epilog_bf_ignore_node_bitmap = xcalloc(rpc_queue_pool_size, sizeof(bitstr_t *));
		para_epilog_idle_node_bitmap = xcalloc(rpc_queue_pool_size, sizeof(bitstr_t *));
		_create_async_queue(q);
	}
}

static void copy_private_thread_bitmaps(int worker_count) {
	int i;
	for (i = 0; i < worker_count; i++) {
		FREE_NULL_BITMAP(para_epilog_cg_node_bitmap[i]);
		para_epilog_cg_node_bitmap[i] = bit_copy(cg_node_bitmap);

		FREE_NULL_BITMAP(para_epilog_up_node_bitmap[i]);
		para_epilog_up_node_bitmap[i] = bit_copy(up_node_bitmap);

		FREE_NULL_BITMAP(para_epilog_avail_node_bitmap[i]);
		para_epilog_avail_node_bitmap[i] = bit_copy(avail_node_bitmap);

		FREE_NULL_BITMAP(para_epilog_bf_ignore_node_bitmap[i]);
		para_epilog_bf_ignore_node_bitmap[i] = bit_copy(bf_ignore_node_bitmap);

		FREE_NULL_BITMAP(para_epilog_idle_node_bitmap[i]);
		para_epilog_idle_node_bitmap[i] = bit_copy(idle_node_bitmap);
	}
}

static void merge_thread_bitmap_to_global(int worker_count)
{
	int i = 0, start = 0, end = 0, bit = 0;

	for (i = 0; i < worker_count; i++) {
		if (i < widened_regional_num) {
			start = i * widened_regional_node_count;
			end = start + widened_regional_node_count - 1;
		} else {
			start = widened_regional_threshold + (i - widened_regional_num) * normal_regional_node_count;
            end = start + normal_regional_node_count - 1;
		}
		if (start >= node_record_count) {
			/* 
			 * When the total number of nodes is less than the number of threads, 
			 * threads exceeding the node count will not be utilized. Their corresponding 
			 * region boundaries will extend beyond the global bitmap boundary, 
			 * and the loop should exit immediately.
			 */
			break;
		}
		if (end >= node_record_count) {
			end = node_record_count - 1;
		}

		if (start > end) {
			start = end;
		}
		debug3("%s: merge region %d start %d end %d into global bitmap.", __func__, i, start, end);

		// idle_node_bitmap
		if (para_epilog_idle_node_bitmap[i]) {
			for (bit = start; bit <= end; bit++) {
				if (bit_test(para_epilog_idle_node_bitmap[i], bit))
					bit_set(idle_node_bitmap, bit);
				else
					bit_clear(idle_node_bitmap, bit);
			}
		}

		// cg_node_bitmap
		if (para_epilog_cg_node_bitmap[i]) {
			for (bit = start; bit <= end; bit++) {
				if (bit_test(para_epilog_cg_node_bitmap[i], bit))
					bit_set(cg_node_bitmap, bit);
				else
					bit_clear(cg_node_bitmap, bit);
			}
		}

		// up_node_bitmap
		if (para_epilog_up_node_bitmap[i]) {
			for (bit = start; bit <= end; bit++) {
				if (bit_test(para_epilog_up_node_bitmap[i], bit))
					bit_set(up_node_bitmap, bit);
				else
					bit_clear(up_node_bitmap, bit);
			}
		}

		// avail_node_bitmap
		if (para_epilog_avail_node_bitmap[i]) {
			for (bit = start; bit <= end; bit++) {
				if (bit_test(para_epilog_avail_node_bitmap[i], bit))
					bit_set(avail_node_bitmap, bit);
				else
					bit_clear(avail_node_bitmap, bit);
			}
		}

		// bf_ignore_node_bitmap
		if (para_epilog_bf_ignore_node_bitmap[i]) {
			for (bit = start; bit <= end; bit++) {
				if (bit_test(para_epilog_bf_ignore_node_bitmap[i], bit))
					bit_set(bf_ignore_node_bitmap, bit);
				else
					bit_clear(bf_ignore_node_bitmap, bit);
			}
		}
	}
}

#endif

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
static void stats_update_callback(async_task_t *task) {
	long delta_usec =
	    (task->end_time.tv_sec - task->start_time.tv_sec) * 1000000 +
	    (task->end_time.tv_usec - task->start_time.tv_usec);

	record_rpc_stats(task->msg, delta_usec);
}

static void *async_worker(void *arg) {
	worker_data_t *worker = (worker_data_t *)arg;
	struct timespec ts = {0, 0};

#if HAVE_SYS_PRCTL_H
	char *name = NULL;
	switch (worker->msg_type) {
		case MESSAGE_EPILOG_COMPLETE:
			name = xstrdup("rpc_queue_epilog_worker");
			break;
		case REQUEST_SUBMIT_BATCH_JOB:
			name = xstrdup("rpc_queue_submit_worker");
			break;
		default:
			error("%s: Invalid message type %s, exit. Only support REQUEST_SUBMIT_BATCH_JOB and MESSAGE_EPILOG_COMPLETE.", __func__, rpc_num2string(worker->msg_type));
			return NULL;
	}
	if (prctl(PR_SET_NAME, name, NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m", __func__, name);
	}
	xfree(name);
#endif

	async_queue_t *queue = worker->queue;
	while (true) {
		async_task_t *task = NULL;

		slurm_mutex_lock(&worker->mutex);

		while (!worker->shutdown && list_count(worker->tasks) == 0) {
			ts.tv_sec = time(NULL) + 1;
			slurm_cond_timedwait(&worker->cond, &worker->mutex, &ts);
		}

		if (worker->shutdown && list_count(worker->tasks) == 0) {
			slurm_mutex_unlock(&worker->mutex);
			break;
		}

		task = list_pop(worker->tasks);
		slurm_mutex_unlock(&worker->mutex);

		if (task) {
			gettimeofday(&task->start_time, NULL);

			task->msg->index = worker->index;
			task->func(task->msg);

			if ((task->msg->conn_fd >= 0) && (close(task->msg->conn_fd) < 0))
				error("close(%d): %m", task->msg->conn_fd);

			gettimeofday(&task->end_time, NULL);

			if (task->stats_callback) {
				task->stats_callback(task);
			}

			/*
	 		 * If atomic.h is enabled, this section of the comments needs to be opened.
			 * if (atomic_fetch_sub(&queue->pending_tasks, 1) == 1) {
			 *  	slurm_mutex_lock(&queue->pending_mutex);
			 *		slurm_cond_signal(&queue->pending_cond);
			 *		slurm_mutex_unlock(&queue->pending_mutex);
			 *	}
			 */
			slurm_mutex_lock(&queue->pending_mutex);
			if (--queue->pending_tasks == 0) {
				slurm_cond_signal(&queue->pending_cond);
			}
			slurm_mutex_unlock(&queue->pending_mutex);

			slurm_free_msg(task->msg);
			xfree(task);
		}
	}

	return NULL;
}

/* Initialize the required resources for a given asynchronous queue */
static void init_async_queue(slurmctld_rpc_t *q) {

	
	if (q->msg_type == REQUEST_SUBMIT_BATCH_JOB) {
		init_submit_resource(q);
	}

	if (q->msg_type == MESSAGE_EPILOG_COMPLETE) {
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_EPILOG_PARALLEL
		if (node_record_count <= rpc_queue_pool_size) {
			normal_regional_node_count = 1;
			widened_regional_num = 0;
			widened_regional_node_count = 0;
			widened_regional_threshold = 0;
		} else {
			normal_regional_node_count = node_record_count / rpc_queue_pool_size;
			widened_regional_num = node_record_count % rpc_queue_pool_size;
			widened_regional_node_count = normal_regional_node_count + 1;
			widened_regional_threshold = widened_regional_num * widened_regional_node_count;
		}

		debug("%s: normal_regional_node_count=%d, widened_regional_node_count=%d, widened_regional_num=%d, widened_regional_threshold=%d, node_record_count=%d, worker_count=%d",
			__func__, normal_regional_node_count, widened_regional_node_count, widened_regional_num, 
			widened_regional_threshold, node_record_count, rpc_queue_pool_size);

		init_epilog_resource(q);
#endif
	}
}

/* Submit the task to the designated worker thread for processing. 
 * If the message type is MESSAGE_EPILOG_COMPLETE, select the worker thread based on the node index; 
 * Otherwise, select the worker thread through polling. After submitting the task, 
 * it will be added to the task list of the target worker thread and trigger a condition variable 
 * to notify the worker thread to process the new task 
 */
static void submit_async_task(slurmctld_rpc_t *q, slurm_msg_t *msg) {
	async_task_t *task = xmalloc(sizeof(async_task_t));
	task->msg = msg;
	task->func = q->func;
	task->stats_callback = stats_update_callback;

	int worker_idx = 0;

	// For parallelization of epilog complete messages, select the worker thread based on the node index.
	if (msg->msg_type == MESSAGE_EPILOG_COMPLETE) {
		node_record_t *node_ptr = NULL;
		epilog_complete_msg_t *epilog_msg = NULL;
		epilog_msg = (epilog_complete_msg_t *)msg->data;
		if (epilog_msg && epilog_msg->node_name && normal_regional_node_count > 0) {
			node_ptr = find_node_record(epilog_msg->node_name);
			if (node_ptr) {
				if (node_record_count <= rpc_queue_pool_size) {
					worker_idx = node_ptr->index;
				} else {
					if (node_ptr->index < widened_regional_threshold && widened_regional_node_count > 0) {
						worker_idx = node_ptr->index / widened_regional_node_count;
					} else {
						worker_idx = (node_ptr->index - widened_regional_threshold) / normal_regional_node_count + widened_regional_num;
					}
				}
				// Ensure that indexing does not exceed bounds.
				worker_idx = (worker_idx >= rpc_queue_pool_size) ? (rpc_queue_pool_size - 1) : worker_idx;
			}
		}
	} else {
		slurm_mutex_lock(&q->async_queue->poll_mutex);
		worker_idx =
		    q->async_queue->next_worker;
		q->async_queue->next_worker = (q->async_queue->next_worker + 1) % q->async_queue->worker_count;
		slurm_mutex_unlock(&q->async_queue->poll_mutex);
	}
	/*
	 * If atomic.h is enabled, this section of the comments needs to be opened.
	 * atomic_fetch_add(&q->async_queue->pending_tasks, 1);	
	 */
	slurm_mutex_lock(&q->async_queue->pending_mutex);
	++q->async_queue->pending_tasks;
	slurm_mutex_unlock(&q->async_queue->pending_mutex);

	debug3("%s: add task to worker %d/%d for msg_type=%u", __func__,
	      worker_idx + 1, q->async_queue->worker_count, msg->msg_type);

	worker_data_t *target_worker = &q->async_queue->workers[worker_idx];
	slurm_mutex_lock(&target_worker->mutex);
	list_append(target_worker->tasks, task);
	slurm_cond_signal(&target_worker->cond);
	slurm_mutex_unlock(&target_worker->mutex);
}

/*
 * This function is used to clean up a given asynchronous queue, including closing all worker threads, 
 * waiting for them to end, cleaning up private resources of each thread, 
 * and cleaning up queue manager resources. 
 */
static void cleanup_async_queue(slurmctld_rpc_t *q) {
	if (!q->async_queue) {
		return;
	}
	int i = 0;
	for (i = 0; i < q->async_queue->worker_count; i++) {
		worker_data_t *worker = &q->async_queue->workers[i];
		slurm_mutex_lock(&worker->mutex);
		worker->shutdown = true;
		slurm_cond_signal(&worker->cond);
		slurm_mutex_unlock(&worker->mutex);
	}

	for (i = 0; i < q->async_queue->worker_count; i++) {
		worker_data_t *worker = &q->async_queue->workers[i];
		slurm_thread_join(worker->thread);
		debug("%s: worker thread %d joined", __func__, i + 1);
	}

	for (i = 0; i < q->async_queue->worker_count; i++) {
		worker_data_t *worker = &q->async_queue->workers[i];
		FREE_NULL_LIST(worker->tasks);
		slurm_mutex_destroy(&worker->mutex);
		slurm_cond_destroy(&worker->cond);
	}

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_SUBMIT_PARALLEL
	if (q->msg_type == REQUEST_SUBMIT_BATCH_JOB) {
		free_para_submit_resource(q->async_queue->worker_count);
	}
#endif
	
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_EPILOG_PARALLEL
	if (q->msg_type == MESSAGE_EPILOG_COMPLETE) {
		free_para_epilog_resource(rpc_queue_pool_size);
	}
#endif

	xfree(q->async_queue->workers);
	slurm_mutex_destroy(&q->async_queue->poll_mutex);
	slurm_mutex_destroy(&q->async_queue->pending_mutex);
	slurm_cond_destroy(&q->async_queue->pending_cond);	
	xfree(q->async_queue);
	q->async_queue = NULL;
}

#endif

static void *_rpc_queue_worker(void *arg)
{
	slurmctld_rpc_t *q = (slurmctld_rpc_t *) arg;
	int processed = 0;
	long processed_usec = 0;
#ifdef __METASTACK_OPT_CACHE_QUERY
	bool local_job_cachedup = false;
	slurmctld_lock_t job_cache_write_lock = {
		NO_LOCK, WRITE_LOCK, NO_LOCK, READ_LOCK, NO_LOCK };
#endif
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
	int async_dispatched = 0;
	struct timeval cycle_start;
	struct timespec ts = {0, 0};
#endif
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_EPILOG_PARALLEL
	bool local_bitmap_initialized = false;
#endif

#if HAVE_SYS_PRCTL_H
	char *name = xstrdup_printf("rpcq-%u", q->msg_type);
	if (prctl(PR_SET_NAME, name, NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m", __func__, "sstate");
	}
	xfree(name);
#endif

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
	q->async_queue = NULL;
	if (rpc_queue_pool_enabled &&
		(q->msg_type == MESSAGE_EPILOG_COMPLETE ||
		 q->msg_type == REQUEST_SUBMIT_BATCH_JOB)) {
		init_async_queue(q);
		switch (q->msg_type) 
		{
			case REQUEST_SUBMIT_BATCH_JOB:
				para_submit_bitmap_init();
				break;

			case MESSAGE_EPILOG_COMPLETE:
				if (!local_bitmap_initialized && enable_para_epilog) {
					copy_private_thread_bitmaps(rpc_queue_pool_size);
				}
				break;

			default:
				break;
		}
	}
#endif

	/*
	 * Acquire on init to simplify the inner loop.
	 * On rpc_queue_init() this will proceed directly to slurm_cond_wait().
	 */
#ifdef __METASTACK_OPT_CACHE_QUERY
	if(q->msg_type == REQUEST_SUBMIT_BATCH_JOB){
		if(cachedup_realtime == 1){
			lock_cache_query(job_cache_write_lock);
			local_job_cachedup = true;
		}
		lock_slurmctld(q->locks);
		if(local_job_cachedup){
			job_cachedup_realtime = 1;
		}else if(cachedup_realtime == 2){
			job_cachedup_realtime = 2;
		}
	}else{
		lock_slurmctld(q->locks);
	}
#else
	lock_slurmctld(q->locks);
#endif

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
	gettimeofday(&cycle_start, NULL);
#endif

	/*
	 * Process as many queued messages as possible in one slurmctld_lock()
	 * acquisition, then fall back to sleep until additional work is queued.
	 */
	while (true) {
		slurm_msg_t *msg = NULL;
		bool highload = false;
		long sleep_usec = 0;

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL		
		struct timeval now;
		// Calculate the time elapsed since the start of the current cycle for RPC latency estimation in asynchronous mode.
		gettimeofday(&now, NULL);		
		long elapsed_usec = (now.tv_sec - cycle_start.tv_sec) * 1000000 +
		                   (now.tv_usec - cycle_start.tv_usec);
		// Make each speed limit effective, if configured.
		if (rpc_queue_pool_enabled && q->async_queue) {
			if ((q->max_per_cycle && (async_dispatched == q->max_per_cycle)) ||
				(q->max_usec_per_cycle && elapsed_usec >= q->max_usec_per_cycle)) {
				/*
				 * When the number or time of distributed messages reaches the limit, 
				 * immediately wait for all asynchronous tasks to complete execution.
				 */
				slurm_mutex_lock(&q->async_queue->pending_mutex);
				/*
	 			 * If atomic.h is enabled, this section of the comments needs to be opened.
				 * while (atomic_load(&q->async_queue->pending_tasks) > 0) {
				 *		slurm_cond_wait(&q->async_queue->pending_cond, 
				 *						&q->async_queue->pending_mutex);
				 * }
				 */
				while (q->async_queue->pending_tasks > 0) {
					ts.tv_sec = time(NULL) + 1;
					slurm_cond_timedwait(&q->async_queue->pending_cond,
									&q->async_queue->pending_mutex, &ts);
				}
				slurm_mutex_unlock(&q->async_queue->pending_mutex);
				highload = true;
			} else {
				msg = list_dequeue(q->work);
			}
		} else {
			if ((q->max_per_cycle && (processed == q->max_per_cycle)) ||
				(q->max_usec_per_cycle &&
				(processed_usec >= q->max_usec_per_cycle))) {
				highload = true;
			} else {
				msg = list_dequeue(q->work);
			}
		}
#endif

		if (!msg) {
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL			
			if (rpc_queue_pool_enabled && q->async_queue) {
				slurm_mutex_lock(&q->async_queue->pending_mutex);
				/*
				 * If atomic.h is enabled, this section of the comments needs to be opened.
				 * while (atomic_load(&q->async_queue->pending_tasks) > 0) {
				 *		slurm_cond_wait(&q->async_queue->pending_cond, 
				 *						&q->async_queue->pending_mutex);
				 * }
				 */
				while (q->async_queue->pending_tasks > 0) {
					ts.tv_sec = time(NULL) + 1;
					slurm_cond_timedwait(&q->async_queue->pending_cond, 
									&q->async_queue->pending_mutex, &ts);
				}
				slurm_mutex_unlock(&q->async_queue->pending_mutex);
				processed = async_dispatched;
			}
#endif

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_EPILOG_PARALLEL
			if (q->msg_type == MESSAGE_EPILOG_COMPLETE) {
				if (local_bitmap_initialized) {
					merge_thread_bitmap_to_global(rpc_queue_pool_size);
					local_bitmap_initialized = false;
					schedule(false);
				}
			}
#endif
#ifdef __METASTACK_OPT_CACHE_QUERY
			if(q->msg_type == REQUEST_SUBMIT_BATCH_JOB){
				job_cachedup_realtime = 0;
				unlock_slurmctld(q->locks);
				if(local_job_cachedup){
					unlock_cache_query(job_cache_write_lock);
					local_job_cachedup = false;
				}
			}else{
				unlock_slurmctld(q->locks);
			}
#else
			unlock_slurmctld(q->locks);
#endif

			if (processed && q->post_func)
				q->post_func();

			if (processed) {
				slurm_mutex_lock(&q->mutex);
				q->cycle_last = processed;
				if (processed > q->cycle_max)
					q->cycle_max = processed;
				record_rpc_queue_stats(q);
				slurm_mutex_unlock(&q->mutex);
			}

			/*
			 * Use yield_sleep if there's more work to be done,
			 * otherwise interval if set, otherwise 500 usec.
			 */
			if (highload && (q->yield_sleep > 0))
				sleep_usec = q->yield_sleep;
			else if (q->interval > 0)
				sleep_usec = q->interval;
			else
				sleep_usec = 500;

			/*
			 * Rate limit RPC processing. Ensure that when we
			 * stop processing we don't immediately start again
			 * by inserting a slight delay.
			 *
			 * This encourages additional RPCs to accumulate,
			 * which is desirable as it lowers pressure on the
			 * slurmctld locks.
			 *
			 * This extends the race described below, but this
			 * is handled properly.
			 */

			log_flag(PROTOCOL, "%s(%s): sleeping %ld usec after processing %d/%u msgs (processed_usec=%ld/%d)",
				 __func__, q->msg_name, sleep_usec,
				 processed, q->max_per_cycle,
				 processed_usec, q->max_usec_per_cycle);
			async_dispatched = 0;
			processed = 0;
			processed_usec = 0;
			usleep(sleep_usec);

			slurm_mutex_lock(&q->mutex);

			if (q->shutdown && !list_count(q->work)) {
				log_flag(PROTOCOL, "%s(%s): shutting down",
					 __func__, q->msg_name);
				slurm_mutex_unlock(&q->mutex);
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
				if (rpc_queue_pool_enabled) {
					cleanup_async_queue(q);
				}
#endif
				return NULL;
			}

			/*
			 * Verify list is empty. Since list_dequeue() above is
			 * called without the mutex held, there is a race with
			 * rpc_enqueue() that this check will solve.
			 */
			if (!list_count(q->work))
				slurm_cond_wait(&q->cond, &q->mutex);

			slurm_mutex_unlock(&q->mutex);
			log_flag(PROTOCOL, "%s(%s): woke up",
				 __func__, q->msg_name);
#ifdef __METASTACK_OPT_CACHE_QUERY
			if(q->msg_type == REQUEST_SUBMIT_BATCH_JOB){
				if(cachedup_realtime == 1){
					lock_cache_query(job_cache_write_lock);
					local_job_cachedup = true;
				}
				lock_slurmctld(q->locks);
				if(local_job_cachedup){
					job_cachedup_realtime = 1;
				}else if(cachedup_realtime == 2){
					job_cachedup_realtime = 2;
				}
			}else{
				lock_slurmctld(q->locks);
			}
#else
			lock_slurmctld(q->locks);
#endif
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
			gettimeofday(&cycle_start, NULL);
#endif
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_SUBMIT_PARALLEL
			if (q->msg_type == REQUEST_SUBMIT_BATCH_JOB) {
				para_submit_bitmap_init();
			}
#endif
		} else {
#ifndef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
			DEF_TIMERS;
			START_TIMER;

			if (q->max_queued) {
				slurm_mutex_lock(&q->mutex);
				q->queued--;
				record_rpc_queue_stats(q);
				slurm_mutex_unlock(&q->mutex);
			}
#else
			slurm_mutex_lock(&q->mutex);
			q->queued--;
			record_rpc_queue_stats(q);
			slurm_mutex_unlock(&q->mutex);
#endif

			msg->flags |= CTLD_QUEUE_PROCESSING;
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
			if (rpc_queue_pool_enabled && q->async_queue) {
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_EPILOG_PARALLEL
				if (msg->msg_type == MESSAGE_EPILOG_COMPLETE) {
					if (!local_bitmap_initialized && enable_para_epilog) {
						copy_private_thread_bitmaps(rpc_queue_pool_size);
						local_bitmap_initialized = true;
					}
				}
#endif
				DEF_TIMERS;
				START_TIMER;
				submit_async_task(q, msg);
				END_TIMER;
				async_dispatched++;
				processed_usec += DELTA_TIMER;
			} else {
				DEF_TIMERS;
				START_TIMER;
				q->func(msg);
				if ((msg->conn_fd >= 0) && (close(msg->conn_fd) < 0))
					error("close(%d): %m", msg->conn_fd);
				END_TIMER;
				record_rpc_stats(msg, DELTA_TIMER);
				slurm_free_msg(msg);
				processed++;
				processed_usec += DELTA_TIMER;
			}
#else
			q->func(msg);
			if ((msg->conn_fd >= 0) && (close(msg->conn_fd) < 0))
				error("close(%d): %m", msg->conn_fd);

			END_TIMER;
			record_rpc_stats(msg, DELTA_TIMER);
			slurm_free_msg(msg);
			processed++;
			processed_usec += DELTA_TIMER;
#endif
		}
	}

	return NULL;
}

static data_t *_load_config(void)
{
	char *file = get_extra_conf_path("rpc_queue.yaml");
	buf_t *buf = create_mmap_buf(file);
	data_t *conf = NULL;

	if (!buf) {
		debug("%s: could not load %s, ignoring", __func__, file);
		xfree(file);
		return NULL;
	}

	if (serialize_g_string_to_data(&conf, buf->head, buf->size,
				       MIME_TYPE_YAML))
		fatal("Failed to decode %s", file);

	FREE_NULL_BUFFER(buf);
	xfree(file);
	return conf;
}

static bool _find_msg_name(const data_t *data, void *needle)
{
	const data_t *type = NULL;

	if (data_get_type(data) != DATA_TYPE_DICT)
		return false;

	type = data_key_get_const(data, "type");

	if (data_get_type(type) != DATA_TYPE_STRING)
		return false;

	return !xstrcasecmp(data_get_string_const(type), needle);
}

static void _apply_config(data_t *conf, slurmctld_rpc_t *q)
{
	data_t *rpc_queue = NULL, *settings = NULL, *field = NULL;
	int64_t int64_tmp;

	if (!conf || !q)
		return;

	rpc_queue = data_key_get(conf, "rpc_queue");
	if (data_get_type(rpc_queue) != DATA_TYPE_LIST)
		return;

	if (!(settings = data_list_find_first(rpc_queue, _find_msg_name,
					      (void *) q->msg_name)))
		return;

	if ((field = data_key_get(settings, "disabled"))) {
		bool disabled = false;
		if (!data_get_bool_converted(field, &disabled)) {
			q->queue_enabled = false;
			return;
		}
	}

	if ((field = data_key_get(settings, "hard_drop")))
		(void) data_get_bool_converted(field, &q->hard_drop);

	if ((field = data_key_get(settings, "max_per_cycle")))
		if (!data_get_int_converted(field, &int64_tmp))
			q->max_per_cycle = int64_tmp;

	if ((field = data_key_get(settings, "max_usec_per_cycle")))
		if (!data_get_int_converted(field, &int64_tmp))
			q->max_usec_per_cycle = int64_tmp;

	if ((field = data_key_get(settings, "max_queued")))
		if (!data_get_int_converted(field, &int64_tmp))
			q->max_queued = int64_tmp;

	if ((field = data_key_get(settings, "yield_sleep")))
		if (!data_get_int_converted(field, &int64_tmp))
			q->yield_sleep = int64_tmp;

	if ((field = data_key_get(settings, "interval")))
		if (!data_get_int_converted(field, &int64_tmp))
			q->interval = int64_tmp;
}

extern void rpc_queue_init(void)
{
	data_t *conf = NULL;

	if (!xstrcasestr(slurm_conf.slurmctld_params, "enable_rpc_queue")) {
		enabled = false;
		return;
	}

	info("enabled experimental rpc queuing system");

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
	enabled = true;
	rpc_queue_thread_pool_init();
#endif

	conf = _load_config();

	for (slurmctld_rpc_t *q = slurmctld_rpcs; q->msg_type; q++) {
		if (!q->queue_enabled)
			continue;

		q->msg_name = rpc_num2string(q->msg_type);

		_apply_config(conf, q);

		/* config may have disabled this queue, check again */
		if (!q->queue_enabled) {
			verbose("disabled rpc_queue for %s", q->msg_name);
			continue;
		}

		q->work = list_create(NULL);
		slurm_cond_init(&q->cond, NULL);
		slurm_mutex_init(&q->mutex);
		q->shutdown = false;

		verbose("starting rpc_queue for %s: max_per_cycle=%u max_usec_per_cycle=%u max_queued=%d hard_drop=%d yield_sleep=%d interval=%d",
			q->msg_name, q->max_per_cycle, q->max_usec_per_cycle,
			q->max_queued, q->hard_drop, q->yield_sleep,
			q->interval);
		slurm_thread_create(&q->thread, _rpc_queue_worker, q);
	}

	FREE_NULL_DATA(conf);
}

extern void rpc_queue_shutdown(void)
{
	if (!enabled)
		return;

	enabled = false;

	/* mark all as shut down */
	for (slurmctld_rpc_t *q = slurmctld_rpcs; q->msg_type; q++) {
		if (!q->queue_enabled)
			continue;

		slurm_mutex_lock(&q->mutex);
		q->shutdown = true;
		slurm_cond_signal(&q->cond);
		slurm_mutex_unlock(&q->mutex);
	}

	/* wait for completion and cleanup */
	for (slurmctld_rpc_t *q = slurmctld_rpcs; q->msg_type; q++) {
		if (!q->queue_enabled)
			continue;

		slurm_thread_join(q->thread);
		FREE_NULL_LIST(q->work);
	}
}

extern bool rpc_queue_enabled(void)
{
	return enabled;
}

extern int rpc_enqueue(slurm_msg_t *msg)
{
	if (!enabled)
		return ESLURM_NOT_SUPPORTED;

	for (slurmctld_rpc_t *q = slurmctld_rpcs; q->msg_type; q++) {
		if (q->msg_type == msg->msg_type) {
			if (!q->queue_enabled)
				break;

			if (q->max_queued) {
				slurm_mutex_lock(&q->mutex);
				if (q->queued >= q->max_queued) {
					q->dropped++;
					record_rpc_queue_stats(q);
					slurm_mutex_unlock(&q->mutex);
					if (q->hard_drop)
						return SLURMCTLD_COMMUNICATIONS_HARD_DROP;
					else
						return SLURMCTLD_COMMUNICATIONS_BACKOFF;
				}
				q->queued++;
				record_rpc_queue_stats(q);
				slurm_mutex_unlock(&q->mutex);
			}
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_RPC_QUEUE_THREAD_POOL
			else {
				slurm_mutex_lock(&q->mutex);
				q->queued++;
				record_rpc_queue_stats(q);
				slurm_mutex_unlock(&q->mutex);				
			}
#endif
			list_enqueue(q->work, msg);
			slurm_mutex_lock(&q->mutex);
			slurm_cond_signal(&q->cond);
			slurm_mutex_unlock(&q->mutex);
			return SLURM_SUCCESS;
		}
	}

	/* RPC does not have a dedicated queue */
	return ESLURM_NOT_SUPPORTED;
}
