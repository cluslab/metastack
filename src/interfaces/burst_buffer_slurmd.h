#ifndef _INTERFACES_BURST_BUFFER_SLURMD_H
#define _INTERFACES_BURST_BUFFER_SLURMD_H

#include "slurm/slurm.h"

/*
 * Initialize the bb_api library infrastructure.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_init(void);

/*
 * Terminate the bb_api library infrastructure. Free memory.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_fini(void);

/*
 **************************************************************************
 *                    B B   A P I   W R A P P E R   F U N C T I O N S    *
 **************************************************************************
 */

/**
 * Create a Parastor cache group keyed by group serial name (group_sn).
 *
 * @param group_sn		Group serial identifier from the backend
 * @param client_cnt		Number of client hosts in the group
 * @param client_hostname_arr	Array of client host names
 * @param group_id		Filled with assigned cache group id on success
 *
 * @return bb_api status: 0 success; -1 internal error; -2 API error; -3 timeout
 */
extern int bb_g_create_bb_group_by_sn(char *group_sn, uint32_t client_cnt, char **client_hostname_arr, uint32_t *group_id);

/**
 * Create a dataset (staging) rule under a cache group.
 *
 * @param group_sn		Group serial name (must match existing group)
 * @param group_id		Numeric cache group id
 * @param path			Accelerated / PFS path backing this dataset
 * @param is_use_metadata	Whether metadata acceleration is enabled
 * @param is_share_cache	true = shared cache mode; false = local cache mode
 * @param dataset_id		Filled with new dataset rule id on success
 *
 * @return bb_api status: 0 success; -1 internal error; -2 API error; -3 timeout
 */
extern int bb_g_create_bb_dataset_by_sn(char *group_sn, uint32_t group_id ,char *path, bool is_use_metadata, bool is_share_cache, uint32_t *dataset_id);

/**
 * Submit a burst-buffer task (e.g. prefetch or recycle) for a dataset.
 *
 * @param dataset_id	Dataset rule id from bb_g_create_bb_dataset_by_sn()
 * @param task_type	Task kind (plugin-defined; e.g. prefetch vs recycle)
 * @param task_id	Filled with new task id on success
 *
 * @return bb_api status: 0 success; -1 internal error; -2 API error; -3 timeout
 */
extern int bb_g_submit_bb_task(uint32_t dataset_id, int task_type, uint32_t *task_id);

/**
 * Wait until a burst-buffer task finishes.
 *
 * @param task_id	Task id returned by bb_g_submit_bb_task()
 * @param task_type	Must match the task type used at submit time
 *
 * @return bb_api status: 0 success (task completed); -1 internal; -2 API; -3 timeout
 */
extern int bb_g_wait_task_complete(uint32_t task_id, int task_type);

/**
 * Destroy a cache group by serial name.
 *
 * @param group_sn	Group serial identifier
 *
 * @return bb_api status: 0 success; -1 internal error; -2 API error; -3 timeout
 */
extern int bb_g_delete_bb_group_by_sn(char *group_sn);

/**
 * Destroy a cache group by numeric id.
 *
 * @param group_id	Cache group id
 *
 * @return bb_api status: 0 success; -1 internal error; -2 API error; -3 timeout
 */
extern int bb_g_delete_bb_group_by_id(uint32_t group_id);

/**
 * Delete a dataset rule by id; group_id and path help the backend recover if
 * the primary delete times out or needs reconciliation.
 *
 * @param dataset_id	Dataset rule id
 * @param group_id	Associated cache group id
 * @param path		Accelerated path for the dataset rule
 *
 * @return bb_api status: 0 success; -1 internal error; -2 API error; -3 timeout
 */
extern int bb_g_delete_bb_dataset_by_id(uint32_t dataset_id, uint32_t group_id, char * path);

/**
 * Cancel a submitted burst-buffer task.
 *
 * @param task_id	Task id to cancel
 *
 * @return bb_api status: 0 success; -1 internal error; -2 API error; -3 timeout
 */
extern int bb_g_cancel_bb_task_by_id(uint32_t task_id);

/**
 * Delete a dataset rule by cache group id and accelerated path.
 *
 * @param group_id	Cache group id
 * @param path		Dataset path under that group
 *
 * @return bb_api status: 0 success; -1 internal error; -2 API error; -3 timeout
 */
extern int bb_g_delete_bb_dataset_by_groupid_path(uint32_t group_id, char *path);

#endif
