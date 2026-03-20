#define _GNU_SOURCE

#include <unistd.h>
#include "slurm/slurm.h"
#include "src/common/log.h"
#include "src/common/read_config.h"
#include "src/common/xstring.h"
#include "src/plugins/burst_buffer/common/burst_buffer_common.h"
#include "bb_curl_wrapper.h"
#include "bb_api.h"
#include "src/common/run_in_daemon.h"
#include "src/common/xmalloc.h"

/*
 * Plugin identity symbols for burst_buffer common code and dlopen:
 * avoids undefined symbol: plugin_type when loading libbb_api.
 */
const char plugin_name[]    = "burst_buffer parastor slurmd plugin";
const char plugin_type[]    = "burst_buffer/parastor_slurmd";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;

static void _test_config();
static bb_state_t bb_state;
static char *directive_str;
static int directive_len = 0;
/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */
extern int init(void)
{
	int rc = SLURM_SUCCESS;
	int count = 3;
	if (!running_in_slurmd()) {
		return SLURM_SUCCESS;
	}
	//slurm_mutex_init(&parastor_thread_mutex);
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
	if (rc != SLURM_SUCCESS) {
		error("%s: failed to obtain permanent API token after retries", __func__);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return rc;
	}
	//bb_alloc_cache(&bb_state);
	slurm_mutex_unlock(&bb_state.bb_mutex);
	return SLURM_SUCCESS;
}

/*
 * fini() is called when the plugin is unloaded. Free all memory.
 */
extern int fini(void)
{
	slurm_mutex_lock(&bb_state.bb_mutex);
	log_flag(BURST_BUF, "%s: parastor slurmd burst_buffer plugin unloaded", __func__);

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

/* Validate burst buffer configuration */
static void _test_config()
{
		/* 24-day max time limit. (2073600 seconds) */
	static uint32_t max_timeout = (60 * 60 * 24 * 24);
	uint32_t max_groups = 2048;
 	uint32_t max_datasets = 8196;
	uint32_t max_node_per_groups = 1024;
	if (bb_state.bb_config.get_sys_state) {
		info("%s: get_sys_state is unused in this plugin, unsetting", plugin_type);
		xfree(bb_state.bb_config.get_sys_state);
	}
	if (bb_state.bb_config.get_sys_status) {
		info("%s: get_sys_status is unused in this plugin, unsetting", plugin_type);
		xfree(bb_state.bb_config.get_sys_status);
	}
	if (bb_state.bb_config.flags & BB_FLAG_EMULATE_CRAY) {
		info("%s: flags=EmulateCray is invalid for this plugin, unsetting", plugin_type);
		bb_state.bb_config.flags &= (~BB_FLAG_EMULATE_CRAY);
	}
	if (bb_state.bb_config.directive_str) {
		directive_str = bb_state.bb_config.directive_str;
		directive_len = strlen(directive_str);
	}

	if (bb_state.bb_config.default_pool) {
		info("%s: DefaultPool=%s is unused for this plugin, unsetting",
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
		warning("%s: OtherTimeout=%u exceeds maximum %u, clamping",
			plugin_type, bb_state.bb_config.other_timeout, max_timeout);
		bb_state.bb_config.other_timeout = max_timeout;
	}
	if (bb_state.bb_config.stage_in_timeout > max_timeout) {
		warning("%s: StageInTimeout=%u exceeds maximum %u, clamping",
			plugin_type, bb_state.bb_config.stage_in_timeout, max_timeout);
		bb_state.bb_config.stage_in_timeout = max_timeout;
	}
	if (bb_state.bb_config.stage_out_timeout > max_timeout) {
		warning("%s: StageOutTimeout=%u exceeds maximum %u, clamping",
			plugin_type, bb_state.bb_config.stage_out_timeout, max_timeout);
		bb_state.bb_config.stage_out_timeout = max_timeout;
	}
	if (bb_state.bb_config.max_groups > max_groups) {
		warning("%s: MaxGroups=%u exceeds maximum %u, clamping",
			plugin_type, bb_state.bb_config.max_groups, max_groups);
		bb_state.bb_config.max_groups = max_groups;
	}
	if (bb_state.bb_config.max_datasets > max_datasets) {
		warning("%s: MaxDatasets=%u exceeds maximum %u, clamping",
			plugin_type, bb_state.bb_config.max_datasets, max_datasets);
		bb_state.bb_config.max_datasets = max_datasets;
	}
	if (bb_state.bb_config.max_clients_per_job > max_node_per_groups) {
		warning("%s: MaxClientsPerJob=%u exceeds maximum %u, clamping",
			plugin_type, bb_state.bb_config.max_clients_per_job,
			max_node_per_groups);
		bb_state.bb_config.max_clients_per_job = max_node_per_groups;
	}
}


/**
 * @brief Create a cache group by serial number (SN) and client hostnames.
 * @param group_id Out: new group id on success
 * @return BB_* / SLURM_* style codes from bb_api (0 success, negative errors)
 */
extern int bb_p_create_bb_group_by_sn(char *group_sn, int client_cnt, char **client_hostname_arr, uint32_t *group_id)
{
	int rc = SLURM_ERROR;
	if (!group_sn || !client_hostname_arr || client_cnt < 0) {
		error("%s: invalid arguments", __func__);
		return SLURM_ERROR;
	}
	/* Ownership transferred to create_params; freed by free_create_params */
	uint32_t *client_ids = xmalloc(client_cnt * sizeof(uint32_t));
	bool query_success = true;
	slurm_mutex_lock(&bb_state.bb_mutex);
	for (int i = 0; i < client_cnt; i++) {
		if (!client_hostname_arr[i]) {
			error("%s: hostname[%d] is NULL", __func__, i);
			query_success = false;
			break;
		}
		rc = query_clientid_by_hostname(client_hostname_arr[i], &client_ids[i], &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			log_flag(BURST_BUF, "%s: resolved client_id=%u for host %s",
				 __func__, client_ids[i], client_hostname_arr[i]);
		} else if (rc == BB_SUCCESS_NO_DATA) {
			error("%s: no client for hostname %s", __func__, client_hostname_arr[i]);
			query_success = false;
			break;
		} else {
			error("%s: query_clientid_by_hostname failed for %s, rc=%d",
			      __func__, client_hostname_arr[i], rc);
			query_success = false;
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (!query_success) {
		xfree(client_ids);
		return SLURM_ERROR;
	}
	create_params_request *create_params = xmalloc(sizeof(create_params_request));
	create_params->group_sn = xstrdup(group_sn);
	create_params->client_count = client_cnt;
	create_params->client_ids = client_ids;
	slurm_mutex_lock(&bb_state.bb_mutex);
	bool create_group_err_logged = false;
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = create_bb_group_by_sn(create_params, group_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			log_flag(BURST_BUF, "%s: created cache group, group_id=%u", __func__, *group_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("%s: create_bb_group_by_sn internal error", __func__);
			create_group_err_logged = true;
			break;
		} else if (rc == BB_API_ERROR) {
			error("%s: create_bb_group_by_sn API error", __func__);
			create_group_err_logged = true;
			break;
		} else if (rc == BB_API_TIMEOUT) {
			log_flag(BURST_BUF, "%s: create timed out; verifying whether group exists", __func__);
			int query_rc = query_bb_groupid_by_sn(group_sn, group_id, &bb_state.bb_config);
			if (query_rc < 0) {
				error("%s: query_bb_groupid_by_sn failed after timeout", __func__);
				create_group_err_logged = true;
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				log_flag(BURST_BUF, "%s: group not found yet, retry %d/%d",
					 __func__, retry_count + 1, bb_state.bb_config.retry_count);
				continue;
			}
			if (query_rc == BB_SUCCESS) {
				log_flag(BURST_BUF, "%s: group exists after timeout query", __func__);
				rc = query_rc;
				break;
			}
		} else {
			error("%s: unexpected rc=%d from create_bb_group_by_sn", __func__, rc);
			create_group_err_logged = true;
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	free_create_params(create_params);
	if (rc < 0 && !create_group_err_logged) {
		error("%s: failed to create cache group sn=%s, rc=%d", __func__, group_sn, rc);
	}
	return rc;
}

extern int bb_p_create_bb_dataset_by_sn(char *group_sn, uint32_t group_id ,char *path, bool is_use_metadata, bool is_share_cache, uint32_t *dataset_id)
{
	int rc = SLURM_ERROR;
	if (!group_sn || !path) {
		error("%s: invalid arguments", __func__);
		return SLURM_ERROR;
	}
	create_params_request *create_params = xmalloc(sizeof(create_params_request));
	create_params->group_sn = xstrdup(group_sn);
	create_params->path = xstrdup(path);
	create_params->group_id = group_id;
	create_params->is_use_metadata = is_use_metadata;
	if (is_share_cache == true) {
		create_params->data_cache_type = SHARE_CACHE;
	} else {
		create_params->data_cache_type = LOCAL_CACHE;
	}
	slurm_mutex_lock(&bb_state.bb_mutex);
	bool create_ds_err_logged = false;
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = create_bb_dataset_by_sn(create_params, dataset_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			log_flag(BURST_BUF, "%s: created dataset rule, dataset_id=%u", __func__, *dataset_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("%s: create_bb_dataset_by_sn internal error", __func__);
			create_ds_err_logged = true;
			break;
		} else if (rc == BB_API_ERROR) {
			error("%s: create_bb_dataset_by_sn API error", __func__);
			create_ds_err_logged = true;
			break;
		} else if (rc == BB_API_TIMEOUT) {
			log_flag(BURST_BUF, "%s: create dataset timed out; verifying", __func__);
			int query_rc = query_datasetid_by_path_groupid(group_id, path, dataset_id, &bb_state.bb_config);
			if (query_rc < 0) {
				error("%s: query_datasetid_by_path_groupid failed after timeout", __func__);
				create_ds_err_logged = true;
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				log_flag(BURST_BUF, "%s: dataset not found yet, retry %d/%d",
					 __func__, retry_count + 1, bb_state.bb_config.retry_count);
				continue;
			}
			if (query_rc == BB_SUCCESS) {
				log_flag(BURST_BUF, "%s: dataset exists after timeout query", __func__);
				rc = query_rc;
				break;
			}
		} else {
			error("%s: create_bb_dataset_by_sn unexpected rc=%d", __func__, rc);
			create_ds_err_logged = true;
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	free_create_params(create_params);
	if (rc < 0 && !create_ds_err_logged) {
		error("%s: failed to create dataset rule, rc=%d", __func__, rc);
	}
	return rc;
}


extern int bb_p_submit_bb_task(uint32_t dataset_id, int task_type, uint32_t *task_id)
{
	int rc = SLURM_ERROR;
	if (dataset_id <= 0 || (task_type != BURST_BUFFER_TASK_TYPE_PREFETCH && task_type != BURST_BUFFER_TASK_TYPE_RECYCLE)) {
		error("%s: invalid arguments", __func__);
		return SLURM_ERROR;
	}
	create_params_request *create_params = xmalloc(sizeof(create_params_request));
	create_params->dataset_id = dataset_id;
	create_params->task_type = task_type;
	create_params->error_action_type = 0;
	slurm_mutex_lock(&bb_state.bb_mutex);
	rc = submit_bb_task(create_params, task_id, &bb_state.bb_config);
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (rc == 0) {
		log_flag(BURST_BUF, "%s: submitted task, task_id=%u", __func__, *task_id);
	} else if (rc == -1) {
		error("%s: submit_bb_task internal error", __func__);
	} else if (rc == -2) {
		error("%s: submit_bb_task API error", __func__);
	} else if (rc == -3) {
		error("%s: submit_bb_task timed out", __func__);
	} else {
		error("%s: submit_bb_task failed, rc=%d", __func__, rc);
	}
	free_create_params(create_params);
	return rc;
}


extern int bb_p_wait_task_complete(uint32_t task_id, int task_type)
{
	if (task_id <= 0 || (task_type != BURST_BUFFER_TASK_TYPE_PREFETCH && task_type != BURST_BUFFER_TASK_TYPE_RECYCLE)) {
		error("%s: invalid arguments", __func__);
		return SLURM_ERROR;
	}
	int rc = SLURM_ERROR;
	time_t HARD_TIMEOUT_SEC = 0;
	slurm_mutex_lock(&bb_state.bb_mutex);
	const time_t CHECK_INTERVAL_SEC = bb_state.bb_config.poll_interval;
	const time_t SOFT_TIMEOUT_SEC = bb_state.bb_config.other_timeout;

	if (task_type == BURST_BUFFER_TASK_TYPE_PREFETCH)
		HARD_TIMEOUT_SEC = bb_state.bb_config.stage_in_timeout;
	else if (task_type == BURST_BUFFER_TASK_TYPE_RECYCLE)
		HARD_TIMEOUT_SEC = bb_state.bb_config.stage_out_timeout;
	else {
		error("%s: invalid task_type=%d", __func__, task_type);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return SLURM_ERROR;
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (HARD_TIMEOUT_SEC <= 0 || SOFT_TIMEOUT_SEC < 0 || CHECK_INTERVAL_SEC <= 0) {
		error("%s: invalid timeout config HARD=%ld SOFT=%ld INTERVAL=%ld",
		      __func__, (long)HARD_TIMEOUT_SEC, (long)SOFT_TIMEOUT_SEC,
		      (long)CHECK_INTERVAL_SEC);
		return SLURM_ERROR;
	}
	
	time_t start_time = time(NULL);
	time_t last_check_time = start_time;
	time_t hard_timeout_time = start_time + HARD_TIMEOUT_SEC;
	bool task_completed = false;
	int query_rc = 0;
	int query_fail_count = 0;
	bb_attribute_task *bb_task = xmalloc(sizeof(bb_attribute_task));

	while (!task_completed) {
		time_t current_time = time(NULL);

		bool need_check = false;
		if (current_time - last_check_time >= CHECK_INTERVAL_SEC) {
			need_check = true;
		} else if (current_time >= hard_timeout_time) {
			/* At hard deadline, poll once even if interval not elapsed */
			need_check = true;
		}

		if (!need_check) {
			sleep(1);
			continue;
		}

		slurm_mutex_lock(&bb_state.bb_mutex);
		query_rc = query_bb_task_by_taskid(task_id, &bb_state.bb_config, bb_task);
		slurm_mutex_unlock(&bb_state.bb_mutex);

		if (query_rc != BB_SUCCESS) {
			if (query_rc == BB_SUCCESS_NO_DATA) {
				task_completed = true;
				rc = BB_SUCCESS;
				break;
			} else if (query_rc < 0) {
				query_fail_count++;
				if (query_fail_count == 1 || (query_fail_count % 10) == 0) {
					log_flag(BURST_BUF,
						 "%s: query_bb_task_by_taskid failed, rc=%d (retry %d)",
						 __func__, query_rc, query_fail_count);
				}
				last_check_time = current_time;
				sleep(1);
				continue;
			}
		} else {
			query_fail_count = 0;
		}

		if (bb_task->task_state == BB_TASK_STATE_COMPLETED) {
			task_completed = true;
			rc = BB_SUCCESS;
			break;
		} else if (bb_task->task_state == BB_TASK_STATE_FAILED ||
			bb_task->task_state == BB_TASK_STATE_CANCELED) {
			error("%s: task failed or canceled, task_id=%u state=%d",
				__func__, task_id, bb_task->task_state);
			free_bb_task(bb_task);
			return SLURM_ERROR;
		} else if (bb_task->task_state == BB_TASK_STATE_SUBMITTING ||
			bb_task->task_state == BB_TASK_STATE_RUNNING) {
			log_flag(BURST_BUF, "%s: task still running, task_id=%u state=%d",
				 __func__, task_id, bb_task->task_state);
		} else {
			error("%s: unexpected task state, task_id=%u state=%d",
			      __func__, task_id, bb_task->task_state);
			free_bb_task(bb_task);
			return SLURM_ERROR;
		}

		time_t elapsed_time = time(NULL) - start_time;
		if (elapsed_time >= HARD_TIMEOUT_SEC) {
			error("%s: wait for task timed out, task_id=%u waited=%lds",
			      __func__, task_id, (long)elapsed_time);
			free_bb_task(bb_task);
			return SLURM_ERROR;
		}

		last_check_time = time(NULL);

		time_t sleep_time = CHECK_INTERVAL_SEC;
		time_t remain_time = HARD_TIMEOUT_SEC - (last_check_time - start_time);
		if (remain_time < sleep_time) {
			sleep_time = remain_time;
		}
		if (sleep_time > 0) {
			sleep(sleep_time);
		}
	}
	free_bb_task(bb_task);
	return rc;

}


extern int bb_p_delete_bb_group_by_sn(char *group_sn)
{
	int rc = SLURM_ERROR;
	if (!group_sn) {
		error("%s: invalid arguments", __func__);
		return SLURM_ERROR;
	}
	slurm_mutex_lock(&bb_state.bb_mutex);
	bool del_sn_err_logged = false;
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = delete_bb_group_by_sn(group_sn, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			log_flag(BURST_BUF, "%s: deleted cache group sn=%s", __func__, group_sn);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("%s: delete_bb_group_by_sn internal error", __func__);
			del_sn_err_logged = true;
			break;
		} else if (rc == BB_API_ERROR) {
			error("%s: delete_bb_group_by_sn API error", __func__);
			del_sn_err_logged = true;
			break;
		} else if (rc == BB_API_TIMEOUT) {
			log_flag(BURST_BUF, "%s: delete timed out; verifying", __func__);
			int query_rc = query_bb_groupid_by_sn(group_sn, &(uint32_t){0}, &bb_state.bb_config);
			if (query_rc < 0) {
				error("%s: query_bb_groupid_by_sn failed after timeout", __func__);
				del_sn_err_logged = true;
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				log_flag(BURST_BUF, "%s: cache group already gone", __func__);
				rc = 0;
				break;
			}
			if (query_rc == BB_SUCCESS) {
				log_flag(BURST_BUF, "%s: delete timeout, retry %d/%d",
					 __func__, retry_count + 1, bb_state.bb_config.retry_count);
				continue;
			}
		} else {
			error("%s: unexpected rc=%d", __func__, rc);
			del_sn_err_logged = true;
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (rc != 0 && !del_sn_err_logged) {
		error("%s: failed to delete cache group sn=%s, rc=%d", __func__, group_sn, rc);
	}
	return rc;
}

extern int bb_p_delete_bb_group_by_id(uint32_t group_id)
{
	int rc = SLURM_ERROR;
	if (group_id == 0) {
		error("%s: invalid arguments", __func__);
		return SLURM_ERROR;
	}
	slurm_mutex_lock(&bb_state.bb_mutex);
	bool del_id_err_logged = false;
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = delete_bb_group_by_id(group_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			log_flag(BURST_BUF, "%s: deleted cache group id=%u", __func__, group_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("%s: delete_bb_group_by_id internal error", __func__);
			del_id_err_logged = true;
			break;
		} else if (rc == BB_API_ERROR) {
			error("%s: delete_bb_group_by_id API error", __func__);
			del_id_err_logged = true;
			break;
		} else if (rc == BB_API_TIMEOUT) {
			log_flag(BURST_BUF, "%s: delete timed out; verifying", __func__);
			int query_rc = has_bb_group_by_id(group_id, &bb_state.bb_config);
			if (query_rc < 0) {
				error("%s: has_bb_group_by_id failed after timeout", __func__);
				del_id_err_logged = true;
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				log_flag(BURST_BUF, "%s: cache group already gone", __func__);
				rc = 0;
				break;
			}
			if (query_rc == BB_SUCCESS) {
				log_flag(BURST_BUF, "%s: delete timeout, retry %d/%d",
					 __func__, retry_count + 1, bb_state.bb_config.retry_count);
				continue;
			}
		} else {
			error("%s: unexpected rc=%d", __func__, rc);
			del_id_err_logged = true;
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (rc != 0 && !del_id_err_logged) {
		error("%s: failed to delete cache group id=%u, rc=%d", __func__, group_id, rc);
	}
	return rc;
}



extern int bb_p_delete_bb_dataset_by_id(uint32_t dataset_id, uint32_t group_id, char * path)
{
	int rc = SLURM_ERROR;
	if (dataset_id <= 0) {
		error("%s: invalid arguments", __func__);
		return SLURM_ERROR;
	}
	slurm_mutex_lock(&bb_state.bb_mutex);
	bool del_ds_err_logged = false;
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = delete_bb_dataset_by_id(dataset_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			log_flag(BURST_BUF, "%s: deleted dataset rule id=%u", __func__, dataset_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("%s: delete_bb_dataset_by_id internal error", __func__);
			del_ds_err_logged = true;
			break;
		} else if (rc == BB_API_ERROR) {
			error("%s: delete_bb_dataset_by_id API error", __func__);
			del_ds_err_logged = true;
			break;
		} else if (rc == BB_API_TIMEOUT) {
			log_flag(BURST_BUF, "%s: delete timed out; verifying", __func__);
			int query_rc = query_datasetid_by_path_groupid(group_id, path, &(uint32_t){0}, &bb_state.bb_config);
			if (query_rc < 0) {
				error("%s: query after delete timeout failed", __func__);
				del_ds_err_logged = true;
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				log_flag(BURST_BUF, "%s: dataset rule already gone", __func__);
				rc = 0;
				break;
			}
			if (query_rc == BB_SUCCESS) {
				log_flag(BURST_BUF, "%s: delete still pending, retry", __func__);
				continue;
			}
		} else {
			error("%s: unexpected rc=%d", __func__, rc);
			del_ds_err_logged = true;
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);

	if (rc != 0 && !del_ds_err_logged) {
		error("%s: failed to delete dataset id=%u, rc=%d", __func__, dataset_id, rc);
	}
	return rc;
}

/**
 * @brief Delete dataset rule resolved by cache group id and path.
 * @return BB_* / API return codes from bb_api
 */
extern int bb_p_delete_bb_dataset_by_groupid_path(uint32_t group_id, char * path)
{
	int rc = SLURM_ERROR;
	if (group_id <= 0 || !path) {
		error("%s: invalid arguments", __func__);
		return SLURM_ERROR;
	}
	uint32_t dataset_id = 0;
	slurm_mutex_lock(&bb_state.bb_mutex);
	rc = query_datasetid_by_path_groupid(group_id, path, &dataset_id, &bb_state.bb_config);
	if (rc != BB_SUCCESS) {
		error("%s: query_datasetid_by_path_groupid failed group_id=%u path=%s rc=%d",
		      __func__, group_id, path, rc);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return rc;
	}
	bool del_path_err_logged = false;
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = delete_bb_dataset_by_id(dataset_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			log_flag(BURST_BUF, "%s: deleted dataset rule id=%u", __func__, dataset_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("%s: delete_bb_dataset_by_id internal error", __func__);
			del_path_err_logged = true;
			break;
		} else if (rc == BB_API_ERROR) {
			error("%s: delete_bb_dataset_by_id API error", __func__);
			del_path_err_logged = true;
			break;
		} else if (rc == BB_API_TIMEOUT) {
			log_flag(BURST_BUF, "%s: delete timed out; verifying", __func__);
			int query_rc = query_datasetid_by_path_groupid(group_id, path, &(uint32_t){0}, &bb_state.bb_config);
			if (query_rc < 0) {
				error("%s: query after delete timeout failed", __func__);
				del_path_err_logged = true;
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				log_flag(BURST_BUF, "%s: dataset rule already gone", __func__);
				rc = 0;
				break;
			}
			if (query_rc == BB_SUCCESS) {
				log_flag(BURST_BUF, "%s: delete still pending, retry", __func__);
				continue;
			}
		} else {
			error("%s: unexpected rc=%d", __func__, rc);
			del_path_err_logged = true;
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);

	if (rc != 0 && !del_path_err_logged) {
		error("%s: failed to delete dataset id=%u, rc=%d", __func__, dataset_id, rc);
	}
	return rc;
}





/* Cancel burst-buffer task by id (wraps cancel_bb_task_by_id). */
extern int bb_p_cancel_bb_task_by_id(uint32_t task_id)
{
	int rc = SLURM_ERROR;
	slurm_mutex_lock(&bb_state.bb_mutex);
	rc = cancel_bb_task_by_id(task_id, &bb_state.bb_config);
	slurm_mutex_unlock(&bb_state.bb_mutex);
	return rc;
}

// #ifdef __METASTACK_NEW_BURSTBUFFER
// extern int bb_p_release_resources(char *groups_sn)
// {
// 	int rc = SLURM_ERROR;
// 	slurm_mutex_lock(&bb_state.bb_mutex);
// 	//rc = cancel_bb_task_by_id(task_id, &bb_state.bb_config);
	
// 	slurm_mutex_unlock(&bb_state.bb_mutex);
// 	return rc;
// }
// #endif