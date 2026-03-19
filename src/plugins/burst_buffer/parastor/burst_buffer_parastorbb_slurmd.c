#define _GNU_SOURCE

#include <unistd.h>
#include "slurm/slurm.h"
#include "src/common/xstring.h"
#include "src/plugins/burst_buffer/common/burst_buffer_common.h"
#include "bb_curl_wrapper.h"
#include "bb_api.h"
#include "src/common/run_in_daemon.h"
#include "src/common/xmalloc.h"

/*
 * 为兼容 burst_buffer 公共代码中对 plugin_type 的引用，
 * 在 libbb_api.so 中提供一套 Slurm 插件识别符号。
 * 同时避免 dlopen 时出现 undefined symbol: plugin_type。
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
	if(rc == SLURM_ERROR) {
		error("failed to get permanent token");
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
	debug3("BURST_BUF PLUG FINISHED");

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
		error("%s: found get_sys_state which is unused in this plugin, unsetting",
		      plugin_type);
		xfree(bb_state.bb_config.get_sys_state);
	}
	if (bb_state.bb_config.get_sys_status) {
		error("%s: found get_sys_status which is unused in this plugin, unsetting",
		      plugin_type);
		xfree(bb_state.bb_config.get_sys_status);
	}
	if (bb_state.bb_config.flags & BB_FLAG_EMULATE_CRAY) {
		error("%s: found flags=EmulateCray which is invalid for this plugin, unsetting",
		      plugin_type);
		bb_state.bb_config.flags &= (~BB_FLAG_EMULATE_CRAY);
	}
	if (bb_state.bb_config.directive_str) {
		directive_str = bb_state.bb_config.directive_str;
		directive_len = strlen(directive_str);
	}

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
	if(bb_state.bb_config.max_clients_per_job > max_node_per_groups) {
		error("%s: MaxClientsPerJob=%u exceeds maximum allowed %u, setting MaxClientsPerJob to maximum",
		      plugin_type, bb_state.bb_config.max_clients_per_job,
		      max_node_per_groups);
		bb_state.bb_config.max_clients_per_job = max_node_per_groups;
	}
}


/**
 * @brief 通过SN创建缓存组
 * @param group_sn 
 * @param client_cnt 客户端数量 
 * @param client_hostname_arr 客户端hostname数组
 * @param group_id 返回创建成功的缓存组ID
 * @return 0成功，-1代码错误，-2接口错误，-3接口超时
 */
extern int bb_p_create_bb_group_by_sn(char *group_sn, int client_cnt, char **client_hostname_arr, uint32_t *group_id)
{
	int rc = SLURM_ERROR;
	if (!group_sn || !client_hostname_arr || client_cnt < 0) {
		error("error params");
		return SLURM_ERROR;
	}
	uint32_t *client_ids = xmalloc(client_cnt * sizeof(int)); //后面指针给创建参数使用，通过创建参数释放
	bool query_success = true;
	slurm_mutex_lock(&bb_state.bb_mutex);
	for (int i = 0; i < client_cnt; i++) {
		if (!client_hostname_arr[i]) {
			error("hostname[%d] is NULL", i);
			query_success = false;
			break;
		}
		rc = query_clientid_by_hostname(client_hostname_arr[i], &client_ids[i], &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			debug("查询 client_id 成功: hostname=%s, client_id=%d", client_hostname_arr[i], client_ids[i]);
		} else if (rc == BB_SUCCESS_NO_DATA) {
			error("hostname %s 对应的 client 不存在", client_hostname_arr[i]);
			query_success = false;
			break;
		} else {
			error("查询 hostname %s 的 client_id 失败, return code=%d", client_hostname_arr[i], rc);
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
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = create_bb_group_by_sn(create_params, group_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			debug("创建缓存组成功,group_id,%u", *group_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("创建缓存组代码错误");
			break;
		} else if (rc == BB_API_ERROR) {
			error("创建缓存组接口返回错误");
			break;
		} else if (rc == BB_API_TIMEOUT) {
			debug("创建缓存组接口超时,查询是否已创建成功");
			slurm_mutex_lock(&bb_state.bb_mutex);
			int query_rc = query_bb_groupid_by_sn(group_sn, group_id, &bb_state.bb_config);
			slurm_mutex_unlock(&bb_state.bb_mutex);
			if (query_rc < 0) {
				error("查询失败");
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				debug("缓存组未创建成功,重试 %d/%d", retry_count + 1, bb_state.bb_config.retry_count);
				continue;
			}
			if (query_rc == BB_SUCCESS) {
				debug("查询成功");
				rc = query_rc;
				break;
			}
		} else {
			error("未知返回结果");
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	free_create_params(create_params);
	if (rc < 0) {
		error("创建缓存组%s失败,错误码: %d", group_sn, rc);
	}
	return rc;
}

extern int bb_p_create_bb_dataset_by_sn(char *group_sn, uint32_t group_id ,char *path, bool is_use_metadata, bool is_share_cache, uint32_t *dataset_id)
{
	int rc = SLURM_ERROR;
	if (!group_sn || !path) {
		error("error params");
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
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = create_bb_dataset_by_sn(create_params, dataset_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			debug("创建数据集规则成功,dataset_id:%d", *dataset_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("创建数据集规则代码错误");
			break;
		} else if (rc == BB_API_ERROR) {
			error("创建数据集规则接口返回错误");
			break;
		} else if (rc == BB_API_TIMEOUT) {
			debug("创建数据集规则接口超时,查询是否已创建成功");
			int query_rc = query_datasetid_by_path_groupid(group_id, path, dataset_id, &bb_state.bb_config);
			if (query_rc < 0) {
				error("查询失败");
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				debug("数据集未创建成功,重试 %d/%d", retry_count + 1, bb_state.bb_config.retry_count);
				continue;
			}
			if (query_rc == BB_SUCCESS) {
				debug("查询成功");
				rc = query_rc;
				break;
			}
		} else {
			error("创建数据集失败,错误码: %d", rc);
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	free_create_params(create_params);
	if (rc < 0) {
		error("创建数据集规则失败");
	}
	return rc;
}


extern int bb_p_submit_bb_task(uint32_t dataset_id, int task_type, uint32_t *task_id)
{
	int rc = SLURM_ERROR;
	if (dataset_id <= 0 || (task_type != BURST_BUFFER_TASK_TYPE_PREFETCH && task_type != BURST_BUFFER_TASK_TYPE_RECYCLE)) {
		error("error params");
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
		debug("提交任务成功,任务ID:%d", *task_id);
	} else if (rc == -1) {
		error("提交任务错误");
	} else if (rc == -2) {
		error("提交任务接口返回错误");
	} else if (rc == -3) {
		error("提交任务超时");
	} else {
		error("提交失败,错误码: %d", rc);
	}
	free_create_params(create_params);
	return rc;
}


extern int bb_p_wait_task_complete(uint32_t task_id, int task_type)
{
	if (task_id <= 0 || (task_type != BURST_BUFFER_TASK_TYPE_PREFETCH && task_type != BURST_BUFFER_TASK_TYPE_RECYCLE)) {
		error("error params");
		return SLURM_ERROR;
	}
	// 定义时间常量
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
		error("Invalid task_type: %d", task_type);
		return SLURM_ERROR;
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (HARD_TIMEOUT_SEC <= 0 || SOFT_TIMEOUT_SEC < 0 || CHECK_INTERVAL_SEC <= 0) {
		error("Invalid timeout values: HARD=%ld, SOFT=%ld, INTERVAL=%ld",
			(long)HARD_TIMEOUT_SEC, (long)SOFT_TIMEOUT_SEC, (long)CHECK_INTERVAL_SEC);
		return SLURM_ERROR;
	}
	
	time_t start_time		  = time(NULL);         			// 记录开始检查的时间
	time_t last_check_time	  = start_time;    				    // 上次检查的时间
	time_t hard_timeout_time  = start_time + HARD_TIMEOUT_SEC;  // 超时时间点
	bool task_completed 	  = false;           
	int query_rc 			  = 0;                    
	bb_attribute_task *bb_task = xmalloc(sizeof(bb_attribute_task));  // 任务属性结构体指针
	//debug("开始等待任务完成,task_id=%u, 检查间隔=%ld秒, 软超时=%ld秒, 硬超时=%ld秒", task_id, (long)CHECK_INTERVAL_SEC, (long)SOFT_TIMEOUT_SEC, (long)HARD_TIMEOUT_SEC);

	while (!task_completed) {
		time_t current_time = time(NULL);
		time_t elapsed_time = current_time - start_time;

		// 检查是否到达检查间隔时间
		bool need_check = false;
		if (current_time - last_check_time >= CHECK_INTERVAL_SEC) {
			need_check = true;
		} else if (current_time >= hard_timeout_time) {
			// 到达硬超时时间点,单独检查一次
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
				last_check_time = current_time;
				sleep(1);
				continue;
			}
		}

		if (bb_task->task_state == BB_TASK_STATE_COMPLETED) {
			task_completed = true;
			rc = BB_SUCCESS;
			break;
		} else if (bb_task->task_state == BB_TASK_STATE_FAILED ||
			bb_task->task_state == BB_TASK_STATE_CANCELED) {
			error("BB-----预热任务失败或已取消,task_id=%u, 任务状态=%d",
				task_id, bb_task->task_state);
			free_bb_task(bb_task);
			return rc;
		} else if (bb_task->task_state == BB_TASK_STATE_SUBMITTING ||
			bb_task->task_state == BB_TASK_STATE_RUNNING) {
			debug("BB-----预热任务仍在进行中,task_id=%u, 任务状态=%d",
				task_id, bb_task->task_state);
		} else {
			error("BB-----预热任务状态未知,task_id=%u, 任务状态=%d", task_id, bb_task->task_state);
			free_bb_task(bb_task);
			return rc;
		}

		elapsed_time = time(NULL) - start_time;
		if (elapsed_time >= HARD_TIMEOUT_SEC) {
			error("等待任务完成超时,task_id=%u, 已等待%ld秒",
				task_id, (long)elapsed_time);
			free_bb_task(bb_task);
			return SLURM_ERROR;
		}

		last_check_time = time(NULL);

		// 计算等待时间
		time_t sleep_time = CHECK_INTERVAL_SEC;
		time_t remain_time = HARD_TIMEOUT_SEC - (last_check_time - start_time);
		if (remain_time < sleep_time) {
			sleep_time = remain_time;
		}
		if (sleep_time > 0) {
			sleep(sleep_time);
		}
	}
	// 清理资源
	free_bb_task(bb_task);
	return rc;

}


extern int bb_p_delete_bb_group_by_sn(char *group_sn)
{
	int rc = SLURM_ERROR;
	if (!group_sn) {
		error("error params");
		return SLURM_ERROR;
	}
	//uint32_t group_id = 0;
	slurm_mutex_lock(&bb_state.bb_mutex);
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = delete_bb_group_by_sn(group_sn, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			debug("删除缓存组%s成功", group_sn);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("删除缓存组代码错误");
			break;
		} else if (rc == BB_API_ERROR) {
			error("删除缓存组接口返回错误");
			break;
		} else if (rc == BB_API_TIMEOUT) {
			debug("删除缓存组接口超时，查询是否已删除成功");
			int query_rc = query_bb_groupid_by_sn(group_sn, &(uint32_t){0}, &bb_state.bb_config);
			if (query_rc < 0) {
				error("查询失败");
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				debug("删除缓存组成功");
				rc = 0;
				break;
			}
			if (query_rc == BB_SUCCESS) {
				debug("删除缓存组超时，重试 %d/%d", retry_count + 1, bb_state.bb_config.retry_count);
				continue;
			}
		} else {
			error("未知返回结果");
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (rc != 0) {
		error("删除缓存组失败，错误码: %d", rc);
	}
	return rc;
}

extern int bb_p_delete_bb_group_by_id(uint32_t group_id)
{
	int rc = SLURM_ERROR;
	if (group_id == 0) {
		error("error params");
		return SLURM_ERROR;
	}
	slurm_mutex_lock(&bb_state.bb_mutex);
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = delete_bb_group_by_id(group_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			debug("删除缓存组%d成功", group_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("删除缓存组代码错误");
			break;
		} else if (rc == BB_API_ERROR) {
			error("删除缓存组接口返回错误");
			break;
		} else if (rc == BB_API_TIMEOUT) {
			debug("删除缓存组接口超时，查询是否已删除成功");
			int query_rc = has_bb_group_by_id(group_id, &bb_state.bb_config);
			if (query_rc < 0) {
				error("查询失败");
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				debug("删除缓存组成功");
				rc = 0;
				break;
			}
			if (query_rc == BB_SUCCESS) {
				debug("删除缓存组超时，重试 %d/%d", retry_count + 1, bb_state.bb_config.retry_count);
				continue;
			}
		} else {
			error("未知返回结果");
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	if (rc != 0) {
		error("删除缓存组失败，错误码: %d", rc);
	}
	return rc;
}



extern int bb_p_delete_bb_dataset_by_id(uint32_t dataset_id, uint32_t group_id, char * path)
{
	int rc = SLURM_ERROR;
	if (dataset_id <= 0) {
		error("error params");
		return SLURM_ERROR;
	}
	slurm_mutex_lock(&bb_state.bb_mutex);
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = delete_bb_dataset_by_id(dataset_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			debug("删除数据集规则%d成功", dataset_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("删除数据集规则代码错误");
			break;
		} else if (rc == BB_API_ERROR) {
			debug("删除数据集规则接口返回错误");
			break;
		} else if (rc == BB_API_TIMEOUT) {
			debug("删除数据集规则接口超时，查询是否已删除成功");
			int query_rc = query_datasetid_by_path_groupid(group_id, path, &(uint32_t){0}, &bb_state.bb_config);
			if (query_rc < 0) {
				error("查询接口异常");
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				debug("删除成功");
				rc = 0;
				break;
			}
			if (query_rc == BB_SUCCESS) {
				debug("删除失败");
				continue;
			}
		} else {
			error("未知返回结果");
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	
	if (rc != 0) {
		error("删除数据集规则%d失败,return code:%d", dataset_id, rc);
	}
	return rc;
}

/**
 * @brief 根据group_id和path删除数据集规则
 * @param group_id 
 * @param path 
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_p_delete_bb_dataset_by_groupid_path(uint32_t group_id, char * path)
{
	int rc = SLURM_ERROR;
	if (group_id <= 0 || !path) {
		error("error params");
		return SLURM_ERROR;
	}
	uint32_t dataset_id = 0;
	slurm_mutex_lock(&bb_state.bb_mutex);
	rc = query_datasetid_by_path_groupid(group_id, path, &dataset_id, &bb_state.bb_config);
	if (rc != BB_SUCCESS) {
		error("通过group_id和加速路径查询数据集规则ID失败,group_id=%d, path=%s, return code=%d", group_id, path, rc);
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return rc;
	}
	for (int retry_count = 0; retry_count < bb_state.bb_config.retry_count; retry_count++) {
		rc = delete_bb_dataset_by_id(dataset_id, &bb_state.bb_config);
		if (rc == BB_SUCCESS) {
			debug("删除数据集规则%d成功", dataset_id);
			break;
		} else if (rc == BB_CODE_ERROR) {
			error("删除数据集规则代码错误");
			break;
		} else if (rc == BB_API_ERROR) {
			debug("删除数据集规则接口返回错误");
			break;
		} else if (rc == BB_API_TIMEOUT) {
			debug("删除数据集规则接口超时，查询是否已删除成功");
			int query_rc = query_datasetid_by_path_groupid(group_id, path, &(uint32_t){0}, &bb_state.bb_config);
			if (query_rc < 0) {
				error("查询接口异常");
				break;
			}
			if (query_rc == BB_SUCCESS_NO_DATA) {
				debug("删除成功");
				rc = 0;
				break;
			}
			if (query_rc == BB_SUCCESS) {
				debug("删除失败");
				continue;
			}
		} else {
			error("未知返回结果");
			break;
		}
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	
	if (rc != 0) {
		error("删除数据集规则%d失败,return code:%d", dataset_id, rc);
	}
	return rc;
}





/*
 * 根据task_id取消BB任务
 * 
 * @param task_id 任务ID
 * @param bb_config 最小配置
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
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