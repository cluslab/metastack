
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <dlfcn.h>

#include "slurm/slurm_errno.h"

#include "src/interfaces/burst_buffer_slurmd.h"

#include "src/common/list.h"
#include "src/common/macros.h"
#include "src/common/pack.h"
#include "src/common/plugin.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/read_config.h" 

/*
 * ============================================================================
 * 配置 bb_api 库路径（普通共享库）
 * 直接从 slurm_conf.plugindir 加载 "libbb_api.so"
 * ============================================================================
 */
#define BB_API_LIB_NAME "libbb_api.so"

/*
 * ============================================================================
 * 操作结构体：定义 bb_api 库中所有函数的函数指针
 * 
 * 添加新函数步骤：
 * 1. 在 slurm_bb_api_ops_t 结构体中添加函数指针
 * 2. 在 bb_api_syms 数组中添加对应的符号名称（必须与库中函数名完全一致）
 * 3. 在文件末尾添加对应的包装函数
 * ============================================================================
 */
typedef struct slurm_bb_api_ops {	
	/* 通过SN创建缓存组 */
	int (*create_bb_group_by_sn) (void *create_params, void *bb_config);
	/* 通过SN创建数据集规则 */
	int (*create_bb_dataset_by_sn) (void *create_params, void *bb_config);
	/* 提交预热任务（通过数据集ID） */
	int (*submit_bb_task) (void *create_params, void *bb_config);
	/* 通过SN获取缓存组ID */
	int (*query_bb_groupid_by_sn) (char *group_sn, void *bb_min_config);
	/* 传入缓存组ID和数据集路径,查询数据集规则 */
	int (*query_datasetid_by_path_groupid) (const int group_id, const char *path, void *bb_config);
	/* 根据task_id查询bb任务 */
	int (*query_bb_tasks_by_taskid) (int task_id, void *bb_config, void *bb_task);
	/* 根据group_sn删除缓存组 */
	int (*delete_bb_group_by_sn) (char *group_sn, void *bb_config);
	/* 根据dataset_id删除数据集规则 */
	int (*delete_bb_dataset_by_id) (int dataset_id, void *bb_config);
	/* 根据task_id取消BB任务 */
	int (*cancel_bb_task_by_id) (int task_id, void *bb_config);
	/* 释放BB结构体 */
	void (*slurm_free_task) (void *object);
	/* 释放创建参数结构体 */
	void (*_bb_g_free_create_params) (void *object);
	/* 释放删除参数结构体 */
	void (*_bb_g_free_delete_params) (void *object);
	
	
} slurm_bb_api_ops_t;

/*
 * 符号表：必须与 slurm_bb_api_ops_t 结构体中的函数指针顺序完全一致
 * 每个符号名称必须与 bb_api 库中导出的函数名完全一致
 */
static const char *bb_api_syms[] = {
	"create_bb_group_by_sn",
	"create_bb_dataset_by_sn",
	"submit_bb_task",
	"query_bb_groupid_by_sn",
	"query_datasetid_by_path_groupid",
	"query_bb_tasks_by_taskid",
	"delete_bb_group_by_sn",
	"delete_bb_dataset_by_id",
	"cancel_bb_task_by_id",
	"slurm_free_task",
	"free_create_params",
	"free_delete_params"
};


/* bb_api 库句柄和操作结构 */
static int g_bb_api_context_cnt = -1;
static slurm_bb_api_ops_t *bb_api_ops = NULL;
static plugin_handle_t g_bb_api_handle = PLUGIN_INVALID_HANDLE;
static pthread_mutex_t g_bb_api_context_lock = PTHREAD_MUTEX_INITIALIZER;

/* 静态函数 */
static void _bb_g_slurm_free_task(void *object);
static void _bb_g_free_create_params(void *object);
static void _bb_g_free_delete_params(void *object);

static int bb_api_init(void)
{
	int rc = SLURM_SUCCESS;
	char *plugin_dir = NULL;
	char *lib_path = NULL;
	int n_syms;
	int i;

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (g_bb_api_context_cnt >= 0)
		goto fini;

	g_bb_api_context_cnt = 0;
	bb_api_ops = xmalloc(sizeof(slurm_bb_api_ops_t));

	/* 获取插件目录并构建库路径 */
	if (!(plugin_dir = xstrdup(slurm_conf.plugindir))) {
		error("%s: No plugin dir configured", __func__);
		rc = SLURM_ERROR;
		goto fail;
	}

	xstrfmtcat(lib_path, "%s/%s", plugin_dir, BB_API_LIB_NAME);

	/* 通过 dlopen 加载普通共享库 */
	(void) dlerror();
	g_bb_api_handle = dlopen(lib_path, RTLD_LAZY | RTLD_GLOBAL);
	if (!g_bb_api_handle) {
		error("%s: cannot load bb_api library %s: %s",
		      __func__, lib_path, dlerror());
		rc = SLURM_ERROR;
		goto fail;
	}

	/* 使用 plugin_get_syms 解析符号到 ops 结构体 */
	n_syms = sizeof(bb_api_syms) / sizeof(char *);
	if (plugin_get_syms(g_bb_api_handle, n_syms,
			    bb_api_syms, (void **)bb_api_ops) < n_syms) {
		error("%s: cannot get all required symbols from %s",
		      __func__, lib_path);
		error("Missing symbols:");
		for (i = 0; i < n_syms; i++) {
			if (!((void **)bb_api_ops)[i])
				error("  - %s", bb_api_syms[i]);
		}
		rc = SLURM_ERROR;
		goto fail;
	}

	g_bb_api_context_cnt = 1;
	goto fini;

fail:
	if (g_bb_api_handle != PLUGIN_INVALID_HANDLE) {
		plugin_unload(g_bb_api_handle);
		g_bb_api_handle = PLUGIN_INVALID_HANDLE;
	}
	xfree(bb_api_ops);
	bb_api_ops = NULL;
	g_bb_api_context_cnt = -1;

fini:
	xfree(lib_path);
	xfree(plugin_dir);
	slurm_mutex_unlock(&g_bb_api_context_lock);
	return rc;
}

static int bb_api_fini(void)
{
	int rc = SLURM_SUCCESS;

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (g_bb_api_context_cnt < 0)
		goto fini;

	if (g_bb_api_handle != PLUGIN_INVALID_HANDLE) {
		plugin_unload(g_bb_api_handle);
		g_bb_api_handle = PLUGIN_INVALID_HANDLE;
	}
	xfree(bb_api_ops);
	bb_api_ops = NULL;
	g_bb_api_context_cnt = -1;

fini:
	slurm_mutex_unlock(&g_bb_api_context_lock);
	return rc;
}

/*
 * ============================================================================
 * 公共接口函数
 * ============================================================================
 */

/*
 * Initialize the bb_api library infrastructure.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_init(void)
{
	return bb_api_init();
}

extern int bb_g_fini(void)
{
	return bb_api_fini();
}

/*
 * ============================================================================
 * bb_api 库函数包装器
 * 
 * 每个包装函数遵循相同的模式：
 * 1. 检查插件是否已初始化,如果没有则初始化
 * 2. 使用互斥锁保护
 * 3. 通过函数指针调用库中的函数
 * 4. 返回结果
 * ============================================================================
 */


extern int bb_g_create_bb_group_by_sn(char *group_sn, int client_cnt, int *client_arr, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}
	if (!group_sn || client_cnt <= 0 || !client_arr || !bb_config) {
		error("error params");
		return SLURM_ERROR;
	}
	create_params_request *create_params = xmalloc(sizeof(create_params_request));
	create_params->group_sn = xstrdup(group_sn);
	create_params->client_count = client_cnt;
	create_params->client_ids = xmalloc(create_params->client_count * sizeof(int));
	memcpy(create_params->client_ids, client_arr, create_params->client_count * sizeof(int));
	for (int retry_count = 0; retry_count < bb_config->retry_count; retry_count++) {
		slurm_mutex_lock(&g_bb_api_context_lock);
		if (bb_api_ops && bb_api_ops->create_bb_group_by_sn) {
			rc = (*(bb_api_ops->create_bb_group_by_sn))(create_params, bb_config);
		} else {
			error("%s: create_bb_group_by_sn not available", __func__);
			slurm_mutex_unlock(&g_bb_api_context_lock);
			_bb_g_free_create_params(create_params);
			return SLURM_ERROR;
		}
		slurm_mutex_unlock(&g_bb_api_context_lock);

		if (rc > 0) {
			debug("创建缓存组成功,group_id,%d", rc);
			break;
		} else if (rc == -1) {
			error("创建缓存组代码错误");
			break;
		} else if (rc == -2) {
			error("创建缓存组接口返回错误");
			break;
		} else if (rc == -3) {
			debug("创建缓存组接口超时,查询是否已创建成功");
			int query_rc = bb_g_query_bb_groupid_by_sn(group_sn, bb_config);
			if (query_rc < 0) {
				error("查询失败");
				break;
			}
			if (query_rc == 0) {
				debug("缓存组未创建成功,重试 %d/%d", retry_count + 1, bb_config->retry_count);
				continue;
			}
			if (query_rc > 0) {
				debug("查询成功");
				rc = query_rc;
				break;
			}
		} else {
			error("未知返回结果");
			break;
		}
	}
	
	if (rc < 0) {
		error("创建缓存组%s失败,错误码: %d", group_sn, rc);
	}
	_bb_g_free_create_params(create_params);
	return rc;

}

extern int bb_g_create_bb_dataset_by_sn(char *group_sn, int group_id ,char *path, bool is_use_metadata, bool is_share_cache, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}
	if (!group_sn || !path || !bb_config) {
		error("error params");
		return SLURM_ERROR;
	}
	create_params_request *create_params = xmalloc(sizeof(create_params_request));
	create_params->group_sn = xstrdup(group_sn);
	create_params->path = xstrdup(path);
	create_params->is_use_metadata = is_use_metadata;
	if (is_share_cache == true) {
		create_params->data_cache_type = SHARE_CACHE;
	} else {
		create_params->data_cache_type = LOCAL_CACHE;
	}
	for (int retry_count = 0; retry_count < bb_config->retry_count; retry_count++) {

		slurm_mutex_lock(&g_bb_api_context_lock);
		if (bb_api_ops && bb_api_ops->create_bb_dataset_by_sn) {
			rc = (*(bb_api_ops->create_bb_dataset_by_sn))(create_params, bb_config);
		} else {
			error("%s: create_bb_dataset_by_sn not available", __func__);
			slurm_mutex_unlock(&g_bb_api_context_lock);
			_bb_g_free_create_params(create_params);
			return SLURM_ERROR;
		}
		slurm_mutex_unlock(&g_bb_api_context_lock);
		if (rc > 0) {
			debug("创建数据集规则成功,dataset_id:%d", rc);
			break;
		} else if (rc == -1) {
			error("创建数据集规则代码错误");
			break;
		} else if (rc == -2) {
			error("创建数据集规则接口返回错误,重试 %d/%d", retry_count + 1, bb_config->retry_count);
			break;
		} else if (rc == -3) {
			debug("创建数据集规则接口超时,查询是否已创建成功");
			int query_rc = bb_g_query_datasetid_by_path_groupid(group_id, path, bb_config);
			if (query_rc < 0) {
				error("查询失败");
				break;
			}
			if (query_rc == 0) {
				debug("缓存组未创建成功,重试 %d/%d", retry_count + 1, bb_config->retry_count);
				continue;
			}
			if (query_rc > 0) {
				debug("查询成功");
				rc = query_rc;
				break;
			}
		} else {
			error("创建数据集失败,错误码: %d", rc);
			break;
		}
	}
	
	if (rc < 0) {
		error("创建数据集规则失败");
	}
	_bb_g_free_create_params(create_params);
	return rc;
}


extern int bb_g_submit_bb_task(int dataset_id, int task_type, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}
	if (dataset_id <= 0 || (task_type != BURST_BUFFER_TASK_TYPE_PREFETCH && task_type != BURST_BUFFER_TASK_TYPE_RECYCLE) || !bb_config) {
		error("error params");
		return SLURM_ERROR;
	}
	create_params_request *create_params = xmalloc(sizeof(create_params_request));
	create_params->dataset_id = dataset_id;
	create_params->task_type = task_type;
	create_params->error_action_type = 0;

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->submit_bb_task) {
		rc = (*(bb_api_ops->submit_bb_task))(create_params, bb_config);
	} else {
		slurm_mutex_unlock(&g_bb_api_context_lock);
		_bb_g_free_create_params(create_params);
		error("%s: submit_bb_task not available", __func__);
		return SLURM_ERROR;

	}
	slurm_mutex_unlock(&g_bb_api_context_lock);

	if (rc > 0) {
		debug("提交任务成功,任务ID:%d", rc);
	} else if (rc == -1) {
		error("提交任务错误");
	} else if (rc == -2) {
		error("提交任务接口返回错误");
	} else if (rc == -3) {
		error("提交任务超时");
	} else {
		error("提交失败,错误码: %d", rc);
	}
	_bb_g_free_create_params(create_params);
	return rc;
}


extern int bb_g_wait_task_complete(int task_id, int task_type, bb_minimal_config_t *bb_config)
{
	if (task_id <= 0 || (task_type != BURST_BUFFER_TASK_TYPE_PREFETCH && task_type != BURST_BUFFER_TASK_TYPE_RECYCLE) || !bb_config) {
		error("error params");
		return SLURM_ERROR;
	}
	// 定义时间常量
	int rc = SLURM_ERROR;
	const int CHECK_INTERVAL_SEC = bb_config->poll_interval;
	const int SOFT_TIMEOUT_SEC = bb_config->other_timeout;
	int HARD_TIMEOUT_SEC = 0;
	if (task_type == BURST_BUFFER_TASK_TYPE_PREFETCH)
		HARD_TIMEOUT_SEC = bb_config->stagein_timeout;
	else if (task_type == BURST_BUFFER_TASK_TYPE_RECYCLE)
		HARD_TIMEOUT_SEC = bb_config->stageout_timeout;
	else {
		error("Invalid task_type: %d", task_type);
		return SLURM_ERROR;
	}
	if (HARD_TIMEOUT_SEC <= 0 || SOFT_TIMEOUT_SEC < 0 || CHECK_INTERVAL_SEC <= 0) {
		error("Invalid timeout values: HARD=%d, SOFT=%d, INTERVAL=%d",
			HARD_TIMEOUT_SEC, SOFT_TIMEOUT_SEC, CHECK_INTERVAL_SEC);
		return SLURM_ERROR;
	}
	
	time_t start_time		  = time(NULL);         			// 记录开始检查的时间
	time_t last_check_time	  = start_time;    				    // 上次检查的时间
	time_t soft_timeout_time  = start_time + SOFT_TIMEOUT_SEC;  // 软超时时间点
	time_t hard_timeout_time  = start_time + HARD_TIMEOUT_SEC;  // 硬超时时间点
	bool soft_timeout_reached = false;     						// 是否已超过软超时时间
	bool task_completed 	  = false;           
	int query_rc 			  = 0;                    
	bb_attribute_task *bb_task = xmalloc(sizeof(bb_attribute_task));  // 任务属性结构体指针
	debug("开始等待任务完成,task_id=%d, 检查间隔=%d秒, 软超时=%d秒, 硬超时=%d秒", task_id, CHECK_INTERVAL_SEC, SOFT_TIMEOUT_SEC, HARD_TIMEOUT_SEC);

	while (!task_completed) {
		time_t current_time = time(NULL);
		if (current_time < start_time) {
			error("系统时间回退，重置开始时间");
			start_time = current_time;
			soft_timeout_time = start_time + SOFT_TIMEOUT_SEC;
			hard_timeout_time = start_time + HARD_TIMEOUT_SEC;
		}
		time_t elapsed_time = current_time - start_time;

		if (elapsed_time >= HARD_TIMEOUT_SEC) {
			error("等待预热任务完成超时（硬超时：%d秒),task_id=%d,已等待%d秒",
				HARD_TIMEOUT_SEC, task_id, elapsed_time);
			_bb_g_slurm_free_task(bb_task);
			return SLURM_ERROR;
		}

		// 检查是否到达检查间隔时间,或者在软/硬超时时间点需要单独检查
		bool need_check = false;
		if (current_time - last_check_time >= CHECK_INTERVAL_SEC) {
			need_check = true;
		} else if (current_time >= soft_timeout_time && !soft_timeout_reached) {
			// 到达软超时时间点,单独检查一次
			need_check = true;
			soft_timeout_reached = true;
		} else if (current_time >= hard_timeout_time) {
			// 到达硬超时时间点,单独检查一次
			need_check = true;
		}

		if (!need_check) {
			// 未到检查时间,等待一小段时间后继续循环
			sleep(1);
			continue;
		}

		// 执行状态查询
		query_rc = bb_g_query_bb_tasks_by_taskid(task_id, bb_config, bb_task);

		// 查询失败,直接返回
		if (query_rc < 0) {
			error("查询预热任务状态失败,task_id=%d, 错误码=%d", task_id, query_rc);
			_bb_g_slurm_free_task(bb_task);
			return rc;
		}

		// 根据是否超过软超时时间决定日志级别
		if (soft_timeout_reached) {
			// 超过软超时时间后,使用info级别输出日志
			info("查询预热任务状态（已超过软超时时间%d秒）,task_id=%d, 查询结果=%d, 任务状态=%d, 已等待%d秒",
				SOFT_TIMEOUT_SEC, task_id, query_rc, bb_task->task_state, elapsed_time);
		} else {
			// 未超过软超时时间,使用debug级别
			debug("查询预热任务状态,task_id=%d, 查询结果=%d, 任务状态=%d, 已等待%d秒",
				task_id, query_rc, bb_task->task_state, elapsed_time);
		}

		// 检查任务是否存在
		if (query_rc == 0) {
			// 任务不存在
			error("预热任务不存在,task_id=%d", task_id);
			_bb_g_slurm_free_task(bb_task);
			return rc;
		}

		// 检查任务状态
		if (bb_task->task_state == BB_TASK_STATE_COMPLETED) {
			task_completed = true;
			rc = BB_SUCCESS;
			info("预热任务完成,task_id=%d, 总耗时=%d秒", task_id, elapsed_time);
			break;
		} else if (bb_task->task_state == BB_TASK_STATE_FAILED || bb_task->task_state == BB_TASK_STATE_CANCELED) {
			error("预热任务失败或已取消,task_id=%d, 任务状态=%d, 已等待%d秒", task_id, bb_task->task_state, elapsed_time);
			_bb_g_slurm_free_task(bb_task);
			return rc;
		} else if (bb_task->task_state == BB_TASK_STATE_SUBMITTING || bb_task->task_state == BB_TASK_STATE_RUNNING) {
			if (soft_timeout_reached) {
				info("预热任务仍在进行中,task_id=%d, 任务状态=%d (SUBMITTING=%d, RUNNING=%d), 已等待%d秒",
					task_id, bb_task->task_state, BB_TASK_STATE_SUBMITTING, BB_TASK_STATE_RUNNING, elapsed_time);
			} else {
				debug("预热任务仍在进行中,task_id=%d, 任务状态=%d, 已等待%d秒", task_id, bb_task->task_state, elapsed_time);
			}
		} else {
			// 未知状态,视为异常,直接返回
			error("预热任务状态未知,task_id=%d, 任务状态=%d, 已等待%d秒",
				task_id, bb_task->task_state, elapsed_time);
			_bb_g_slurm_free_task(bb_task);
			return rc;
		}

		last_check_time = current_time;

		// 计算下次检查前的等待时间
		time_t next_check_time = last_check_time + CHECK_INTERVAL_SEC;
		time_t wait_until = next_check_time;

		// 如果软超时或硬超时时间更早到达,则等待到那个时间点
		if (!soft_timeout_reached && soft_timeout_time < wait_until) {
			wait_until = soft_timeout_time;
		}
		if (hard_timeout_time < wait_until) {
			wait_until = hard_timeout_time;
		}

		time_t sleep_time = wait_until - current_time;
		if (sleep_time > 0) {
			sleep(sleep_time);
		}
	}
	// 清理资源
	_bb_g_slurm_free_task(bb_task);
	return rc;

}


extern int bb_g_query_bb_groupid_by_sn(char *group_sn, bb_minimal_config_t *bb_min_config)
{
	int rc = SLURM_ERROR;

	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->query_bb_groupid_by_sn) {
		rc = (*(bb_api_ops->query_bb_groupid_by_sn))(group_sn, bb_min_config);
	} else {
		error("%s: query_bb_groupid_by_sn not available", __func__);
	}
	slurm_mutex_unlock(&g_bb_api_context_lock);

	return rc;
}

extern int bb_g_query_datasetid_by_path_groupid(const int group_id, const char *path, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;

	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->query_datasetid_by_path_groupid) {
		rc = (*(bb_api_ops->query_datasetid_by_path_groupid))(group_id, path, bb_config);
	} else {
		error("%s: query_datasetid_by_path_groupid not available", __func__);
	}
	slurm_mutex_unlock(&g_bb_api_context_lock);

	return rc;
}

extern int bb_g_query_bb_tasks_by_taskid(int task_id, bb_minimal_config_t *bb_config, bb_attribute_task *bb_task)
{
	int rc = SLURM_ERROR;

	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->query_bb_tasks_by_taskid) {
		rc = (*(bb_api_ops->query_bb_tasks_by_taskid))(task_id, bb_config, bb_task);
	} else {
		error("%s: query_bb_tasks_by_taskid not available", __func__);
	}
	slurm_mutex_unlock(&g_bb_api_context_lock);

	return rc;
}

extern int bb_g_delete_bb_group_by_sn(char *group_sn, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}
	if (!group_sn || !bb_config) {
		error("error params");
		return SLURM_ERROR;
	}

	for (int retry_count = 0; retry_count < bb_config->retry_count; retry_count++) {
		slurm_mutex_lock(&g_bb_api_context_lock);
		if (bb_api_ops && bb_api_ops->delete_bb_group_by_sn) {
			rc = (*(bb_api_ops->delete_bb_group_by_sn))(group_sn, bb_config);
		} else {
			slurm_mutex_unlock(&g_bb_api_context_lock);
			error("%s: delete_bb_group_by_sn not available", __func__);
			return SLURM_ERROR;
		}
		slurm_mutex_unlock(&g_bb_api_context_lock);

		if (rc == 0) {
			debug("删除缓存组%s成功", group_sn);
			break;
		} else if (rc == -1) {
			error("删除缓存组代码错误");
			break;
		} else if (rc == -2) {
			error("删除缓存组接口返回错误");
			break;
		} else if (rc == -3) {
			debug("删除缓存组接口超时，查询是否已删除成功");
			int query_rc = bb_g_query_bb_groupid_by_sn(group_sn, bb_config);
			if (query_rc < 0) {
				error("查询失败");
				break;
			}
			if (query_rc == 0) {
				debug("删除缓存组成功");
				rc = 0;
				break;
			}
			if (query_rc > 0) {
				debug("删除缓存组超时，重试 %d/%d", retry_count + 1, bb_config->retry_count);
				continue;
			}
		} else {
			error("未知返回结果");
		}
	}
	
	if (rc != 0) {
		error("删除缓存组失败，尝试 %d 次后仍失败，错误码: %d", bb_config->retry_count, rc);
	}
	return rc;
}

extern int bb_g_delete_bb_dataset_by_id(int dataset_id, int group_id, char * path,bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}
	if (dataset_id <= 0 || !bb_config) {
		error("error params");
		return SLURM_ERROR;
	}
	for (int retry_count = 0; retry_count < bb_config->retry_count; retry_count++) {

		slurm_mutex_lock(&g_bb_api_context_lock);
		if (bb_api_ops && bb_api_ops->delete_bb_dataset_by_id) {
			rc = (*(bb_api_ops->delete_bb_dataset_by_id))(dataset_id, bb_config);
		} else {
			slurm_mutex_unlock(&g_bb_api_context_lock);
			error("%s: delete_bb_dataset_by_id not available", __func__);
			return SLURM_ERROR;
		}
		slurm_mutex_unlock(&g_bb_api_context_lock);

		if (rc == 0) {
			debug("删除数据集规则%d成功", dataset_id);
			break;
		} else if (rc == -1) {
			error("删除数据集规则代码错误");
			break;
		} else if (rc == -2) {
			debug("删除数据集规则接口返回错误");
			break;
		} else if (rc == -3) {
			debug("删除数据集规则接口超时，查询是否已删除成功");
			int query_rc = bb_g_query_datasetid_by_path_groupid(group_id, path, bb_config);
			if (query_rc < 0) {
				error("查询接口异常");
				break;
			}
			if (query_rc == 0) {
				debug("删除成功");
				rc = 0;
				break;
			}
			if (query_rc > 0) {
				debug("删除失败");
				continue;
			}
		} else {
			error("未知返回结果");
			break;
		}
	}
	
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
extern int bb_g_cancel_bb_task_by_id(int task_id, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;

	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->cancel_bb_task_by_id) {
		rc = (*(bb_api_ops->cancel_bb_task_by_id))(task_id, bb_config);
	} else {
		error("%s: cancel_bb_task_by_id not available", __func__);
	}
	slurm_mutex_unlock(&g_bb_api_context_lock);

	return rc;
}



/**
 * @brief 释放task结构体
 * @return
 */
static void _bb_g_slurm_free_task(void *object)
{
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return ;
		}
	}
	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->slurm_free_task) {
		(*(bb_api_ops->slurm_free_task))(object);
	} else {
		error("%s: slurm_free_task not available", __func__);
	}
	slurm_mutex_unlock(&g_bb_api_context_lock);
	return ;

}

/**
 * @brief 释放创建参数结构体
 * @return
 */
static void _bb_g_free_create_params(void *object)
{
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return ;
		}
	}
	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->slurm_free_task) {
		(*(bb_api_ops->_bb_g_free_create_params))(object);
	} else {
		error("%s: _bb_g_free_create_params not available", __func__);
	}
	slurm_mutex_unlock(&g_bb_api_context_lock);
	return ;

}

/**
 * @brief 释放删除参数结构体
 * @return
 */
static void _bb_g_free_delete_params(void *object)
{
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return ;
		}
	}
	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->slurm_free_task) {
		(*(bb_api_ops->_bb_g_free_delete_params))(object);
	} else {
		error("%s: _bb_g_free_delete_params not available", __func__);
	}
	slurm_mutex_unlock(&g_bb_api_context_lock);
	return ;

}

