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
#define GROUP_SIZE  10         /* 默认缓存组大小 */
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
/*
 * 为兼容 burst_buffer 公共代码中对 plugin_type 的引用，
 * 在 libbb_api.so 中提供一套 Slurm 插件识别符号。
 * 同时避免 dlopen 时出现 undefined symbol: plugin_type。
 */
const char plugin_name[]    = "bb_api library for parastor burst buffer";
const char plugin_type[]    = "burst_buffer/parastor/bb_api";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;



extern int bb_p_create_bb_group_by_sn(char *group_sn, int client_cnt, int *client_arr, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;

	create_params_request *create_params = xmalloc(sizeof(create_params_request));
	create_params->group_sn = xstrdup(group_sn);
	create_params->client_count = client_cnt;
	create_params->client_ids = xmalloc(create_params->client_count * sizeof(int));
	memcpy(create_params->client_ids, client_arr, create_params->client_count * sizeof(int));
	for (int retry_count = 0; retry_count < bb_config->retry_count; retry_count++) {
		rc = create_bb_group_by_sn(create_params, bb_config);
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
			int query_rc = query_bb_groupid_by_sn(group_sn, bb_config);
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
	free_create_params(create_params);
	return rc;

}

extern int bb_p_create_bb_dataset_by_sn(char *group_sn, int group_id ,char *path, bool is_use_metadata, bool is_share_cache, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
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
		rc = create_bb_dataset_by_sn(create_params, bb_config);
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
			int query_rc = query_datasetid_by_path_groupid(group_id, path, bb_config);
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
	free_create_params(create_params);
	return rc;
}


extern int bb_p_submit_bb_task(int dataset_id, int task_type, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	if (dataset_id <= 0 || (task_type != BURST_BUFFER_TASK_TYPE_PREFETCH && task_type != BURST_BUFFER_TASK_TYPE_RECYCLE) || !bb_config) {
		error("error params");
		return SLURM_ERROR;
	}
	create_params_request *create_params = xmalloc(sizeof(create_params_request));
	create_params->dataset_id = dataset_id;
	create_params->task_type = task_type;
	create_params->error_action_type = 0;
	rc = submit_bb_task(create_params, bb_config);

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
	free_create_params(create_params);
	return rc;
}


extern int bb_p_wait_task_complete(int task_id, int task_type, bb_minimal_config_t *bb_config)
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
			free_bb_task(bb_task);
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
		query_rc = query_bb_tasks_by_taskid(task_id, bb_config, bb_task);

		// 查询失败,直接返回
		if (query_rc < 0) {
			error("查询预热任务状态失败,task_id=%d, 错误码=%d", task_id, query_rc);
			free_bb_task(bb_task);
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
			free_bb_task(bb_task);
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
			free_bb_task(bb_task);
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
			free_bb_task(bb_task);
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
	free_bb_task(bb_task);
	return rc;

}


extern int bb_p_query_bb_groupid_by_sn(char *group_sn, bb_minimal_config_t *bb_min_config)
{
	int rc = SLURM_ERROR;
	rc = query_bb_groupid_by_sn(group_sn, bb_min_config);
	return rc;
}

extern int bb_p_query_datasetid_by_path_groupid(const int group_id, const char *path, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	rc = query_datasetid_by_path_groupid(group_id, path, bb_config);
	return rc;
}

extern int bb_p_query_bb_tasks_by_taskid(int task_id, bb_minimal_config_t *bb_config, bb_attribute_task *bb_task)
{
	int rc = SLURM_ERROR;
	rc = query_bb_tasks_by_taskid(task_id, bb_config, bb_task);
	return rc;
}

extern int bb_p_delete_bb_group_by_sn(char *group_sn, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	if (!group_sn || !bb_config) {
		error("error params");
		return SLURM_ERROR;
	}
	for (int retry_count = 0; retry_count < bb_config->retry_count; retry_count++) {
		rc = delete_bb_group_by_sn(group_sn, bb_config);
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

extern int bb_p_delete_bb_dataset_by_id(int dataset_id, int group_id, char * path,bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	if (dataset_id <= 0 || !bb_config) {
		error("error params");
		return SLURM_ERROR;
	}
	for (int retry_count = 0; retry_count < bb_config->retry_count; retry_count++) {
		rc = delete_bb_dataset_by_id(dataset_id, bb_config);
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
extern int bb_p_cancel_bb_task_by_id(int task_id, bb_minimal_config_t *bb_config)
{
	int rc = SLURM_ERROR;
	rc = cancel_bb_task_by_id(task_id, bb_config);
	return rc;
}

