
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <dlfcn.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"
#include "src/common/timers.h"

#include "src/common/list.h"
#include "src/common/macros.h"
#include "src/common/pack.h"
#include "src/common/plugin.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/read_config.h" 

extern int bb_g_init(void);
extern int bb_g_fini(void);
/*
 * ============================================================================
 * 操作结构体：定义 bb_api 库中所有函数的函数指针
 * 
 * 添加新函数步骤：
 * 1. 在 slurm_bb_slurmd_ops_t 结构体中添加函数指针
 * 2. 在 bb_slurmd_api_syms 数组中添加对应的符号名称（必须与库中函数名完全一致）
 * 3. 在文件末尾添加对应的包装函数
 * ============================================================================
 */
typedef struct slurm_bb_ops {	
	/* 通过SN创建缓存组 */
	int (*bb_p_create_bb_group_by_sn) (char *group_sn, int client_cnt, char **client_hostname_arr, uint32_t *group_id);
	/* 通过SN创建数据集规则 */
	int (*bb_p_create_bb_dataset_by_sn) (char *group_sn, int group_id ,char *path, bool is_use_metadata, bool is_share_cache, uint32_t *dataset_id);
	/* 提交任务（通过数据集ID） */
	int (*bb_p_submit_bb_task) (uint32_t dataset_id, int task_type, uint32_t *task_id);
	/* 等待任务完成 */
	int (*bb_p_wait_task_complete) (uint32_t task_id, int task_type);
	/* 根据group_sn删除缓存组 */
	int (*bb_p_delete_bb_group_by_sn) (char *group_sn);
	/* 根据dataset_id删除数据集规则 */
	int (*bb_p_delete_bb_dataset_by_id) (uint32_t dataset_id, uint32_t group_id, char * path);
	/* 根据group_id和path删除数据集规则 */
	int (*bb_p_delete_bb_dataset_by_groupid_path) (uint32_t group_id, char * path);
	/* 根据task_id取消BB任务 */
	int (*bb_p_cancel_bb_task_by_id) (uint32_t task_id);
} slurm_bb_slurmd_ops_t;

/*
 * 符号表：必须与 slurm_bb_slurmd_ops_t 结构体中的函数指针顺序完全一致
 * 每个符号名称必须与 bb_api 库中导出的函数名完全一致
 */
static const char *syms[] = {
	"bb_p_create_bb_group_by_sn",
	"bb_p_create_bb_dataset_by_sn",
	"bb_p_submit_bb_task",
	"bb_p_wait_task_complete",
	"bb_p_delete_bb_group_by_sn",
	"bb_p_delete_bb_dataset_by_id",
	"bb_p_delete_bb_dataset_by_groupid_path",
	"bb_p_cancel_bb_task_by_id",
};



static int g_context_cnt = -1;
static slurm_bb_slurmd_ops_t *ops = NULL;
static plugin_context_t **g_context = NULL;
static char *bb_plugin_list = NULL;
static pthread_mutex_t g_context_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Initialize the burst buffer infrastructure.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_init(void)
{
	int rc = SLURM_SUCCESS;
	char *last = NULL, *names;
	char *plugin_type = "burst_buffer";
	char *type;

	slurm_mutex_lock(&g_context_lock);
	if (g_context_cnt >= 0)
		goto fini;

	bb_plugin_list = xstrdup(slurm_conf.bb_type);
	g_context_cnt = 0;
	if ((bb_plugin_list == NULL) || (bb_plugin_list[0] == '\0'))
		goto fini;

	names = bb_plugin_list;
	while ((type = strtok_r(names, ",", &last))) {
		xrecalloc(ops, g_context_cnt + 1, sizeof(slurm_bb_slurmd_ops_t));
		xrecalloc(g_context, g_context_cnt + 1,
			  sizeof(plugin_context_t *));
		if (xstrncmp(type, "burst_buffer/", 13) == 0)
			type += 13; /* backward compatibility */
		type = xstrdup_printf("burst_buffer/%s_slurmd", type);
		g_context[g_context_cnt] = plugin_context_create(
			plugin_type, type, (void **)&ops[g_context_cnt],
			syms, sizeof(syms));
		if (!g_context[g_context_cnt]) {
			error("cannot create %s context for %s",
			      plugin_type, type);
			rc = SLURM_ERROR;
			xfree(type);
			break;
		}

		xfree(type);
		g_context_cnt++;
		names = NULL; /* for next iteration */
	}

	/*
	 * Although the burst buffer plugin interface was designed to support
	 * multiple burst buffer plugins, this currently does not work. For
	 * now, do not allow multiple burst buffer plugins to be configured.
	 */
	if (g_context_cnt > 1) {
		error("%d burst buffer plugins configured; can not run with more than one burst buffer plugin",
		      g_context_cnt);
		rc = SLURM_ERROR;
	}

fini:
	slurm_mutex_unlock(&g_context_lock);

	if (rc != SLURM_SUCCESS)
		bb_g_fini();

	return rc;
}

extern int bb_g_fini(void)
{
	int i, j, rc = SLURM_SUCCESS;

	slurm_mutex_lock(&g_context_lock);
	if (g_context_cnt < 0)
		goto fini;

	for (i = 0; i < g_context_cnt; i++) {
		if (g_context[i]) {
			j = plugin_context_destroy(g_context[i]);
			if (j != SLURM_SUCCESS)
				rc = j;
		}
	}
	xfree(ops);
	xfree(g_context);
	xfree(bb_plugin_list);
	g_context_cnt = -1;

fini:	slurm_mutex_unlock(&g_context_lock);
	return rc;
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


extern int bb_g_create_bb_group_by_sn(char *group_sn, int client_cnt, char **client_hostname_arr, uint32_t *group_id)
{
	DEF_TIMERS;
	int rc = 0;
	START_TIMER;
	xassert(g_context_cnt >= 0);
	//slurm_mutex_lock(&g_context_lock);
	for (int i = 0; i < g_context_cnt; i++) {
		rc = (*(ops[i].bb_p_create_bb_group_by_sn))(group_sn, client_cnt, client_hostname_arr, group_id);
	}
	//slurm_mutex_unlock(&g_context_lock);
	END_TIMER2(__func__);
	return rc;
}

extern int bb_g_create_bb_dataset_by_sn(char *group_sn, uint32_t group_id ,char *path, bool is_use_metadata, bool is_share_cache, uint32_t *dataset_id)
{
	DEF_TIMERS;
	int rc = 0;
	START_TIMER;
	xassert(g_context_cnt >= 0);
	//slurm_mutex_lock(&g_context_lock);
	for (int i = 0; i < g_context_cnt; i++) {
		rc = (*(ops[i].bb_p_create_bb_dataset_by_sn))(group_sn, group_id, path, is_use_metadata, is_share_cache, dataset_id);
	}
	//slurm_mutex_unlock(&g_context_lock);
	END_TIMER2(__func__);
	return rc;
}


extern int bb_g_submit_bb_task(uint32_t dataset_id, int task_type, uint32_t *task_id)
{
	DEF_TIMERS;
	int rc = 0;
	START_TIMER;
	xassert(g_context_cnt >= 0);
	//slurm_mutex_lock(&g_context_lock);
	for (int i = 0; i < g_context_cnt; i++) {
		rc = (*(ops[i].bb_p_submit_bb_task))(dataset_id, task_type, task_id);
	}
	//slurm_mutex_unlock(&g_context_lock);
	END_TIMER2(__func__);
	return rc;
}


extern int bb_g_wait_task_complete(uint32_t task_id, int task_type)
{
	DEF_TIMERS;
	int rc = 0;
	START_TIMER;
	xassert(g_context_cnt >= 0);
	//slurm_mutex_lock(&g_context_lock);
	for (int i = 0; i < g_context_cnt; i++) {
		rc = (*(ops[i].bb_p_wait_task_complete))(task_id, task_type);
	}
	//slurm_mutex_unlock(&g_context_lock);
	END_TIMER2(__func__);
	return rc;
}

extern int bb_g_delete_bb_group_by_sn(char *group_sn)
{
	DEF_TIMERS;
	int rc = 0;
	START_TIMER;
	xassert(g_context_cnt >= 0);
	//slurm_mutex_lock(&g_context_lock);
	for (int i = 0; i < g_context_cnt; i++) {
		rc = (*(ops[i].bb_p_delete_bb_group_by_sn))(group_sn);
	}
	//slurm_mutex_unlock(&g_context_lock);
	END_TIMER2(__func__);
	return rc;
}

extern int bb_g_delete_bb_dataset_by_id(uint32_t dataset_id, uint32_t group_id, char * path)
{
	DEF_TIMERS;
	int rc = 0;
	START_TIMER;
	xassert(g_context_cnt >= 0);
	//slurm_mutex_lock(&g_context_lock);
	for (int i = 0; i < g_context_cnt; i++) {
		rc = (*(ops[i].bb_p_delete_bb_dataset_by_id))(dataset_id, group_id, path);
	}
	//slurm_mutex_unlock(&g_context_lock);
	END_TIMER2(__func__);
	return rc;
}

extern int bb_g_delete_bb_dataset_by_groupid_path(uint32_t dataset_id, uint32_t group_id, char * path)
{
	DEF_TIMERS;
	int rc = 0;
	START_TIMER;
	xassert(g_context_cnt >= 0);
	//slurm_mutex_lock(&g_context_lock);
	for (int i = 0; i < g_context_cnt; i++) {
		rc = (*(ops[i].bb_p_delete_bb_dataset_by_groupid_path))(group_id, path);
	}
	//slurm_mutex_unlock(&g_context_lock);
	END_TIMER2(__func__);
	return rc;
}



extern int bb_g_cancel_bb_task_by_id(uint32_t task_id)
{
	DEF_TIMERS;
	START_TIMER;
	int rc = 0;
	xassert(g_context_cnt >= 0);
	//slurm_mutex_lock(&g_context_lock);
	for (int i = 0; i < g_context_cnt; i++) {
		rc = (*(ops[i].bb_p_cancel_bb_task_by_id))(task_id);
	}
	//slurm_mutex_unlock(&g_context_lock);
	END_TIMER2(__func__);
	return rc;
}

