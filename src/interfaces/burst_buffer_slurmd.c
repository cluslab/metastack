
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <dlfcn.h>

#include "slurm/slurm.h"
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
	/* 测试函数：验证库是否正确加载 */
	int (*bb_api_test_function) (void);
	/* 示例函数：获取 groups 列表 */
	List (*get_groups_burst_buffer) (void *query_params, void *bb_min_config, void *resp_out);
	
} slurm_bb_api_ops_t;

/*
 * 符号表：必须与 slurm_bb_api_ops_t 结构体中的函数指针顺序完全一致
 * 每个符号名称必须与 bb_api 库中导出的函数名完全一致
 */
static const char *bb_api_syms[] = {
	"bb_api_test_function",
	"get_groups_burst_buffer",
	/* 
	 * 添加新函数时，在这里添加对应的符号名称，例如：
	 * "get_datasets_burst_buffer",
	 * "create_burst_buffer_group",
	 * ... 等等
	 */
};

/* bb_api 库句柄和操作结构 */
static int g_bb_api_context_cnt = -1;
static slurm_bb_api_ops_t *bb_api_ops = NULL;
static plugin_handle_t g_bb_api_handle = PLUGIN_INVALID_HANDLE;
static pthread_mutex_t g_bb_api_context_lock = PTHREAD_MUTEX_INITIALIZER;


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
 * 1. 检查插件是否已初始化，如果没有则初始化
 * 2. 使用互斥锁保护
 * 3. 通过函数指针调用库中的函数
 * 4. 返回结果
 * ============================================================================
 */

/*
 * 测试函数：验证 bb_api 库是否正确加载
 * 
 * RET: 0 on success, SLURM_ERROR on failure
 */
extern int bb_g_bb_api_test_function(void)
{
	int rc = SLURM_ERROR;

	/* 自动初始化插件（如果尚未初始化） */
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return SLURM_ERROR;
		}
	}

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->bb_api_test_function) {
		rc = (*(bb_api_ops->bb_api_test_function))();
	} else {
		error("%s: bb_api_test_function not available", __func__);
	}
	slurm_mutex_unlock(&g_bb_api_context_lock);

	return rc;
}
