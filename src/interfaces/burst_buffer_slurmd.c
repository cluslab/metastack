/*****************************************************************************\
 *  burst_buffer_slurmd.c - driver for bb_api library plugin interface
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

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/interfaces/burst_buffer_slurmd.h"

#include "src/common/list.h"
#include "src/common/macros.h"
#include "src/common/pack.h"
#include "src/common/plugin.h"
#include "src/common/plugrack.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

/*
 * ============================================================================
 * 配置 bb_api 库的插件路径
 * 可以通过修改 BB_API_PLUGIN_TYPE 和 BB_API_PLUGIN_NAME 来指定不同的库
 * ============================================================================
 */
#define BB_API_PLUGIN_TYPE "burst_buffer"
#define BB_API_PLUGIN_NAME "parastor/bb_api"

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
	/* 示例函数：获取 groups 列表 */
	List (*get_groups_burst_buffer) (void *query_params, void *bb_min_config, void *resp_out);
	
	/* 
	 * 在这里添加更多函数指针，例如：
	 * List (*get_datasets_burst_buffer) (void *query_params, void *bb_min_config, void *resp_out);
	 * int (*create_burst_buffer_group) (void *create_params, void *bb_min_config, void *resp_out);
	 * ... 等等
	 */
} slurm_bb_api_ops_t;

/*
 * 符号表：必须与 slurm_bb_api_ops_t 结构体中的函数指针顺序完全一致
 * 每个符号名称必须与 bb_api 库中导出的函数名完全一致
 */
static const char *bb_api_syms[] = {
	"get_groups_burst_buffer",
	/* 
	 * 添加新函数时，在这里添加对应的符号名称，例如：
	 * "get_datasets_burst_buffer",
	 * "create_burst_buffer_group",
	 * ... 等等
	 */
};

/* bb_api 插件上下文 */
static int g_bb_api_context_cnt = -1;
static slurm_bb_api_ops_t *bb_api_ops = NULL;
static plugin_context_t *g_bb_api_context = NULL;
static pthread_mutex_t g_bb_api_context_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * ============================================================================
 * 初始化 bb_api 库插件
 * 
 * Returns a Slurm errno.
 * ============================================================================
 */
static int bb_api_init(void)
{
	int rc = SLURM_SUCCESS;
	char *plugin_type = BB_API_PLUGIN_TYPE;
	char *type = BB_API_PLUGIN_NAME;

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (g_bb_api_context_cnt >= 0)
		goto fini;

	g_bb_api_context_cnt = 0;
	bb_api_ops = xmalloc(sizeof(slurm_bb_api_ops_t));
	
	g_bb_api_context = plugin_context_create(
		plugin_type, type, (void **)bb_api_ops,
		bb_api_syms, sizeof(bb_api_syms));
	
	if (!g_bb_api_context) {
		error("cannot create %s context for %s",
		      plugin_type, type);
		xfree(bb_api_ops);
		bb_api_ops = NULL;
		g_bb_api_context_cnt = -1;
		rc = SLURM_ERROR;
		goto fini;
	}

	g_bb_api_context_cnt = 1;

fini:
	slurm_mutex_unlock(&g_bb_api_context_lock);
	return rc;
}

/*
 * ============================================================================
 * 清理 bb_api 库插件
 * 
 * Returns a Slurm errno.
 * ============================================================================
 */
static int bb_api_fini(void)
{
	int rc = SLURM_SUCCESS;

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (g_bb_api_context_cnt < 0)
		goto fini;

	if (g_bb_api_context) {
		rc = plugin_context_destroy(g_bb_api_context);
		g_bb_api_context = NULL;
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

/*
 * Terminate the bb_api library infrastructure. Free memory.
 *
 * Returns a Slurm errno.
 */
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
 * 示例函数：获取 groups 列表
 * 
 * query_params IN - 查询参数
 * bb_min_config IN - 最小配置
 * resp_out OUT - 响应输出
 * RET groups 列表，失败返回 NULL
 */
extern List bb_g_get_groups_burst_buffer(void *query_params, void *bb_min_config, void *resp_out)
{
	List result = NULL;

	/* 自动初始化插件（如果尚未初始化） */
	if (g_bb_api_context_cnt < 0) {
		if (bb_api_init() != SLURM_SUCCESS) {
			error("%s: failed to initialize bb_api plugin", __func__);
			return NULL;
		}
	}

	slurm_mutex_lock(&g_bb_api_context_lock);
	if (bb_api_ops && bb_api_ops->get_groups_burst_buffer) {
		result = (*(bb_api_ops->get_groups_burst_buffer))(query_params, bb_min_config, resp_out);
	} else {
		error("%s: get_groups_burst_buffer function not available", __func__);
	}
	slurm_mutex_unlock(&g_bb_api_context_lock);

	return result;
}

/*
 * ============================================================================
 * 如何添加新的 bb_api 库函数调用：
 * 
 * 假设你要添加一个新函数：get_datasets_burst_buffer
 * 
 * 步骤 1: 在 slurm_bb_api_ops_t 结构体中添加函数指针
 *   List (*get_datasets_burst_buffer) (void *query_params, void *bb_min_config, void *resp_out);
 * 
 * 步骤 2: 在 bb_api_syms 数组中添加符号名称（必须与库中函数名完全一致）
 *   "get_datasets_burst_buffer",
 * 
 * 步骤 3: 在文件末尾添加包装函数（复制下面的模板并修改）：
 * 
 * extern List bb_g_get_datasets_burst_buffer(void *query_params, void *bb_min_config, void *resp_out)
 * {
 *     List result = NULL;
 * 
 *     if (g_bb_api_context_cnt < 0) {
 *         if (bb_api_init() != SLURM_SUCCESS) {
 *             error("%s: failed to initialize bb_api plugin", __func__);
 *             return NULL;
 *         }
 *     }
 * 
 *     slurm_mutex_lock(&g_bb_api_context_lock);
 *     if (bb_api_ops && bb_api_ops->get_datasets_burst_buffer) {
 *         result = (*(bb_api_ops->get_datasets_burst_buffer))(query_params, bb_min_config, resp_out);
 *     } else {
 *         error("%s: get_datasets_burst_buffer function not available", __func__);
 *     }
 *     slurm_mutex_unlock(&g_bb_api_context_lock);
 * 
 *     return result;
 * }
 * 
 * 注意：
 * - 函数指针类型必须与 bb_api 库中的函数签名完全匹配
 * - 符号名称必须与库中导出的函数名完全一致
 * - 包装函数名通常以 bb_g_ 开头，后面跟库中的函数名
 * ============================================================================
 */
