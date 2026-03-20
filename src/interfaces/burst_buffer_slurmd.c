
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
#include "src/common/log.h"

extern int bb_g_init(void);
extern int bb_g_fini(void);
/*
 * ============================================================================
 * slurm_bb_slurmd_ops_t — function pointers resolved from the bb_api shared
 * library (Parastor slurmd burst buffer plugin).
 *
 * To add an entry point:
 * 1. Add a member here in the same order as dlsym resolution expects.
 * 2. Append the exported symbol name to syms[] (must match the library).
 * 3. Add a bb_g_* wrapper at the end of this file.
 * ============================================================================
 */
typedef struct slurm_bb_ops {
	/* Create a cache group identified by group serial name (group_sn) */
	int (*bb_p_create_bb_group_by_sn) (char *group_sn, int client_cnt, char **client_hostname_arr, uint32_t *group_id);
	/* Create a dataset rule under the group (by group_sn) */
	int (*bb_p_create_bb_dataset_by_sn) (char *group_sn, int group_id ,char *path, bool is_use_metadata, bool is_share_cache, uint32_t *dataset_id);
	/* Submit a burst buffer task (prefetch / recycle) for dataset_id */
	int (*bb_p_submit_bb_task) (uint32_t dataset_id, int task_type, uint32_t *task_id);
	/* Block until the task completes */
	int (*bb_p_wait_task_complete) (uint32_t task_id, int task_type);
	/* Delete cache group by group_sn */
	int (*bb_p_delete_bb_group_by_sn) (char *group_sn);
	/* Delete cache group by numeric group_id */
	int (*bb_p_delete_bb_group_by_id) (uint32_t group_id);
	/* Delete dataset rule by dataset_id (group_id and path aid recovery on timeout) */
	int (*bb_p_delete_bb_dataset_by_id) (uint32_t dataset_id, uint32_t group_id, char * path);
	/* Delete dataset rule by group_id and accelerated path */
	int (*bb_p_delete_bb_dataset_by_groupid_path) (uint32_t group_id, char * path);
	/* Cancel a burst buffer task by task_id */
	int (*bb_p_cancel_bb_task_by_id) (uint32_t task_id);
	/* Release all resources for group_sn (optional; not currently wired in syms[]) */
	int (*bb_p_release_resources) (char *group_sn);
} slurm_bb_slurmd_ops_t;

/*
 * Symbol table for dlsym: order must match slurm_bb_slurmd_ops_t members.
 * Each string must match an exported symbol in the bb_api library.
 */
static const char *syms[] = {
	"bb_p_create_bb_group_by_sn",
	"bb_p_create_bb_dataset_by_sn",
	"bb_p_submit_bb_task",
	"bb_p_wait_task_complete",
	"bb_p_delete_bb_group_by_sn",
	"bb_p_delete_bb_group_by_id",
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
		if (xstrcmp(type, "burst_buffer/parastorbb_slurmd") == 0) {
			g_context[g_context_cnt] = plugin_context_create(
				plugin_type, type, (void **)&ops[g_context_cnt],
				syms, sizeof(syms));
			if (!g_context[g_context_cnt]) {
				error("unable to create %s plugin context for %s",
				      plugin_type, type);
				rc = SLURM_ERROR;
				xfree(type);
				break;
			}
			log_flag(BURST_BUF, "%s: loaded %s", __func__, type);

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
		error("%d burst buffer plugins configured; only one slurmd burst buffer plugin is supported",
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
 * bb_api wrappers -- each forwards to the loaded plugin's function pointer.
 * (Locking around ops[] was historically optional; xassert ensures init.)
 * ============================================================================
 */


extern int bb_g_create_bb_group_by_sn(char *group_sn, uint32_t client_cnt, char **client_hostname_arr, uint32_t *group_id)
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

extern int bb_g_delete_bb_group_by_id(uint32_t group_id)
{
	DEF_TIMERS;
	int rc = 0;
	START_TIMER;
	xassert(g_context_cnt >= 0);
	//slurm_mutex_lock(&g_context_lock);
	for (int i = 0; i < g_context_cnt; i++) {
		rc = (*(ops[i].bb_p_delete_bb_group_by_id))(group_id);
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

extern int bb_g_delete_bb_dataset_by_groupid_path(uint32_t group_id, char *path)
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
