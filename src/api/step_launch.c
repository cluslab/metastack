/*****************************************************************************\
 *  step_launch.c - launch a parallel job step
 *****************************************************************************
 *  Copyright (C) 2006-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2009 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Christopher J. Morrone <morrone2@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
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

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <netdb.h>		/* for gethostbyname */
#include <netinet/in.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "slurm/slurm.h"

#include "src/common/cpu_frequency.h"
#include "src/common/eio.h"
#include "src/common/fd.h"
#include "src/common/forward.h"
#include "src/common/hostlist.h"
#include "src/common/job_options.h"
#include "src/common/macros.h"
#include "src/common/net.h"
#include "src/common/plugstack.h"
#include "src/common/read_config.h"
#include "src/common/slurm_auth.h"
#include "src/common/slurm_cred.h"
#include "src/common/slurm_mpi.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/slurm_time.h"
#include "src/common/strlcpy.h"
#include "src/common/uid.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "src/api/step_launch.h"
#include "src/api/pmi_server.h"

#include "src/srun/libsrun/step_ctx.h"

#define STEP_ABORT_TIME 2

extern char **environ;

/**********************************************************************
 * General declarations for step launch code
 **********************************************************************/
static int _launch_tasks(slurm_step_ctx_t *ctx,
			 launch_tasks_request_msg_t *launch_msg,
			 uint32_t timeout, char *nodelist);
static char *_lookup_cwd(void);
static void _print_launch_msg(launch_tasks_request_msg_t *msg,
			      char *hostname, int nodeid);

/**********************************************************************
 * Message handler declarations
 **********************************************************************/
static bool   force_terminated_job = false;
static int    task_exit_signal = 0;

static int  _msg_thr_create(struct step_launch_state *sls, int num_nodes);
static void _handle_msg(void *arg, slurm_msg_t *msg);
static int  _cr_notify_step_launch(slurm_step_ctx_t *ctx);
static void *_check_io_timeout(void *_sls);

static struct io_operations message_socket_ops = {
	.readable = &eio_message_socket_readable,
	.handle_read = &eio_message_socket_accept,
	.handle_msg = &_handle_msg
};


/**********************************************************************
 * API functions
 **********************************************************************/

/*
 * slurm_step_launch_params_t_init - initialize a user-allocated
 *      slurm_step_launch_params_t structure with default values.
 *	This function will NOT allocate any new memory.
 * IN ptr - pointer to a structure allocated by the user.
 *      The structure will be initialized.
 */
extern void slurm_step_launch_params_t_init(slurm_step_launch_params_t *ptr)
{
	static slurm_step_io_fds_t fds = SLURM_STEP_IO_FDS_INITIALIZER;

	/* Initialize all values to zero ("NULL" for pointers) */
	memset(ptr, 0, sizeof(slurm_step_launch_params_t));

	ptr->buffered_stdio = true;
	memcpy(&ptr->local_fds, &fds, sizeof(fds));
	ptr->gid = getgid();
	ptr->cpu_freq_min = NO_VAL;
	ptr->cpu_freq_max = NO_VAL;
	ptr->cpu_freq_gov = NO_VAL;
	ptr->het_job_node_offset  = NO_VAL;
	ptr->het_job_id   = NO_VAL;
	ptr->het_job_nnodes  = NO_VAL;
	ptr->het_job_ntasks  = NO_VAL;
	ptr->het_job_offset  = NO_VAL;
	ptr->het_job_step_cnt = NO_VAL;
	ptr->het_job_task_offset = NO_VAL;
}

/*
 * For a hetjob step, rebuild the MPI data structure to show what is running
 * in a single MPI_COMM_WORLD
 */
static void _rebuild_mpi_layout(slurm_step_ctx_t *ctx,
				const slurm_step_launch_params_t *params)
{
	slurm_step_layout_t *new_step_layout, *orig_step_layout;

	if (params->het_job_offset == NO_VAL)
		return;

	if (params->het_job_id && (params->het_job_id != NO_VAL))
		ctx->launch_state->mpi_info->het_job_id = params->het_job_id;

	ctx->launch_state->mpi_info->het_job_task_offset =
		params->het_job_task_offset;
	new_step_layout = xmalloc(sizeof(slurm_step_layout_t));
	orig_step_layout = ctx->launch_state->mpi_info->step_layout;
	ctx->launch_state->mpi_info->step_layout = new_step_layout;
	if (orig_step_layout->front_end) {
		new_step_layout->front_end =
			xstrdup(orig_step_layout->front_end);
	}
	new_step_layout->node_cnt = params->het_job_nnodes;
	new_step_layout->node_list = xstrdup(params->het_job_node_list);
	new_step_layout->plane_size = orig_step_layout->plane_size;
	new_step_layout->start_protocol_ver =
		orig_step_layout->start_protocol_ver;
	new_step_layout->tasks = params->het_job_task_cnts;
	new_step_layout->task_cnt = params->het_job_ntasks;
	new_step_layout->task_dist = orig_step_layout->task_dist;
	new_step_layout->tids = params->het_job_tids;
}

/*
 * slurm_step_launch - launch a parallel job step
 * IN ctx - job step context generated by slurm_step_ctx_create
 * IN params - job step parameters
 * IN callbacks - Identify functions to be called when various events occur
 * RET SLURM_SUCCESS or SLURM_ERROR (with errno set)
 */
extern int slurm_step_launch(slurm_step_ctx_t *ctx,
			     const slurm_step_launch_params_t *params,
			     const slurm_step_launch_callbacks_t *callbacks)
{
	launch_tasks_request_msg_t launch;
	char **env = NULL;
	char **mpi_env = NULL;
	int rc = SLURM_SUCCESS;
	bool preserve_env = params->preserve_env;
	uint32_t mpi_plugin_id;

	debug("Entering %s", __func__);
	memset(&launch, 0, sizeof(launch));

	if ((ctx == NULL) || (ctx->magic != STEP_CTX_MAGIC)) {
		error("%s: Not a valid slurm_step_ctx_t", __func__);
		slurm_seterrno(EINVAL);
		return SLURM_ERROR;
	}

	/* Initialize the callback pointers */
	if (callbacks != NULL) {
		/* copy the user specified callback pointers */
		memcpy(&(ctx->launch_state->callback), callbacks,
		       sizeof(slurm_step_launch_callbacks_t));
	} else {
		/* set all callbacks to NULL */
		memset(&(ctx->launch_state->callback), 0,
		       sizeof(slurm_step_launch_callbacks_t));
	}

	mpi_plugin_id = mpi_g_client_init((char **)&params->mpi_plugin_name);
	if (!mpi_plugin_id) {
		slurm_seterrno(SLURM_MPI_PLUGIN_NAME_INVALID);
		return SLURM_ERROR;
	}

	_rebuild_mpi_layout(ctx, params);

	mpi_env = xmalloc(sizeof(char *));  /* Needed for setenvf used by MPI */
	if ((ctx->launch_state->mpi_state =
	     mpi_g_client_prelaunch(ctx->launch_state->mpi_info, &mpi_env))
	    == NULL) {
		slurm_seterrno(SLURM_MPI_PLUGIN_PRELAUNCH_SETUP_FAILED);
		return SLURM_ERROR;
	}

	/* Create message receiving sockets and handler thread */
	rc = _msg_thr_create(ctx->launch_state,
			     ctx->step_resp->step_layout->node_cnt);
	if (rc != SLURM_SUCCESS)
		return rc;

	/* Start tasks on compute nodes */
	memcpy(&launch.step_id, &ctx->step_req->step_id,
	       sizeof(launch.step_id));
	launch.uid = ctx->step_req->user_id;
	launch.gid = params->gid;
	launch.argc = params->argc;
	launch.argv = params->argv;
	launch.spank_job_env = params->spank_job_env;
	launch.spank_job_env_size = params->spank_job_env_size;
	launch.container = xstrdup(params->container);
	launch.cred = ctx->step_resp->cred;
	launch.het_job_node_offset = params->het_job_node_offset;
	launch.het_job_step_cnt = params->het_job_step_cnt;
	launch.het_job_id = params->het_job_id;
	launch.het_job_nnodes = params->het_job_nnodes;
	launch.het_job_ntasks = params->het_job_ntasks;
	launch.het_job_offset = params->het_job_offset;
	launch.het_job_task_offset = params->het_job_task_offset;
	launch.het_job_task_cnts = params->het_job_task_cnts;
	launch.het_job_tids = params->het_job_tids;
	launch.het_job_tid_offsets = params->het_job_tid_offsets;
	launch.het_job_node_list = params->het_job_node_list;
	if (params->env == NULL) {
		/*
		 * If the user didn't specify an environment, then use the
		 * environment of the running process
		 */
		env_array_merge(&env, (const char **)environ);
	} else {
		env_array_merge(&env, (const char **)params->env);
	}
	if (params->het_job_ntasks != NO_VAL)
		preserve_env = true;
	env_array_for_step(&env, ctx->step_resp, &launch,
			   ctx->launch_state->resp_port[0], preserve_env);
	env_array_merge(&env, (const char **)mpi_env);
	env_array_free(mpi_env);

	launch.envc = envcount(env);
	launch.env = env;
	if (params->cwd)
		launch.cwd = xstrdup(params->cwd);
	else
		launch.cwd = _lookup_cwd();
	launch.alias_list	= params->alias_list;
	launch.mpi_plugin_id = mpi_plugin_id;
	launch.nnodes		= ctx->step_resp->step_layout->node_cnt;
	launch.ntasks		= ctx->step_resp->step_layout->task_cnt;
	launch.slurmd_debug	= params->slurmd_debug;
	launch.switch_job	= ctx->step_resp->switch_job;
	launch.profile		= params->profile;
	launch.task_prolog	= params->task_prolog;
	launch.task_epilog	= params->task_epilog;
	launch.cpu_bind_type	= params->cpu_bind_type;
	launch.cpu_bind		= params->cpu_bind;
	launch.cpu_freq_min	= params->cpu_freq_min;
	launch.cpu_freq_max	= params->cpu_freq_max;
	launch.cpu_freq_gov	= params->cpu_freq_gov;
	launch.tres_bind	= params->tres_bind;
	launch.tres_freq	= params->tres_freq;
	launch.mem_bind_type	= params->mem_bind_type;
	launch.mem_bind		= params->mem_bind;
	launch.accel_bind_type	= params->accel_bind_type;
	launch.flags		= 0;
	if (params->multi_prog)
		launch.flags	|= LAUNCH_MULTI_PROG;
	launch.cpus_per_task	= params->cpus_per_task;
	launch.tres_per_task	= ctx->step_req->tres_per_task;

	launch.threads_per_core	= params->threads_per_core;
	launch.ntasks_per_board = params->ntasks_per_board;
	launch.ntasks_per_core  = params->ntasks_per_core;
	launch.ntasks_per_tres  = params->ntasks_per_tres;
	launch.ntasks_per_socket= params->ntasks_per_socket;

	if (params->no_alloc)
		launch.flags	|= LAUNCH_NO_ALLOC;
	if (ctx->step_req->flags & SSF_OVERCOMMIT)
		launch.flags |= LAUNCH_OVERCOMMIT;

	launch.task_dist	= params->task_dist;
	launch.partition	= params->partition;
	if (params->pty)
		launch.flags |= LAUNCH_PTY;
	launch.acctg_freq	= params->acctg_freq;
	launch.open_mode        = params->open_mode;
	launch.options          = job_options_create();
	launch.complete_nodelist =
		xstrdup(ctx->step_resp->step_layout->node_list);
	spank_set_remote_options (launch.options);
	if (params->parallel_debug)
		launch.flags |= LAUNCH_PARALLEL_DEBUG;

	launch.tasks_to_launch = ctx->step_resp->step_layout->tasks;
	launch.global_task_ids = ctx->step_resp->step_layout->tids;

	launch.select_jobinfo  = ctx->step_resp->select_jobinfo;

	launch.ofname = params->remote_output_filename;
	launch.efname = params->remote_error_filename;
	launch.ifname = params->remote_input_filename;

#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	launch.apptype = params->apptype;
#endif

	if (params->buffered_stdio)
		launch.flags |= LAUNCH_BUFFERED_IO;
	if (params->labelio)
		launch.flags |= LAUNCH_LABEL_IO;
	ctx->launch_state->io =
		client_io_handler_create(params->local_fds,
					 ctx->step_req->num_tasks,
					 launch.nnodes,
					 ctx->step_resp->cred,
					 params->labelio,
					 params->het_job_offset,
					 params->het_job_task_offset);
	if (!ctx->launch_state->io) {
		rc = SLURM_ERROR;
		goto fail1;
	}
	/*
	 * The client_io_t gets a pointer back to the slurm_launch_state
	 * to notify it of I/O errors.
	 */
	ctx->launch_state->io->sls = ctx->launch_state;

	if (client_io_handler_start(ctx->launch_state->io) != SLURM_SUCCESS) {
		rc = SLURM_ERROR;
		goto fail1;
	}
	launch.num_io_port = ctx->launch_state->io->num_listen;
	launch.io_port = xcalloc(launch.num_io_port, sizeof(uint16_t));
	memcpy(launch.io_port, ctx->launch_state->io->listenport,
	       (sizeof(uint16_t) * launch.num_io_port));
	/*
	 * If the io timeout is > 0, create a flag to ping the stepds
	 * if io_timeout seconds pass without stdio traffic to/from
	 * the node.
	 */
	ctx->launch_state->io_timeout = slurm_conf.msg_timeout;

	launch.num_resp_port = ctx->launch_state->num_resp_port;
	launch.resp_port = xcalloc(launch.num_resp_port, sizeof(uint16_t));
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	launch.watch_dog         = ctx->step_resp->watch_dog;	
	launch.watch_dog_script  = ctx->step_resp->watch_dog_script;	
	launch.init_time         = ctx->step_resp->init_time;	
	launch.period            = ctx->step_resp->period;	
	launch.enable_all_nodes  = ctx->step_resp->enable_all_nodes;	
	launch.enable_all_stepds = ctx->step_resp->enable_all_stepds;			
	launch.style_step        = ctx->step_resp->style_step;	
#endif
	memcpy(launch.resp_port, ctx->launch_state->resp_port,
	       (sizeof(uint16_t) * launch.num_resp_port));
	rc = _launch_tasks(ctx, &launch, params->msg_timeout,
			   launch.complete_nodelist);

	/* clean up */
	xfree(launch.resp_port);
	xfree(launch.io_port);

fail1:
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION	
	xfree(launch.watch_dog);
	xfree(launch.watch_dog_script);
#endif
	xfree(launch.user_name);
	xfree(launch.complete_nodelist);
	xfree(launch.cwd);
	env_array_free(env);
	FREE_NULL_LIST(launch.options);
	return rc;
}

/*
 * slurm_step_launch_add - Add tasks to a step that was already started
 * IN ctx - job step context generated by slurm_step_ctx_create
 * IN first_ctx - job step context generated by slurm_step_ctx_create for
 *		first component of the job step
 * IN params - job step parameters
 * IN node_list - list of extra nodes to add
 * RET SLURM_SUCCESS or SLURM_ERROR (with errno set)
 */
extern int slurm_step_launch_add(slurm_step_ctx_t *ctx,
				 slurm_step_ctx_t *first_ctx,
				 const slurm_step_launch_params_t *params,
				 char *node_list)
{
	launch_tasks_request_msg_t launch;
	char **env = NULL;
	char **mpi_env = NULL;
	int rc = SLURM_SUCCESS;
	uint16_t resp_port = 0;
	bool preserve_env = params->preserve_env;
	uint32_t mpi_plugin_id;

	debug("Entering %s", __func__);

	if ((ctx == NULL) || (ctx->magic != STEP_CTX_MAGIC)) {
		error("%s: Not a valid slurm_step_ctx_t", __func__);
		slurm_seterrno(EINVAL);
		return SLURM_ERROR;
	}

	mpi_plugin_id = mpi_g_client_init((char **)&params->mpi_plugin_name);
	if (!mpi_plugin_id) {
		slurm_seterrno(SLURM_MPI_PLUGIN_NAME_INVALID);
		return SLURM_ERROR;
	}

	memset(&launch, 0, sizeof(launch));

	/* Start tasks on compute nodes */
	memcpy(&launch.step_id, &ctx->step_req->step_id,
	       sizeof(launch.step_id));
	launch.uid = ctx->step_req->user_id;
	launch.gid = params->gid;
	launch.argc = params->argc;
	launch.argv = params->argv;
	launch.spank_job_env = params->spank_job_env;
	launch.spank_job_env_size = params->spank_job_env_size;
	launch.cred = ctx->step_resp->cred;
	launch.het_job_step_cnt = params->het_job_step_cnt;
	launch.het_job_id = params->het_job_id;
	launch.het_job_nnodes = params->het_job_nnodes;
	launch.het_job_ntasks = params->het_job_ntasks;
	launch.het_job_offset = params->het_job_offset;
	launch.het_job_task_offset = params->het_job_task_offset;
	launch.het_job_task_cnts = params->het_job_task_cnts;
	launch.het_job_tids = params->het_job_tids;
	launch.het_job_tid_offsets = params->het_job_tid_offsets;
	launch.het_job_node_list = params->het_job_node_list;
	if (params->env == NULL) {
		/*
		 * if the user didn't specify an environment, grab the
		 * environment of the running process
		 */
		env_array_merge(&env, (const char **)environ);
	} else {
		env_array_merge(&env, (const char **)params->env);
	}
	if (first_ctx->launch_state->resp_port)
		resp_port = first_ctx->launch_state->resp_port[0];
	if (params->het_job_ntasks != NO_VAL)
		preserve_env = true;
	env_array_for_step(&env, ctx->step_resp, &launch, resp_port,
			   preserve_env);
	env_array_merge(&env, (const char **)mpi_env);
	env_array_free(mpi_env);

	launch.envc = envcount(env);
	launch.env = env;
	if (params->cwd)
		launch.cwd = xstrdup(params->cwd);
	else
		launch.cwd = _lookup_cwd();
	launch.alias_list	= params->alias_list;
	launch.mpi_plugin_id = mpi_plugin_id;
	launch.nnodes		= ctx->step_resp->step_layout->node_cnt;
	launch.ntasks		= ctx->step_resp->step_layout->task_cnt;
	launch.slurmd_debug	= params->slurmd_debug;
	launch.switch_job	= ctx->step_resp->switch_job;
	launch.profile		= params->profile;
	launch.task_prolog	= params->task_prolog;
	launch.task_epilog	= params->task_epilog;
	launch.cpu_bind_type	= params->cpu_bind_type;
	launch.cpu_bind		= params->cpu_bind;
	launch.cpu_freq_min	= params->cpu_freq_min;
	launch.cpu_freq_max	= params->cpu_freq_max;
	launch.cpu_freq_gov	= params->cpu_freq_gov;
	launch.tres_bind	= params->tres_bind;
	launch.tres_freq	= params->tres_freq;
	launch.mem_bind_type	= params->mem_bind_type;
	launch.mem_bind		= params->mem_bind;
	launch.accel_bind_type	= params->accel_bind_type;
	launch.flags = 0;
	if (params->multi_prog)
		launch.flags |= LAUNCH_MULTI_PROG;
	launch.cpus_per_task	= params->cpus_per_task;
	launch.task_dist	= params->task_dist;
	launch.partition	= params->partition;
	if (params->pty)
		launch.flags |= LAUNCH_PTY;
	launch.acctg_freq	= params->acctg_freq;

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION	
	// launch.watch_dog         = params->watch_dog;	
	// launch.watch_dog_script  = params->watch_dog_script;	
	// launch.init_time         = params->init_time;	
	// launch.period            = params->period;	
	// launch.enable_all_nodes  = params->enable_all_nodes;	
	// launch.enable_all_stepds = params->enable_all_stepds;			
	// launch.style_step        = params->style_step;	
	launch.watch_dog         = ctx->step_resp->watch_dog;	
	launch.watch_dog_script  = ctx->step_resp->watch_dog_script;	
	launch.init_time         = ctx->step_resp->init_time;	
	launch.period            = ctx->step_resp->period;	
	launch.enable_all_nodes  = ctx->step_resp->enable_all_nodes;	
	launch.enable_all_stepds = ctx->step_resp->enable_all_stepds;			
	launch.style_step        = ctx->step_resp->style_step;	
#endif	
	launch.open_mode        = params->open_mode;
	launch.options          = job_options_create();
	launch.complete_nodelist =
		xstrdup(ctx->step_resp->step_layout->node_list);

	spank_set_remote_options (launch.options);
	if (params->parallel_debug)
		launch.flags |= LAUNCH_PARALLEL_DEBUG;

	launch.tasks_to_launch = ctx->step_resp->step_layout->tasks;
	launch.global_task_ids = ctx->step_resp->step_layout->tids;

	launch.select_jobinfo  = ctx->step_resp->select_jobinfo;

	launch.ofname = params->remote_output_filename;
	launch.efname = params->remote_error_filename;
	launch.ifname = params->remote_input_filename;
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	launch.apptype = params->apptype;
#endif
	if (params->buffered_stdio)
		launch.flags	|= LAUNCH_BUFFERED_IO;
	if (params->labelio)
		launch.flags	|= LAUNCH_LABEL_IO;
	ctx->launch_state->io =
		client_io_handler_create(params->local_fds,
					 ctx->step_req->num_tasks,
					 launch.nnodes,
					 ctx->step_resp->cred,
					 params->labelio,
					 params->het_job_offset,
					 params->het_job_task_offset);
	if (!ctx->launch_state->io) {
		rc = SLURM_ERROR;
		goto fail1;
	}
	/*
	 * The client_io_t gets a pointer back to the slurm_launch_state
	 * to notify it of I/O errors.
	 */
	ctx->launch_state->io->sls = ctx->launch_state;

	if (client_io_handler_start(ctx->launch_state->io) != SLURM_SUCCESS) {
		rc = SLURM_ERROR;
		goto fail1;
	}
	launch.num_io_port = ctx->launch_state->io->num_listen;
	launch.io_port = xcalloc(launch.num_io_port, sizeof(uint16_t));
	memcpy(launch.io_port, ctx->launch_state->io->listenport,
	       (sizeof(uint16_t) * launch.num_io_port));
	/*
	 * If the io timeout is > 0, create a flag to ping the stepds
	 * if io_timeout seconds pass without stdio traffic to/from
	 * the node.
	 */
	ctx->launch_state->io_timeout = slurm_conf.msg_timeout;

	if (first_ctx->launch_state->num_resp_port &&
	    first_ctx->launch_state->resp_port) {
		launch.num_resp_port = first_ctx->launch_state->num_resp_port;
		launch.resp_port = xcalloc(launch.num_resp_port,
					   sizeof(uint16_t));
		memcpy(launch.resp_port, first_ctx->launch_state->resp_port,
		       (sizeof(uint16_t) * launch.num_resp_port));
	}

	rc = _launch_tasks(ctx, &launch, params->msg_timeout, node_list);

fail1:
	/* clean up */
	xfree(launch.user_name);
	xfree(launch.resp_port);
	xfree(launch.io_port);

	xfree(launch.cwd);
	env_array_free(env);
	FREE_NULL_LIST(launch.options);

	return rc;
}

static void _step_abort(slurm_step_ctx_t *ctx)
{
	struct step_launch_state *sls = ctx->launch_state;

	if (!sls->abort_action_taken) {
		slurm_kill_job_step(ctx->job_id, ctx->step_resp->job_step_id,
				    SIGKILL);
		sls->abort_action_taken = true;
	}
}

/*
 * Block until all tasks have started.
 */
int slurm_step_launch_wait_start(slurm_step_ctx_t *ctx)
{
	struct step_launch_state *sls = ctx->launch_state;
	struct timespec ts;

	ts.tv_sec  = time(NULL);
	ts.tv_nsec = 0;
	ts.tv_sec += 600;	/* 10 min allowed for launch */

	/* Wait for all tasks to start */
	slurm_mutex_lock(&sls->lock);
	while (bit_set_count(sls->tasks_started) < sls->tasks_requested) {
		if (sls->abort) {
			_step_abort(ctx);
			slurm_mutex_unlock(&sls->lock);
			return SLURM_ERROR;
		}
		if (pthread_cond_timedwait(&sls->cond, &sls->lock, &ts) ==
		    ETIMEDOUT) {
			error("timeout waiting for task launch, "
			      "started %d of %d tasks",
			      bit_set_count(sls->tasks_started),
			      sls->tasks_requested);
			sls->abort = true;
			_step_abort(ctx);
			slurm_cond_broadcast(&sls->cond);
			slurm_mutex_unlock(&sls->lock);
			return SLURM_ERROR;
		}
	}

	_cr_notify_step_launch(ctx);

	slurm_mutex_unlock(&sls->lock);
	return SLURM_SUCCESS;
}

/*
 * Block until all tasks have finished (or failed to start altogether).
 */
void slurm_step_launch_wait_finish(slurm_step_ctx_t *ctx)
{
	struct step_launch_state *sls;
	struct timespec ts = {0, 0};
	bool time_set = false;
	int errnum;

	if (!ctx || (ctx->magic != STEP_CTX_MAGIC))
		return;

	sls = ctx->launch_state;

	/* Wait for all tasks to complete */
	slurm_mutex_lock(&sls->lock);
	while (bit_set_count(sls->tasks_exited) < sls->tasks_requested) {
		if (!sls->abort) {
			slurm_cond_wait(&sls->cond, &sls->lock);
		} else {
			if (!sls->abort_action_taken) {
				slurm_kill_job_step(ctx->job_id,
						    ctx->step_resp->
						    job_step_id,
						    SIGKILL);
				sls->abort_action_taken = true;
			}
			if (!time_set) {
				/* Only set the time once, because we only want
				 * to wait STEP_ABORT_TIME, no matter how many
				 * times the condition variable is signaled.
				 */
				ts.tv_sec = time(NULL) + STEP_ABORT_TIME
					+ slurm_conf.kill_wait;
				time_set = true;
				/* FIXME - should this be a callback? */
				info("Job step aborted: Waiting up to "
				     "%d seconds for job step to finish.",
				     slurm_conf.kill_wait + STEP_ABORT_TIME);
			}

			errnum = pthread_cond_timedwait(&sls->cond,
							&sls->lock, &ts);
			if (errnum == ETIMEDOUT) {
				error("Timed out waiting for job step to "
				      "complete");
				/*
				 * Send kill again, in case steps were still
				 * launching the first time.
				 * FIXME - eventually the slurmd should
				 *   be made smart enough to really ensure
				 *   that a killed step never starts.
				 */
				slurm_kill_job_step(ctx->job_id,
						    ctx->step_resp->job_step_id,
						    SIGKILL);
				client_io_handler_abort(sls->io);
				break;
			} else if (errnum != 0) {
				error("Error waiting on condition in"
				      " slurm_step_launch_wait_finish: %m");
				client_io_handler_abort(sls->io);
				break;
			}
		}
	}
	if (sls->abort && !time_set)
		info("Job step aborted");	/* no need to wait */

	if (!force_terminated_job && task_exit_signal)
		info("Force Terminated %ps", &ctx->step_req->step_id);

	/*
	 * task_exit_signal != 0 when srun receives a message that a task
	 * exited with a SIGTERM or SIGKILL.  Without this test, a hang in srun
	 * might occur when a node gets a hard power failure, and TCP does not
	 * indicate that the I/O connection closed.  The I/O thread could
	 * block waiting for an EOF message, even though the remote process
	 * has died.  In this case, use client_io_handler_abort to force the
	 * I/O thread to stop listening for stdout or stderr and shutdown.
	 */
	if (task_exit_signal)
		client_io_handler_abort(sls->io);

	/* Then shutdown the message handler thread */
	if (sls->msg_handle)
		eio_signal_shutdown(sls->msg_handle);

	slurm_mutex_unlock(&sls->lock);
	if (sls->msg_thread)
		pthread_join(sls->msg_thread, NULL);
	slurm_mutex_lock(&sls->lock);
	pmi_kvs_free();

	if (sls->msg_handle) {
		eio_handle_destroy(sls->msg_handle);
		sls->msg_handle = NULL;
	}

	/* Shutdown the IO timeout thread, if one exists */
	if (sls->io_timeout_thread_created) {
		sls->halt_io_test = true;
		slurm_cond_broadcast(&sls->cond);

		slurm_mutex_unlock(&sls->lock);
		pthread_join(sls->io_timeout_thread, NULL);
		slurm_mutex_lock(&sls->lock);
	}

	/* Then wait for the IO thread to finish */
	slurm_mutex_unlock(&sls->lock);
	client_io_handler_finish(sls->io);
	slurm_mutex_lock(&sls->lock);

	client_io_handler_destroy(sls->io);
	sls->io = NULL;

	sls->mpi_rc = mpi_g_client_fini(sls->mpi_state);
	slurm_mutex_unlock(&sls->lock);
}

/*
 * Abort an in-progress launch, or terminate the fully launched job step.
 *
 * Can be called from a signal handler.
 */
void slurm_step_launch_abort(slurm_step_ctx_t *ctx)
{
	struct step_launch_state *sls;

	if (!ctx || ctx->magic != STEP_CTX_MAGIC)
		return;

	sls = ctx->launch_state;

	slurm_mutex_lock(&sls->lock);
	sls->abort = true;
	slurm_cond_broadcast(&sls->cond);
	slurm_mutex_unlock(&sls->lock);
}

/*
 * Forward a signal to all those nodes with running tasks
 */
extern void slurm_step_launch_fwd_signal(slurm_step_ctx_t *ctx, int signo)
{
	int node_id, j, num_tasks;
	slurm_msg_t req;
	signal_tasks_msg_t msg;
	hostlist_t hl;
	char *name = NULL;
	List ret_list = NULL;
	ListIterator itr;
	ret_data_info_t *ret_data_info = NULL;
	int rc = SLURM_SUCCESS;
	struct step_launch_state *sls = ctx->launch_state;
	bool retry = false;
	int retry_cnt = 0;

	/* common to all tasks */
	memset(&msg, 0, sizeof(msg));
	memcpy(&msg.step_id, &ctx->step_req->step_id, sizeof(msg.step_id));
	msg.signal      = (uint16_t) signo;

	slurm_mutex_lock(&sls->lock);

	hl = hostlist_create(NULL);
	for (node_id = 0;
	     node_id < ctx->step_resp->step_layout->node_cnt;
	     node_id++) {
		bool active = false;
		num_tasks = sls->layout->tasks[node_id];
		for (j = 0; j < num_tasks; j++) {
			if (!bit_test(sls->tasks_exited,
				      sls->layout->tids[node_id][j])) {
				/* this one has active tasks */
				active = true;
				break;
			}
		}

		if (!active)
			continue;

		if (ctx->step_resp->step_layout->front_end) {
			hostlist_push_host(hl,
				      ctx->step_resp->step_layout->front_end);
			break;
		} else {
			name = nodelist_nth_host(sls->layout->node_list,
						 node_id);
			hostlist_push_host(hl, name);
			free(name);
		}
	}

	slurm_mutex_unlock(&sls->lock);

	if (!hostlist_count(hl)) {
		verbose("no active tasks in %ps to send signal %d",
		        &ctx->step_req->step_id, signo);
		hostlist_destroy(hl);
		return;
	}
	name = hostlist_ranged_string_xmalloc(hl);
	hostlist_destroy(hl);

RESEND:	slurm_msg_t_init(&req);
	slurm_msg_set_r_uid(&req, SLURM_AUTH_UID_ANY);
	req.msg_type = REQUEST_SIGNAL_TASKS;
	req.data     = &msg;

	if (ctx->step_resp->use_protocol_ver)
		req.protocol_version = ctx->step_resp->use_protocol_ver;

	debug2("sending signal %d to %ps on hosts %s",
	       signo, &ctx->step_req->step_id, name);

	if (!(ret_list = slurm_send_recv_msgs(name, &req, 0))) {
		error("fwd_signal: slurm_send_recv_msgs really failed badly");
		xfree(name);
		return;
	}

	itr = list_iterator_create(ret_list);
	while ((ret_data_info = list_next(itr))) {
		rc = slurm_get_return_code(ret_data_info->type,
					   ret_data_info->data);
		/*
		 * Report error unless it is "Invalid job id" which
		 * probably just means the tasks exited in the meanwhile.
		 */
		if ((rc != 0) && (rc != ESLURM_INVALID_JOB_ID) &&
		    (rc != ESLURMD_JOB_NOTRUNNING) && (rc != ESRCH) &&
		    (rc != EAGAIN) &&
		    (rc != ESLURM_TRANSITION_STATE_NO_UPDATE)) {
			error("Failure sending signal %d to %ps on node %s: %s",
			      signo, &ctx->step_req->step_id,
			      ret_data_info->node_name,
			      slurm_strerror(rc));
		}
		if ((rc == EAGAIN) || (rc == ESLURM_TRANSITION_STATE_NO_UPDATE))
			retry = true;
	}
	list_iterator_destroy(itr);
	FREE_NULL_LIST(ret_list);
	if (retry) {
		retry = false;
		if (retry_cnt++ < 4) {
			sleep(retry_cnt);
			goto RESEND;
		}
	}
	xfree(name);
}

/**********************************************************************
 * Functions used by step_ctx code, but not exported throught the API
 **********************************************************************/
/*
 * Create a launch state structure for a specified step context, "ctx".
 */
struct step_launch_state *step_launch_state_create(slurm_step_ctx_t *ctx)
{
	struct step_launch_state *sls;
	slurm_step_layout_t *layout = ctx->step_resp->step_layout;
	int ii;

	sls = xmalloc(sizeof(struct step_launch_state));
	sls->slurmctld_socket_fd = -1;
	sls->tasks_requested = layout->task_cnt;
	sls->tasks_started = bit_alloc(layout->task_cnt);
	sls->tasks_exited = bit_alloc(layout->task_cnt);
	sls->node_io_error = bit_alloc(layout->node_cnt);
	sls->io_deadline = xcalloc(layout->node_cnt, sizeof(time_t));
	sls->io_timeout_thread_created = false;
	sls->io_timeout = 0;
	sls->halt_io_test = false;
	sls->layout = layout;
	sls->resp_port = NULL;
	sls->abort = false;
	sls->abort_action_taken = false;
	/* NOTE: No malloc() of sls->mpi_info required */
	memcpy(&sls->mpi_info->step_id, &ctx->step_req->step_id,
	       sizeof(sls->mpi_info->step_id));
	sls->mpi_info->het_job_id = NO_VAL;
	sls->mpi_info->het_job_task_offset = NO_VAL;
	sls->mpi_info->step_layout = layout;
	sls->mpi_state = NULL;
	slurm_mutex_init(&sls->lock);
	slurm_cond_init(&sls->cond, NULL);

	for (ii = 0; ii < layout->node_cnt; ii++) {
		sls->io_deadline[ii] = (time_t)NO_VAL;
	}
	return sls;
}

/*
 * If a steps size has changed update the launch_state structure for a
 * specified step context, "ctx".
 */
void step_launch_state_alter(slurm_step_ctx_t *ctx)
{
	struct step_launch_state *sls = ctx->launch_state;
	slurm_step_layout_t *layout = ctx->step_resp->step_layout;
	int ii;

	xassert(sls);
	sls->tasks_requested = layout->task_cnt;
	bit_realloc(sls->tasks_started, layout->task_cnt);
	bit_realloc(sls->tasks_exited, layout->task_cnt);
	bit_realloc(sls->node_io_error, layout->node_cnt);
	xrealloc(sls->io_deadline, sizeof(time_t) * layout->node_cnt);
	sls->layout = sls->mpi_info->step_layout = layout;

	for (ii = 0; ii < layout->node_cnt; ii++) {
		sls->io_deadline[ii] = (time_t)NO_VAL;
	}
}

/*
 * Free the memory associated with the a launch state structure.
 */
void step_launch_state_destroy(struct step_launch_state *sls)
{
	/* First undo anything created in step_launch_state_create() */
	slurm_mutex_destroy(&sls->lock);
	slurm_cond_destroy(&sls->cond);
	FREE_NULL_BITMAP(sls->tasks_started);
	FREE_NULL_BITMAP(sls->tasks_exited);
	FREE_NULL_BITMAP(sls->node_io_error);
	xfree(sls->io_deadline);

	/* Now clean up anything created by slurm_step_launch() */
	if (sls->resp_port != NULL) {
		xfree(sls->resp_port);
	}
}

/**********************************************************************
 * CR functions
 **********************************************************************/

/* connect to srun_cr */
static int _connect_srun_cr(char *addr)
{
	struct sockaddr_un sa;
	unsigned int sa_len;
	int fd, rc;

	if (!addr) {
		error("%s: socket path name is NULL", __func__);
		return -1;
	}
	if (strlen(addr) >= sizeof(sa.sun_path)) {
		error("%s: socket path name too long (%s)", __func__, addr);
		return -1;
	}

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		error("failed creating cr socket: %m");
		return -1;
	}
	memset(&sa, 0, sizeof(sa));

	sa.sun_family = AF_UNIX;
	strlcpy(sa.sun_path, addr, sizeof(sa.sun_path));
	sa_len = strlen(sa.sun_path) + sizeof(sa.sun_family);

	while (((rc = connect(fd, (struct sockaddr *)&sa, sa_len)) < 0) &&
	       (errno == EINTR));

	if (rc < 0) {
		debug2("failed connecting cr socket: %m");
		close(fd);
		return -1;
	}
	return fd;
}

/* send job_id, step_id, node_list to srun_cr */
static int _cr_notify_step_launch(slurm_step_ctx_t *ctx)
{
	int fd, len, rc = 0;
	char *cr_sock_addr = NULL;

	cr_sock_addr = getenv("SLURM_SRUN_CR_SOCKET");
	if (cr_sock_addr == NULL) { /* not run under srun_cr */
		return 0;
	}

	if ((fd = _connect_srun_cr(cr_sock_addr)) < 0) {
		debug2("failed connecting srun_cr. take it not running under "
		       "srun_cr.");
		return 0;
	}
	if (write(fd, &ctx->job_id, sizeof(uint32_t)) != sizeof(uint32_t)) {
		error("failed writing job_id to srun_cr: %m");
		rc = -1;
		goto out;
	}
	if (write(fd, &ctx->step_resp->job_step_id, sizeof(uint32_t)) !=
	    sizeof(uint32_t)) {
		error("failed writing job_step_id to srun_cr: %m");
		rc = -1;
		goto out;
	}
	len = strlen(ctx->step_resp->step_layout->node_list);
	if (write(fd, &len, sizeof(int)) != sizeof(int)) {
		error("failed writing nodelist length to srun_cr: %m");
		rc = -1;
		goto out;
	}
	if (write(fd, ctx->step_resp->step_layout->node_list, len + 1) !=
	    (len + 1)) {
		error("failed writing nodelist to srun_cr: %m");
		rc = -1;
	}
 out:
	close (fd);
	return rc;
}

/**********************************************************************
 * Message handler functions
 **********************************************************************/
static void *_msg_thr_internal(void *arg)
{
	struct step_launch_state *sls = (struct step_launch_state *)arg;

	eio_handle_mainloop(sls->msg_handle);

	return NULL;
}

static inline int
_estimate_nports(int nclients, int cli_per_port)
{
	div_t d;
	d = div(nclients, cli_per_port);
	return d.rem > 0 ? d.quot + 1 : d.quot;
}

static int _msg_thr_create(struct step_launch_state *sls, int num_nodes)
{
	int sock = -1;
	uint16_t port;
	eio_obj_t *obj;
	int i, rc = SLURM_SUCCESS;
	uint16_t *ports;

	debug("Entering _msg_thr_create()");

	sls->msg_handle = eio_handle_create(slurm_conf.eio_timeout);
	sls->num_resp_port = _estimate_nports(num_nodes, 48);
	sls->resp_port = xcalloc(sls->num_resp_port, sizeof(uint16_t));

	/* multiple jobs (easily induced via no_alloc) and highly
	 * parallel jobs using PMI sometimes result in slow message
	 * responses and timeouts. Raise the default timeout for srun. */
	if (!message_socket_ops.timeout)
		message_socket_ops.timeout = slurm_conf.msg_timeout * 8000;

	ports = slurm_get_srun_port_range();
	for (i = 0; i < sls->num_resp_port; i++) {
		int cc;

		if (ports)
			cc = net_stream_listen_ports(&sock, &port, ports, false);
		else
			cc = net_stream_listen(&sock, &port);
		if (cc < 0) {
			error("unable to initialize step launch listening "
			      "socket: %m");
			return SLURM_ERROR;
		}
		sls->resp_port[i] = port;
		obj = eio_obj_create(sock, &message_socket_ops, (void *)sls);
		eio_new_initial_obj(sls->msg_handle, obj);
	}
	/* finally, add the listening port that we told the slurmctld about
	 * eariler in the step context creation phase */
	if (sls->slurmctld_socket_fd > -1) {
		obj = eio_obj_create(sls->slurmctld_socket_fd,
				     &message_socket_ops, (void *)sls);
		eio_new_initial_obj(sls->msg_handle, obj);
	}

	slurm_thread_create(&sls->msg_thread, _msg_thr_internal, sls);
	return rc;
}

static void
_launch_handler(struct step_launch_state *sls, slurm_msg_t *resp)
{
	launch_tasks_response_msg_t *msg = resp->data;
	int i;

	slurm_mutex_lock(&sls->lock);
	if ((msg->count_of_pids > 0) &&
	    bit_test(sls->tasks_started, msg->task_ids[0])) {
		debug("%s: duplicate launch response received from node %s",
		      __func__, msg->node_name);
		slurm_mutex_unlock(&sls->lock);
		return;
	}

	if (msg->return_code) {
		for (i = 0; i < msg->count_of_pids; i++) {
			error("task %u launch failed: %s",
			      msg->task_ids[i],
			      slurm_strerror(msg->return_code));
			bit_set(sls->tasks_started, msg->task_ids[i]);
			bit_set(sls->tasks_exited, msg->task_ids[i]);
		}
	} else {
		for (i = 0; i < msg->count_of_pids; i++)
			bit_set(sls->tasks_started, msg->task_ids[i]);
	}
	if (sls->callback.task_start != NULL)
		(sls->callback.task_start)(msg);

	slurm_cond_broadcast(&sls->cond);
	slurm_mutex_unlock(&sls->lock);

}

static void
_exit_handler(struct step_launch_state *sls, slurm_msg_t *exit_msg)
{
	task_exit_msg_t *msg = (task_exit_msg_t *) exit_msg->data;
	void (*task_finish)(task_exit_msg_t *);
	int i;

	if ((msg->step_id.job_id != sls->mpi_info->step_id.job_id) ||
	    (msg->step_id.step_id != sls->mpi_info->step_id.step_id)) {
		debug("Received MESSAGE_TASK_EXIT from wrong job: %ps",
		      &msg->step_id);
		return;
	}

	/* Record SIGTERM and SIGKILL termination codes to
	 * recognize abnormal termination */
	if (WIFSIGNALED(msg->return_code)) {
		i = WTERMSIG(msg->return_code);
		if ((i == SIGKILL) || (i == SIGTERM))
			task_exit_signal = i;
	}

	slurm_mutex_lock(&sls->lock);
	task_finish = sls->callback.task_finish;
	slurm_mutex_unlock(&sls->lock);
	if (task_finish != NULL)
		(task_finish)(msg);	/* Outside of lock for performance */

	slurm_mutex_lock(&sls->lock);
	for (i = 0; i < msg->num_tasks; i++) {
		debug("task %u done", msg->task_id_list[i]);
		bit_set(sls->tasks_exited, msg->task_id_list[i]);
	}

	slurm_cond_broadcast(&sls->cond);
	slurm_mutex_unlock(&sls->lock);
}

static void
_job_complete_handler(struct step_launch_state *sls, slurm_msg_t *complete_msg)
{
	srun_job_complete_msg_t *step_msg =
		(srun_job_complete_msg_t *) complete_msg->data;

#ifdef __METASTACK_BUG_SRUN_RECVMSG_VERIF	
	char *slurm_job_id = NULL;
	uint32_t job_id = 0;

	slurm_job_id = getenv("SLURM_JOB_ID");

	if (slurm_job_id == NULL) {
		return;
	}

	job_id = atol(slurm_job_id);

	if (step_msg->step_id == NO_VAL) {
		verbose("Complete job %u received",
			step_msg->job_id);
		if (step_msg->job_id != job_id) {
			verbose("%s: Ignoring job_complete for job %u because our job ID is %u",
				__func__, step_msg->job_id, job_id);
			return;
		}	
	} else {
		verbose("Complete %ps received", &step_msg->step_id);
		if (step_msg->step_id != job_id) {
			verbose("%s: Ignoring job_complete for job %u because our job ID is %u",
				__func__, step_msg->step_id, job_id);
			return;
		}		
	}	
#else
	if (step_msg->step_id == NO_VAL) {
		verbose("Complete job %u received",
			step_msg->job_id);
	} else {
		verbose("Complete %ps received", &step_msg->step_id);
	}
#endif

	if (sls->callback.step_complete)
		(sls->callback.step_complete)(step_msg);

	force_terminated_job = true;
	slurm_mutex_lock(&sls->lock);
	sls->abort = true;
	slurm_cond_broadcast(&sls->cond);
	slurm_mutex_unlock(&sls->lock);
}

static void
_timeout_handler(struct step_launch_state *sls, slurm_msg_t *timeout_msg)
{
	srun_timeout_msg_t *step_msg =
		(srun_timeout_msg_t *) timeout_msg->data;

	if (sls->callback.step_timeout)
		(sls->callback.step_timeout)(step_msg);

	slurm_mutex_lock(&sls->lock);
	slurm_cond_broadcast(&sls->cond);
	slurm_mutex_unlock(&sls->lock);
}

/*
 * Take the list of node names of down nodes and convert into an
 * array of nodeids for the step.  The nodeid array is passed to
 * client_io_handler_downnodes to notify the IO handler to expect no
 * further IO from that node.
 */
static void
_node_fail_handler(struct step_launch_state *sls, slurm_msg_t *fail_msg)
{
	srun_node_fail_msg_t *nf = fail_msg->data;
	hostlist_t fail_nodes, all_nodes;
	hostlist_iterator_t fail_itr;
	int num_node_ids;
	int *node_ids;
	int i, j;
	int node_id, num_tasks;

	error("Node failure on %s", nf->nodelist);

	fail_nodes = hostlist_create(nf->nodelist);
	fail_itr = hostlist_iterator_create(fail_nodes);
	num_node_ids = hostlist_count(fail_nodes);
	node_ids = xcalloc(num_node_ids, sizeof(int));

	slurm_mutex_lock(&sls->lock);
	all_nodes = hostlist_create(sls->layout->node_list);
	/* find the index number of each down node */
	for (i = 0; i < num_node_ids; i++) {
#ifdef HAVE_FRONT_END
		node_id = 0;
#else
		char *node = hostlist_next(fail_itr);
		node_id = node_ids[i] = hostlist_find(all_nodes, node);
		if (node_id < 0) {
			error(  "Internal error: bad SRUN_NODE_FAIL message. "
				"Node %s not part of this job step", node);
			free(node);
			continue;
		}
		free(node);
#endif

		/* find all of the tasks that should run on this node and
		 * mark them as having started and exited.  If they haven't
		 * started yet, they never will, and likewise for exiting.
		 */
		num_tasks = sls->layout->tasks[node_id];
		for (j = 0; j < num_tasks; j++) {
			debug2("marking task %d done on failed node %d",
			       sls->layout->tids[node_id][j], node_id);
			bit_set(sls->tasks_started,
				sls->layout->tids[node_id][j]);
			bit_set(sls->tasks_exited,
				sls->layout->tids[node_id][j]);
		}
	}

	client_io_handler_downnodes(sls->io, node_ids, num_node_ids);
	slurm_cond_broadcast(&sls->cond);
	slurm_mutex_unlock(&sls->lock);

	xfree(node_ids);
	hostlist_iterator_destroy(fail_itr);
	hostlist_destroy(fail_nodes);
	hostlist_destroy(all_nodes);
}

/*
 * Receive a message when a slurmd cold starts, that the step on that node
 * may have died.  Verify that tasks on these nodes(s) are still alive,
 * and abort the job step if they are not.
 * This message could be the result of the slurmd daemon cold-starting
 * or a race condition when tasks are starting or terminating.
 */
static void
_step_missing_handler(struct step_launch_state *sls, slurm_msg_t *missing_msg)
{
	srun_step_missing_msg_t *step_missing = missing_msg->data;
	hostlist_t fail_nodes, all_nodes;
	hostlist_iterator_t fail_itr;
	char *node;
	int num_node_ids;
	int i, j;
	int node_id;
	bool  test_message_sent;
	int   num_tasks;
	bool  active;

	debug("Step %ps missing from node(s) %s",
	      &step_missing->step_id, step_missing->nodelist);

	slurm_mutex_lock(&sls->lock);

	if (!sls->io_timeout_thread_created) {
		sls->io_timeout_thread_created = true;
		slurm_thread_create(&sls->io_timeout_thread,
				    _check_io_timeout, sls);
	}

	fail_nodes = hostlist_create(step_missing->nodelist);
	fail_itr = hostlist_iterator_create(fail_nodes);
	num_node_ids = hostlist_count(fail_nodes);

	all_nodes = hostlist_create(sls->layout->node_list);

	for (i = 0; i < num_node_ids; i++) {
		node = hostlist_next(fail_itr);
		node_id = hostlist_find(all_nodes, node);
		if (node_id < 0) {
			error("Internal error: bad SRUN_STEP_MISSING message. "
			      "Node %s not part of this job step", node);
			free(node);
			continue;
		}
		free(node);

		/*
		 * If all tasks for this node have either not started or already
		 * exited, ignore the missing step message for this node.
		 */
		num_tasks = sls->layout->tasks[node_id];
		active = false;
		for (j = 0; j < num_tasks; j++) {
			if (bit_test(sls->tasks_started,
				     sls->layout->tids[node_id][j]) &&
			    !bit_test(sls->tasks_exited,
				      sls->layout->tids[node_id][j])) {
				active = true;
				break;
			}
		}
		if (!active)
			continue;

		/* If this is true, an I/O error has already occurred on the
		 * stepd for the current node, and the job should abort */
		if (bit_test(sls->node_io_error, node_id)) {
			error("Aborting, step missing and io error on node %d",
			      node_id);
			sls->abort = true;
			slurm_cond_broadcast(&sls->cond);
			break;
		}

		/*
		 * A test is already is progress. Ignore message for this node.
		 */
		if (sls->io_deadline[node_id] != NO_VAL) {
			debug("Test in progress for node %d, ignoring message",
			      node_id);
			continue;
		}

		sls->io_deadline[node_id] = time(NULL) + sls->io_timeout;

		debug("Testing connection to node %d", node_id);
		if (client_io_handler_send_test_message(sls->io, node_id,
							&test_message_sent)) {
			/*
			 * If unable to test a connection, assume the step
			 * is having problems and abort.  If unable to test,
			 * the system is probably having serious problems, so
			 * aborting the step seems reasonable.
			 */
			error("Aborting, can not test connection to node %d.",
			      node_id);
			sls->abort = true;
			slurm_cond_broadcast(&sls->cond);
			break;
		}

		/*
		 * test_message_sent should be true unless this node either
		 * hasn't started or already finished.  Poke the io_timeout
		 * thread to make sure it will abort the job if the deadline
		 * for receiving a response passes.
		 */
		if (test_message_sent) {
			slurm_cond_broadcast(&sls->cond);
		} else {
			sls->io_deadline[node_id] = (time_t)NO_VAL;
		}
	}
	slurm_mutex_unlock(&sls->lock);

	hostlist_iterator_destroy(fail_itr);
	hostlist_destroy(fail_nodes);
	hostlist_destroy(all_nodes);
}

/* This RPC typically used to send a signal an external program that
 * is usually wrapped by srun.
 */
static void
_step_step_signal(struct step_launch_state *sls, slurm_msg_t *signal_msg)
{
	job_step_kill_msg_t *step_signal = signal_msg->data;
	debug2("Signal %u requested for step %ps", step_signal->signal,
	       &step_signal->step_id);
	if (sls->callback.step_signal)
		(sls->callback.step_signal)(step_signal->signal);

}

/*
 * Identify the incoming message and call the appropriate handler function.
 */
static void
_handle_msg(void *arg, slurm_msg_t *msg)
{
	struct step_launch_state *sls = (struct step_launch_state *)arg;
	uid_t req_uid;
	uid_t uid = getuid();
	srun_user_msg_t *um;
	int rc;

	req_uid = auth_g_get_uid(msg->auth_cred);

	if ((req_uid != slurm_conf.slurm_user_id) && (req_uid != 0) &&
	    (req_uid != uid)) {
		error ("Security violation, slurm message from uid %u",
		       (unsigned int) req_uid);
 		return;
	}

	switch (msg->msg_type) {
	case RESPONSE_LAUNCH_TASKS:
		debug2("received task launch");
		_launch_handler(sls, msg);
		slurm_send_rc_msg(msg, SLURM_SUCCESS);
		break;
	case MESSAGE_TASK_EXIT:
		debug2("received task exit");
		_exit_handler(sls, msg);
		slurm_send_rc_msg(msg, SLURM_SUCCESS);
		break;
	case SRUN_PING:
		debug3("slurmctld ping received");
		slurm_send_rc_msg(msg, SLURM_SUCCESS);
		break;
	case SRUN_JOB_COMPLETE:
		debug2("received job step complete message");
		_job_complete_handler(sls, msg);
		break;
	case SRUN_TIMEOUT:
		debug2("received job step timeout message");
		_timeout_handler(sls, msg);
		break;
	case SRUN_USER_MSG:
		um = msg->data;
		info("%s", um->msg);
		break;
	case SRUN_NODE_FAIL:
		debug2("received srun node fail");
		_node_fail_handler(sls, msg);
		break;
	case SRUN_STEP_MISSING:
		debug2("received notice of missing job step");
		_step_missing_handler(sls, msg);
		break;
	case SRUN_STEP_SIGNAL:
		debug2("received step signal RPC");
		_step_step_signal(sls, msg);
		break;
	case PMI_KVS_PUT_REQ:
		debug2("PMI_KVS_PUT_REQ received");
		rc = pmi_kvs_put((kvs_comm_set_t *) msg->data);
		slurm_send_rc_msg(msg, rc);
		break;
	case PMI_KVS_GET_REQ:
		debug2("PMI_KVS_GET_REQ received");
		rc = pmi_kvs_get((kvs_get_msg_t *) msg->data);
		slurm_send_rc_msg(msg, rc);
		break;
	default:
		error("%s: received spurious message type: %u",
		      __func__, msg->msg_type);
		break;
	}
	return;
}

/**********************************************************************
 * Task launch functions
 **********************************************************************/

/* Since the slurmd usually controls the finishing of tasks to the
 * controller this needs to happen here if there was a problem with a
 * task launch to the slurmd since there will not be cleanup of this
 * anywhere else.
 */
static int _fail_step_tasks(slurm_step_ctx_t *ctx, char *node, int ret_code)
{
	slurm_msg_t req;
	step_complete_msg_t msg;
	int rc = -1;
	int nodeid = 0;
	struct step_launch_state *sls = ctx->launch_state;

#ifndef HAVE_FRONT_END
	/* It is always 0 for front end systems */
	nodeid = nodelist_find(ctx->step_resp->step_layout->node_list, node);
#endif

	slurm_mutex_lock(&sls->lock);
	for (int i = 0; i < sls->layout->tasks[nodeid]; i++) {
		debug2("marking task %d done on failed node %d",
		       sls->layout->tids[nodeid][i], nodeid);
		bit_set(sls->tasks_started, sls->layout->tids[nodeid][i]);
		bit_set(sls->tasks_exited, sls->layout->tids[nodeid][i]);
	}

	sls->abort = true;
	slurm_cond_broadcast(&sls->cond);
	slurm_mutex_unlock(&sls->lock);

	memset(&msg, 0, sizeof(msg));
	memcpy(&msg.step_id, &ctx->step_req->step_id, sizeof(msg.step_id));
	msg.range_first = msg.range_last = nodeid;
	msg.step_rc = ret_code;

	slurm_msg_t_init(&req);
	req.msg_type = REQUEST_STEP_COMPLETE;
	req.data = &msg;

	if (ctx->step_resp->use_protocol_ver)
		req.protocol_version = ctx->step_resp->use_protocol_ver;

	if (slurm_send_recv_controller_rc_msg(&req, &rc,
					      working_cluster_rec) < 0)
	       return SLURM_ERROR;

	return SLURM_SUCCESS;
}

static int _launch_tasks(slurm_step_ctx_t *ctx,
			 launch_tasks_request_msg_t *launch_msg,
			 uint32_t timeout, char *nodelist)
{
#ifdef HAVE_FRONT_END
	slurm_cred_arg_t *cred_args;
#endif
	slurm_msg_t msg;
	List ret_list = NULL;
	ListIterator ret_itr;
	ret_data_info_t *ret_data = NULL;
	int rc = SLURM_SUCCESS;
	int tot_rc = SLURM_SUCCESS;

	debug("Entering _launch_tasks");
	if (ctx->verbose_level) {
		char *name = NULL;
		hostlist_t hl = hostlist_create(nodelist);
		int i = 0;
		while ((name = hostlist_shift(hl))) {
			_print_launch_msg(launch_msg, name, i++);
			free(name);
		}
		hostlist_destroy(hl);
	}

	/*
	 * Extend timeout based upon BatchStartTime to permit for a long
	 * running Prolog
	 */
	if (timeout <= 0) {
		timeout = (slurm_conf.msg_timeout +
		           slurm_conf.batch_start_timeout) * 1000;
	}

	slurm_msg_t_init(&msg);
	slurm_msg_set_r_uid(&msg, SLURM_AUTH_UID_ANY);
	msg.msg_type = REQUEST_LAUNCH_TASKS;
	msg.data = launch_msg;

	if (ctx->step_resp->use_protocol_ver)
		msg.protocol_version = ctx->step_resp->use_protocol_ver;
	else
		msg.protocol_version = SLURM_PROTOCOL_VERSION;

#ifdef HAVE_FRONT_END
	cred_args = slurm_cred_get_args(ctx->step_resp->cred);
	//info("hostlist=%s", cred_args->step_hostlist);
	ret_list = slurm_send_recv_msgs(cred_args->step_hostlist, &msg, timeout);
	slurm_cred_unlock_args(ctx->step_resp->cred);
#else
	ret_list = slurm_send_recv_msgs(nodelist, &msg, timeout);
#endif
	if (ret_list == NULL) {
		error("slurm_send_recv_msgs failed miserably: %m");
		return SLURM_ERROR;
	}
	ret_itr = list_iterator_create(ret_list);
	while ((ret_data = list_next(ret_itr))) {
		rc = slurm_get_return_code(ret_data->type,
					   ret_data->data);
		debug("launch returned msg_rc=%d err=%d type=%d",
		      rc, ret_data->err, ret_data->type);
		if (rc != SLURM_SUCCESS) {
			if (ret_data->err)
				tot_rc = ret_data->err;
			else
				tot_rc = rc;

			_fail_step_tasks(ctx, ret_data->node_name, tot_rc);

			errno = tot_rc;
			tot_rc = SLURM_ERROR;
			error("Task launch for %ps failed on node %s: %m",
			      &ctx->step_req->step_id, ret_data->node_name);

		} else {
#if 0 /* only for debugging, might want to make this a callback */
			errno = ret_data->err;
			info("Launch success on node %s",
			     ret_data->node_name);
#endif
		}
	}
	list_iterator_destroy(ret_itr);
	FREE_NULL_LIST(ret_list);

	if (tot_rc != SLURM_SUCCESS)
		return tot_rc;
	return rc;
}

/* returns an xmalloc cwd string, or NULL if lookup failed. */
static char *_lookup_cwd(void)
{
	char buf[PATH_MAX];

	if (getcwd(buf, PATH_MAX) != NULL) {
		return xstrdup(buf);
	} else {
		return NULL;
	}
}

static void _print_launch_msg(launch_tasks_request_msg_t *msg,
			      char *hostname, int nodeid)
{
	int i;
	char *tmp_str = NULL, *task_list = NULL;
	hostlist_t hl = hostlist_create(NULL);

	for (i=0; i<msg->tasks_to_launch[nodeid]; i++) {
		xstrfmtcat(tmp_str, "%u", msg->global_task_ids[nodeid][i]);
		hostlist_push_host(hl, tmp_str);
		xfree(tmp_str);
	}
	task_list = hostlist_ranged_string_xmalloc(hl);
	hostlist_destroy(hl);

	info("launching %ps on host %s, %u tasks: %s",
	     &msg->step_id, hostname, msg->tasks_to_launch[nodeid], task_list);
	xfree(task_list);

	debug3("uid:%u gid:%u cwd:%s %d", msg->uid, msg->gid, msg->cwd, nodeid);
}

/*
 * Notify the step_launch_state that an I/O connection went bad.
 * If the node is suspected to be down, abort the job.
 */
int
step_launch_notify_io_failure(step_launch_state_t *sls, int node_id)
{
	slurm_mutex_lock(&sls->lock);

	bit_set(sls->node_io_error, node_id);
	debug("IO error on node %d", node_id);

	/*
	 * sls->io_deadline[node_id] != (time_t)NO_VAL  means that
	 * the _step_missing_handler was called on this node.
	 */
	if (sls->io_deadline[node_id] != (time_t)NO_VAL) {
		error("Aborting, io error and missing step on node %d",
		      node_id);
		sls->abort = true;
		slurm_cond_broadcast(&sls->cond);
	} else {

		/* FIXME
		 * If stepd dies or we see I/O error with stepd.
		 * Do not abort the whole job but collect all
		 * taks on the node just like if they exited.
		 *
		 * Keep supporting 'srun -N x --pty bash'
		 */
		if (getenv("SLURM_PTY_PORT") == NULL) {
			error("%s: aborting, io error with slurmstepd on node %d",
			      __func__, node_id);
			sls->abort = true;
			slurm_cond_broadcast(&sls->cond);
		}
	}

	slurm_mutex_unlock(&sls->lock);

	return SLURM_SUCCESS;
}


/*
 * This is called 1) after a node connects for the first time and 2) when
 * a message comes in confirming that a connection is okay.
 *
 * Just in case the node was marked questionable very early in the
 * job step setup, clear this flag if/when the node makes its initial
 * connection.
 */
int
step_launch_clear_questionable_state(step_launch_state_t *sls, int node_id)
{
	slurm_mutex_lock(&sls->lock);
	sls->io_deadline[node_id] = (time_t)NO_VAL;
	slurm_mutex_unlock(&sls->lock);
	return SLURM_SUCCESS;
}

static void *
_check_io_timeout(void *_sls)
{
	int ii;
	time_t now, next_deadline;
	struct timespec ts = {0, 0};
	step_launch_state_t *sls = (step_launch_state_t *)_sls;

	slurm_mutex_lock(&sls->lock);

	while (1) {
		if (sls->halt_io_test || sls->abort)
			break;

		now = time(NULL);
		next_deadline = (time_t)NO_VAL;

		for (ii = 0; ii < sls->layout->node_cnt; ii++) {
			if (sls->io_deadline[ii] == (time_t)NO_VAL)
				continue;

			if (sls->io_deadline[ii] <= now) {
				sls->abort = true;
				slurm_cond_broadcast(&sls->cond);
				error(  "Cannot communicate with node %d.  "
					"Aborting job.", ii);
				break;
			} else if (next_deadline == (time_t)NO_VAL ||
				   sls->io_deadline[ii] < next_deadline) {
				next_deadline = sls->io_deadline[ii];
			}
		}
		if (sls->abort)
			break;

		if (next_deadline == (time_t)NO_VAL) {
			debug("io timeout thread: no pending deadlines, "
			      "sleeping indefinitely");
			slurm_cond_wait(&sls->cond, &sls->lock);
		} else {
			debug("io timeout thread: sleeping %lds until deadline",
			       (long)(next_deadline - time(NULL)));
			ts.tv_sec = next_deadline;
			slurm_cond_timedwait(&sls->cond, &sls->lock, &ts);
		}
	}
	slurm_mutex_unlock(&sls->lock);
	return NULL;
}
