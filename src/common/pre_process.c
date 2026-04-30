/*****************************************************************************\
 *  pre_process.c
 *****************************************************************************
 *  Copyright (C) 2002-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2009 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Jay Windley <jwindley@lnxi.com>.
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

//__METASTACK_OPT_HIGH_THROUGHPUT_MEM_POOL

#include "src/common/pre_process.h"

slurmctld_pre_process_t *pre_process_data = NULL;

extern void pre_process_init()
{
	if(!pre_process_data){
		pre_process_data = xmalloc(sizeof(*pre_process_data));
		slurm_cond_init(&pre_process_data->cond, NULL);
		slurm_mutex_init(&pre_process_data->mutex);
		pre_process_data->purge_jobacctinfo_list = NULL;
		pre_process_data->pre_jobacctinfo_list = NULL;
		pre_process_data->pre_job_record_list = NULL;
		pre_process_data->purge_buf_list = NULL;
		pre_process_data->pre_init_buf_4K = NULL;
		pre_process_data->pre_init_buf_16K = NULL;
		pre_process_data->pre_init_buf_256K = NULL;
		pre_process_data->pre_init_buf_1M = NULL;
		pre_process_data->shutdown = false;
	}
}

extern void pre_process_fini()
{
	if(pre_process_data){
		FREE_NULL_LIST(pre_process_data->purge_jobacctinfo_list);
		FREE_NULL_LIST(pre_process_data->pre_jobacctinfo_list);
		FREE_NULL_LIST(pre_process_data->pre_job_record_list);
		FREE_NULL_LIST(pre_process_data->purge_buf_list);
		FREE_NULL_LIST(pre_process_data->pre_init_buf_4K);
		FREE_NULL_LIST(pre_process_data->pre_init_buf_16K);
		FREE_NULL_LIST(pre_process_data->pre_init_buf_256K);
		FREE_NULL_LIST(pre_process_data->pre_init_buf_1M);
		slurm_mutex_destroy(&pre_process_data->mutex);
		slurm_cond_destroy(&pre_process_data->cond);
		xfree(pre_process_data);
	}
}

extern void pre_process_update()
{
	if(pre_process_data){
		slurm_mutex_lock(&pre_process_data->mutex);
		slurm_cond_signal(&pre_process_data->cond);
		slurm_mutex_unlock(&pre_process_data->mutex);
	}
}

/*pre_process_shutdown: Shut down the cache query thread.*/
extern void pre_process_shutdown()
{
	if(pre_process_data){
		slurm_mutex_lock(&pre_process_data->mutex);
		pre_process_data->shutdown = true;
		slurm_cond_signal(&pre_process_data->cond);
		slurm_mutex_unlock(&pre_process_data->mutex);
	}
}