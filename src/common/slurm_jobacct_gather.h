/*****************************************************************************\
 *  slurm_jobacct_gather.h - implementation-independent job completion logging
 *  API definitions
 *****************************************************************************
 *  Copyright (C) 2003 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette@llnl.com> et. al.
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  Copyright (C) 2005 Hewlett-Packard Development Company, L.P.
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

/*****************************************************************************\
 *  Modification history
 *
 *  19 Jan 2005 by Andy Riebs <andy.riebs@hp.com>
 *       This file is derived from the file slurm_JOBACCT.c, written by
 *       Morris Jette, et al.
\*****************************************************************************/

#ifndef __SLURM_JOBACCT_GATHER_H__
#define __SLURM_JOBACCT_GATHER_H__

#include <inttypes.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurmdb.h"

#include "src/common/macros.h"
#include "src/common/pack.h"
#include "src/common/list.h"
#include "src/common/xmalloc.h"
#include "src/common/slurm_acct_gather.h"

#include "src/slurmd/slurmstepd/slurmstepd_job.h"

#define PROTOCOL_TYPE_SLURM 0
#define PROTOCOL_TYPE_DBD 1

#define CPU_TIME_ADJ 1000

typedef struct {
	uint32_t taskid; /* contains which task number it was on */
	uint32_t nodeid; /* contains which node number it was on */
	stepd_step_rec_t *job; /* contains stepd job pointer */
} jobacct_id_t;

struct jobacctinfo {
	pid_t pid;
	uint64_t sys_cpu_sec;
	uint32_t sys_cpu_usec;
	uint64_t user_cpu_sec;
	uint32_t user_cpu_usec;
	uint32_t act_cpufreq; /* actual cpu frequency */
	acct_gather_energy_t energy;
	double last_total_cputime;
	double this_sampled_cputime;
	uint32_t current_weighted_freq;
	uint32_t current_weighted_power;
	uint32_t tres_count; /* count of tres in the usage array's */
	uint32_t *tres_ids; /* array of tres_count of the tres id's */
	List tres_list; /* list of tres we are dealing with */
	uint64_t *tres_usage_in_max; /* tres max usage in data */
	uint64_t *tres_usage_in_max_nodeid; /* tres max usage in data node id */
	uint64_t *tres_usage_in_max_taskid; /* tres max usage in data task id */
	uint64_t *tres_usage_in_min; /* tres min usage in data */
	uint64_t *tres_usage_in_min_nodeid; /* tres min usage in data node id */
	uint64_t *tres_usage_in_min_taskid; /* tres min usage in data task id */
	uint64_t *tres_usage_in_tot; /* total usage in, in megabytes */
	uint64_t *tres_usage_out_max; /* tres max usage out data */
	uint64_t *tres_usage_out_max_nodeid; /* tres max usage data node id */
	uint64_t *tres_usage_out_max_taskid; /* tres max usage data task id */
	uint64_t *tres_usage_out_min; /* tres min usage out data */
	uint64_t *tres_usage_out_min_nodeid; /* tres min usage data node id */
	uint64_t *tres_usage_out_min_taskid; /* tres min usage data task id */
	uint64_t *tres_usage_out_tot; /* total usage out, in megabytes */

	jobacct_id_t id;
	int dataset_id; /* dataset associated to this task when profiling */

	/* FIXME: these need to be arrays like above */
	double last_tres_usage_in_tot;
	double last_tres_usage_out_tot;
	time_t cur_time;
	time_t last_time;
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
	struct timeval first_acct_time;
	struct timeval pre1_acct_time;
	struct timeval cur_acct_time;

	double first_total_cputime;
	double pre1_total_cputime;

	double avg_cpu_util;
	double max_cpu_util;
	double min_cpu_util;
	double cpu_util;
#endif
#ifdef __METASTACK_LOAD_ABNORMAL
	uint64_t flag;
	double cpu_step_ave;
	double cpu_step_max;
	double cpu_step_min;
	double cpu_step_real;

	uint64_t mem_step_max;
	uint64_t mem_step_min;
	uint64_t mem_step;

	uint64_t vmem_step_max;
	uint64_t vmem_step_min;
	uint64_t vmem_step;

	uint64_t step_pages;
	uint64_t acct_flag;
	uint64_t cpu_count;
	uint64_t pid_count;
	uint64_t node_count;
	/* record event time */
	uint64_t  *cpu_start;
	uint64_t  *cpu_end;
	uint64_t  *pid_start;
	uint64_t  *pid_end;
	uint64_t  *node_start;
	uint64_t  *node_end;
	
	/* sstat display */
	uint64_t node_alloc_cpu;
    uint64_t timer;
	uint32_t cpu_threshold;	
#endif
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
    List pjobs;
#endif
};

#ifdef __METASTACK_LOAD_ABNORMAL

#define MAX_SIZE 1000000               /*fifo max size*/
#define LOAD_LOW   0x0000000000000001  /*cpu event*/
#define PROC_AB    0x0000000000000010  /*pocess status read only*/
#define JNODE_STAT 0x0000000000000100
#define JOBACCTINFO_START_END_ARRAY_SIZE 200

struct jobinfostat {
	double job_avg_cpu;
	double job_cpu_util;
	uint64_t pid_status;
};

typedef struct {
	bool send_flag;  /*enable send step data to jobacct*/
	double cpu_step_real;
	double cpu_step_ave;
	uint64_t mem_step;
	uint64_t vmem_step;
	uint64_t step_pages;
	uint64_t load_flag; /*exception criteria*/
	uint32_t cpu_threshold;
	time_t cpu_start;
	time_t cpu_end;
	time_t pid_start;
	time_t pid_end;
	time_t node_start;
	time_t node_end;
	uint64_t node_alloc_cpu;
    uint64_t timer;
} write_t;

typedef struct {
	pthread_mutex_t lock;
	uint64_t load_flag; /*exception criteria*/
	bool update;
	bool step;  /*enable step*/
	double cpu_step_real; 
	double cpu_step_ave;
	uint64_t mem_step;
	uint64_t vmem_step;
	uint64_t step_pages;
	time_t start;
} collection_t;
extern collection_t share_data;

typedef struct {
	pthread_cond_t cond;
	pthread_mutex_t lock;
	int count_nodelist;
	int rank_gather;
	int parent_rank_gather;
	int children_gather;
	int depth_gather;
	int max_depth_gather;
	int wait_child_count; /*number node of child count now*/
	double step_cpu;
	double step_cpu_ave;
	uint64_t step_mem;
	uint64_t step_vmem;
	uint64_t load_status;
	uint64_t page_fault;
	time_t start;
	slurm_addr_t parent_addr_gather;
	bitstr_t *bits;
	bool wait_children;
	uint64_t node_alloc_cpu;
} step_gather_t;
extern step_gather_t step_gather;
#endif

/* Define jobacctinfo_t below to avoid including extraneous slurm headers */
#ifndef __jobacctinfo_t_defined
#  define  __jobacctinfo_t_defined
   typedef struct jobacctinfo jobacctinfo_t;     /* opaque data type */
#endif

extern int jobacct_gather_init(void); /* load the plugin */
extern int jobacct_gather_fini(void); /* unload the plugin */

#ifdef __METASTACK_LOAD_ABNORMAL
extern int  jobacct_gather_startpoll(uint16_t frequency, acct_gather_rank_t job_set);
extern int	jobacct_gather_stepdpoll(uint16_t frequency, acct_gather_rank_t job_set);
extern void jobacctinfo_pack_detial(jobacctinfo_t *jobacct, uint16_t rpc_version,
			     uint16_t protocol_type, buf_t *buffer);
extern int jobacctinfo_unpack_detial(jobacctinfo_t **jobacct, uint16_t rpc_version,
			      uint16_t protocol_type, buf_t *buffer, bool alloc); 	
extern void jobacctinfo_aggregate_2(jobacctinfo_t *dest, jobacctinfo_t *from);
#endif
extern int  jobacct_gather_endpoll(void);
extern void jobacct_gather_suspend_poll(void);
extern void jobacct_gather_resume_poll(void);

extern int jobacct_gather_add_task(pid_t pid, jobacct_id_t *jobacct_id,
				   int poll);
/* must free jobacctinfo_t if not NULL */
extern jobacctinfo_t *jobacct_gather_stat_task(pid_t pid);
/*
 * Find task by pid and remove from tracked task list.
 *
 * IN pid - pid of task to find or 0 to find first task
 * RET ptr (must free jobacctinfo_t if not NULL)
 */
extern jobacctinfo_t *jobacct_gather_remove_task(pid_t pid);

extern int jobacct_gather_set_proctrack_container_id(uint64_t id);
extern int jobacct_gather_set_mem_limit(slurm_step_id_t *step_id,
					uint64_t mem_limit);
extern void jobacct_gather_handle_mem_limit(uint64_t total_job_mem,
					    uint64_t total_job_vsize);

extern jobacctinfo_t *jobacctinfo_create(jobacct_id_t *jobacct_id);
extern void jobacctinfo_destroy(void *object);
extern int jobacctinfo_setinfo(jobacctinfo_t *jobacct,
			       enum jobacct_data_type type, void *data,
			       uint16_t protocol_version);
extern int jobacctinfo_getinfo(jobacctinfo_t *jobacct,
			       enum jobacct_data_type type, void *data,
			       uint16_t protocol_version);
extern void jobacctinfo_pack(jobacctinfo_t *jobacct, uint16_t rpc_version,
			     uint16_t protocol_type, buf_t *buffer);
extern int jobacctinfo_unpack(jobacctinfo_t **jobacct, uint16_t rpc_version,
			      uint16_t protocol_type, buf_t *buffer,
			      bool alloc);

extern void jobacctinfo_aggregate(jobacctinfo_t *dest, jobacctinfo_t *from);

extern void jobacctinfo_2_stats(slurmdb_stats_t *stats, jobacctinfo_t *jobacct);

extern void jobacct_common_free_jobacct(void *object);

extern long jobacct_gather_get_clk_tck();

//#ifdef __METASTACK_OPT_CACHE_QUERY
//extern jobacctinfo_t *jobacctinfo_extract(jobacctinfo_t *src_jobacct);
//#endif
#endif /*__SLURM_JOBACCT_GATHER_H__*/
