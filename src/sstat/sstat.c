/*****************************************************************************\
 *  sstat.c - job accounting reports for Slurm's slurmdb/log plugin
 *****************************************************************************
 *  Copyright (C) 2008 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette1@llnl.gov>
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

#include "sstat.h"

/*
 * Globals
 */
sstat_parameters_t params;
#ifdef __METASTACK_OPT_PRINT_COMMAND
print_field_t fields[] = {
	{10, "AveCPU", print_fields_str, PRINT_AVECPU, 0},
	{10, "AveCPUFreq", print_fields_str, PRINT_ACT_CPUFREQ, 0},
	{12, "AveDiskRead", print_fields_str, PRINT_AVEDISKREAD, 0},
	{12, "AveDiskWrite", print_fields_str, PRINT_AVEDISKWRITE, 0},
	{10, "AvePages", print_fields_str, PRINT_AVEPAGES, 0},
	{10, "AveRSS", print_fields_str, PRINT_AVERSS, 0},
	{10, "AveVMSize", print_fields_str, PRINT_AVEVSIZE, 0},
	{14, "ConsumedEnergy", print_fields_str, PRINT_CONSUMED_ENERGY, 0},
	{17, "ConsumedEnergyRaw", print_fields_uint64,
	 PRINT_CONSUMED_ENERGY_RAW, 0},
	{-12, "JobID", print_fields_str, PRINT_JOBID, 0},
	{12, "MaxDiskRead", print_fields_str, PRINT_MAXDISKREAD, 0},
	{15, "MaxDiskReadNode", print_fields_str, PRINT_MAXDISKREADNODE, 0},
	{15, "MaxDiskReadTask", print_fields_uint, PRINT_MAXDISKREADTASK, 0},
	{12, "MaxDiskWrite", print_fields_str, PRINT_MAXDISKWRITE, 0},
	{16, "MaxDiskWriteNode", print_fields_str, PRINT_MAXDISKWRITENODE, 0},
	{16, "MaxDiskWriteTask", print_fields_uint, PRINT_MAXDISKWRITETASK, 0},
	{8, "MaxPages", print_fields_str, PRINT_MAXPAGES, 0},
	{12, "MaxPagesNode", print_fields_str, PRINT_MAXPAGESNODE, 0},
	{14, "MaxPagesTask", print_fields_uint, PRINT_MAXPAGESTASK, 0},
	{10, "MaxRSS", print_fields_str, PRINT_MAXRSS, 0},
	{10, "MaxRSSNode", print_fields_str, PRINT_MAXRSSNODE, 0},
	{10, "MaxRSSTask", print_fields_uint, PRINT_MAXRSSTASK, 0},
	{10, "MaxVMSize", print_fields_str, PRINT_MAXVSIZE, 0},
	{14, "MaxVMSizeNode", print_fields_str, PRINT_MAXVSIZENODE, 0},
	{14, "MaxVMSizeTask", print_fields_uint, PRINT_MAXVSIZETASK, 0},
	{10, "MinCPU", print_fields_str, PRINT_MINCPU, 0},
	{10, "MinCPUNode", print_fields_str, PRINT_MINCPUNODE, 0},
	{10, "MinCPUTask", print_fields_uint, PRINT_MINCPUTASK, 0},
	{20, "Nodelist", print_fields_str, PRINT_NODELIST, 0},
	{8, "NTasks", print_fields_uint, PRINT_NTASKS, 0},
	{20, "Pids", print_fields_str, PRINT_PIDS, 0},
	{10, "ReqCPUFreq", print_fields_str, PRINT_REQ_CPUFREQ_MIN, 0}, /*vestigial*/
	{13, "ReqCPUFreqMin", print_fields_str, PRINT_REQ_CPUFREQ_MIN, 0},
	{13, "ReqCPUFreqMax", print_fields_str, PRINT_REQ_CPUFREQ_MAX, 0},
	{13, "ReqCPUFreqGov", print_fields_str, PRINT_REQ_CPUFREQ_GOV, 0},
	{14, "TRESUsageInAve", print_fields_str, PRINT_TRESUIA, 0},
	{14, "TRESUsageInMax", print_fields_str, PRINT_TRESUIM, 0},
	{18, "TRESUsageInMaxNode", print_fields_str, PRINT_TRESUIMN, 0},
	{18, "TRESUsageInMaxTask", print_fields_str, PRINT_TRESUIMT, 0},
	{14, "TRESUsageInMin", print_fields_str, PRINT_TRESUIMI, 0},
	{18, "TRESUsageInMinNode", print_fields_str, PRINT_TRESUIMIN, 0},
	{18, "TRESUsageInMinTask", print_fields_str, PRINT_TRESUIMIT, 0},
	{14, "TRESUsageInTot", print_fields_str, PRINT_TRESUIT, 0},
	{15, "TRESUsageOutAve", print_fields_str, PRINT_TRESUOA, 0},
	{15, "TRESUsageOutMax", print_fields_str, PRINT_TRESUOM, 0},
	{19, "TRESUsageOutMaxNode", print_fields_str, PRINT_TRESUOMN, 0},
	{19, "TRESUsageOutMaxTask", print_fields_str, PRINT_TRESUOMT, 0},
	{15, "TRESUsageOutMin", print_fields_str, PRINT_TRESUOMI, 0},
	{19, "TRESUsageOutMinNode", print_fields_str, PRINT_TRESUOMIN, 0},
	{19, "TRESUsageOutMinTask", print_fields_str, PRINT_TRESUOMIT, 0},
	{15, "TRESUsageOutTot", print_fields_str, PRINT_TRESUOT, 0},
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
	{7, "CPUUtil", print_fields_str, PRINT_CPUREALUTIL},
	{10, "AveCPUUtil", print_fields_str, PRINT_CPUUTILAVE},
	{10, "MaxCPUUtil", print_fields_str, PRINT_MAXCPUUTIL},
	{10, "MinCPUUtil", print_fields_str, PRINT_MINCPUUTIL},
	{8, "TotalRSS", print_fields_str, PRINT_TOTALRSS},
#endif
	{0, NULL, NULL, 0, 0}};
#else
print_field_t fields[] = {
	{10, "AveCPU", print_fields_str, PRINT_AVECPU},
	{10, "AveCPUFreq", print_fields_str, PRINT_ACT_CPUFREQ},
	{12, "AveDiskRead", print_fields_str, PRINT_AVEDISKREAD},
	{12, "AveDiskWrite", print_fields_str, PRINT_AVEDISKWRITE},
	{10, "AvePages", print_fields_str, PRINT_AVEPAGES},
	{10, "AveRSS", print_fields_str, PRINT_AVERSS},
	{10, "AveVMSize", print_fields_str, PRINT_AVEVSIZE},
	{14, "ConsumedEnergy", print_fields_str, PRINT_CONSUMED_ENERGY},
	{17, "ConsumedEnergyRaw", print_fields_uint64,
	 PRINT_CONSUMED_ENERGY_RAW},
	{-12, "JobID", print_fields_str, PRINT_JOBID},
	{12, "MaxDiskRead", print_fields_str, PRINT_MAXDISKREAD},
	{15, "MaxDiskReadNode", print_fields_str, PRINT_MAXDISKREADNODE},
	{15, "MaxDiskReadTask", print_fields_uint, PRINT_MAXDISKREADTASK},
	{12, "MaxDiskWrite", print_fields_str, PRINT_MAXDISKWRITE},
	{16, "MaxDiskWriteNode", print_fields_str, PRINT_MAXDISKWRITENODE},
	{16, "MaxDiskWriteTask", print_fields_uint, PRINT_MAXDISKWRITETASK},
	{8, "MaxPages", print_fields_str, PRINT_MAXPAGES},
	{12, "MaxPagesNode", print_fields_str, PRINT_MAXPAGESNODE},
	{14, "MaxPagesTask", print_fields_uint, PRINT_MAXPAGESTASK},
	{10, "MaxRSS", print_fields_str, PRINT_MAXRSS},
	{10, "MaxRSSNode", print_fields_str, PRINT_MAXRSSNODE},
	{10, "MaxRSSTask", print_fields_uint, PRINT_MAXRSSTASK},
	{10, "MaxVMSize", print_fields_str, PRINT_MAXVSIZE},
	{14, "MaxVMSizeNode", print_fields_str, PRINT_MAXVSIZENODE},
	{14, "MaxVMSizeTask", print_fields_uint, PRINT_MAXVSIZETASK},
	{10, "MinCPU", print_fields_str, PRINT_MINCPU},
	{10, "MinCPUNode", print_fields_str, PRINT_MINCPUNODE},
	{10, "MinCPUTask", print_fields_uint, PRINT_MINCPUTASK},
	{20, "Nodelist", print_fields_str, PRINT_NODELIST},
	{8, "NTasks", print_fields_uint, PRINT_NTASKS},
	{20, "Pids", print_fields_str, PRINT_PIDS},
	{10, "ReqCPUFreq", print_fields_str, PRINT_REQ_CPUFREQ_MIN}, /*vestigial*/
	{13, "ReqCPUFreqMin", print_fields_str, PRINT_REQ_CPUFREQ_MIN},
	{13, "ReqCPUFreqMax", print_fields_str, PRINT_REQ_CPUFREQ_MAX},
	{13, "ReqCPUFreqGov", print_fields_str, PRINT_REQ_CPUFREQ_GOV},
	{14, "TRESUsageInAve", print_fields_str, PRINT_TRESUIA},
	{14, "TRESUsageInMax", print_fields_str, PRINT_TRESUIM},
	{18, "TRESUsageInMaxNode", print_fields_str, PRINT_TRESUIMN},
	{18, "TRESUsageInMaxTask", print_fields_str, PRINT_TRESUIMT},
	{14, "TRESUsageInMin", print_fields_str, PRINT_TRESUIMI},
	{18, "TRESUsageInMinNode", print_fields_str, PRINT_TRESUIMIN},
	{18, "TRESUsageInMinTask", print_fields_str, PRINT_TRESUIMIT},
	{14, "TRESUsageInTot", print_fields_str, PRINT_TRESUIT},
	{15, "TRESUsageOutAve", print_fields_str, PRINT_TRESUOA},
	{15, "TRESUsageOutMax", print_fields_str, PRINT_TRESUOM},
	{19, "TRESUsageOutMaxNode", print_fields_str, PRINT_TRESUOMN},
	{19, "TRESUsageOutMaxTask", print_fields_str, PRINT_TRESUOMT},
	{15, "TRESUsageOutMin", print_fields_str, PRINT_TRESUOMI},
	{19, "TRESUsageOutMinNode", print_fields_str, PRINT_TRESUOMIN},
	{19, "TRESUsageOutMinTask", print_fields_str, PRINT_TRESUOMIT},
	{15, "TRESUsageOutTot", print_fields_str, PRINT_TRESUOT},
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
	{7, "CPUUtil", print_fields_str, PRINT_CPUREALUTIL},
	{10, "AveCPUUtil", print_fields_str, PRINT_CPUUTILAVE},
	{10, "MaxCPUUtil", print_fields_str, PRINT_MAXCPUUTIL},
	{10, "MinCPUUtil", print_fields_str, PRINT_MINCPUUTIL},
	{8, "TotalRSS", print_fields_str, PRINT_TOTALRSS},
#endif	
	{0, NULL, NULL, 0}};
#endif
List jobs = NULL;
slurmdb_job_rec_t job;
slurmdb_step_rec_t step;
List print_fields_list = NULL;
ListIterator print_fields_itr = NULL;
int field_count = 0;

#ifdef __METASTACK_LOAD_ABNORMAL
#define LOAD_LOW 0x0000000000000001
#define PROC_AB  0x0000000000000010
#define NODE_ABL 0x0000000000000100

typedef enum {
	LOW_CPU_UTIL,
	PROC_ABNORMAL,
	NODE_ABNORMAL,
} abnormal_event_type_t;
const char* event_str[] = {
	"Cpu utilization exception ",
	"Job process anomaly ",
	"Job node anomaly "
};
void _event_print(uint64_t count, uint64_t  *start, uint64_t  *end, abnormal_event_type_t flag) {
	char *nodenames = NULL;
	char *event_desc = NULL;
	printf("\n*********************************************************************************\n");
	switch (flag) {
	case LOW_CPU_UTIL:
		printf("Display of CPU exception events\n");
		event_desc = xstrdup(event_str[LOW_CPU_UTIL]);
		break;
	case PROC_ABNORMAL:
		printf("Display of PROCESS exception events\n");
		event_desc = xstrdup(event_str[PROC_ABNORMAL]);
		break;
	case NODE_ABNORMAL:
		printf("Display of Node exception events\n");
		event_desc = xstrdup(event_str[NODE_ABNORMAL]);
		break;
	}
	if(count <= JOBACCTINFO_START_END_ARRAY_SIZE) {
		for(int i = 0 ; i < count ; ++i){
			time_t tmptimeValueStart = (time_t)start[i];
			time_t tmptimeValueEnd = (time_t)end[i];

			char timeStringStart[100];
			char timeStringEnd[100];
			struct tm timeInfoStart;
			struct tm timeInfoEnd;
			localtime_r(&tmptimeValueStart, &timeInfoStart);
			localtime_r(&tmptimeValueEnd, &timeInfoEnd);
			strftime(timeStringStart, sizeof(timeStringStart), "%Y-%m-%d-%H:%M:%S", &timeInfoStart);
			strftime(timeStringEnd, sizeof(timeStringEnd), "%Y-%m-%d-%H:%M:%S", &timeInfoEnd);
			nodenames = xstrdup(event_desc);
			xstrfmtcat(nodenames, "between %s and %s ", timeStringStart, timeStringEnd);
			printf("\n%s\n",nodenames);
			if(nodenames) 
				xfree(nodenames);
		}
	} else{
		for(int i = 0 ; i < JOBACCTINFO_START_END_ARRAY_SIZE; ++i) {
			time_t tmptimeValueStart = (time_t)start[(count + i) % JOBACCTINFO_START_END_ARRAY_SIZE];
			time_t tmptimeValueEnd = (time_t)end[(count + i) % JOBACCTINFO_START_END_ARRAY_SIZE];

			char timeStringStart[100];
			char timeStringEnd[100];
			struct tm timeInfoStart;
			struct tm timeInfoEnd;
			localtime_r(&tmptimeValueStart, &timeInfoStart);
			localtime_r(&tmptimeValueEnd, &timeInfoEnd);
			strftime(timeStringStart, sizeof(timeStringStart), "%Y-%m-%d-%H:%M:%S", &timeInfoStart);
			strftime(timeStringEnd, sizeof(timeStringEnd), "%Y-%m-%d-%H:%M:%S", &timeInfoEnd);
			nodenames = xstrdup(event_desc);
			xstrfmtcat(nodenames, "between %s and %s ", timeStringStart, timeStringEnd);
			printf("\n%s\n",nodenames);
			if(nodenames) 
				xfree(nodenames);
		}
	}
	if(event_desc)
		xfree(event_desc);		
}

#endif

int _do_stat(slurm_step_id_t *step_id, char *nodelist,
	     uint32_t req_cpufreq_min, uint32_t req_cpufreq_max,
	     uint32_t req_cpufreq_gov, uint16_t use_protocol_ver)
{
	job_step_stat_response_msg_t *step_stat_response = NULL;
	int rc = SLURM_SUCCESS;
	ListIterator itr;
	jobacctinfo_t *total_jobacct = NULL;
	job_step_stat_t *step_stat = NULL;
	int ntasks = 0;
	int tot_tasks = 0;
	hostlist_t hl = NULL;
	char *ave_usage_tmp = NULL;
#ifdef __METASTACK_LOAD_ABNORMAL
	double all_task_mem_tmp = 0;
	double all_task_vmem_tmp = 0;

  	double all_task_mem_tmp_max = 0;
	double all_task_mem_tmp_min = 0;
	
  	double all_task_vmem_tmp_max = 0;
	double all_task_vmem_tmp_min = 0;

#endif

	debug("requesting info for %ps", step_id);
#ifdef __METASTACK_LOAD_ABNORMAL
	/*
	 *After using -d to specify the display of anomalous acquisition data, each job step will summarize 
	 *the data to the head node, so there is no need to send an RPC get message to all nodes, and you 
	 *only need to communicate with the head node.
	 */
	if(params.opt_event == 1) {
		hostlist_t hl = hostlist_create(nodelist);
		hostlist_sort(hl);
		/*
		 *Use subscript 0 to get the head node
		 */
		char *head_node = hostlist_nth(hl, 0);
		if ((rc = slurm_job_step_stat(step_id,
				      head_node, use_protocol_ver,
				      &step_stat_response)) != SLURM_SUCCESS) {
			if (rc == ESLURM_INVALID_JOB_ID) {
				debug("%ps has already completed",
					step_id);
			} else {
				error("problem getting step_layout for %ps: %s",
					step_id, slurm_strerror(rc));
			}
			slurm_job_step_pids_response_msg_free(step_stat_response);
			return rc;
		}
		/*
		 *The memory address corresponding to the string obtained using hostlist_nth needs to be passed 
		 *through free() instead of xfree()
		 */
		if(head_node)
			free(head_node);
		hostlist_destroy(hl);
	} else if((rc = slurm_job_step_stat(step_id,
				      nodelist, use_protocol_ver,
				      &step_stat_response)) != SLURM_SUCCESS) {
		if (rc == ESLURM_INVALID_JOB_ID) {
			debug("%ps has already completed",
			      step_id);
		} else {
			error("problem getting step_layout for %ps: %s",
			      step_id, slurm_strerror(rc));
		}
		slurm_job_step_pids_response_msg_free(step_stat_response);
		return rc;
	}
#endif

	memset(&job, 0, sizeof(slurmdb_job_rec_t));
	job.jobid = step_id->job_id;

	memset(&step, 0, sizeof(slurmdb_step_rec_t));

	memset(&step.stats, 0, sizeof(slurmdb_stats_t));

	step.job_ptr = &job;
	memcpy(&step.step_id, step_id, sizeof(step.step_id));
	step.nodes = xmalloc(BUF_SIZE);
	step.req_cpufreq_min = req_cpufreq_min;
	step.req_cpufreq_max = req_cpufreq_max;
	step.req_cpufreq_gov = req_cpufreq_gov;
	step.stepname = NULL;
	step.state = JOB_RUNNING;
	hl = hostlist_create(NULL);
	itr = list_iterator_create(step_stat_response->stats_list);
	while ((step_stat = list_next(itr))) {
		if (!step_stat->step_pids || !step_stat->step_pids->node_name)
			continue;
		if (step_stat->step_pids->pid_cnt > 0 ) {
			int i;
			for(i=0; i<step_stat->step_pids->pid_cnt; i++) {
				if (step.pid_str)
					xstrcat(step.pid_str, ",");
				xstrfmtcat(step.pid_str, "%u",
					   step_stat->step_pids->pid[i]);
			}
		}

		if (params.pid_format) {
			step.nodes = step_stat->step_pids->node_name;
			print_fields(&step);
			xfree(step.pid_str);
		} else {
			hostlist_push_host(hl, step_stat->step_pids->node_name);
			ntasks += step_stat->num_tasks;
			if (step_stat->jobacct) {
				if (!assoc_mgr_tres_list &&
				    step_stat->jobacct->tres_list) {
					assoc_mgr_lock_t locks =
						{ .tres = WRITE_LOCK };
					assoc_mgr_lock(&locks);
					assoc_mgr_post_tres_list(
						step_stat->jobacct->tres_list);
					assoc_mgr_unlock(&locks);
					/*
					 * assoc_mgr_post_tres_list destroys the
					 * input list
					 */
					step_stat->jobacct->tres_list = NULL;
				}

				/*
				 * total_jobacct has to be created after
				 * assoc_mgr is set up.
				 */
				if (!total_jobacct)
					total_jobacct =
						jobacctinfo_create(NULL);

				jobacctinfo_aggregate(total_jobacct,
						      step_stat->jobacct);
#ifdef __METASTACK_LOAD_ABNORMAL							
				jobacctinfo_aggregate_2(total_jobacct,
						      step_stat->jobacct);
#endif
			}
		}
	}
	list_iterator_destroy(itr);
#ifdef __METASTACK_LOAD_ABNORMAL
 	char arrTest1[] = "batch";
	/* If the linux acquisition plug-in is not enabled, skip the judgment branch */
	if((params.opt_event == 1) && total_jobacct) {
		if(step.step_id.step_id == -5) {
			printf("\n*********************************************************************************\n");
			printf("Resource Consumption Information of %d.%s\n", step.step_id.job_id, arrTest1);
			if(total_jobacct->node_alloc_cpu == 0)
				printf("The data may need to wait for updates\n");
			else
				printf("(the update interval is %ld minutes , the threshold value is %d (%ldcpus * %ld%%)))\n", total_jobacct->timer / 60 , total_jobacct->cpu_threshold  , total_jobacct->node_alloc_cpu , (uint64_t)total_jobacct->cpu_threshold / total_jobacct->node_alloc_cpu);
		} else if (step.step_id.step_id != -4){
			printf("\n*********************************************************************************\n");
			printf("Resource Consumption Information of %d.%d\n", step.step_id.job_id,step.step_id.step_id);
			if(total_jobacct->node_alloc_cpu == 0)
				printf("The data may need to wait for updates\n");
			else
				printf("(the update interval is %ld minutes , the threshold value is %d (%ldcpus * %ld%%)))\n", total_jobacct->timer / 60 , total_jobacct->cpu_threshold  , total_jobacct->node_alloc_cpu ,  (uint64_t)total_jobacct->cpu_threshold / total_jobacct->node_alloc_cpu);
		}
		if(step.step_id.step_id != -4) {
			printf("*********************************************************************************\n");
			if(total_jobacct) {
				if((total_jobacct->flag & LOAD_LOW ) != 0) {
					_event_print(total_jobacct->cpu_count, total_jobacct->cpu_start, total_jobacct->cpu_end, LOW_CPU_UTIL);
				}
				if((total_jobacct->flag & PROC_AB ) != 0) {
					_event_print(total_jobacct->pid_count, total_jobacct->pid_start, total_jobacct->pid_end, PROC_ABNORMAL);
				}
				if((total_jobacct->flag & NODE_ABL ) != 0) {
					_event_print(total_jobacct->node_count, total_jobacct->node_start, total_jobacct->node_end, NODE_ABNORMAL);
				}
				if((total_jobacct->cpu_step_ave < 0) && (total_jobacct->cpu_step_max <0) && (total_jobacct->cpu_step_min<0)) {
					printf("\nAverage CPU utilization of job steps please waitting\n");
					printf("Maximum CPU utilization of job steps please waitting\n");
					printf("Minimum CPU utilization of job steps please waitting\n");
					printf("Current CPU utilization of job steps please waitting\n");
				} else {
					printf("\nAverage CPU utilization of job steps     %.2f%%  \n",total_jobacct->cpu_step_ave);
					printf("Maximum CPU utilization of job steps     %.2f%%  \n",total_jobacct->cpu_step_max);
					printf("Minimum CPU utilization of job steps     %.2f%%  \n",(total_jobacct->cpu_step_min == INFINITE64 ? 0.0 : total_jobacct->cpu_step_min));
					printf("Current CPU utilization of job steps     %.2f%%  \n",total_jobacct->cpu_step_real);
				}
				printf("---------------------------------------------------------------------------------\n");
				if(total_jobacct->step_pages<=0) {
					total_jobacct->step_pages = 0;
				}
				printf("Current page faults of job steps         %ld \n",total_jobacct->step_pages);			

				all_task_mem_tmp_max = total_jobacct->mem_step_max;
				all_task_mem_tmp_min = total_jobacct->mem_step_min;

				all_task_vmem_tmp_max = total_jobacct->vmem_step_max;
				all_task_vmem_tmp_min = total_jobacct->vmem_step_min;
				/* mem real */		
				all_task_mem_tmp = total_jobacct->mem_step;
				/* vmem real */	
				all_task_vmem_tmp = total_jobacct->vmem_step;
			}
		}
	}
#endif
	if (total_jobacct) {
		jobacctinfo_2_stats(&step.stats, total_jobacct);
		jobacctinfo_destroy(total_jobacct);
	}

	slurm_job_step_pids_response_msg_free(step_stat_response);
	/* we printed it out already */
	if (params.pid_format)
		goto getout;

	hostlist_sort(hl);
	hostlist_ranged_string(hl, BUF_SIZE, step.nodes);
	hostlist_destroy(hl);
	tot_tasks += ntasks;

	if (tot_tasks) {
		step.stats.act_cpufreq /= (double)tot_tasks;

		ave_usage_tmp = step.stats.tres_usage_in_ave;
		step.stats.tres_usage_in_ave = slurmdb_ave_tres_usage(
			ave_usage_tmp, tot_tasks);
		xfree(ave_usage_tmp);
		ave_usage_tmp = step.stats.tres_usage_out_ave;
		step.stats.tres_usage_out_ave = slurmdb_ave_tres_usage(
			ave_usage_tmp, tot_tasks);
		xfree(ave_usage_tmp);

		step.ntasks = tot_tasks;
	}
#ifdef __METASTACK_LOAD_ABNORMAL
	if((params.opt_event == 1) && tot_tasks ) {

		char outbuf_tmp[34] = {'0'};
		char outbuf_tmp_max[34] = {'0'};
		char outbuf_tmp_min[34] = {'0'};

		char vmem_outbuf_tmp[34] = {'0'};
		char vmem_outbuf_tmp_max[34] = {'0'};
		char vmem_outbuf_tmp_min[34] = {'0'};

		if(step.step_id.step_id != -4) {

			if(all_task_vmem_tmp_max >=0) {
				/* mem usage conversion */
				convert_num_unit((double)all_task_mem_tmp, outbuf_tmp,
						sizeof(outbuf_tmp), UNIT_NONE, UNIT_MEGA,
						params.convert_flags);
				/* vmem usage conversion */
				convert_num_unit((double)all_task_vmem_tmp, vmem_outbuf_tmp,
						sizeof(vmem_outbuf_tmp), UNIT_NONE, UNIT_MEGA,
						params.convert_flags);

				convert_num_unit((double)all_task_mem_tmp_max, outbuf_tmp_max,
						sizeof(outbuf_tmp_max), UNIT_NONE, UNIT_MEGA,
						params.convert_flags);
				convert_num_unit((double)all_task_mem_tmp_min, outbuf_tmp_min,
						sizeof(outbuf_tmp_min), UNIT_NONE, UNIT_MEGA,
						params.convert_flags);

				convert_num_unit((double)all_task_vmem_tmp_max, vmem_outbuf_tmp_max,
						sizeof(vmem_outbuf_tmp_max), UNIT_NONE, UNIT_MEGA,
						params.convert_flags);
				convert_num_unit((double)all_task_vmem_tmp_min, vmem_outbuf_tmp_min,
						sizeof(vmem_outbuf_tmp_min), UNIT_NONE, UNIT_MEGA,
						params.convert_flags);
			}

			if(all_task_vmem_tmp_max >= 0 ) {
				printf("Maximum mem utilization of job steps     %s  \n", outbuf_tmp_max);
				printf("Minimum mem utilization of job steps     %s  \n", outbuf_tmp_min);
				printf("Current mem utilization of job steps     %s  \n", outbuf_tmp);
			} else {
				printf("Maximum mem utilization of job steps please waitting\n");
				printf("Minimum mem utilization of job steps please waitting\n");	
				printf("Current mem utilization of job steps please waitting\n");		
			}
			if(all_task_vmem_tmp_max >= 0 ) {
				printf("Maximum vmem utilization of job steps    %s  \n", vmem_outbuf_tmp);
				printf("Minimum vmem utilization of job steps    %s  \n", vmem_outbuf_tmp_max);
				printf("Current vmem utilization of job steps    %s  \n", vmem_outbuf_tmp_min);
			} else {
				printf("Maximum vmem utilization of job steps please waitting \n");
				printf("Minimum vmem utilization of job steps please waitting\n");
				printf("Vmem utilization of job steps please waitting\n");
			}
			printf("\n");
		}
	}
#endif

#ifdef __METASTACK_LOAD_ABNORMAL
	if(params.opt_event != 1)
		print_fields(&step);
#endif

getout:

	xfree(step.stats.tres_usage_in_max);
	xfree(step.stats.tres_usage_out_max);
	xfree(step.stats.tres_usage_in_max_taskid);
	xfree(step.stats.tres_usage_out_max_taskid);
	xfree(step.stats.tres_usage_in_max_nodeid);
	xfree(step.stats.tres_usage_out_max_nodeid);
	xfree(step.stats.tres_usage_in_ave);
	xfree(step.stats.tres_usage_out_ave);

	return rc;
}

int main(int argc, char **argv)
{
	ListIterator itr = NULL;
	slurm_step_id_t step_id = {
		.job_id = 0,
		.step_id = NO_VAL,
		.step_het_comp = NO_VAL,
	};
	slurm_selected_step_t *selected_step = NULL;

	slurm_conf_init(NULL);
	print_fields_list = list_create(NULL);
	print_fields_itr = list_iterator_create(print_fields_list);

	parse_command_line(argc, argv);
	if (!params.opt_job_list || !list_count(params.opt_job_list)) {
		error("You didn't give me any jobs to stat.");
		return 1;
	}

#ifdef __METASTACK_LOAD_ABNORMAL
	if(params.opt_event != 1) 
		print_fields_header(print_fields_list);
#endif
	itr = list_iterator_create(params.opt_job_list);
	while ((selected_step = list_next(itr))) {
		resource_allocation_response_msg_t *resp;
		job_step_info_response_msg_t *step_info = NULL;

		memcpy(&step_id, &selected_step->step_id, sizeof(step_id));

		if (slurm_get_job_steps(0, step_id.job_id, step_id.step_id,
					&step_info, SHOW_ALL)) {
			error("couldn't get steps for job %u",
			      selected_step->step_id.job_id);
			continue;
		} else if (!step_info->job_step_count) {
			if (step_id.step_id == NO_VAL)
				error("No steps running for job %u",
				      selected_step->step_id.job_id);
			else
				error("%ps not found running.",
				      &selected_step->step_id);

			continue;
		}

		if (slurm_allocation_lookup(step_id.job_id, &resp)) {
			error("No steps running for job %u",
			      selected_step->step_id.job_id);
			continue;
		} else if (resp->alias_list) {
			set_nodes_alias(resp->alias_list);
		}
		slurm_free_resource_allocation_response_msg(resp);

		for (int i = 0; i < step_info->job_step_count; i++) {
			/* If no stepid was requested set it to the first one */
			if (step_id.step_id == NO_VAL) {
				/*
				 * If asking for no particular step skip the
				 * special steps.
				 */
				if (!params.opt_all_steps &&
				    (step_info->job_steps[i].step_id.step_id >
				     SLURM_MAX_NORMAL_STEP_ID))
					continue;
				step_id.step_id = step_info->job_steps[i].
					step_id.step_id;
			}

			if (!params.opt_all_steps &&
			    !verify_step_id(&step_info->job_steps[i].step_id,
					    &step_id))
				continue;

			_do_stat(&step_info->job_steps[i].step_id,
				 step_info->job_steps[i].nodes,
				 step_info->job_steps[i].cpu_freq_min,
				 step_info->job_steps[i].cpu_freq_max,
				 step_info->job_steps[i].cpu_freq_gov,
				 step_info->job_steps[i].start_protocol_ver);
		}
	}
	list_iterator_destroy(itr);

	xfree(params.opt_field_list);
	FREE_NULL_LIST(params.opt_job_list);
	if (print_fields_itr)
		list_iterator_destroy(print_fields_itr);
	FREE_NULL_LIST(print_fields_list);

	return 0;
}
