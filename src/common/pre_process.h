#ifndef _PRE_PROCESS_H_
#define _PRE_PROCESS_H_

//__METASTACK_OPT_HIGH_THROUGHPUT_MEM_POOL

#include "config.h"

#include <inttypes.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "slurm/slurm.h"
#include "list.h"
#include "xmalloc.h"
#include "macros.h"

/* All memory pool structures.*/
typedef struct {
	pthread_cond_t cond;
	pthread_mutex_t mutex;
	bool shutdown;
	list_t *purge_jobacctinfo_list;
	list_t *pre_jobacctinfo_list;
	list_t *pre_job_record_list;
	list_t *purge_buf_list;
	list_t *pre_init_buf_4K;
	list_t *pre_init_buf_16K;
	list_t *pre_init_buf_256K;
	list_t *pre_init_buf_1M;
} slurmctld_pre_process_t;

extern slurmctld_pre_process_t *pre_process_data;

/* The quantity ratio of 4K : 16K : 256K : 1M is 768 : 7488 : 128 : 32.
 * The memory ratio of 4K : 16K : 256K : 1M is 3 : 117 : 4 : 4.
*/
#define BUF_POOL_RATIO_4K 3/128  
#define BUF_POOL_RATIO_16K 117/128
#define BUF_POOL_RATIO_256K 4/128
#define BUF_POOL_RATIO_1M 4/128
#define JOB_POOL_NUM 2   /* Number of job-type memory pools */

#define BUF_SIZE_4K 4096
#define BUF_SIZE_16K 16384
#define BUF_SIZE_256K 262144
#define BUF_SIZE_1M 1048576

/* Memory pool initialization, update, shutdown, and deletion.*/
extern void pre_process_init();
extern void pre_process_fini();
extern void pre_process_update();
extern void pre_process_shutdown();

#endif