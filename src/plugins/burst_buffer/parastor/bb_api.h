#ifndef BB_API_H
#define BB_API_H

#include <stdio.h>
#include <stdlib.h>
#include "src/plugins/burst_buffer/common/burst_buffer_common.h"
#include "bb_curl_wrapper.h"
#include <string.h>
#include <jansson.h>


// URL最大长度
#define URL_MAX_LEN 10000
#define TOKEN_MAX_LEN 10000

typedef enum {
    RESULT_ARRAY = 0,
    RESULT_GROUP, /* query group type */
    RESULT_DATASET, /* query dataset type */
    RESULT_CLIENT, /* query client type */
    RESULT_TASK, /* query task type */
    GROUP_CREATE ,/* create group type */
    DATASET_CREATE,
    TASK_SUBMIT,
    NO_RESULT/* there is no result in response , only basic resp info */
} result_type;

// /* 定义缓存组结构体 bb_cache_group */
// typedef struct {
//     int id;                    // 缓存组ID
//     int client_num;            // 客户端数量
//     int *client_ids;           // 客户端ID数组指针
//     int del_delay_time;        // 删除延迟时间
//     int fault_delay_time;      // 故障延迟时间
//     double hit_bytes_rate;     // 命中字节率
//     double hit_io_num_rate;    // 命中IO数量率
//     double meta_hit_io_num_rate; // 元数据命中IO数量率
// } bb_attribute_group;

// /* response info of datasets */
// typedef struct {
//     char *burstBufferDataSetCacheMode;
//     char *burstBufferDataSetCacheType;
//     char *burstBufferMetaDataSetCacheMode;
//     int64_t create_time;
//     bool dataOpen;
//     char *data_cache_mode;
//     char *data_cache_type;
//     int fs_id;
//     int group_id;
//     int id;
//     char *idesc;
//     int key;
//     int64_t last_submit_task_time;
//     char *last_submit_task_type;
//     bool lock_flag;
//     char *meta_data_cache_mode;
//     char *path;
//     int path_version;
//     char *state;
//     bool use_data;
//     bool use_meta_data;
//     int version;
// } bb_attribute_dataset;

// /* ip info of bb_attribute_client */
// typedef struct {
//    int id;       // 客户端ID,对应client_id
//    char *hostname;
//    char *ip;
//    long long total_size;/* bytes */
//    long long used_size; /* bytes */
//    int version;
//    int ips_count;
//    int devices_count;
//    int groups_count; // 节点当前加入缓存组数量
//    int *groups_ids;;
//    char *cap_dev_name;
// } bb_attribute_client;

// typedef struct {
//     int task_id;
//     int dataset_id;
//     int group_id;
//     char *task_state;
//     char *task_type; 
//     long long begin_time;
//     long long end_time;
//     long long completed_bytes;
//     int total_node_num;
//     int completed_node_num;
//     int canceled_node_num;
//     char *error_action_type;
//     int failed_node_num;
//     char **failed_node_infos;
//     int exit_code;
// } bb_attribute_task;

typedef enum {
    BURST_BUFFER_TASK_TYPE_NULL = 0, /* set this type if not need this param */
    BURST_BUFFER_TASK_TYPE_PREFETCH, /* 预热 */
    BURST_BUFFER_TASK_TYPE_RECYCLE /* 回收 */
} bb_task_type;

typedef enum {
    BB_TASK_STATE_NULL = 0,/* set this type if not need this param */
    BB_TASK_STATE_SUBMITTING,
    BB_TASK_STATE_RUNNING,
    BB_TASK_STATE_COMPLETED,
    BB_TASK_STATE_FAILED,
    BB_TASK_STATE_CANCELED
} bb_task_state_type;

/*
* struct bb_state;
* struct query_params_request;
* before call API,must initialize the structure,and set must params 
*/
typedef struct { 
    int start; /* Query starting from which record */
    int limit; /* Number of records to query */
    // int calc_count; /* Current page number */

    /* groups para */
    char* ids; /* Cache Group List */
    char* client_ids;
    char* client_ips;
    char* host_name; 
    int client_ids_count;
    
    int client_ips_count; 
    int client_ip_match_mode; /* 0: Exact match, 1: Fuzzy match */
    int host_name_count;    
    int host_name_match_mode; /* 0: Exact match, 1: Fuzzy match */

   /* datasets result */
    const char *path;/* dataset path, fuzzy match*/
    int group_id ;/* cache group id . Default value is 0, indicating no restriction */
    uint32_t max_clients_join;

    int task_id; // 
    int dataset_id; // dataset id
    /* clients result */
    bb_task_type task_type;
    bb_task_state_type task_state;
} query_params_request;


typedef enum {
    QUERY_CALL = 0,
    CREATE_CALL,
    DELETE_CALL,
    CANCEL_CALL
} call_type;

typedef enum {
    LOCAL_CACHE = 0, /* local cache */
    SHARE_CACHE /* share cache */
} data_cache_type;

typedef struct {
    /* create group params */
    int client_count; /*  The client_count and client_ids must be entered at the same time. */
    const int *client_ids;
    int del_delay_time; /* delay time for delete operation,defalut is 3600s */
    int fault_delay_time; /* fault_delay_time: delay time for fault operation,defalut is 3600s */

    /* create datasets params */
    const char *path;
    int group_id;
    bool is_use_metadata;
    data_cache_type data_cache_type;

    /* submit task params */
    int dataset_id;
    bb_task_type task_type;
    /* 
    Type of execution behavior after exception.
    *0:The task is interrupted after a single node fails.
    *1:The task continues after a single node fails. 
    */
    int error_action_type; 
}create_params_request;

typedef struct {
    int group_id;
    int dataset_id;
    int task_id;
} delete_params_request;
/* get permanent token */
extern int get_permanent_token(bb_config_t *bb_config);
/* get groups list */
extern List get_groups_burst_buffer(query_params_request* query_params, bb_minimal_config_t *bb_min_config, bb_response *resp_out);
/* get datasets list */
extern List get_datasets_burst_buffer(query_params_request *query_params, bb_minimal_config_t *bb_min_config, bb_response *resp_out);
/* get set burst buffer clients and task list */
extern int get_set_burst_buffer_clients_and_tasks(query_params_request *query_params,  bb_minimal_config_t *bb_min_config, result_type type, bb_response *resp_out);
/* get burst buffer task list */
extern int get_burst_buffer_tasks(query_params_request *query_params, bb_state_t *bb_state, bb_response *resp_out);
/* get single task,return data of task into bb_task */
extern int get_single_burst_buffer_tasks( int task_id, bb_attribute_task *bb_task,  bb_minimal_config_t *bb_min_config, bb_response *resp_out);
/* Create a cache group by client ids */
extern int create_burst_buffer_group(create_params_request *create_params,  bb_minimal_config_t *bb_min_config, bb_response *resp_out);
/* 
* delete a cache group by client ids 
* NOTE: before deleting the cache group, make sure to delete the datasets under this group first.
*/
extern int delete_burst_buffer_group(delete_params_request *delete_params, bb_minimal_config_t *bb_config, bb_response *resp_out);

/* Create a cache dataset */
extern int create_burst_buffer_dataset(create_params_request *create_params,bb_minimal_config_t *bb_min_config, bb_response *resp_out);

/* Delete a cache dataset by dataset_id */
extern int delete_burst_buffer_dataset(delete_params_request *delete_params, bb_minimal_config_t *bb_config, bb_response *resp_out);

/* Submit bb task, include prefetch and recycle*/
extern int submit_burst_buffer_task(create_params_request *create_params, bb_config_t *bb_config, bb_response *resp_out);


/* Cancel a task by task_id */
extern int cancel_burst_buffer_task(bb_config_t *bb_config, delete_params_request *delete_params, bb_response *resp_out);

/* Not yet implemented: POSIX BB cache group immediate adjustment mapping */
extern int remap_burst_buffer_group();
/* Not yet implemented: Add client to the cache group */
extern int add_burst_buffer_client_to_group();
/* Not yet implemented: Remove client from the cache group */
extern int remove_burst_buffer_client_from_group();
/* Not yet implemented: Locking the dataset will not trigger the automatic recycling mechanism. */
extern int lock_burst_buffer_dataset();
/* Not yet implemented: Unlock dataset */
extern int unlock_burst_buffer_dataset();




extern void bb_response_free(bb_response *resp);
/* 释放缓存组 */
extern void slurm_free_group(void *object);
/* 释放数据集机 */
extern void slurm_free_dataset(void *object);
/* 释放客户端 */
extern void slurm_free_client(void *object);
/* 释放任务 */
extern void slurm_free_task(void *object);
/* list_find_first 查找函数 */
extern int _find_client_key(void *x, void *key);

extern int _find_group_key(void *x, void *key);

extern int _find_dataset_key(void *x, void *key);

#endif
