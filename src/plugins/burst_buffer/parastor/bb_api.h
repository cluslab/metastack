#ifndef BB_API_H
#define BB_API_H

#include <stdio.h>
#include <stdlib.h>
// #include "slurm/slurm.h"
// #include "slurm/slurm.h"
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

/*
struct bb_state;
struct query_params_request;
before call API,must initialize the structure,and set must params 
1. 查询缓存组所需参数
    - start: 查询起始记录数
    - limit: 查询记录数
    - group_sn: 缓存组唯一标识符，
    - ids:缓存组id列表
2. 查询数据集规则所需参数
    - start: 查询起始记录数
    - limit: 查询记录数
    - path: 数据集路径，模糊匹配
    - group_id: 缓存组ID，默认值为0，表示不限制
    - task_type: 必填，设置为BURST_BUFFER_TASK_TYPE_NULL
    - task_state： 必填，设置为BB_TASK_STATE_NULL
3. 查询BB任务所需参数
    - start: 查询起始记录数
    - limit: 查询记录数
    - group_id: 缓存组ID，默认值为0，表示不限制
    - task_id: 任务ID
    - task_type: 任务类型，设置为BURST_BUFFER_TASK_TYPE_NULL表示不
    - task_state： 任务状态，设置为BB_TASK_STATE_NULL表示不限制
4.查询client所需参数
    - start: 查询起始记录数
    - limit: 查询记录数
    - client_ids:可选,格式为xxx1,xxx2
    - client_ips:可选,客户端IP，目前接口只支持单个
    - client_ip_match_mode:可选，0:精确查询，1:模糊查询
    - host_name:可选,客户端hostname，目前接口只支持单个
    - host_name_match_mode:选，0:精确查询，1:模糊查询
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
    char* group_sn;
    
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


/*
创建、提交操作参数的结构体
参数说明
1. 通过SN创建缓存组
 - group_sn     缓存组唯一标识符
 - clietn_count 客户端数量
 - client_ids   客户端ID数组指针
2. 通过SN创建数据集规则
 - group_sn        缓存组唯一标识符
 - path            数据集路径
 - is_use_metadata 是否使用元数据缓存
 - data_cache_type 缓存类型
3. 提交任务
 - dataset_id      数据集ID
 - task_type       任务类型
 - error_action_type 异常后执行行为类型,0:存在节点失败后中止; 1:存在节点失败后继续
*/
typedef struct {
    /* create group params */
    char *group_sn;
    int client_count; /*  The client_count and client_ids must be entered at the same time. */
    int *client_ids;
    int del_delay_time; /* delay time for delete operation,defalut is 3600s */
    int fault_delay_time; /* fault_delay_time: delay time for fault operation,defalut is 3600s */

    /* create datasets params */
    char *path;
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

/**
 * @brief 创建、提交操作参数的结构体
 * 1. 删除缓存组
 *  - group_id 删除缓存组的group_id
 *  - group_sn 删除缓存组的SN，覆盖group_id参数
 * 2. 删除数据集规则
 *  - dataset_id 删除数据集规则的ID
 * 3. 取消BB任务
 *  - task_id 取消任务的ID
 */
typedef struct {
    int group_id;
    char *group_sn;
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

/* get single task,return data of task into bb_task */
extern int get_single_burst_buffer_tasks( int task_id, bb_attribute_task *bb_task,  bb_minimal_config_t *bb_min_config, bb_response *resp_out);
/* Create a cache group by client ids */
extern int create_burst_buffer_group(create_params_request *create_params,  bb_minimal_config_t *bb_min_config, bb_response *resp_out);



/** 
 * @brief 通过SN创建缓存组
 * @param create_params 参数，详细参考create_params_request结构体注释
 * @param bb_config 最小配置参数
 * @param resp_out 通用响应体
 * @return 0>表示成功且返回缓存组ID，-1表示代码错误，-2表示接口错误，-3表示接口超时
 */
extern int create_bb_group_by_sn(create_params_request *create_params, bb_config_t *bb_config);

/** 
 * @brief 通过SN创建数据集规则
 * @param create_params 参数，详细参考create_params_request结构体注释
 * @param bb_config 最小配置参数
 * @param resp_out 通用响应体
 * @return 0>表示成功且返回数据集规则ID，-1表示代码错误，-2表示接口错误，-3表示接口超时
 */
extern int create_bb_dataset_by_sn(create_params_request *create_params, bb_config_t *bb_config);

 /**
 * @brief 提交预热任务（通过数据集ID）
 * @param create_params 提交参数，详见create_params_request注释
 * @param bb_config 最小配置参数
 * @return 成功返回task_id (>0);  -1:代码错误; -2:接口错误; -3:接口超时
 */
extern int submit_bb_task(create_params_request *create_params, bb_config_t *bb_config);

/**
 * @brief 通过SN获取缓存组ID
 * @param group_sn 缓存组的SN
 * @param bb_min_config bb最小配置
 * @return 存在返回group_id; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int query_bb_groupid_by_sn(char *group_sn, bb_config_t *bb_min_config);

/**
 * @brief 传入缓存组ID和数据集路径，查询数据集规则
 * @param group_id 缓存组ID
 * @param path 数据集路径
 * @return 存在返回task_id; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int query_datasetid_by_path_groupid(const int group_id, const char *path, bb_config_t *bb_config);

/**
 * @brief 根据group_id、path查询bb任务
 * @param task_id 入参，传入缓存组ID
 * @param bb_config 入参，最小配置文件
 * @param bb_task 出参，传入初始化后变量指针，返回bb_task
 * @return 存在返回task_id; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int query_bb_tasks_by_taskid(int task_id, bb_config_t *bb_config, bb_attribute_task *bb_task);

/**
 * @brief 传入hostname获取对应client_id
 * @param hostname 
 * @param bb_config 
 * @return 成功返回clietnid; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int query_clientid_by_hostname(const char *hostname, bb_config_t *bb_config);

/**
 * @brief 根据group_sn删除缓存组
 * @param group_sn 入参：缓存组sn
 * @param bb_config 入参：最小配置
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int delete_bb_group_by_sn(char *group_sn, bb_config_t *bb_config);

/**
 * @brief 根据dataset_id删除缓存组
 * @param dataset_id 
 * @param bb_config 
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int delete_bb_dataset_by_id(int dataset_id, bb_config_t *bb_config);


/**
 * @brief 根据task_id删除BB任务
 * @param task_id 
 * @param bb_config 
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int cancel_bb_task_by_id(int task_id, bb_config_t *bb_config);


/* 
* delete a cache group by client ids 
* NOTE: before deleting the cache group, make sure to delete the datasets under this group first.
*/
extern int delete_burst_buffer_group(delete_params_request *delete_params, bb_minimal_config_t *bb_config, bb_response *resp_out);

/* Create a cache dataset */
extern int create_burst_buffer_dataset(create_params_request *create_params,bb_config_t *bb_min_config, bb_response *resp_out);

/* Delete a cache dataset by dataset_id */
extern int delete_burst_buffer_dataset(delete_params_request *delete_params, bb_config_t *bb_config, bb_response *resp_out);

/* Submit bb task, include prefetch and recycle*/
extern int submit_burst_buffer_task(create_params_request *create_params, bb_config_t *bb_config, bb_response *resp_out);


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


extern void free_bb_response(bb_response *resp);
/* 释放缓存组 */
extern void free_bb_group(void *object);
/* 释放数据集机 */
extern void free_bb_dataset(void *object);
/* 释放客户端 */
extern void free_bb_client(void *object);
/* 释放任务 */
extern void free_bb_task(void *object);
/* list_find_first 查找函数 */
extern int _find_client_key(void *x, void *key);

extern int _find_group_key(void *x, void *key);

extern int _find_dataset_key(void *x, void *key);

/* 接口参数结构体清理函数 */
extern void free_query_params(query_params_request *query_params);
extern void free_create_params(create_params_request *create_params);
extern void free_delete_params(delete_params_request *delete_params);

#endif
