#ifndef _INTERFACES_BURST_BUFFER_SLURMD_H
#define _INTERFACES_BURST_BUFFER_SLURMD_H

#include "slurm/slurm.h"
#include "src/plugins/burst_buffer/parastor/bb_api.h"


/*
 * Initialize the bb_api library infrastructure.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_init(void);

/*
 * Terminate the bb_api library infrastructure. Free memory.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_fini(void);

/*
 **************************************************************************
 *                    B B   A P I   W R A P P E R   F U N C T I O N S    *
 **************************************************************************
 */


/**
 * @brief 通过SN创建缓存组
 * @param group_sn 
 * @param client_cnt client_arr中元素个数，必须严格对应
 * @param client_arr 客户端ID数组
 * @param bb_config 
 * @return 0>表示成功且返回缓存组ID，-1表示代码错误，-2表示接口错误，-3表示接口超时
 */
extern int bb_g_create_bb_group_by_sn(char *group_sn, int client_cnt, int *client_arr, bb_minimal_config_t *bb_config);

/**
 * @brief 通过SN和加速目录创建数据集规则
 * @param group_sn 
 * @param group_id
 * @param path 需要加速的目录（PSBB格式，eg:bb_hpc:/dir）
 * @param is_use_metadata 是否加速元数据
 * @param is_share_cache  数据为共享缓存or本地缓存
 * @param bb_config 
 * @return 0>表示成功且返回数据集规则ID，-1表示代码错误，-2表示接口错误，-3表示接口超时
 */
extern int bb_g_create_bb_dataset_by_sn(char *group_sn, int group_id ,char *path, bool is_use_metadata, bool is_share_cache, bb_minimal_config_t *bb_config);

/**
 * @brief 提交预热任务（通过数据集ID）
 * @param dataset_id 
 * @param task_type 任务类型：1为预热，2为回收
 * @param create_params 
 * @param bb_config 
 * @return 成功返回task_id (>0);  -1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_submit_bb_task(int dataset_id, int task_type, bb_minimal_config_t *bb_config);

/**
 * @brief 通过SN获取缓存组ID
 * @param group_sn 
 * @param bb_min_config 
 * @return 存在返回group_id; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_query_bb_groupid_by_sn(char *group_sn, bb_minimal_config_t *bb_min_config);

/**
 * @brief 等待任务预热完成
 * @param task_id 
 * @param task_type 任务类型：1为预热，2为回收
 * @param bb_config othertimeout为软时间，stageintimeout为硬时间
 * @return 0:成功完成预热; -1:预热失败
 */
extern int bb_g_wait_task_complete(int task_id, int task_type, bb_minimal_config_t *bb_config);

/**
 * @brief 根据缓存组ID和数据集路径，查询数据集规则
 * @param group_id 
 * @param path 
 * @param bb_config 
 * @return 存在返回dataset_id; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_query_datasetid_by_path_groupid(const int group_id, const char *path, bb_minimal_config_t *bb_config);

/**
 * @brief 根据task_id查询bb任务
 * @param task_id 
 * @param bb_config 
 * @param bb_task 
 * @return 存在返回task_id; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_query_bb_tasks_by_taskid(int task_id, bb_minimal_config_t *bb_config, bb_attribute_task *bb_task);

/**
 * @brief 根据group_sn删除缓存组
 * @param group_sn 
 * @param bb_config 
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_delete_bb_group_by_sn(char *group_sn, bb_minimal_config_t *bb_config);

/**
 * @brief 根据dataset_id删除数据集规则
 * @param dataset_id 
 * @param group_id 用于查询是否删除
 * @param path 用于查询是否删除
 * @param bb_config 
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_delete_bb_dataset_by_id(int dataset_id, int group_id, char * path,bb_minimal_config_t *bb_config);

/*
 * 根据task_id取消BB任务
 * 
 * @param task_id 任务ID
 * @param bb_config 最小配置
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_cancel_bb_task_by_id(int task_id, bb_minimal_config_t *bb_config);


// /**
//  * @brief 释放task结构体
//  * @return 
//  */
// extern void bb_g_slurm_free_task(void *object);



#endif
