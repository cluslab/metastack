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

/*
 * Test function to verify bb_api library loading
 * 
 * RET: 0 on success, SLURM_ERROR on failure
 */
extern int bb_g_bb_api_test_function(void);



/*
 * 通过SN创建缓存组
 * 
 * @param create_params 参数，详细参考create_params_request结构体注释
 * @param bb_config 最小配置参数
 * @return 0>表示成功且返回缓存组ID，-1表示代码错误，-2表示接口错误，-3表示接口超时
 */
extern int bb_g_create_bb_group_by_sn(create_params_request *create_params, bb_minimal_config_t *bb_config);

/*
 * 通过SN创建数据集规则
 * 
 * @param create_params 参数，详细参考create_params_request结构体注释
 * @param bb_config 最小配置参数
 * @return 0>表示成功且返回数据集规则ID，-1表示代码错误，-2表示接口错误，-3表示接口超时
 */
extern int bb_g_create_bb_dataset_by_sn(create_params_request *create_params, bb_minimal_config_t *bb_config);

/*
 * 提交预热任务（通过数据集ID）
 * 
 * @param create_params 提交参数，详见create_params_request注释
 * @param bb_config 最小配置参数
 * @return 成功返回task_id (>0);  -1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_submit_bb_task(create_params_request *create_params, bb_minimal_config_t *bb_config);

/*
 * 通过SN获取缓存组ID
 * 
 * @param group_sn 缓存组的SN
 * @param bb_min_config bb最小配置
 * @return 存在返回group_id; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_query_bb_groupid_by_sn(char *group_sn, bb_minimal_config_t *bb_min_config);

/*
 * 传入缓存组ID和数据集路径，查询数据集规则
 * 
 * @param group_id 缓存组ID
 * @param path 数据集路径
 * @param bb_config 最小配置参数
 * @return 存在返回dataset_id; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_query_datasetid_by_path_groupid(const int group_id, const char *path, bb_minimal_config_t *bb_config);

/*
 * 根据task_id查询bb任务
 * 
 * @param task_id 任务ID
 * @param bb_config 最小配置文件
 * @param bb_task 出参，传入初始化后变量指针，返回bb_task
 * @return 存在返回task_id; 0:不存在；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_query_bb_tasks_by_taskid(int task_id, bb_minimal_config_t *bb_config, bb_attribute_task *bb_task);

/*
 * 根据group_sn删除缓存组
 * 
 * @param group_sn 缓存组sn
 * @param bb_config 最小配置
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_delete_bb_group_by_sn(char *group_sn, bb_minimal_config_t *bb_config);

/*
 * 根据dataset_id删除数据集规则
 * 
 * @param dataset_id 数据集ID
 * @param bb_config 最小配置
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_delete_bb_dataset_by_id(int dataset_id, bb_minimal_config_t *bb_config);

/*
 * 根据task_id取消BB任务
 * 
 * @param task_id 任务ID
 * @param bb_config 最小配置
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_cancel_bb_task_by_id(int task_id, bb_minimal_config_t *bb_config);


/**
 * @brief 释放task结构体
 * @return 
 */
extern void bb_g_slurm_free_task(void *object);



#endif
