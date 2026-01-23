#ifndef _INTERFACES_BURST_BUFFER_SLURMD_H
#define _INTERFACES_BURST_BUFFER_SLURMD_H

#include "slurm/slurm.h"

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
 * @brief 根据缓存组SN创建缓存组
 * @param group_sn 
 * @param client_cnt 缓存组中客户端数量
 * @param client_hostname_arr  客户端hostname数组
 * @param group_id 返回创建成功的缓存组ID
 * @return 0:成功；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_create_bb_group_by_sn(char *group_sn, int client_cnt, char **client_hostname_arr, uint32_t *group_id);

/**
 * @brief 创建数据集规则
 * @param group_sn 缓存组sn
 * @param group_id 缓存组ID
 * @param path 加速路径
 * @param is_use_metadata 元数据是否加速 
 * @param is_share_cache 缓存方式（true为共享缓存,false为本地缓存）
 * @return 0>表示成功且返回数据集规则ID，-1表示代码错误，-2表示接口错误，-3表示接口超时
 */
extern int bb_g_create_bb_dataset_by_sn(char *group_sn, uint32_t group_id ,char *path, bool is_use_metadata, bool is_share_cache, uint32_t *dataset_id);

/**
 * @brief 提交任务
 * @param dataset_id 数据集规则ID
 * @param task_type 1:预热; 2:回收
 * @param task_id 返回创建成功的任务ID
 * @return 0:成功提交；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_submit_bb_task(uint32_t dataset_id, int task_type, uint32_t *task_id);

/**
 * @brief 阻塞等待任务完成
 * @param task_id 任务ID
 * @param task_type 1:预热; 2:回收
 * @return 0:任务完成；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_wait_task_complete(uint32_t task_id, int task_type);

/**
 * @brief 根据group_sn删除缓存组
 * @param group_sn 
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_delete_bb_group_by_sn(char *group_sn);

/**
 * @brief 根据dataset_id删除数据集规则
 * @param dataset_id 数据集ID
 * @param group_id 用于超时后查询数据集规则
 * @param path 用于超时后查询数据集规则
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_delete_bb_dataset_by_id(uint32_t dataset_id, uint32_t group_id, char * path);

/**
 * @brief 根据task_id取消BB任务
 * @param task_id 任务ID
 * @return 0:成功取消；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_cancel_bb_task_by_id(uint32_t task_id);
/**
 * @brief 根据group_id删除缓存组
 * @param group_id 
 * @return 0:成功删除；-1:代码错误; -2:接口错误; -3:接口超时
 */
extern int bb_g_delete_bb_dataset_by_groupid_path(uint32_t dataset_id, uint32_t group_id, char * path);
#endif
