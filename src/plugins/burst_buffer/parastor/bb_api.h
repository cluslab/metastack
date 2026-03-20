#ifndef BB_API_H
#define BB_API_H

#include <stdio.h>
#include <stdlib.h>
// #include "slurm/slurm.h"
#include "src/plugins/burst_buffer/common/burst_buffer_common.h"
#include "bb_curl_wrapper.h"
#include <string.h>
#include <jansson.h>


/* Maximum URL length for REST calls */
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
 * query_params_request — filters for burst-buffer list/query APIs.
 * Initialize the struct before use and set the fields relevant to the call.
 *
 * 1) Query cache groups
 *    - start, limit: paging
 *    - group_sn: serial number (takes precedence over group_id when both set)
 *    - ids: group id list (reserved / unused)
 *    - group_id: single group id; keep group_sn NULL when using id alone
 *
 * 2) Query datasets
 *    - start, limit: paging
 *    - path: dataset path (fuzzy match)
 *    - group_id: cache group filter; 0 means no filter
 *    - task_type, task_state: set to BURST_BUFFER_TASK_TYPE_NULL and BB_TASK_STATE_NULL
 *
 * 3) Query burst-buffer tasks
 *    - start, limit: paging
 *    - group_id: cache group filter; 0 means no filter
 *    - task_id: task id
 *    - task_type: BURST_BUFFER_TASK_TYPE_NULL to skip filter
 *    - task_state: BB_TASK_STATE_NULL for no state filter
 *
 * 4) Query clients
 *    - start, limit: paging
 *    - client_ids: optional, comma-separated ids
 *    - client_ips: optional; API accepts a single IP for now
 *    - client_ip_match_mode: optional, 0 exact, 1 fuzzy
 *    - host_name: optional; single hostname for now
 *    - host_name_match_mode: optional, 0 exact, 1 fuzzy
 */
typedef struct { 
    uint32_t start; /* Query starting from which record */
    uint32_t limit; /* Number of records to query */
    char* ids; /* Cache group id list */
    char* client_ids;
    char* client_ips;
    char* host_name; 
    uint32_t client_ids_count;
    char* group_sn;
    
    uint32_t client_ips_count; 
    int client_ip_match_mode; /* 0: Exact match, 1: Fuzzy match */
    uint32_t host_name_count;    
    int host_name_match_mode; /* 0: Exact match, 1: Fuzzy match */

    /* Dataset query fields */
    const char *path; /* dataset path, fuzzy match */
    uint32_t group_id ;/* cache group id . Default value is 0, indicating no restriction */
    uint32_t max_clients_join;

    uint32_t task_id;
    uint32_t dataset_id;
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
 * create_params_request — parameters for create/submit operations.
 *
 * 1) Create cache group by SN
 *    - group_sn, client_count, client_ids
 * 2) Create dataset rule (by SN)
 *    - group_sn, path, is_use_metadata, data_cache_type
 * 3) Submit task
 *    - dataset_id, task_type, error_action_type (see field comment below)
 */
typedef struct {
    /* create group params */
    char *group_sn;
    uint32_t client_count; /*  The client_count and client_ids must be entered at the same time. */
    uint32_t *client_ids;
    time_t del_delay_time; /* delete delay; default 3600s */
    time_t fault_delay_time; /* fault-handling delay; default 3600s */

    /* create datasets params */
    char *path;
    uint32_t group_id;
    bool is_use_metadata;
    data_cache_type data_cache_type;

    /* submit task params */
    uint32_t dataset_id;
    bb_task_type task_type;
    /* 
    Type of execution behavior after exception.
    *0:The task is interrupted after a single node fails.
    *1:The task continues after a single node fails. 
    */
    int error_action_type; 

}create_params_request;

/**
 * delete_params_request — delete/cancel operations (use one of group_id or group_sn for groups).
 * - group_id / group_sn: delete cache group (SN overrides id when both set)
 * - dataset_id: delete dataset rule
 * - task_id: cancel burst-buffer task
 */
typedef struct {
    uint32_t group_id;
    char *group_sn;
    uint32_t dataset_id;
    uint32_t task_id;
} delete_params_request;

/**
 * @brief Obtain a long-lived API token from Parastor.
 * @param bb_config Burst-buffer connection config
 * @return 0 on success, negative codes on failure (see implementation)
 */
extern int get_permanent_token(bb_config_t *bb_config);

/**
 * @brief Create a cache group using the group serial number (SN).
 * @param create_params See create_params_request
 * @param group_id Out: new group id
 * @param bb_config Minimal burst-buffer config
 * @return 0 success, -1 code error, -2 API error, -3 timeout
 */
extern int create_bb_group_by_sn(create_params_request *create_params, uint32_t *group_id, bb_config_t *bb_config);

/**
 * @brief Create a dataset rule (associates path with cache group via SN).
 * @param create_params See create_params_request
 * @param dataset_id Out: new dataset rule id
 * @param bb_config Minimal burst-buffer config
 * @return 0 success, -1 code error, -2 API error, -3 timeout
 */
extern int create_bb_dataset_by_sn(create_params_request *create_params, uint32_t *dataset_id, bb_config_t *bb_config);

/**
 * @brief Submit a prefetch/recycle task for a dataset.
 * @param create_params Submit parameters (dataset_id, task_type, etc.)
 * @param task_id Out: new task id
 * @param bb_config Minimal burst-buffer config
 * @return 0 success, -1 code error, -2 API error, -3 timeout
 */
extern int submit_bb_task(create_params_request *create_params, uint32_t *task_id, bb_config_t *bb_config);

/**
 * @brief Resolve cache group id from serial number.
 * @param group_sn Group SN string
 * @param group_id Out: group id
 * @param bb_min_config Burst-buffer config
 * @return 0 success, 1 not found, other negative codes on error
 */
extern int query_bb_groupid_by_sn(char *group_sn, uint32_t *group_id, bb_config_t *bb_min_config);

/**
 * @brief Check whether a cache group exists by id.
 * @param group_id Group id to test
 * @param bb_min_config Burst-buffer config
 * @return 0 if exists, 1 if not, negative on transport/API errors
 */
extern int has_bb_group_by_id(uint32_t group_id, bb_config_t *bb_min_config);

/**
 * @brief Look up dataset rule id by cache group and path.
 * @param group_id Cache group id
 * @param path Dataset path
 * @param dataset_id Out: dataset rule id
 * @return 0 success, 1 not found, negative on error
 */
extern int query_datasetid_by_path_groupid(uint32_t group_id, const char *path, uint32_t *dataset_id, bb_config_t *bb_config);

/**
 * @brief Query a burst-buffer task by task id.
 * @param task_id Task id
 * @param bb_config Burst-buffer config
 * @param bb_task Out: filled task attributes
 * @return 0 success, 1 no data, negative on error
 */
extern int query_bb_task_by_taskid(uint32_t task_id, bb_config_t *bb_config, bb_attribute_task *bb_task);

/**
 * @brief Resolve client id by host name.
 * @param hostname Client hostname to match
 * @param client_id Out: client id
 * @param bb_config Burst-buffer config
 * @return 0 success, 1 not found, negative on error
 */
extern int query_clientid_by_hostname(const char *hostname, uint32_t *client_id, bb_config_t *bb_config);

/**
 * @brief Delete a cache group by serial number.
 * @param group_sn Group SN
 * @param bb_config Burst-buffer config
 * @return 0 on success, negative codes on failure
 */
extern int delete_bb_group_by_sn(char *group_sn, bb_config_t *bb_config);

/**
 * @brief Delete a cache group by id.
 * @param group_id Group id
 * @param bb_config Burst-buffer config
 * @return 0 on success, negative codes on failure
 */
extern int delete_bb_group_by_id(uint32_t group_id, bb_config_t *bb_config);

/**
 * @brief Delete a dataset rule by id.
 * @param dataset_id Dataset rule id
 * @param bb_config Burst-buffer config
 * @return 0 on success, negative codes on failure
 */
extern int delete_bb_dataset_by_id(uint32_t dataset_id, bb_config_t *bb_config);

/**
 * @brief Cancel a burst-buffer task by id.
 * @param task_id Task id
 * @param bb_config Burst-buffer config
 * @return 0 on success, negative codes on failure
 */
extern int cancel_bb_task_by_id(uint32_t task_id, bb_config_t *bb_config);

/**
 * @brief List cache group ids used by Slurm (SN prefix 'j').
 * @param bb_min_config Minimal Parastor config
 * @param used_groupid_arr Out: reallocated id array
 * @param used_groups_cnt Out: number of ids
 * @return 0 success, negative on error
 */
extern int get_used_groupid_arr(bb_minimal_config_t *bb_min_config, uint32_t **used_groupid_arr, uint32_t *used_groups_cnt);

/**
 * @brief Collect group_id from every dataset (paged Parastor scan).
 * @param bb_min_config Minimal Parastor config
 * @param groupid_arr Out: reallocated array of group ids
 * @param groups_cnt Out: number of entries
 * @return 0 success, negative on error
 */
extern int get_groupid_of_all_datasets(bb_minimal_config_t *bb_min_config, uint32_t **groupid_arr, uint32_t *groups_cnt);




extern void free_bb_response(bb_response *resp);
extern void free_bb_group(void *object);
extern void free_bb_dataset(void *object);
extern void free_bb_client(void *object);
extern void free_bb_task(void *object);

extern int _find_group_key(void *x, void *key);

extern int _find_dataset_key(void *x, void *key);

extern void free_query_params(query_params_request *query_params);
extern void free_create_params(create_params_request *create_params);
extern void free_delete_params(delete_params_request *delete_params);




/*  Not yet implemented: POSIX BB cache group immediate adjustment mapping         */
extern int remap_burst_buffer_group();
/* Not yet implemented: Add client to the cache group */
extern int add_burst_buffer_client_to_group();
/* Not yet implemented: Remove client from the cache group */
extern int remove_burst_buffer_client_from_group();
/* Not yet implemented: Locking the dataset will not trigger the automatic recycling mechanism. */
extern int lock_burst_buffer_dataset();
/* Not yet implemented: Unlock dataset */
extern int unlock_burst_buffer_dataset();


#endif
