#include "bb_api.h"
#include <stdbool.h>
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/list.h"
#include "src/plugins/burst_buffer/common/burst_buffer_common.h"

static int parse_json_dataset_result(json_t *dataset_obj, bb_attribute_dataset *dataset);
static int parse_json_groups_result(json_t *group_obj, bb_attribute_group *group);
static int parse_json_client_result(json_t *client_obj, bb_attribute_client *client);
static int parse_json_tasks_result(json_t *task_obj, bb_attribute_task *task);

/* Declaration Helper Function */
static int json_string_set_response(const char* json_str, bb_response* resp_out, result_type type);
static int task_json_string_get_response(const char* json_str, bb_response* resp_out, bb_attribute_task *bb_task);
/* get temporary token */
static char *_get_token_from_header(const char *ip, int port, const char *user, const char *password);
/* get permanent token */
static char *_get_permanent_token_from_header(const char *ip, int port, const char *user, const char *password);   
/* encode password for calling permanent token */
static char *_encode_password(const char *password);
/* helper function for _encode_password */
static char *_base64_encode(const unsigned char *data, size_t len);
static int json_group_get_response(const char* json_str, query_params_request *query_params, List list, bb_response* resp_out);
static int json_dataset_get_response(const char* json_str, query_params_request *query_params, List list, bb_response* resp_out);

/**
* Parse the JSON-formatted group result and populate the group structure.
* @param group_obj Pointer to the JSON object containing group information.
* @param group Pointer to the group structure used to store the parsed data.
*/
static int parse_json_groups_result(json_t *group_obj, bb_attribute_group *group)
{
    int rc = SLURM_SUCCESS;
    if (!group_obj || !group) {
        debug("parse_json_groups_result 参数为空\n");
        return SLURM_ERROR;
    }
    /* example
     * curl -k --location --request GET 'https://11.16.123.210:8443/burst-buffer/cache-groups' --header 'token:xxxxxx'
     * {"detail_err_msg":"","err_msg":"","err_no":0,"result":
     * {"cache_groups":
     * [{"client_ids":[1000003],"
     * client_num":1,
     * "del_delay_time":3600,
     * "fault_delay_time":3600,
     * "hit_bytes_rate":0,
     * "hit_io_num_rate":0,
     * "id":2,
     * "meta_hit_io_num_rate":0.00},
     * {"client_ids":[1000004],"client_num":1,"del_delay_time":3600,"fault_delay_time":3600,"hit_bytes_rate":0,
     * "hit_io_num_rate":0,"id":3,"meta_hit_io_num_rate":0}],
     * "isExact":false,
     * "limit":0,
     * "searches":[],
     * "sort":"NONE",
     * "start":0,"
     * total":2},
     * "sync":true,
     * "time_stamp":0,
     * "time_zone_offset":0,
     * "trace_id":"[37af548c89]"}
     */
    memset(group, 0, sizeof(*group));
    json_t *client_ids          = json_object_get(group_obj, "client_ids");
    group->id                   = json_integer_value(json_object_get(group_obj, "id"));
    group->client_num           = json_integer_value(json_object_get(group_obj, "client_num"));
    group->del_delay_time       = json_integer_value(json_object_get(group_obj, "del_delay_time"));
    group->fault_delay_time     = json_integer_value(json_object_get(group_obj, "fault_delay_time"));
    group->hit_bytes_rate       = json_real_value(json_object_get(group_obj, "hit_bytes_rate"));
    group->hit_io_num_rate      = json_real_value(json_object_get(group_obj, "hit_io_num_rate"));
    group->meta_hit_io_num_rate = json_real_value(json_object_get(group_obj, "meta_hit_io_num_rate"));
    /* check whether client_ids exists and is an array.
     * the following if statement is temporarily not used.
     */ 
    if (client_ids && json_is_array(client_ids)) {
        int n             = json_array_size(client_ids);
        group->client_ids = xcalloc(n, sizeof(int));
        for (int j = 0; j < n; j++) { 
            json_t *id_json = json_array_get(client_ids, j);
            if (json_is_integer(id_json)) {  
                group->client_ids[j] = (int)json_integer_value(id_json);
            }
        }
    } 
    return rc;
}
/**
* Parse the JSON-formatted dataset result and populate the dataset structure.
* @param dataset_obj Pointer to the JSON object containing dataset information.
* @param dataset Pointer to the dataset structure used to store the parsed data.
*/
static int parse_json_dataset_result(json_t *dataset_obj, bb_attribute_dataset *dataset)
{
    int rc = SLURM_SUCCESS;
    if (!dataset_obj || !dataset) {
        error(" get dataset result failed, dataset_obj or dataset is null");
        return SLURM_ERROR;
    }
    memset(dataset, 0, sizeof(*dataset));
    /* example 
     * curl -k --location --request GET 'https://11.16.123.210:8443/burst-buffer/datasets' --header 
     * 'token:xxxxx'
     * result:{"detail_err_msg":"","err_msg":"","err_no":0,"result":
     * {"data_sets":
     * [{"burstBufferDataSetCacheMode":"READONLY",
     * "burstBufferDataSetCacheType":"SHARE",
     * "burstBufferMetaDataSetCacheMode":"READWRITE",
     * "create_time":1760496145587,
     * "dataOpen":true,
     * "data_cache_mode":"READONLY",
     * "data_cache_type":"SHARE",
     * "fs_id":19,
     * "group_id":3,
     * "id":2,
     * "idesc":"AAEAAAAAAAATAAwIgSQBAAwBAQAAAAAAABAAAAD///8=",
     * "key":2,
     * "last_submit_task_time":0,"last_submit_task_type":
     * "BURST_BUFFER_TASK_TYPE_UNKNOWN",
     * "lock_flag":false,
     * "meta_data_cache_mode":"READWRITE",
     * "path":"bb_hpc:/test_for_dataset",
     * "path_version":1,
     * "state":"USED",
     * "use_data":true,
     * "use_meta_data":true,
     * "version":0},
     * {"burstBufferDataSetCacheMode":"READONLY","burstBufferDataSetCacheType":"SHARE","burstBufferMetaDataSetCacheMode":
     * "READWRITE","create_time":1760496119509,"dataOpen":true,"data_cache_mode":"READONLY","data_cache_type":"SHARE",
     * "fs_id":19,"group_id":2,"id":1,"idesc":"AAEAAAAAAAATAAwIgSQBAAwBAQAAAAAAABAAAAD///8=","key":1,"last_submit_task_time":0,
     * "last_submit_task_type":"BURST_BUFFER_TASK_TYPE_UNKNOWN","lock_flag":false,"meta_data_cache_mode":"READWRITE","path":
     * "bb_hpc:/test_for_dataset","path_version":1,"state":"USED","use_data":true,"use_meta_data":true,"version":0
     * }],
     * "isExact":false,
     * "limit":0,
     * "searches":[],
     * "sort":"NONE",
     * "start":0,
     * "total":2},
     * "sync":true,
     * "time_stamp":0,
     * "time_zone_offset":0,
     * "trace_id":"[3756a1f17b]"}
     */
    dataset->burstBufferDataSetCacheMode     = xstrdup(json_string_value(json_object_get(dataset_obj, "burstBufferDataSetCacheMode")));
    dataset->burstBufferDataSetCacheType     = xstrdup(json_string_value(json_object_get(dataset_obj, "burstBufferDataSetCacheType")));
    dataset->burstBufferMetaDataSetCacheMode = xstrdup(json_string_value(json_object_get(dataset_obj, "burstBufferMetaDataSetCacheMode")));
    dataset->create_time                     = json_integer_value(json_object_get(dataset_obj, "create_time"));
    dataset->dataOpen                        = json_is_true(json_object_get(dataset_obj, "dataOpen"));
    dataset->data_cache_mode                 = xstrdup(json_string_value(json_object_get(dataset_obj, "data_cache_mode")));
    dataset->data_cache_type                 = xstrdup(json_string_value(json_object_get(dataset_obj, "data_cache_type")));
    dataset->fs_id                           = json_integer_value(json_object_get(dataset_obj, "fs_id"));
    dataset->group_id                        = json_integer_value(json_object_get(dataset_obj, "group_id"));
    dataset->id                              = json_integer_value(json_object_get(dataset_obj, "id"));
    dataset->idesc                           = xstrdup(json_string_value(json_object_get(dataset_obj, "idesc")));
    dataset->key                             = json_integer_value(json_object_get(dataset_obj, "key"));
    dataset->last_submit_task_time           = json_integer_value(json_object_get(dataset_obj, "last_submit_task_time"));
    dataset->last_submit_task_type           = xstrdup(json_string_value(json_object_get(dataset_obj, "last_submit_task_type")));
    dataset->lock_flag                       = json_is_true(json_object_get(dataset_obj, "lock_flag"));
    dataset->meta_data_cache_mode            = xstrdup(json_string_value(json_object_get(dataset_obj, "meta_data_cache_mode")));
    dataset->path                            = xstrdup(json_string_value(json_object_get(dataset_obj, "path")));
    dataset->path_version                    = json_integer_value(json_object_get(dataset_obj, "path_version"));
    dataset->state                           = xstrdup(json_string_value(json_object_get(dataset_obj, "state")));
    dataset->use_data                        = json_is_true(json_object_get(dataset_obj, "use_data"));
    dataset->use_meta_data                   = json_is_true(json_object_get(dataset_obj, "use_meta_data"));
    dataset->version                         = json_integer_value(json_object_get(dataset_obj, "version"));
    return rc;
}


/**
* Parse the JSON-formatted client result and populate the client structure.
* @param client_obj Pointer to the JSON object containing client information.
* @param client Pointer to the client structure used to store the parsed data.
*/
static int parse_json_client_result(json_t *client_obj, bb_attribute_client *client)
{
    int rc = SLURM_SUCCESS;
    if (!client_obj || !client) {
        error(" get client result failed, client_obj or client is null");
        return SLURM_ERROR;
    }
    memset(client, 0, sizeof(*client));
    /* example 
     * curl  -k --location --request GET 'https://11.16.123.210:8443/burst-buffer/clients' --header 
     * 'token:xxxxx'
     * resp:{" "detail_err_msg": "",
     * "err_msg": "",
     * "err_no": 0,
     * "result": {
     * "clients": 
     * [{"avail_bytes": 480103624704,
     * "burst_buffer_cache_ips": 
     * [{"dev_name": "ib3","ip_address": "14.16.122.53","ip_family": "IPv4","net_state": "NORMAL","rate": 400000,"type": "INFINIBAND"}],
     * "burst_buffer_device_num": 1,
     * "burst_buffer_devices": 
     * [{"avail_bytes": 480103624704,"capacity": 480103981056,"dev_name": "/dev/nvme1n1","dev_type": "BURST_BUFFER_DEVICE_TYPE_BLOCK","id": 1000001,"logical_state": "BURST_BUFFER_DEVICE_STATE_OK","media": "NVME","total_bytes": 480103981056,"used_bytes": 356352,"uuid": "05529e38-5f86-4fff-ab51-32d992059569"}],
     * "burst_buffer_state": "CONFIGURED_NORMAL",
     * "cache_group_ids": [2],
     * "client_ip": "172.16.122.53",
     *  "hostname": "b11r3n03",
     * "id": 1000003,
     * "key": 1000003,
     * "reported": false,
     * "service_state": "SERV_STATE_OK",
     * "total_bytes": 480103981056,
     * "used_bytes": 356352,
     * "version": 5}],
     * "isExact": false,
     * "limit": 0,
     *  "searches": [],
     * "sort": "NONE",
     * "start": 0,
     * "total": 3},
     * "sync": true,
     * "time_stamp": 0,
     * "time_zone_offset": 0,
     * "trace_id": "[60902a29fa]"}}
     */
    xfree(client->hostname);
    xfree(client->ip);
    client->id         = json_integer_value(json_object_get(client_obj, "id"));
    client->hostname   = xstrdup(json_string_value(json_object_get(client_obj, "hostname")));
    client->ip         = xstrdup(json_string_value(json_object_get(client_obj, "client_ip")));
    client->total_size = json_integer_value(json_object_get(client_obj, "total_bytes"));
    client->used_size  = json_integer_value(json_object_get(client_obj, "used_bytes"));
    client->version    = json_integer_value(json_object_get(client_obj, "version"));
    /* don't need to parse ips, just use client_ip
    json_t *ips = json_object_get(client_obj, "burst_buffer_cache_ips");
    if (ips && json_is_array(ips)) {
        int ip_count = json_array_size(ips);
        client->ips_count = ip_count;
        for (int i = 0; i < ip_count; i++) {
            json_t *ip_item = json_array_get(ips, i);
            bb_cache_ip *ipobj = xmalloc(sizeof(bb_cache_ip));
            parse_cache_ip(ip_item, ipobj);
            list_append(client->list_ips, ipobj);
        }
    }*/

    json_t *devices = json_object_get(client_obj, "burst_buffer_devices");
    if (devices && json_is_array(devices)) {
        int device_count = json_array_size(devices);
        client->devices_count = device_count;
        for (int i = 0; i < device_count; i++) {
            json_t *dev_item = json_array_get(devices, i);
             xstrfmtcat(client->cap_dev_name, "%s,", json_string_value(json_object_get(dev_item, "dev_name")));
        }
    }
    json_t *groups_ids = json_object_get(client_obj, "cache_group_ids");
    if (groups_ids && json_is_array(groups_ids)) {
        int n = json_array_size(groups_ids);
        client->groups_count = n;
        client->groups_ids = xcalloc(n, sizeof(int));
        for (int j = 0; j < n; j++) { 
            client->groups_ids[j] = json_integer_value(json_array_get(groups_ids, j));
        }
    }   
    return rc;
}

/**
* Parse the JSON-formatted task result and populate the task structure.
* @param task_obj Pointer to the JSON object containing task information.
* @param task Pointer to the task structure used to store the parsed data.
*/
static int parse_json_tasks_result(json_t *task_obj, bb_attribute_task *task)
{
    int rc = SLURM_SUCCESS;
    if (!task_obj || !task) {
        debug("parse_json_tasks_result 参数为空\n");
        return SLURM_ERROR;
    }
    /* example
     *  curl -k --location --request GET 'https://11.16.123.210:8443/burst-buffer/tasks' --header 'token: xxxxx'
     * {"detail_err_msg":"","err_msg":"","err_no":0,
     *"result": {
     *"isExact": false,
     *"limit": 0,
     *"searches": [],
     *"sort": "NONE",
     *"start": 0,
     *"tasks": [{
        "begin_time": 1761529084351,
        "cache_group_id": 4,
        "canceled_node_num": 0,
        "completed_bytes": 658974720905,
        "completed_node_num": 1,
        "dataset_id": 3,
        "end_time": 1761529359927,
        "error_action_type": "BURST_BUFFER_TASK_ERROR_ACTION_INTERRUPT",
        "exit_code": 0,
        "failed_node_infos": [],
        "failed_node_num": 0,
        "id": 15,
        "state": "COMPLETED",
        "total_node_num": 1,
        "type": "BURST_BUFFER_TASK_TYPE_PREFETCH"
      }],"total": 15},
     *"sync": true,
     *"time_stamp": 0,
     *"time_zone_offset": 0,
     *"trace_id": "[1141e7d888c]"
     */
     
    memset(task, 0, sizeof(*task));

    task->task_id            = json_integer_value(json_object_get(task_obj, "id"));
    task->dataset_id         = json_integer_value(json_object_get(task_obj, "dataset_id"));
    task->group_id           = json_integer_value(json_object_get(task_obj, "cache_group_id"));
    task->begin_time         = json_integer_value(json_object_get(task_obj, "begin_time"));
    task->end_time           = json_integer_value(json_object_get(task_obj, "end_time"));
    task->completed_bytes    = json_integer_value(json_object_get(task_obj, "completed_bytes"));
    task->total_node_num     = json_integer_value(json_object_get(task_obj, "total_node_num"));
    task->completed_node_num = json_integer_value(json_object_get(task_obj, "completed_node_num"));
    task->canceled_node_num  = json_integer_value(json_object_get(task_obj, "canceled_node_num"));
    task->failed_node_num    = json_integer_value(json_object_get(task_obj, "failed_node_num"));
    task->exit_code          = json_integer_value(json_object_get(task_obj, "exit_code"));
    json_t *failed_node_infos = json_object_get(task_obj, "failed_node_infos");
    if (failed_node_infos && json_is_array(failed_node_infos)) {
        int n = json_array_size(failed_node_infos);
        task->failed_node_infos = xcalloc(n, sizeof(char *));
        for (int i = 0; i < n;i++)
            task->failed_node_infos[i] = xstrdup(json_string_value(json_array_get(failed_node_infos, i)));
    }
    task->task_state = xstrdup(json_string_value(json_object_get(task_obj, "state")));
    task->task_type = xstrdup(json_string_value(json_object_get(task_obj, "type")));

    return rc;
}


static int json_string_set_response(const char* json_str, bb_response* resp_out, result_type type)
{
    int rc = SLURM_SUCCESS;
    json_error_t error_t;
    json_t *root = json_loads(json_str, 0, &error_t);
    if (!root) {
        debug("JSON parse error: %s", error_t.text);
        return SLURM_ERROR;
    }
    /* parse basic info */
    resp_out->err_no           = json_integer_value(json_object_get(root, "err_no"));
    resp_out->err_msg          = xstrdup(json_string_value(json_object_get(root, "err_msg")));
    resp_out->detail_err_msg   = xstrdup(json_string_value(json_object_get(root, "detail_err_msg")));
    resp_out->sync             = json_is_true(json_object_get(root, "sync"));
    resp_out->time_stamp       = json_integer_value(json_object_get(root, "time_stamp"));
    resp_out->time_zone_offset = json_integer_value(json_object_get(root, "time_zone_offset"));
    resp_out->trace_id         = xstrdup(json_string_value(json_object_get(root, "trace_id")));

    /* If there is no result  in the response, return directly after parsing the basic information  */
    if (type == NO_RESULT) {
        json_decref(root);
        return ESPANK_SUCCESS;
    }
    /* get result*/
    json_t *result = json_object_get(root, "result");
    if (!result) {
        debug("result 为空\n");
        json_decref(root);
        return SLURM_ERROR;
    }
    switch (type) {
        case GROUP_CREATE: {
            resp_out->group_id = json_integer_value(json_object_get(json_array_get(result, 0), "id"));
            break;
        }
        case TASK_SUBMIT: {
            resp_out->task_id = json_integer_value(json_object_get(json_array_get(result, 0), "id"));
            break;
        }
        case DATASET_CREATE: {
            resp_out->dataset_id = json_integer_value(json_object_get(json_array_get(result, 0), "id"));
            break;
        }

        default:
            debug("Unknown result type: %d\n", type);
            break;
    }

    json_decref(root);
    return ESPANK_SUCCESS;
}
static int json_group_get_response(const char* json_str, query_params_request *query_params, List list, bb_response* resp_out)
{
    json_error_t error_t;
 
    json_t *root                = json_loads(json_str, 0, &error_t);
    if (!root || !list) {
        debug("JSON parse error: %s", error_t.text);
        return SLURM_ERROR;
    }
    /* parse basic info */
    resp_out->err_no           = json_integer_value(json_object_get(root, "err_no"));
    resp_out->err_msg          = xstrdup(json_string_value(json_object_get(root, "err_msg")));
    resp_out->detail_err_msg   = xstrdup(json_string_value(json_object_get(root, "detail_err_msg")));
    resp_out->sync             = json_is_true(json_object_get(root, "sync"));
    resp_out->time_stamp       = json_integer_value(json_object_get(root, "time_stamp"));
    resp_out->time_zone_offset = json_integer_value(json_object_get(root, "time_zone_offset"));
    resp_out->trace_id         = xstrdup(json_string_value(json_object_get(root, "trace_id")));
    /* get result*/
    json_t *result = json_object_get(root, "result");
    if (!result) {
        debug("result is NULL");
        json_decref(root);
        return SLURM_ERROR;
    }   

    /* analysis result to bb_attribute_group */
    json_t *groups = json_object_get(result, "cache_groups");
    resp_out->group_count = json_integer_value(json_object_get(result, "total"));
    if (groups && json_is_array(groups)) {
        int page_group_count = json_array_size(groups);
        /* Dynamic update count by pagention result*/
        for (int i = 0; i < page_group_count; i++) {
            bb_attribute_group* bb_groups = xmalloc(sizeof(bb_attribute_group));  
            json_t *group_obj = json_array_get(groups, i);
            parse_json_groups_result(group_obj, bb_groups);
            bb_attribute_group* bb_groups_tmp = NULL;
            bb_groups_tmp = list_remove_first(list, _find_group_key, &bb_groups->id);
            if(bb_groups_tmp)
                slurm_free_group(bb_groups_tmp);
            list_append(list, bb_groups);           
        }
    } else {
        json_decref(root);
        return SLURM_ERROR;
    }
    json_decref(root);
    return SLURM_SUCCESS;
}

static int json_dataset_get_response(const char* json_str, query_params_request *query_params, List list, bb_response* resp_out)
{
    int rc = SLURM_SUCCESS;
    json_error_t error_t;
    json_t *root = json_loads(json_str, 0, &error_t);
    if (!root) {
        debug("JSON parse error: %s", error_t.text);
        return SLURM_ERROR;
    }
    /* parse basic info */
    resp_out->err_no = json_integer_value(json_object_get(root, "err_no"));
    resp_out->err_msg = xstrdup(json_string_value(json_object_get(root, "err_msg")));
    resp_out->detail_err_msg = xstrdup(json_string_value(json_object_get(root, "detail_err_msg")));
    resp_out->sync = json_is_true(json_object_get(root, "sync"));
    resp_out->time_stamp = json_integer_value(json_object_get(root, "time_stamp"));
    resp_out->time_zone_offset = json_integer_value(json_object_get(root, "time_zone_offset"));
    resp_out->trace_id = xstrdup(json_string_value(json_object_get(root, "trace_id")));

    /* get result*/
    json_t *result = json_object_get(root, "result");
    if (!result) {
        debug("result is NULL");
        json_decref(root);
        return SLURM_ERROR;
    }

    /* analysis result to bb_attribute_dataset */
    json_t *data_sets = json_object_get(result, "data_sets");
    resp_out->dataset_count = json_integer_value(json_object_get(result, "total"));
    if (data_sets && json_is_array(data_sets)) {
        int page_dataset_cnt = json_array_size(data_sets);
        for (int i = 0; i < page_dataset_cnt; i++) {
            json_t *dataset_obj = json_array_get(data_sets, i);
            bb_attribute_dataset *bb_dataset = NULL;
            bb_attribute_dataset *bb_dataset_tmp = NULL;
            bb_dataset = xmalloc(sizeof(bb_attribute_dataset));
            parse_json_dataset_result(dataset_obj, bb_dataset);
            if (list)
                bb_dataset_tmp = list_remove_first(list, _find_dataset_key, &bb_dataset->id);
            if (bb_dataset_tmp)
                slurm_free_dataset(bb_dataset_tmp);
            list_append(list, bb_dataset);
        }
    } else {
        json_decref(root);
        return SLURM_ERROR;
    }
    json_decref(root);
    return rc;
}


static int json_string_get_response_client_and_task(const char* json_str, result_type type, query_params_request 
                                                                *query_params, bb_response* resp_out)
{
    int rc = SLURM_SUCCESS;
    json_error_t error_t;
    json_t *root = json_loads(json_str, 0, &error_t);
    if (!root) {
        debug("JSON parse error: %s", error_t.text);
        return SLURM_ERROR;
    }
    /* parse basic info */
    resp_out->err_no           = json_integer_value(json_object_get(root, "err_no"));
    resp_out->err_msg          = xstrdup(json_string_value(json_object_get(root, "err_msg")));
    resp_out->detail_err_msg   = xstrdup(json_string_value(json_object_get(root, "detail_err_msg")));
    resp_out->sync             = json_is_true(json_object_get(root, "sync"));
    resp_out->time_stamp       = json_integer_value(json_object_get(root, "time_stamp"));
    resp_out->time_zone_offset = json_integer_value(json_object_get(root, "time_zone_offset"));
    resp_out->trace_id         = xstrdup(json_string_value(json_object_get(root, "trace_id")));

    /* If there is no result  in the response, return directly after parsing the basic information  */
    if (type == NO_RESULT) {
        json_decref(root);
        return SLURM_SUCCESS;
    }
    /* get result*/
    json_t *result = json_object_get(root, "result");
    if (!result) {
        debug("result is NULL");
        json_decref(root);
        return SLURM_ERROR;
    }
    switch (type) {

    /* analysis result to bb_attribute_client */
    case RESULT_CLIENT: {
        json_t *clients = json_object_get(result, "clients");
        resp_out->client_count = json_integer_value(json_object_get(result, "total"));

        if (clients && json_is_array(clients)) {
            int page_client_cnt = json_array_size(clients);
            for (int i = 0; i < page_client_cnt; i++) {     
                bb_attribute_client *bb_client      = NULL;
                json_t *clients_obj                 = json_array_get(clients, i);
                bb_client                           = xmalloc(sizeof(bb_attribute_client));
                parse_json_client_result(clients_obj, bb_client);
                resp_out->last_client_id            = bb_client->id; //TODO: check the last client id
                resp_out->client_join_groups_counts = bb_client->groups_count;
                resp_out->bb_client                 = bb_client;
            }
        } else {
            json_decref(root);
            return SLURM_ERROR;
        }
        break;
    }

    // /* analysis result to bb_attribute_task */
    // case RESULT_TASK: {
    //     json_t *tasks = json_object_get(result, "tasks");
    //     resp_out->task_count = json_integer_value(json_object_get(result, "total"));
    //     if (tasks && json_is_array(tasks)) {
    //         int page_task_count = json_array_size(tasks);
    //         for (int i = 0; i < page_task_count; i++) {
    //             json_t *task_obj = json_array_get(tasks, i);
    //             bb_attribute_task *bb_tasks = xmalloc(sizeof(bb_attribute_task));
    //             parse_json_tasks_result(task_obj, bb_tasks);
    //             bb_attribute_task *bb_tasks_tmp = NULL;
    //             if (bb_state->list_tasks)
    //                 bb_tasks_tmp = list_remove_first(bb_state->list_tasks, _find_task_key, bb_tasks->task_id);
    //             if (bb_tasks_tmp) {
    //                 slurm_free_task(bb_tasks_tmp);
    //             }
    //             list_append(bb_state->list_tasks, bb_tasks);
    //         }
    //     } else {
    //         json_decref(root);
    //         return SLURM_ERROR;
    //     }
    //     break;
    // }

    default:
        debug("Unknown result type: %d\n", type);
        break;
    }

    json_decref(root);
    return rc;
}

static int task_json_string_get_response(const char* json_str, bb_response* resp_out, bb_attribute_task *bb_task)
{
    int rc = SLURM_SUCCESS;
    json_error_t error_t;
    json_t *root = json_loads(json_str, 0, &error_t);
    if (!root) {
        debug("JSON parse error: %s", error_t.text);
        return SLURM_ERROR;
    }
    /* parse basic info */
    resp_out->err_no           = json_integer_value(json_object_get(root, "err_no"));
    resp_out->err_msg          = xstrdup(json_string_value(json_object_get(root, "err_msg")));
    resp_out->detail_err_msg   = xstrdup(json_string_value(json_object_get(root, "detail_err_msg")));
    resp_out->sync             = json_is_true(json_object_get(root, "sync"));
    resp_out->time_stamp       = json_integer_value(json_object_get(root, "time_stamp"));
    resp_out->time_zone_offset = json_integer_value(json_object_get(root, "time_zone_offset"));
    resp_out->trace_id         = xstrdup(json_string_value(json_object_get(root, "trace_id")));

    /* get result*/
    json_t *result = json_object_get(root, "result");
    if (!result) {
        debug("result is NULL");
        json_decref(root);
        return SLURM_ERROR;
    }
    json_t *tasks = json_object_get(result, "tasks");
    resp_out->task_count = json_integer_value(json_object_get(result, "total"));
    if (tasks && json_is_array(tasks)) {          
            json_t *task_obj = json_array_get(tasks, 0);/* 只应该解析出一个 */
            parse_json_tasks_result(task_obj, bb_task);
    } else {
        json_decref(root);
        return SLURM_ERROR;
    }
    json_decref(root);
    return SLURM_SUCCESS;
}

// static int json_string_get_response(const char* json_str, bb_response* resp_out, result_type type, query_params_request *query_params)
// {
//     int rc = SLURM_SUCCESS;
//     json_error_t error_t;
//     json_t *root = json_loads(json_str, 0, &error_t);
//     if (!root) {
//         debug("JSON parse error: %s", error_t.text);
//         return SLURM_ERROR;
//     }
//     /* parse basic info */
//     resp_out->err_no           = json_integer_value(json_object_get(root, "err_no"));
//     resp_out->err_msg          = xstrdup(json_string_value(json_object_get(root, "err_msg")));
//     resp_out->detail_err_msg   = xstrdup(json_string_value(json_object_get(root, "detail_err_msg")));
//     resp_out->sync             = json_is_true(json_object_get(root, "sync"));
//     resp_out->time_stamp       = json_integer_value(json_object_get(root, "time_stamp"));
//     resp_out->time_zone_offset = json_integer_value(json_object_get(root, "time_zone_offset"));
//     resp_out->trace_id         = xstrdup(json_string_value(json_object_get(root, "trace_id")));

//     /* If there is no result  in the response, return directly after parsing the basic information  */
//     if (type == NO_RESULT) {
//         json_decref(root);
//         return SLURM_SUCCESS;
//     }
//     /* get result*/
//     json_t *result = json_object_get(root, "result");
//     if (!result) {
//         debug("result is NULL");
//         json_decref(root);
//         return SLURM_ERROR;
//     }
//     switch (type) {


//     /* analysis result to bb_attribute_task */
//     case RESULT_TASK: {
//         json_t *tasks = json_object_get(result, "tasks");
//         resp_out->task_count = json_integer_value(json_object_get(result, "total"));
//         if (tasks && json_is_array(tasks)) {
//             int page_task_count = json_array_size(tasks);
//             for (int i = 0; i < page_task_count; i++) {
//                 json_t *task_obj = json_array_get(tasks, i);
//                 bb_attribute_task *bb_tasks = xmalloc(sizeof(bb_attribute_task));
//                 parse_json_tasks_result(task_obj, bb_tasks);
//                 list_append(resp_out->list_tasks, bb_tasks);
//             }
//         } else {
//             json_decref(root);
//             return SLURM_ERROR;
//         }
//         break;
//     }

//     default:
//         debug("Unknown result type: %d\n", type);
//         break;
//     }

//     json_decref(root);
//     return SLURM_SUCCESS;
// }

static char *concatenate_group_strings(bb_minimal_config_t *bb_config, void *params, call_type type)
{
    char *url_api = NULL;
    char *tmp_params_str = NULL;
    char *json_string = NULL;
    char *body = NULL;
    if(!bb_config || !params)
        return NULL;
    switch (type) {
    case QUERY_CALL: {
        query_params_request *query_params = (query_params_request *)params;
        /* check whether the start and limit
         * in the query parameters are valid (non-negative)
         */
        debug("we have url timeout(%d) but not used ", bb_config->other_timeout);
        if (query_params->start >= 0 || query_params->limit >= 0) {
            if (query_params->limit > 0)
                xstrfmtcat(tmp_params_str, "start=%d&limit=%d", query_params->start, query_params->limit);
            else
                xstrfmtcat(tmp_params_str, "start=%d", query_params->start);
        }
        /* host_name_match_mode */
        if (query_params->ids != NULL) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "ids=%s", query_params->ids);
            else
                xstrfmtcat(tmp_params_str, "&ids=%s", query_params->ids);
        }
        /*assemble the full query for cache groups*/
        xstrfmtcat(url_api, "https://%s:%d/burst-buffer/cache-groups?%s", 
            bb_config->para_stor_addr, bb_config->para_stor_port, tmp_params_str);
        if (call_rest_api_with_token(url_api, "GET", NULL, bb_config->token, &json_string) != 0) {
            error("API call failed");
            xfree(json_string);
            xfree(tmp_params_str);
            xfree(url_api);
            return NULL;
        }
        xfree(tmp_params_str);
        xfree(url_api);
        return json_string;
        break;
    }
    case CREATE_CALL:{
        create_params_request *create_params = (create_params_request *)params;
        if (create_params->client_count <= 0 || create_params->client_ids == NULL){
            debug("Invalid client_count and client_ids \n");
            return NULL;
        } 
        /* START---assembl body*/
        xstrfmtcat(body, "{");
        /* client_ids */
        xstrfmtcat(body, "\"client_ids\":[");
        for (int i = 0; i < create_params->client_count; i++) 
            xstrfmtcat(body, "%d,", create_params->client_ids[i]);
        xstrfmtcat(body, "]");
        /* del_delay_time */
        if (create_params->del_delay_time > 0)
            xstrfmtcat(body, ",\"del_delay_time\":%d", create_params->del_delay_time);
        /* fault_delay_time */
        if (create_params->fault_delay_time > 0)
            xstrfmtcat(body, ",\"fault_delay_time\":%d", create_params->fault_delay_time);
        xstrfmtcat(body, "}");
        //debug("the request body of create groups:%s", body);
        /* END---assembl body*/      

        xstrfmtcat(url_api, "https://%s:%d/burst-buffer/cache-groups",
            bb_config->para_stor_addr, bb_config->para_stor_port);
        debug("we have url timeout(%d) but not used ", bb_config->other_timeout);
        if (call_rest_api_with_token(url_api, "POST", body, bb_config->token, &json_string) != 0) {
            error("API call failed");
            xfree(json_string);
            xfree(body);
            xfree(url_api);
            return NULL;
        }
        xfree(body);
        xfree(url_api);
        return json_string;
        break;
    }

    case DELETE_CALL:{
        delete_params_request *delete_params = (delete_params_request *)params;

        if (delete_params->group_id <= 0 ){
            debug("invaild group_id to delete group \n");
            return NULL;
        }
        xstrfmtcat(tmp_params_str, "id=%d", delete_params->group_id);  
        xstrfmtcat(url_api, "https://%s:%d/burst-buffer/cache-groups?%s", 
            bb_config->para_stor_addr, bb_config->para_stor_port, tmp_params_str);
        debug("the request url of delete groups:%s", url_api);    
        debug("we have url timeout(%d) but not used ", bb_config->other_timeout);
        if (call_rest_api_with_token(url_api, "DELETE", NULL, bb_config->token, &json_string) != 0) {
            error("API call failed");
            xfree(json_string);
            xfree(body);
            xfree(url_api);
            return NULL;
        }
        xfree(body);
        xfree(url_api);
        return json_string;
        break;
    }

    default:
        debug("Unsupported call_type in concatenate_group_strings\n");
        return NULL;
        break;
    } //switch end

}

static char *concatenate_dataset_strings(bb_minimal_config_t *bb_config, void *params, call_type type)
{
    char *url_api = NULL;
    char *tmp_params_str = NULL;
    char *json_string = NULL;
    char *body = NULL;
    if(!bb_config || !params)
        return NULL;
    switch (type) {
    case QUERY_CALL: {
        query_params_request *query_params = (query_params_request *)params;
        /*check whether the start and limit
         *in the query parameters are valid (non-negative)
         */
         /* start & limit*/
        if (query_params->start >= 0 || query_params->limit >= 0) {
            if (query_params->limit > 0)
                xstrfmtcat(tmp_params_str, "start=%d&limit=%d", query_params->start, query_params->limit);
            else
                xstrfmtcat(tmp_params_str, "start=%d", query_params->start);
        }
        /* path */
        if (query_params->path) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "path=%s", query_params->path);
            else
                xstrfmtcat(tmp_params_str, "&path=%s", query_params->path);
        }
        /* group_id */
        if (query_params->group_id > 0) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "group_id=%d", query_params->group_id);
            else
                xstrfmtcat(tmp_params_str, "&group_id=%d", query_params->group_id);
        }
        /*assemble the full query for cache datasets*/
        xstrfmtcat(url_api, "https://%s:%d/burst-buffer/datasets?%s",
            bb_config->para_stor_addr, bb_config->para_stor_port, tmp_params_str);
        if (call_rest_api_with_token(url_api, "GET", NULL, bb_config->token, &json_string) != 0) {
            error("API call failed");
            xfree(json_string);
            xfree(tmp_params_str);
            xfree(url_api);
            return NULL;
        }
        xfree(tmp_params_str);
        xfree(url_api);
        return json_string;

    }

    case CREATE_CALL: {
        create_params_request *create_params = (create_params_request *)params;
        if (create_params->path == NULL || create_params->group_id <= 0 || (create_params->is_use_metadata != true && create_params->is_use_metadata != false)) {
            debug("Invalid path,group_id or is_user_metadata \n");
            return NULL;
        }

        /* START---assembl body*/
        xstrfmtcat(body, "{");
        xstrfmtcat(body, "\"path\":\"%s\"", create_params->path);
        xstrfmtcat(body, ",\"group_id\":%d", create_params->group_id);
        if (create_params->is_use_metadata == true && create_params->data_cache_type == LOCAL_CACHE) {
            xstrfmtcat(body, ",\"ordinary_mode\":\"%s\"", "METADATA_RW_DATA_R");
        }
        if (create_params->is_use_metadata == true && create_params->data_cache_type == SHARE_CACHE) {
            xstrfmtcat(body, ",\"ordinary_mode\":\"%s\"", "METADATA_RW_DATA_SHARE_R");
        }
        if (create_params->is_use_metadata == false && create_params->data_cache_type == LOCAL_CACHE) {
            xstrfmtcat(body, ",\"use_data\":%s,\"data_cache_type\":\"%s\"","true","LOCAL");
        }
        if (create_params->is_use_metadata == false && create_params->data_cache_type == SHARE_CACHE) {
            xstrfmtcat(body, ",\"use_data\":%s,\"data_cache_type\":\"%s\"","true","SHARE");
        }
        xstrfmtcat(body, "}");
        //debug("the request body of create dataset:%s", body);
        /* END---assembl body*/

        xstrfmtcat(url_api, "https://%s:%d/burst-buffer/datasets",
            bb_config->para_stor_addr, bb_config->para_stor_port);
        debug("we have url timeout(%d) but not used ", bb_config->other_timeout);
        if (call_rest_api_with_token(url_api, "POST", body, bb_config->token, &json_string) != 0) {
            error("API call failed");
            xfree(json_string);
            xfree(body);
            xfree(url_api);
            return NULL;
        }
        xfree(body);
        xfree(url_api);
        return json_string;
        break;
    }

    case DELETE_CALL: {
        delete_params_request *delete_params = (delete_params_request *)params;

        if (delete_params->dataset_id <= 0) {
            debug("invaild dataset_id to delete dataset \n");
            return NULL;
        }
        xstrfmtcat(url_api, "https://%s:%d/burst-buffer/datasets/%d",
            bb_config->para_stor_addr, bb_config->para_stor_port, delete_params->dataset_id);
        debug("the request url of delete dataset:%s", url_api);
        debug("we have url timeout(%d) but not used ", bb_config->other_timeout);
        if (call_rest_api_with_token(url_api, "DELETE", NULL, bb_config->token, &json_string) != 0) {
            error("API call failed");
            xfree(json_string);
            xfree(body);
            xfree(url_api);
            return NULL;
        }
        xfree(body);
        xfree(url_api);
        return json_string;
        break;
    }

    default:
        debug("Unsupported call_type in concatenate_group_strings\n");
        return NULL;
        break;
    } //switch end        
}

static char *concatenate_client_strings(bb_minimal_config_t *bb_config, const query_params_request* query_params) 
{

    if (query_params == NULL && bb_config == NULL) {
        debug("Invalid parameters to concatenate_dataset_strings\n");
        return NULL;
    }
    char* url_api        = NULL;
    char* tmp_params_str = NULL;
    char* json_string    = NULL;
    /*check whether the start and limit 
     *in the query parameters are valid (non-negative)
     */
     /* start & limit*/
    if (query_params->start >= 0 || query_params->limit >= 0) {
        if (query_params->limit > 0)
            xstrfmtcat(tmp_params_str, "start=%d&limit=%d", query_params->start, query_params->limit);
        else
            xstrfmtcat(tmp_params_str, "start=%d", query_params->start);
    }
    /* ids */
    if (query_params->ids) {
        if (!tmp_params_str)
            xstrfmtcat(tmp_params_str, "client_ids=%s", query_params->ids);
        else
            xstrfmtcat(tmp_params_str, "&client_ids=%s", query_params->ids);
    }
    /* client_ip ,now only can pass one client ip*/
    if (query_params->client_ips && query_params->client_ips_count == 1) {
        if (!tmp_params_str)
            xstrfmtcat(tmp_params_str, "client_ip=%s", query_params->client_ips);
        else
            xstrfmtcat(tmp_params_str, "&client_ip=%s", query_params->client_ips);
    }
    /* client_ip_match_mode . Pass it  without client_ip  doesn't work */
    if (query_params->client_ip_match_mode == 0 || query_params->client_ip_match_mode == 1) {
        if (!tmp_params_str)
            xstrfmtcat(tmp_params_str, "client_ip_match_mode=%d", query_params->client_ip_match_mode);
        else
            xstrfmtcat(tmp_params_str, "&client_ip_match_mode=%d", query_params->client_ip_match_mode);
    }
    /* host_name ,now only can pass one hostname */
    if (query_params->host_name ) {
        if (!tmp_params_str)
            xstrfmtcat(tmp_params_str, "host_name=%s", query_params->host_name);
        else
            xstrfmtcat(tmp_params_str, "&host_name=%s", query_params->host_name);
    }
    /* host_name_match_mode . Pass it  without host_name doesn't work */
    if (query_params->host_name_match_mode == 0 || query_params->host_name_match_mode == 1) {
        if (!tmp_params_str)
            xstrfmtcat(tmp_params_str, "host_name_match_mode=%d", query_params->host_name_match_mode);
        else
            xstrfmtcat(tmp_params_str, "&host_name_match_mode=%d", query_params->host_name_match_mode);
    }

    //debug("query param of get clients:%s ", tmp_params_str);
    /* assemble the full query for client */
    xstrfmtcat(url_api,"https://%s:%d/burst-buffer/clients?%s",
                             bb_config->para_stor_addr, bb_config->para_stor_port, tmp_params_str);
    if (call_rest_api_with_token(url_api, "GET", NULL,  bb_config->token, &json_string) != 0) {
        error("API call failed");
        xfree(json_string);
        xfree(tmp_params_str);
        xfree(url_api);
        return NULL;
    }
    xfree(tmp_params_str);
    xfree(url_api);
    return json_string;
}

static char *concatenate_task_strings(bb_minimal_config_t *bb_config, void *params, call_type type)
{
    char *url_api = NULL;
    char *tmp_params_str = NULL;
    char *json_string = NULL;
    char *body = NULL;
    if(!bb_config || !params)
        return NULL;
    switch (type) {
    case QUERY_CALL: {
        query_params_request *query_params = (query_params_request *)params;
        /*check whether the start and limit
         *in the query parameters are valid (non-negative)
         */
         /* start & limit*/
        if (query_params->start >= 0 || query_params->limit >= 0) {
            if (query_params->limit > 0)
                xstrfmtcat(tmp_params_str, "start=%d&limit=%d", query_params->start, query_params->limit);
            else
                xstrfmtcat(tmp_params_str, "start=%d", query_params->start);
        }
        /* other params */
        if (query_params->group_id > 0) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "group_id=%d", query_params->group_id);
            else
                xstrfmtcat(tmp_params_str, "&group_id=%d", query_params->group_id);
        }
        if (query_params->task_id > 0) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "task_id=%d", query_params->task_id);
            else
                xstrfmtcat(tmp_params_str, "&task_id=%d", query_params->task_id);
        }
        if (query_params->task_type == BURST_BUFFER_TASK_TYPE_PREFETCH) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "type=%s", "BURST_BUFFER_TASK_TYPE_PREFETCH");
            else
                xstrfmtcat(tmp_params_str, "&type=%s", "BURST_BUFFER_TASK_TYPE_PREFETCH");
        }
        if (query_params->task_type == BURST_BUFFER_TASK_TYPE_RECYCLE) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "type=%s", "BURST_BUFFER_TASK_TYPE_RECYCLE");
            else
                xstrfmtcat(tmp_params_str, "&type=%s", "BURST_BUFFER_TASK_TYPE_RECYCLE");
        }
        if (query_params->task_state == BB_TASK_STATE_SUBMITTING) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "states=%s", "SUBMITTING");
            else
                xstrfmtcat(tmp_params_str, "&states=%s", "SUBMITTING");
        }
        if (query_params->task_state == BB_TASK_STATE_RUNNING) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "states=%s", "RUNNING");
            else
                xstrfmtcat(tmp_params_str, "&states=%s", "RUNNING");
        }
        if (query_params->task_state == BB_TASK_STATE_COMPLETED) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "states=%s", "COMPLETED");
            else
                xstrfmtcat(tmp_params_str, "&states=%s", "COMPLETED");
        }
        if (query_params->task_state == BB_TASK_STATE_FAILED) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "states=%s", "FAILED");
            else
                xstrfmtcat(tmp_params_str, "&states=%s", "FAILED");
        }
        if (query_params->task_state == BB_TASK_STATE_CANCELED) {
            if (!tmp_params_str)
                xstrfmtcat(tmp_params_str, "states=%s", "CANCELED");
            else
                xstrfmtcat(tmp_params_str, "&states=%s", "CANCELED");
        }
        /*assemble the full query for tasks*/
        xstrfmtcat(url_api, "https://%s:%d/burst-buffer/tasks?%s",
            bb_config->para_stor_addr, bb_config->para_stor_port, tmp_params_str);
        debug("the query task url is :%s", url_api);
        if (call_rest_api_with_token(url_api, "GET", NULL, bb_config->token, &json_string) != 0) {
            error("API call failed");
            xfree(json_string);
            xfree(tmp_params_str);
            xfree(url_api);
            return NULL;
        }
        xfree(tmp_params_str);
        xfree(url_api);
        return json_string;
    }
    case CREATE_CALL: {
        create_params_request *create_params = (create_params_request *)params;
        if (create_params->dataset_id <= 0 || (create_params->error_action_type != 0 && create_params->error_action_type != 1)) {
            debug("Invalid dataset_id or error_action_typ \n");
            return NULL;
        }
        /* START---assembl body*/
        xstrfmtcat(body, "{");
        /* dataset_id */
        xstrfmtcat(body, "\"dataset_id\":%d", create_params->dataset_id);
        /* type */
        if (create_params->task_type == BURST_BUFFER_TASK_TYPE_PREFETCH)
            xstrfmtcat(body, ",\"type\":\"%s\"", "BURST_BUFFER_TASK_TYPE_PREFETCH");
        if (create_params->task_type == BURST_BUFFER_TASK_TYPE_RECYCLE)
            xstrfmtcat(body, ",\"type\":\"%s\"", "BURST_BUFFER_TASK_TYPE_RECYCLE");
        /* error_action_type */
        xstrfmtcat(body, ",\"error_action_type\":%d", create_params->error_action_type);
        xstrfmtcat(body, "}");
        //debug("the request body of create tasks:%s", body);
        /* END---assembl body*/

        xstrfmtcat(url_api, "https://%s:%d/burst-buffer/tasks",
            bb_config->para_stor_addr, bb_config->para_stor_port);
        debug("we have url timeout(%d) but not used ", bb_config->other_timeout);
        if (call_rest_api_with_token(url_api, "POST", body, bb_config->token, &json_string) != 0) {
            error("API call failed");
            xfree(json_string);
            xfree(body);
            xfree(url_api);
            return NULL;
        }
        xfree(body);
        xfree(url_api);
        return json_string;
        break;
    }

    case CANCEL_CALL: {
        delete_params_request *delete_params = (delete_params_request *)params;
        if (delete_params->task_id <= 0) {
            debug("invaild task_id to cancel task \n");
            return NULL;
        }
        xstrfmtcat(url_api, "https://%s:%d/burst-buffer/tasks/%d:cancel",
            bb_config->para_stor_addr, bb_config->para_stor_port, delete_params->task_id);
        debug("the request url of cancel task:%s", url_api);
        debug("we have url timeout(%d) but not used ", bb_config->other_timeout);
        if (call_rest_api_with_token(url_api, "PUT", NULL, bb_config->token, &json_string) != 0) {
            error("API call failed");
            xfree(json_string);
            xfree(body);
            xfree(url_api);
            return NULL;
        }
        xfree(body);
        xfree(url_api);
        return json_string;
        break;
    }

    default:
        debug("Unsupported call_type in concatenate_group_strings\n");
        return NULL;
        break;
    } //switch end        
}

extern void slurm_free_group(void *object)
{
    bb_attribute_group *result = (bb_attribute_group *)object;
	if (result) {
		xfree(result->client_ids);
		xfree(result);
	}
}

extern void slurm_free_dataset(void *object)
{
    bb_attribute_dataset *result = (bb_attribute_dataset *)object;
	if (result) {
		xfree(result->burstBufferDataSetCacheMode);
		xfree(result->burstBufferDataSetCacheType);
		xfree(result->burstBufferMetaDataSetCacheMode);
		xfree(result->data_cache_mode);
		xfree(result->data_cache_type);
		xfree(result->idesc);
		xfree(result->last_submit_task_type);
		xfree(result->meta_data_cache_mode);
		xfree(result->path);
		xfree(result->state);
		xfree(result);
	}
}
extern void slurm_free_client(void *object)
{
    bb_attribute_client *result = (bb_attribute_client *)object;
	if (result) {
		xfree(result->hostname);
		xfree(result->ip);
		xfree(result->groups_ids);
        xfree(result->cap_dev_name);
		xfree(result);
	}
}

extern void slurm_free_task(void *object)
{
    bb_attribute_task *result = (bb_attribute_task *)object;
    if (result) {
        xfree(result->error_action_type);
        xfree(result->task_type);
        xfree(result->task_state);
        for (int i = 0; i < result->failed_node_num; i++)
            xfree(result->failed_node_infos[i]);
        xfree(result->failed_node_infos);
        xfree(result);
    }
}

extern int _find_group_key(void *x, void *key)
{
    bb_attribute_group * group_key = (bb_attribute_group *) x;
    int id = *(int *)key;
    if (group_key->id == id)
        return 1;
    return 0;
}

extern int _find_dataset_key(void *x, void *key)
{
    bb_attribute_dataset * dataset_key = (bb_attribute_dataset *) x;
    int id = *(int *)key;
    if (dataset_key->id == id)
        return 1;
    return 0;
}


extern int _find_client_key(void *x, void *key)
{
    bb_attribute_client * client_key = (bb_attribute_client *) x;
    if (xstrcmp(client_key->hostname, (char *) key))
        return 1;
    return 0;
}

extern int _find_task_key(void *x, void *key)
{
    bb_attribute_task * task_key = (bb_attribute_task *) x;
    int id = *(int *)key;
    if (task_key->task_id == id)
        return 1;
    return 0;
}



extern void bb_response_free(bb_response *resp)
{
    if (!resp)
        return;
    xfree(resp->trace_id);
    xfree(resp->err_msg);
    xfree(resp->detail_err_msg);
    xfree(resp);
}


/* Get set the number of groups */
extern List get_groups_burst_buffer(query_params_request* query_params,  bb_minimal_config_t *bb_min_config, bb_response *resp_out)
{
    if (query_params == NULL || resp_out == NULL ) {
        debug("Invalid parameters to get_groups_burst_buffer");
        return NULL;
    }
  
    int  ret                = NULL;
    char *json_string       = NULL;
    List init_list_groups   = NULL;
    init_list_groups   =  list_create(slurm_free_group);
   
    /* init page status */
    // resp_out->dataset_count = 0;
    int count_flag = 0;
    int tmp_count = 0;
    /* Paging query */
    do {
        json_string = NULL;
        if(query_params->limit == 0)
                query_params->limit = 100;
        count_flag +=  query_params->limit;

        tmp_count = resp_out->group_count;
        json_string = concatenate_group_strings(bb_min_config, query_params, QUERY_CALL);

        if (json_string == NULL || ret == SLURM_ERROR) {
            debug("failed to concatenate strings");
            return NULL;
        }
        /* retrieve query results */
        ret = json_group_get_response(json_string, query_params, init_list_groups, resp_out);
        if (ret == SLURM_ERROR || resp_out->err_no != 0) {
            //debug("failed to obtain cache group or dataset in one page");
            xfree(json_string);
            return NULL;
        }
        //debug("successfully obtain cache group or dataset in one page");
        xfree(json_string);
        /* 更新start参数 */
        query_params->start += query_params->limit;


    } while (count_flag < tmp_count);

    return init_list_groups;
}

extern List get_datasets_burst_buffer(query_params_request *query_params, bb_minimal_config_t *bb_min_config, bb_response *resp_out)
{
    if (bb_min_config == NULL || query_params == NULL || resp_out == NULL) {
        debug("Invalid parameters to get_datasets_burst_buffer");
        return NULL;
    }
    char *json_string = NULL;
    List init_list_datasets = list_create(slurm_free_dataset);
    /* init page status */
    int count_flag = 0;
    int tmp_count = 0;
    /* Paging query */
    do {
        json_string = NULL;
        if (query_params->limit == 0)
            query_params->limit = 100;
        count_flag += query_params->limit;
        tmp_count = resp_out->dataset_count;
        json_string = concatenate_dataset_strings(bb_min_config, query_params, QUERY_CALL);
        if (json_string == NULL) {
            debug("failed to concatenate strings");
            return NULL;
        }
        /* retrieve query results */
        int ret = json_dataset_get_response(json_string,  query_params, init_list_datasets, resp_out);
        if (ret == SLURM_ERROR || resp_out->err_no != 0) {
            //debug("failed to obtain cache group or dataset in one page");
            xfree(json_string);
            return NULL;
        }
        //debug("successfully obtain cache group or dataset in one page");
        xfree(json_string);
        /* 更新start参数 */
        query_params->start += query_params->limit;
    } while (count_flag < tmp_count);
    return init_list_datasets;
}


/* get clients info   */
extern int get_set_burst_buffer_clients_and_tasks( query_params_request *query_params,
                                            bb_minimal_config_t *bb_min_config, result_type type, bb_response *resp_out)
{

    if (bb_min_config == NULL  || query_params == NULL) {
        debug("Invalid parameters to get_set_burst_buffer_clients_and_tasks\n");
        return SLURM_ERROR;
    }
    int ret           = 0;
    char *json_string = NULL;
    int count_flag    = 0;
    int tmp_count     = 0;
    /* Paging query */
    do {
        json_string = NULL;
        if (query_params->limit == 0)
            query_params->limit = 100;
        count_flag += query_params->limit;
        /* 更新start参数 */
        if(type == RESULT_CLIENT) {
            tmp_count = resp_out->client_count;
            json_string = concatenate_client_strings(bb_min_config, query_params);
        } else if(type == RESULT_TASK) {
            tmp_count = resp_out->task_count;
            json_string = concatenate_task_strings(bb_min_config, query_params, query_params->task_type);
        }
        
        if (json_string == NULL) {
            debug("Failed to concatenate strings in get_set_burst_buffer_clients_and_tasks");
            return SLURM_ERROR;
        }
        /* 获取查询结果 */
        ret = json_string_get_response_client_and_task(json_string, type, query_params, resp_out);
        if (ret == SLURM_ERROR || resp_out->err_no != 0) {
            //debug("failed to obtain clients or tasks in one page");
            xfree(json_string);
            return SLURM_ERROR;
        }


        //debug("successfully obtain clients in one page");
        xfree(json_string);

        query_params->start += query_params->limit;
    } while (count_flag < tmp_count);

    return ret;
}


/* 获取单个task，输出为task */
extern int get_single_burst_buffer_tasks( int task_id, bb_attribute_task *bb_task,
                                            bb_minimal_config_t *bb_config, bb_response *resp_out)
{

    if (bb_config == NULL || resp_out == NULL || bb_task == NULL) {
        debug("Invalid parameters to get_single_burst_buffer_tasks\n");
        return SLURM_ERROR;
    }
    int ret = 0;
    char *json_string = NULL;

    query_params_request query_params = (query_params_request){ 0 };
    query_params.start = 0;
    query_params.limit = 1;
    query_params.task_type = BURST_BUFFER_TASK_TYPE_NULL;
    query_params.task_state = BB_TASK_STATE_NULL;
    query_params.task_id = task_id;
    json_string = concatenate_task_strings(bb_config, &query_params, QUERY_CALL);

    if (json_string == NULL) {
        debug("Failed to concatenate strings in get_single_burst_buffer_tasks");
        return SLURM_ERROR;
    }
    /* 获取查询结果 */
    ret = task_json_string_get_response(json_string, resp_out, bb_task);
    if (ret == SLURM_ERROR || resp_out->err_no != 0) {
        debug("failed to obtain single task");
        xfree(json_string);
        return SLURM_ERROR;
    }
    /* 检查查询到的task数据ID是否正确 */
    if (bb_task->task_id != task_id) {
        error("get task id error, the task_id  is %d, but return task_id is %d",task_id, bb_task->task_id);
        xfree(json_string);
        return SLURM_ERROR;
    }

    debug("successfully obtain task");
    xfree(json_string);

    return ret;
}

// extern int get_burst_buffer_tasks(query_params_request *query_params, bb_state_t *bb_state, bb_response *resp_out)
// {
//     if (bb_state == NULL || resp_out == NULL || query_params == NULL) {
//         debug("Invalid parameters to get_burst_buffer_tasks\n");
//         return SLURM_ERROR;
//     }

//     int ret = 0;
//     char *json_string = NULL;

//     /* init page status */
//    resp_out->dataset_count = 0;
//     int count_flag = 0;
//     /* Paging query */
//     do {
//         json_string = NULL;
//         if (query_params->limit == 0)
//             query_params->limit = 100;
//         count_flag += query_params->limit;
//         json_string = concatenate_task_strings(&bb_state->bb_config, query_params, QUERY_CALL);
//         if (json_string == NULL) {
//             debug("Failed to concatenate strings in get_burst_buffer_tasks");
//             return SLURM_ERROR;
//         }
//         /* 获取查询结果 */
//         ret = json_string_get_response(json_string,resp_out, RESULT_TASK, query_params);
//         if (ret == SLURM_ERROR || resp_out->err_no != 0) {
//             debug("failed to obtain clients in one page");
//             xfree(json_string);
//             return SLURM_ERROR;
//         }
//         debug("successfully obtain clients in one page");
//         xfree(json_string);
//         /* 更新start参数 */
//         query_params->start += query_params->limit;
//     } while (count_flag < resp_out->task_count);

//     return ret;
// }


/* Create a cache group by client ids */
extern int create_burst_buffer_group(create_params_request *create_params, bb_minimal_config_t *bb_config, bb_response *resp_out)
{
    if( bb_config == NULL || resp_out == NULL || create_params == NULL ){
        debug("Invalid parameters to create_burst_buffer_group\n");
        return -1;
    }
    int ret = 0;
    char* json_string = NULL;
    if (!bb_config->token) {
        debug("failed to get token");
        return SLURM_ERROR;
    }
    bb_attribute_group *bb_group_tmp = NULL;
    json_string = concatenate_group_strings(bb_config, create_params, CREATE_CALL);
    if (json_string == NULL) {
        debug("failed to concatenate strings in create_burst_buffer_group ");
        return SLURM_ERROR;
    }
    /* retrieve create results */
    ret = json_string_set_response(json_string, resp_out, GROUP_CREATE);
    if(ret == SLURM_ERROR || resp_out->err_no != 0) {
        debug("failed to create cache group");
        xfree(json_string);
        return SLURM_ERROR;
    } 
    resp_out->bb_group     = (bb_attribute_group *)xmalloc(sizeof(bb_attribute_group));
    resp_out->bb_group->id  =  resp_out->group_id;
    xfree(json_string);
    return ret;
}

/* 
* delete a cache group by client ids 
* NOTE: before deleting the cache group, make sure to delete the datasets under this group first.
*/
extern int delete_burst_buffer_group(delete_params_request *delete_params, bb_minimal_config_t *bb_config, bb_response *resp_out)
{
    bb_attribute_group *bb_group_tmp = NULL;
    if( bb_config == NULL || resp_out == NULL || delete_params == NULL ){
        debug("invalid parametes to delete group");
        return SLURM_ERROR;
    }
    int ret =0;
    char* json_string = NULL;
    json_string = concatenate_group_strings(bb_config, delete_params, DELETE_CALL);
    if (json_string == NULL) {
        debug("failed to concatenate strings in delete_burst_buffer_group ");
        return SLURM_ERROR;
    }
    /* retrieve create results */
    ret = json_string_set_response(json_string, resp_out, NO_RESULT);
    if(ret == SLURM_ERROR || resp_out->err_no != 0) {
        debug("failed to delete cache group:%s",resp_out->detail_err_msg);
        xfree(json_string);
        return SLURM_ERROR;
    }
    xfree(json_string);
    return ret;
}

extern int create_burst_buffer_dataset(create_params_request *create_params, bb_minimal_config_t *bb_config, bb_response *resp_out)
{
    if (bb_config == NULL || resp_out == NULL || create_params == NULL) {
        debug("Invalid parameters to create_burst_buffer_group\n");
        return -1;
    }
    int ret = 0;
    char *json_string = NULL;
    json_string = concatenate_dataset_strings(bb_config, create_params, CREATE_CALL);
    if (json_string == NULL) {
        debug("failed to concatenate strings in create_burst_buffer_dataset ");
        return SLURM_ERROR;
    }
    /* retrieve create results */
    ret = json_string_set_response(json_string, resp_out, DATASET_CREATE);
    if (ret == SLURM_ERROR || resp_out->err_no != 0) {
        debug("failed to create dataset");
        xfree(json_string);
        return SLURM_ERROR;
    }
    resp_out->bb_dataset = (bb_attribute_dataset *)xmalloc(sizeof(bb_attribute_dataset));
    resp_out->bb_dataset->id = resp_out->dataset_id;
    xfree(json_string);
    return ret;
}

extern int delete_burst_buffer_dataset(delete_params_request *delete_params, bb_minimal_config_t *bb_config, bb_response *resp_out)
{
    if (bb_config == NULL || resp_out == NULL || delete_params == NULL) {
        debug("invalid parametes to delete group");
        return SLURM_ERROR;
    }
    int ret = 0;
    char *json_string = NULL;
    json_string = concatenate_dataset_strings(bb_config, delete_params, DELETE_CALL);
    if (json_string == NULL) {
        debug("failed to concatenate strings in delete_burst_buffer_dataset ");
        return SLURM_ERROR;
    }
    /* retrieve create results */
    ret = json_string_set_response(json_string, resp_out, NO_RESULT);
    if (ret == SLURM_ERROR || resp_out->err_no != 0) {
        debug("failed to delete dataset:%s", resp_out->detail_err_msg);
        xfree(json_string);
        return SLURM_ERROR;
    }
    debug("successfully delete dataset");
    xfree(json_string);
    return ret;
}
/* 预热数据集 */
extern int submit_burst_buffer_task(create_params_request *create_params, bb_config_t *bb_config, bb_response *resp_out){
    if (bb_config == NULL || resp_out == NULL || create_params == NULL) {
        debug("Invalid parameters to submit_burst_buffer_task\n");
        return -1;
    }
    int ret = 0;
    char *json_string = NULL;

    json_string = concatenate_task_strings(bb_config, create_params, CREATE_CALL);
    if (json_string == NULL) {
        debug("failed to concatenate strings in create_burst_buffer_dataset ");
        return SLURM_ERROR;
    }
    /* retrieve create results */
    ret = json_string_set_response(json_string, resp_out, TASK_SUBMIT);
    if (ret == SLURM_ERROR || ret !=0 || resp_out->err_no != 0) {
        debug("failed to submit task");
        xfree(json_string);
        return SLURM_ERROR;
    }
    resp_out->bb_task = (bb_attribute_task *)xmalloc(sizeof(bb_attribute_task));
    resp_out->bb_task->task_id = resp_out->task_id;
    xfree(json_string);
    return ret;
}


/* Cancel a task by task_id */
extern int cancel_burst_buffer_task(bb_config_t *bb_config, delete_params_request *delete_params, bb_response *resp_out)
{
    if (bb_config == NULL || resp_out == NULL || delete_params == NULL) {
        debug("invalid parametes to delete task");
        return SLURM_ERROR;
    }
    int ret = 0;
    char *json_string = NULL;
    json_string = concatenate_task_strings(bb_config, delete_params, CANCEL_CALL);
    if (json_string == NULL) {
        debug("failed to concatenate strings in delete_burst_buffer_task ");
        return SLURM_ERROR;
    }
    /* retrieve create results */
    ret = json_string_set_response(json_string, resp_out, NO_RESULT);
    if (ret == SLURM_ERROR || resp_out->err_no != 0) {
        debug("failed to cancel task:%s", resp_out->detail_err_msg);
        xfree(json_string);
        return SLURM_ERROR;
    }
    debug("successfully cancel task");
    xfree(json_string);
    return ret;
}

/* Not yet implemented: POSIX BB cache group immediate adjustment mapping */
extern int remap_burst_buffer_group()
{

};
/* Not yet implemented: Add client to the cache group */
extern int add_burst_buffer_client_to_group()
{
    
};
/* Not yet implemented: Remove client from the cache group */
extern int remove_burst_buffer_client_from_group()
{
    
};
/* Not yet implemented: Locking the dataset will not trigger the automatic recycling mechanism. */
extern int lock_burst_buffer_dataset()
{
    
};
/* Not yet implemented: Unlock dataset */
extern int unlock_burst_buffer_dataset()
{
    
};

/* Get permanent token */
extern int get_permanent_token(bb_config_t *bb_config)
{
    char *token = _get_permanent_token_from_header(bb_config->para_stor_addr, bb_config->para_stor_port,
        bb_config->para_stor_user_name, bb_config->para_stor_password);
    if (!token) {
        error("failed to get token");
        return SLURM_ERROR;
    }

    if (bb_config->token)
        xfree(bb_config->token);
    bb_config->token = xstrdup(token);
    xfree(token);
    debug("sucess get token: %s", bb_config->token);
    return SLURM_SUCCESS ;
}


/* get token from header */
static char *_get_token_from_header(const char *ip, int port,
    const char *user, const char *password)
{
    if (!ip || !user || !password) {
        debug("Invalid parameters to get_token_from_header");
        return NULL;
    }
    char url_token[URL_MAX_LEN];
    char* token = (char*)xmalloc(TOKEN_MAX_LEN);
    snprintf(url_token, sizeof(url_token), "https://%s:%d/restLogin", ip, port);

    if (rest_login(url_token, user, password, token) != 0) {
        debug("Login failed");     
        xfree(token);
        return NULL;
    }
    token[TOKEN_MAX_LEN - 1] = '\0';
    /*   DONT FOEGET FREE */
    return token;
}

/* get permanent token from header */
static char *_get_permanent_token_from_header(const char *ip, int port,
    const char *user, const char *password)
{

    if (!ip || !user || !password) {
        debug("no ip,useror password parameters to get_token_from_header");
        return NULL;
    }
    char url_token[URL_MAX_LEN];
    char *token = (char *)xmalloc(TOKEN_MAX_LEN);
    snprintf(url_token, sizeof(url_token), "https://%s:%d/restLogin", ip, port);

    char *encoded_password = _encode_password(password);
    if (encoded_password == NULL) {
        error("Failed to encode password");
        xfree(token);
        return NULL;
    }

    if (permanent_rest_login(url_token, user, encoded_password, token) != 0) {
        debug("Login failed");
        xfree(encoded_password);
        xfree(token);
        return NULL;
    }

    token[TOKEN_MAX_LEN - 1] = '\0';

    /*   DONT FOEGET FREE */
    xfree(encoded_password);
    return token;
}

/* base64_encode */
static char *_base64_encode(const unsigned char *data, size_t len)
{
    const char base64_table[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t out_len = 4 * ((len + 2) / 3);
    char *out = xmalloc(out_len + 1);
    if (!out) return NULL;

    size_t i, j;
    for (i = 0, j = 0; i < len;) {
        unsigned int val = 0;
        int n = 0;
        for (int k = 0; k < 3; k++) {
            val <<= 8;
            if (i < len) {
                val |= data[i++];
                n++;
            }
        }
        out[j++] = base64_table[(val >> 18) & 0x3F];
        out[j++] = base64_table[(val >> 12) & 0x3F];
        out[j++] = (n > 1) ? base64_table[(val >> 6) & 0x3F] : '=';
        out[j++] = (n > 2) ? base64_table[val & 0x3F] : '=';
    }
    out[out_len] = '\0';
    return out;
}

/* encode password by base64 function */
static char *_encode_password(const char *password)
{
    if (!password) {
        error("Password is NULL");
        return NULL;
    }

    const int OFFSET_NUMBER = 5;
    const char SEP = '_';
    char *tmp = NULL;  
    size_t len = strlen(password);

    for (size_t i = 0; i < len; i++) {
        int v = (unsigned char)password[i] + OFFSET_NUMBER;

        if (i > 0)
            xstrcatchar(tmp, SEP);
        xstrfmtcat(tmp, "%d", v);
    }
    
    char *out = _base64_encode((unsigned char *)tmp, strlen(tmp));
    xfree(tmp);
    return out;
}