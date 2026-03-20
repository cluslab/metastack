# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import base64
import json
import re
import time
import argparse

# 统一盒子宽度，用于对齐右侧竖线
BOX_WIDTH = 62
BOX_INNER_WIDTH = BOX_WIDTH - 2


def box_border():
    print "+" + "-" * BOX_INNER_WIDTH + "+"


def box_log(message, indent=0):
    """
    在盒子内部打印一行，左右都有 |，并按固定宽度对齐。
    indent 为前置空格数量，用来做层级缩进。
    """
    prefix = " " * indent
    text = prefix + message
    if len(text) > BOX_INNER_WIDTH:
        text = text[:BOX_INNER_WIDTH]
    print "|" + ("%-*s" % (BOX_INNER_WIDTH, text)) + "|"

def check_root():
    """Check if the current user has root privileges."""
    if os.geteuid() != 0:
        # Python 2 syntax: use print >> to output to stderr
        print >> sys.stderr, "Error: This script must be run as root."
        sys.exit(1)

def get_job_ids():
    """Call squeue to get a list of job IDs (Python 2 compatible)."""
    try:
        # Use subprocess.check_output for Python 2
        # Note: check_output in Python 2 returns a string
        output = subprocess.check_output(["squeue", "-h", "--format=%A"])
        
        # Split output by lines and filter out empty lines
        job_ids = [line.strip() for line in output.splitlines() if line.strip()]
        return job_ids
        
    except subprocess.CalledProcessError as e:
        print >> sys.stderr, "Failed to get job list:", e
        return []
    except OSError:
        print >> sys.stderr, "Error: 'squeue' command not found in the system."
        return []
    
def get_burst_buffer_config_path():
    """
    Retrieve the SLURM_CONF path using scontrol, 
    then replace 'slurm.conf' with 'burst_buffer.conf'.
    """
    try:
        # 1. Execute command: scontrol show config | grep SLURM_CONF
        # We use Popen and check_output to chain the command
        proc = subprocess.Popen(["scontrol", "show", "config"], stdout=subprocess.PIPE)
        output = subprocess.check_output(["grep", "SLURM_CONF"], stdin=proc.stdout)
        proc.wait()

        # 2. Parse the output (expected format: SLURM_CONF = /path/to/slurm.conf)
        # Split by '=', take the second part, and strip whitespace
        slurm_conf_path = output.split('=')[1].strip()
        
        # 3. Replace the filename with burst_buffer.conf
        # Get the directory name and join it with the new filename
        conf_dir = os.path.dirname(slurm_conf_path)
        burst_buffer_path = os.path.join(conf_dir, "burst_buffer.conf")
        
        return burst_buffer_path

    except Exception as e:
        print >> sys.stderr, "Error retrieving or parsing SLURM_CONF:", e
        return None

def parse_burst_buffer_config(file_path):
    """
    Read the burst_buffer.conf file and extract specific configuration values.
    Returns a dictionary with the extracted keys.
    """
    config_data = {}
    keys_to_extract = [
        "ParaStorAddr", 
        "ParaStorAddrPort", 
        "ParaStorUserName", 
        "ParaStorUserPasswd"
    ]

    try:
        if not os.path.exists(file_path):
            print >> sys.stderr, "Error: File not found:", file_path
            return None

        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                
                # Split only on the first '='
                if "=" in line:
                    key, value = line.split("=", 1)
                    if key in keys_to_extract:
                        config_data[key] = value.strip()
        
        return config_data

    except Exception as e:
        print >> sys.stderr, "Error reading config file:", e
        return None
    
def encode_password(password):
    """
    Encrypt the password by adding an offset of 5 to each character,
    joining them with '_', and then base64 encoding the result.
    """
    if password is None:
        print "Error: Password is None"
        return None

    offset_number = 5
    sep = '_'
    
    # 1. Apply offset and format: e.g., "abc" -> "102_103_104"
    encoded_values = []
    for char in password:
        # ord(char) gets the ASCII value
        v = ord(char) + offset_number
        encoded_values.append(str(v))
    
    tmp_string = sep.join(encoded_values)
    
    # 2. Base64 encode the resulting string
    # In Python 2, base64.b64encode takes a string and returns a string
    out = base64.b64encode(tmp_string)
    
    return out

def call_rest_api(addr, port, user, encoded_pass):
    url = "https://%s:%s/restLogin" % (addr, port)
    
    payload = {
        "username": user,
        "password": encoded_pass,
        "permanentTokenFlag": True,
        "clientType": "REST"
    }
    json_data = json.dumps(payload)

    # 1. 使用 -i 替代 -v，-i 会在输出的开头包含 HTTP 响应头
    # -s 保持静默，不显示下载进度条
    curl_cmd = [
        "curl", "-s", "-k", "-i", "-X", "POST", url,
        "-H", "Content-Type:application/json",
        "-d", json_data
    ]

    print "--- Sending API Request ---"
    
    try:
        # 执行命令并获取输出
        full_response = subprocess.check_output(curl_cmd)
        
        # 2. 解析 Token
        # Token 在 Header 部分，格式通常为 "token: xxxxxxx\r\n"
        token = None
        lines = full_response.splitlines()
        for line in lines:
            if line.lower().startswith("token:"):
                # 提取冒号后面的部分并去空格
                token = line.split(":", 1)[1].strip()
                break
        
        if token:
            print "Token extracted successfully."
            return token
        else:
            print >> sys.stderr, "Error: Token not found in response headers."
            return None

    except subprocess.CalledProcessError as e:
        print >> sys.stderr, "Error calling REST API:", e
        return None

def get_cache_group_sns(addr, port, token):
    """
    发送 GET 请求获取 cache-groups 列表，并提取包含 sn 的 id 映射。
    返回格式: { "sn_value": id_value }
    """
    # 1. 构造 URL
    url = "https://%s:%s/burst-buffer/cache-groups?sn" % (addr, port)
    
    # 2. 构造 curl 命令
    # 注意：这里将 token 放在 Header 中
    curl_cmd = [
        "curl", "-s", "-k", "-X", "GET", url,
        "-H", "token: %s" % token,
        "-H", "Content-Type:application/json"
    ]

    print "--- Fetching Cache Groups ---"
    
    try:
        # 执行请求并获取响应体 (JSON)
        response_str = subprocess.check_output(curl_cmd)
        
        # 3. 解析 JSON
        data = json.loads(response_str)
        
        # 4. 提取数据
        # 路径为: result -> cache_groups
        sn_id_map = {}
        
        if "result" in data and "cache_groups" in data["result"]:
            groups = data["result"]["cache_groups"]
            for group in groups:
                # 检查该条目是否包含 'sn' 字段
                if "sn" in group:
                    sn_val = group["sn"]
                    id_val = group["id"]
                    sn_id_map[sn_val] = id_val
        
        return sn_id_map

    except Exception as e:
        print >> sys.stderr, "Error fetching cache groups:", e
        return {}

def process_expired_cache_jobs(job_list, sn_mapping):
    """
    检查 sn_mapping 中的 SN 是否符合 j{job_id}n{number} 格式。
    如果对应的 job_id 不在 job_list 中，则返回该条目的详细信息。
    """
    expired_jobs = []
    # 正则匹配 j{数字}n{数字}
    pattern = re.compile(r'^j(\d+)n\d+$')

    for sn, cache_id in sn_mapping.items():
        match = pattern.match(sn)
        if match:
            job_id_from_sn = match.group(1)
            
            # 如果从 SN 提取的 Job ID 不在当前 Slurm 队列中
            if job_id_from_sn not in job_list:
                expired_jobs.append({
                    "job_id": job_id_from_sn,
                    "cache_id": cache_id,
                    "sn": sn
                })
    return expired_jobs


# --- 【修改点 1】: 将此函数修改为返回完整的数据集对象列表，而不是单纯的 ID 列表 ---
def get_datasets(addr, port, token, group_id):
    """
    根据 group_id 查询其名下的所有数据集详细信息。
    返回包含数据集字典（如 id, type 等）的列表。
    """
    url = "https://%s:%s/burst-buffer/datasets?group_id=%s" % (addr, port, group_id)
    
    curl_cmd = [
        "curl", "-s", "-k", "-X", "GET", url,
        "-H", "token: %s" % token,
        "-H", "Content-Type:application/json"
    ]

    try:
        response_str = subprocess.check_output(curl_cmd)
        data = json.loads(response_str)
        
        datasets = []
        if "result" in data and "data_sets" in data["result"]:
            datasets = data["result"]["data_sets"]
            
        return datasets

    except Exception as e:
        print >> sys.stderr, "Error fetching datasets for Group %s: %s" % (group_id, e)
        return []

# --- 【修改点 2】: 调整参数，使其直接接收 dataset_id, group_id 和 type ---
def recycle_task(addr, port, token, dataset_id, group_id, ds_type):
    """
    发送 POST 请求以回收指定的任务，并校验返回的 Task ID 是否为数字。
    """
    url = "https://%s:%s/burst-buffer/tasks" % (addr, port)
    
    payload = {
        "dataset_id": dataset_id,
        "group_id": group_id,
        "type": ds_type,
        "error_action_type": "1"
    }
    json_data = json.dumps(payload)

    curl_cmd = [
        "curl", "-s", "-k", "-X", "POST", url,
        "-H", "token: %s" % token,
        "-H", "Content-Type:application/json",
        "-d", json_data
    ]

    try:
        response_str = subprocess.check_output(curl_cmd)
        response_data = json.loads(response_str)
        
        new_task_id = None
        # 解析路径: result[0].id
        if "result" in response_data and len(response_data["result"]) > 0:
            new_task_id = response_data["result"][0].get("id")

        # 校验：必须是数字 (int 或 long) 或者能代表数字的字符串
        if new_task_id is not None:
            try:
                # 尝试转换为整数进行校验
                actual_id = int(new_task_id)
                box_log("Successfully recycled Dataset ID: %s (New Task ID: %d)" % (dataset_id, actual_id), indent=2)
                return actual_id
            except (ValueError, TypeError):
                # 如果转换失败，说明不是数字
                print >> sys.stderr, "Error: Task ID received is not a number: %s" % new_task_id
                sys.exit(1) # 报错退出
        else:
            # 如果 result 存在但没有 id 字段，也视为异常
            print >> sys.stderr, "Error: API response does not contain a valid Task ID."
            sys.exit(1)

    except Exception as e:
        print >> sys.stderr, "Critical Error during recycling task: %s" % e
        sys.exit(1) # 发生网络或其它严重异常也直接退出

def get_task_status(addr, port, token, task_id):
    """
    根据 task_id 查询任务状态。
    """
    url = "https://%s:%s/burst-buffer/tasks?task_id=%s" % (addr, port, task_id)
    
    curl_cmd = [
        "curl", "-s", "-k", "-X", "GET", url,
        "-H", "token: %s" % token,
        "-H", "Content-Type:application/json"
    ]

    try:
        response_str = subprocess.check_output(curl_cmd)
        data = json.loads(response_str)
        
        # 解析路径: result -> tasks[0] -> state
        if "result" in data and "tasks" in data["result"] and len(data["result"]["tasks"]) > 0:
            status = data["result"]["tasks"][0].get("state")
            return status
        return None
    except Exception as e:
        print >> sys.stderr, "Error checking task status: %s" % e
        return None

def wait_for_task_completed(addr, port, token, task_id, timeout_sec=600, interval_sec=10):
    """
    循环轮询指定任务的状态，直到状态变为 'COMPLETED'。
    如果超时或出错，则退出程序。
    """
    box_log("[Monitoring] Waiting for Task ID %s to reach COMPLETED state..." % task_id, indent=5)
    start_time = time.time()
    
    while True:
        current_state = get_task_status(addr, port, token, task_id)
        
        # 检查状态是否为已完成
        if current_state == "COMPLETED":
            box_log("Task %s reached COMPLETED state." % task_id, indent=5)
            return True
        
        # 检查是否超时
        elapsed = time.time() - start_time
        if elapsed > timeout_sec:
            print >> sys.stderr, "\nError: Timeout! Task %s failed to complete within %d seconds." % (task_id, timeout_sec)
            sys.exit(1)
        
        # 打印进度并继续循环
        box_log("Task %s current state: %s (Elapsed: %ds/%ds)" % (task_id, current_state, int(elapsed), timeout_sec), indent=5)
        
        # 等待下一次重试
        time.sleep(interval_sec)

def delete_dataset(addr, port, token, ds_id):
    """
    删除指定的数据集。
    对应命令: curl -k --location --request DELETE 'https://{addr}:{port}/burst-buffer/datasets/{ds_id}'
    """
    url = "https://%s:%s/burst-buffer/datasets/%s" % (addr, port, ds_id)
    
    curl_cmd = [
        "curl", "-s", "-k", 
        "--location", 
        "--request", "DELETE", 
        url, 
        "--header", "token: %s" % token
    ]
    
    try:
        subprocess.check_output(curl_cmd)
        box_log("Successfully deleted Dataset ID: %s" % ds_id, indent=2)
        return True
    except Exception as e:
        print >> sys.stderr, "Failed to delete Dataset %s: %s" % (ds_id, e)
        return False
        
def delete_cache_group(addr, port, token, group_id):
    """
    根据 group_id 删除缓存组。
    对应命令: curl -k --location --request DELETE 'https://{addr}:{port}/burst-buffer/cache-groups?id={group_id}'
    """
    url = "https://%s:%s/burst-buffer/cache-groups?id=%s" % (addr, port, group_id)
    
    curl_cmd = [
        "curl", "-s", "-k", 
        "--location", 
        "--request", "DELETE", 
        url, 
        "--header", "token: %s" % token
    ]

    try:
        response_str = subprocess.check_output(curl_cmd)
        
        # 处理可能为空的返回体
        if not response_str.strip():
            box_log("[Group] Successfully deleted Cache Group ID: %s" % group_id)
            return True
            
        data = json.loads(response_str)
        if data.get("err_no") == 0:
            box_log("[Group] Successfully deleted Cache Group ID: %s" % group_id)
            return True
        else:
            print >> sys.stderr, "Failed to delete Cache Group %s: %s" % (group_id, data.get("err_msg"))
            return False
    except Exception as e:
        print >> sys.stderr, "Error during delete_cache_group: %s" % e
        return False

def scancel_job(job_id):
    """
    使用 scancel 命令取消指定的 Slurm 作业。
    """
    box_log("[Job] Canceling Job ID: %s..." % job_id)
    try:
        # 使用 subprocess.call 执行命令
        cmd = ["scancel", str(job_id), "-H"]
        subprocess.check_call(cmd)
        box_log("[Job] Successfully canceled Job %s." % job_id)
        return True
    except subprocess.CalledProcessError as e:
        print >> sys.stderr, "Failed to cancel Job %s: %s" % (job_id, e)
        return False


def parse_args():
    """
    解析命令行参数：
      - 无参数：保持原有逻辑，只清理“已过期”的缓存组；
      - -j JOB / --job JOB：只处理 j{JOB}n(number) 对应的缓存组及其任务/数据集，并删除。
    """
    parser = argparse.ArgumentParser(
        description="Clean up burst buffer cache groups, datasets and tasks."
    )
    parser.add_argument(
        "-j", "--job",
        dest="job_id",
        help="Only clean resources for a specific job id, e.g. 1100."
    )
    return parser.parse_args()


def main():
    global para_addr, para_port, para_user, para_pass, token  # 声明为全局变量

    # 解析命令行参数
    args = parse_args()
    target_job_id = args.job_id

    #  Check permissions immediately
    check_root()

    # Get the burst buffer config path
    config_path = get_burst_buffer_config_path()
    if config_path:
        print "Burst buffer config path is:", config_path
    else:
        print >> sys.stderr, "Unable to determine burst buffer config path."
        return

    #  Parse the burst buffer config file
    config = parse_burst_buffer_config(config_path)
    
    if config:
        # 检查所有必需的键是否存在
        required_keys = ["ParaStorAddr", "ParaStorAddrPort", "ParaStorUserName", "ParaStorUserPasswd"]
        missing_keys = [key for key in required_keys if key not in config]
        
        if not missing_keys:
            # 所有必需的配置都存在，可以安全使用
            para_addr = config["ParaStorAddr"]
            para_port = config["ParaStorAddrPort"]
            para_user = config["ParaStorUserName"]
            para_pass = config["ParaStorUserPasswd"]
        else:
            print "Missing required config items: %s" % ", ".join(missing_keys)
            return
    else:
        print("cannot parse burst buffer config file.")
        return

    # Encode the password and print it
    encode_parastor_pass = encode_password(para_pass)

    # 执行 API 调用 (传入拼接好的参数)
    token = call_rest_api(para_addr, para_port, para_user, encode_parastor_pass)
    if not token:
        print "Error: Failed to call REST API."
        return 

    # 获取 SN 和 ID 的对应关系
    sn_mapping = get_cache_group_sns(para_addr, para_port, token)

    # 根据是否指定 -j 参数决定过滤逻辑
    if target_job_id:
        # 只匹配 j{JOB}n(number) 的缓存组，不再检查当前 squeue 队列
        print "Target job id specified: %s" % target_job_id
        pattern = re.compile(r'^j%sn\d+$' % re.escape(target_job_id))
        expired_list = []

        for sn, cache_id in sn_mapping.items():
            if pattern.match(sn):
                expired_list.append({
                    "job_id": target_job_id,
                    "cache_id": cache_id,
                    "sn": sn
                })
        if not expired_list:
            print "No cache groups found for Job %s." % target_job_id
            return
    else:
        # 原有逻辑：获取当前系统中的 Job ID 列表，找出“已过期”的缓存组
        job_list = get_job_ids()
        expired_list = process_expired_cache_jobs(job_list, sn_mapping)
    
    if expired_list:
        print "\n=== 检测到需要清理的缓存组，共 %d 个 ===" % len(expired_list)
        for idx, item in enumerate(expired_list, start=1):
            cache_id = item['cache_id']
            job_id   = item['job_id']
            sn       = item['sn']

            # --- 分组日志块：开始（使用 ASCII 盒子，统一宽度） ---
            print ""
            box_border()
            box_log("Cache Group %d / %d" % (idx, len(expired_list)))
            box_border()
            box_log("[CacheGroup] Group ID: %s" % cache_id)
            box_log("[CacheGroup] Job ID  : %s" % job_id)
            box_log("[CacheGroup] SN      : %s" % sn)
            box_border()
            box_log("Datasets")
            box_border()

            # 获取该组下的所有数据集（此时返回的是对象列表）
            datasets = get_datasets(para_addr, para_port, token, cache_id)

            if datasets:
                box_log("[Dataset] Found %d datasets, preparing to recycle..." % len(datasets))
                for ds in datasets:
                    ds_id = ds.get("id")
                    box_log("- dataset id: %s" % ds_id, indent=2)
                    # 从数据集中提取 type，如果没有则默认传入 "BURST_BUFFER_TASK_TYPE_PREFETCH"
                    ds_type = ds.get("type", "BURST_BUFFER_TASK_TYPE_PREFETCH")

                    if ds_id is not None:
                        task_tid = recycle_task(para_addr, para_port, token, ds_id, cache_id, ds_type)
                        if not task_tid:
                            box_log("! Failed to recycle Dataset %s" % ds_id, indent=5)
                        else:
                            box_log("* Recycle task id: %s, waiting to complete..." % task_tid, indent=5)
                            wait_for_task_completed(para_addr, para_port, token, task_tid)
                        # 任务完成后，安全删除数据集
                        delete_dataset(para_addr, para_port, token, ds_id)
            else:
                box_log("[Dataset] No datasets found in this group.")

            # 删除缓存组并取消作业
            box_border()
            box_log("Delete Group / Job")
            box_border()
            delete_cache_group(para_addr, para_port, token, cache_id)
            scancel_job(job_id)

            # --- 分组日志块：结束 ---
            box_border()
            box_log("End Cache Group")
            box_border()
            
    else:
        print "所有缓存组对应的 Job 均在运行中，无需清理。"

if __name__ == "__main__":
    main()