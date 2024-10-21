#!/bin/bash
# =========0714======
exec_path=`pwd`
cd `dirname $0`
script_path=`pwd`

if [ $# -lt 3 ];then
    echo "lack param: $0 task_path container_name job_id"
    exit 1
fi

task_path=$1
container_name=$2
job_id=$3
distributed=0
if [ $# -gt 3 ];then
	distributed=$4
fi

SLURM_INSTALL_PATH=/opt/gridview/slurm
GRES_CONF=$SLURM_INSTALL_PATH/etc/gres.conf
source $SLURM_INSTALL_PATH/etc/sothisai/function.sh
source $SLURM_INSTALL_PATH/etc/sothisai/basic_env.sh

log_error_message(){
    echo "$1" |tee -ia $ERROR_MESSAGE
}

log_start_container(){
    echo "$1" |tee -ia $START_MESSAGE
}


ERROR_MESSAGE=`getTaskMessagePath $task_path`
START_MESSAGE=`getPrepareContainerLogPath $task_path`
LOGFILE=`getSlurmPrologFilePath $task_path $job_id $SLURMD_NODENAME`
docker_file=`getDockerInstanceFilePath $task_path $job_id`
container_file=`getContainerFile $task_path $container_name`

function initDockerRunParams(){
#NV_GPU=$finalGpuIDs nvidia-docker run $RESOURCE_LIMITS --name ${finalDockerName} -d ${docker_mounts} ${docker_env} ${finalImageName} sh -c \"cat /proc/1/environ| tr '\0' '\n' | awk -v ex='export ' '{printf ex;print $1}'> /etc/profile.d/sothisai.sh;/usr/sbin/sshd -D\"
    if [ ! -f $task_path/$container_name ];then
        log_error_message "$task_path/$container_name not existed!"
        exit 1
    fi
    source $task_path/$container_name
    TASK_ROLE="$TASK_ROLE"
    TASK_ROLE_X="$TASK_ROLE_X"
    TASK_NODE="$TASK_NODE"
    TASK_TYPE="$TASK_TYPE"

    USERHOME="$USERHOME"
    ACCELERATOR_TYPE="$ACCELERATOR_TYPE"

    DC_NAME="$DC_NAME"
    DC_GPUIDS="$DC_GPUIDS"
    if [ "$DC_GPUIDS" = "-" ];then
        DC_GPUIDS=""
    fi

    DC_HOST_PORT="$DC_HOST_PORT"
    DC_CONTAINER_PORT="$DC_CONTAINER_PORT"
    SHARING_PATH="$SHARING_PATH"

    DC_RESOURCE_LIMITS="$DC_RESOURCE_LIMITS"
    DC_PORTS_MAP="$DC_PORTS_MAP"
    if [ -z $TASK_TYPE ];then
        DC_MOUNTS="$DC_MOUNTS"
    elif [[ ${TASK_TYPE} = "ssh" ]] ||  [[ ${TASK_TYPE} = "rstudio" ]] || [[ ${TASK_TYPE} = "codeserver" ]];then
        DC_MOUNTS="$DC_FRAMEWORK_MOUNTS"
    elif [ ${TASK_TYPE} = "jupyter" ]; then
        #jupyter加上sharingPath
        DC_MOUNTS="$DC_FRAMEWORK_MOUNTS -v ${SHARING_PATH}:${SHARING_PATH}:ro"
    else
    	DC_MOUNTS="$DC_MOUNTS"
    fi
    # local time mounts
    docker_time_mounts=$(formatLocalTimeMount)
    DC_MOUNTS="$DC_MOUNTS $docker_time_mounts"
    DC_DEVICES="$DC_DEVICES"
    DC_ENV="$DC_ENV"
    DC_ENTRYPOINT="${DC_ENTRYPOINT}"
    if [ -z $DC_ENTRYPOINT ];then
        DC_ENTRYPOINT=" --entrypoint='/bin/sh' "
    fi
    ALIAS="$ALIAS"
    SHARED_HOSTS="$SHARED_HOSTS"
    DC_IMAGENAME="$DC_IMAGENAME"
    DC_CMD_ARG="$DC_CMD_ARG"
    DC_IB_PARAM=""
    if [ ! -z "$ib_param" ] && [ "$ib_param" = "true" ]; then
        uverbs=$(getUverbsNum)
        rocm=$(getRocmNum)
        DC_IB_PARAM="--ulimit memlock=-1 --cap-add=IPC_LOCK"
        for ((i=0; i<uverbs; i++)); do
            DC_IB_PARAM="$DC_IB_PARAM --device=/dev/infiniband/uverbs$i"
        done
#        DC_IB_PARAM="$DC_IB_PARAM --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/uverbs3"
        if [ "$rocm" = "1" ]; then
            DC_IB_PARAM="$DC_IB_PARAM --device=/dev/infiniband/rdma_cm"
        fi
    fi


}

function pullImage()
{
        log_start_container " "
        log_start_container "======开始拉取镜像======"
        log_start_container "`date`"
        log_start_container "docker pull ${DC_IMAGENAME}"
	api_version=$(docker version --format '{{.Server.APIVersion}}')
        curl --unix-socket /var/run/docker.sock -X POST "http://localhost/v${api_version}/images/create?fromImage=${DC_IMAGENAME}" 1>$task_path/${job_id}_PULL.log 2>/tmp/${DC_NAME}.tmp
        if [ $? != 0 ];then
                log_error_message " "
                log_error_message "========容器信息摘要========="
                cat /tmp/${DC_NAME}.tmp | tee -ia ${ERROR_MESSAGE} ${START_MESSAGE} ; rm -f /tmp/${DC_NAME}.tmp
                log_error_message "========================="
                log_error_message " "
                log_error_message "========问题及解决方案========"
                log_error_message "【问题原因】：拉取镜像阶段失败"
                if ! ls /usr/bin/docker >/dev/null 2>&1 ;then
                        log_error_message "【解决方案】：联系管理员部署容器组件（节点：$SLURMD_NODENAME）"
                elif ! docker ps >/dev/null 2>&1 ;then
                        log_error_message "【解决方案】：联系管理员检查容器服务状态（节点：$SLURMD_NODENAME）"
                else
                        log_error_message "【解决方案】：联系管理员检查镜像库状态"
                fi
                log_error_message "=========================="
                log_start_container "======拉取镜像失败======"
                exit 0
        else
                log_start_container "======拉取镜像成功======"
        fi
}

function startContainer()
{
	if [ "$ACCELERATOR_TYPE" = "gpu" ];then
		if [ -z $DC_GPUIDS ];then
			DC_RUN="timeout 60m docker run --gpus '\"device=none\"' "
		else
			DC_RUN="NV_GPU=$DC_GPUIDS timeout 60m nvidia-docker run"
		fi
	else
		DC_RUN="timeout 60m docker run"
	fi

	if [ ! -z $ALIAS ];then
		if [ ! -z $SHARED_HOSTS ];then
			EXTRA_PARAM="--hostname=${ALIAS} -v ${SHARED_HOSTS}:/etc/hosts"
		else
			EXTRA_PARAM="--hostname=${ALIAS}"
		fi
	fi
        docker_run_cmd="${DC_RUN} ${DC_RESOURCE_LIMITS} --name ${DC_NAME} -d ${EXTRA_PARAM}  ${DC_PORTS_MAP} ${DC_MOUNTS} ${DC_DEVICES} ${DC_IB_PARAM} ${DC_ENV} ${DC_ENTRYPOINT} ${DC_IMAGENAME} ${DC_CMD_ARG}"
	
        log_start_container " "
        log_start_container "======开始启动容器======"
        log_start_container "`date`"
        log_prolog_detail "$docker_run_cmd"
        docker_run_out=`eval "$docker_run_cmd 2>&1"`
        if [ $? != 0 ];then
                log_error_message " "
                log_error_message "========容器信息摘要========="
                echo "${docker_run_out}" | tee -ia ${ERROR_MESSAGE} ${START_MESSAGE}
                log_error_message "========================"
                log_error_message " "
                log_error_message "========问题及解决方案========"
                log_error_message "【问题原因】：启动容器阶段失败"
                if ! ls /usr/bin/docker >/dev/null 2>&1 ;then
                        log_error_message "【解决方案】：联系管理员部署容器组件（节点：$SLURMD_NODENAME）"
                elif ! docker ps >/dev/null 2>&1 ;then
                        log_error_message "【解决方案】：联系管理员检查容器服务状态（节点：$SLURMD_NODENAME）"
                else
                        log_error_message "【解决方案】：联系管理员检查容器启动参数"
                fi
                log_error_message "=========================="
                log_start_container "======启动容器失败======"
                exit 0
        else
                log_start_container "${DC_NAME} 启动完成"
                log_start_container "======启动容器成功======"
        fi
}

function checkContainerStatus()
{
        log_start_container " "
        log_start_container "=====容器状态信息======="
        log_start_container "`date`"
        container_status=`docker inspect -f '{{.State.Status}}' $DC_NAME`
        log_start_container "容器状态：$container_status"
        log_start_container "======================"
        if [ $container_status != 'running' ];then
                log_error_message " "
                log_error_message "========容器信息摘要========="
                log_error_message "`docker logs $DC_NAME`"
                log_error_message "========================="
                log_error_message " "
                log_error_message "========问题及解决方案========"
                log_error_message "【问题原因】：启动容器阶段失败"
                log_error_message "【解决方案】：根据容器日志校验镜像内的组件(sshd)是否完整"
                log_error_message "=========================="
                exit 0
        fi
}

function dockerRun(){
	pullImage
	startContainer
	#checkContainerStatus
}

function recordDockerInfo(){
	#save the docker file
	gpu_number=`echo "$DC_GPUIDS"| awk -F, '{print NF}'`
	#DC_GPUIDS=0,1  format_gpuids=0:1
	format_gpuids=$(echo ${DC_GPUIDS} | sed 's/,/:/g')
	local dockerInfo=`getDockerInspInfo`
	sudo -u $USER_NAME  touch $docker_file
	echo " -> dockerInfo=${dockerInfo}" >> $LOGFILE
	if [ $distributed -eq 0 ];then
		echo "ALIAS=${ALIAS},CPU_USAGE=0,GPU_USAGE=0,RAM_USAGE=0,GPU_MEM_USAGE=0,TYPE=${TASK_ROLE},INDEX=${TASK_ROLE_X},NODE=${TASK_NODE},NAME=${DC_NAME},GPUIDS=${format_gpuids},"${dockerInfo} > $docker_file
	elif [ $distributed -eq 2 ];then
		mkdir ${TASK_PATH}/container_names/
		echo "ALIAS=${TASK_ROLE}-${TASK_ROLE_X},CPU_USAGE=0,GPU_USAGE=0,RAM_USAGE=0,GPU_MEM_USAGE=0,TYPE=${TASK_ROLE},INDEX=${TASK_ROLE_X},NODE=${TASK_NODE},NAME=${DC_NAME},GPUIDS=${format_gpuids},"${dockerInfo} > $task_path/container_names/$container_name
	else
		echo "ALIAS=${TASK_ROLE}-${TASK_ROLE_X},CPU_USAGE=0,GPU_USAGE=0,RAM_USAGE=0,GPU_MEM_USAGE=0,TYPE=${TASK_ROLE},INDEX=${TASK_ROLE_X},NODE=${TASK_NODE},NAME=${DC_NAME},GPUIDS=${format_gpuids},"${dockerInfo} > $container_file
	fi
    
}

function getDockerInspInfo()
{
	local dockerinfo=""
	local mount=""
	local mount_array=""
	infos=(`docker inspect $DC_NAME|jq -r '.[0].Id, .[0].State.Status, .[0].NetworkSettings.Networks.bridge.IPAddress, .[0].HostConfig.Memory/1024/1024, .[0].HostConfig.CpusetCpus'`)
	if [ $? -eq 0 ]; then
		#base info
		dockerinfo="ID=${infos[0]},STATUS=${infos[1]},IPADDR=${infos[2]},MEMORY=${infos[3]}"
		if [ "${infos[4]}" != "null" ]; then
			cpu_count=$(echo "${infos[4]}" | awk -F"," '{print NF-1}')
			let cpu_count=cpu_count+1
			dockerinfo="${dockerinfo},CPU=$cpu_count"
		fi
		dockerinfo="${dockerinfo},GPU=${gpu_number},CONTAINERPORT=${DC_CONTAINER_PORT},HOSTPORT=${DC_HOST_PORT}"
		#mount info
        	mount=`docker inspect $DC_NAME --format="{{json .Mounts}}" | jq -S .[] | egrep -e "\"Source\"|\"Destination\"" | awk -F: '{print $2}' | awk -F '"' '{print $2}'`
		mount_array=(${mount// / })
		for (( index=0;index<${#mount_array[@]};index+=2 ));
		do
			mountInfo="${mountInfo};${mount_array[$(($index+1))]}:${mount_array[${index}]}"
		done
		mountInfo=`echo $mountInfo | sed 's/^.//'`
		#total info
		dockerinfo="${dockerinfo},MOUNT=$mountInfo"
	fi
	echo $dockerinfo
	return 1
}

initDockerRunParams
dockerRun
recordDockerInfo
echo "over" >> $LOGFILE
date >> $LOGFILE



