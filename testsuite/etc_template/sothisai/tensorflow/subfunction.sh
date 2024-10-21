#! /bin/bash
SLURM_INSTALL_PATH=/opt/gridview/slurm
source $SLURM_INSTALL_PATH/etc/sothisai/basic_env.sh

# max time to wait file is 5 min
export MAX_WAIT_TIME=300

# name of parameter server
export PS_ROLE_NAME="ps"
# name of parameter server
export WORKER_ROLE_NAME="worker"
# name of tuning master server
export MASTER_ROLE_NAME="master"


# ******************************************
# ******************************************
# ******************************************
# ******************************************

# ================== distribute env ========================
function log_logfile(){
    if [ ! -z "${LOGFILE}" ];then
        echo -e "$1" | tee -ia  $LOGFILE
    else
        echo -e "$1"
    fi
}

function initDistTFTaskEnv(){
    initDistributeEnv ps "$ps_gpu_enable" "$ps_gpu_number" "$ps_cpu_number" "$ps_ramMB" "$ps_number" worker "$worker_gpu_enable" "$worker_gpu_number" "$worker_cpu_number" "$worker_ramMB" "$worker_number"
}

function initTFTuningEnv(){
    initDistributeEnv master "$master_gpu_enable" "$master_gpu_number" "$master_cpu_number" "$master_ramMB" "$master_number" worker "$worker_gpu_enable" "$worker_gpu_number" "$worker_cpu_number" "$worker_ramMB" "$worker_number"
}

function initDistributeEnv()
{
	#
	# We recommand that
	# 1. cworker must do not use GPU
	# 2. each gworker must use 1 GPU
	#
    # cpu worker - cwk
    # gpu worker - gwk
    log_logfile "initDistributeEnv Params : $*"
    log_logfile "MAX_GPUS_PER_NODE:$MAX_GPUS_PER_NODE"
    local cwk_role=$1
    local cwk_gpu_enable=$2
    local cwk_gpu_number=$3
    local cwk_cpu_number=$4
    local cwk_ramMB=$5
    local cwk_number=$6

    local gwk_role=$7
    local gwk_gpu_enable=$8
    local gwk_gpu_number=$9
    local gwk_cpu_number=${10}
    local gwk_ramMB=${11}
    local gwk_number=${12}

	if [ $cwk_gpu_enable -ge 1 ] &&[ $cwk_gpu_number -gt $MAX_GPUS_PER_NODE ]; then
		log_logfile "****** GPUs of $cwk_role is too large (Req: $cwk_gpu_number, Configured: $MAX_GPUS_PER_NODE)"
		return 1
	fi

	if [ $gwk_gpu_enable -ge 1 ] && [ $gwk_gpu_number -gt $MAX_GPUS_PER_NODE ]; then
		log_logfile "****** GPUs of $gwk_role is too large (Req: $gwk_gpu_number, Configured: $MAX_GPUS_PER_NODE)"
		return 1
	fi
    
    # ============================
    # request total resources 
	TOTAL_TASKS=`expr $cwk_number + $gwk_number`

	local cwkTotalCPUs=`expr $cwk_number \* $cwk_cpu_number`
	local gwkTotalCPUs=`expr $gwk_number \* $gwk_cpu_number`

	TOTAL_CPUS=`expr $cwkTotalCPUs + $gwkTotalCPUs`

    local cwkTotalGPUS=0
    local gwkTotalGPUS=0

    if [ $gwk_gpu_enable -ge 1 ]; then
        gwkTotalGPUS=`expr $gwk_number \* $gwk_gpu_number`
    fi
    TOTAL_GPUS=`expr $cwkTotalGPUS + $gwkTotalGPUS`
    # ============================
    # Dispatch
	local num=0
	local left=0
    # Dispatch Worker
	MAX_GWK_PER_NODE=`expr $MAX_GPUS_PER_NODE / $gwk_gpu_number`
	if [ $gwk_number -le $MAX_GWK_PER_NODE ]; then
        # one node
		GWK_PER_NODE=$gwk_number
		GWK_NODE_NUM=1
	else
        # mulitple nodes
		iii=$MAX_GWK_PER_NODE
		while((iii>=1))
		do
		# Try to distribute gpu workers equally on nodes without wasting GPU
			num=`expr $gwk_number / $iii`
			left=`expr $gwk_number % $iii`
		#	echo "  -> number: $num, left: $left"
			if [ $left -eq 0 ]; then
				GWK_PER_NODE=$iii
				GWK_NODE_NUM=$num
				break
			fi
			let "iii-=1"
		done
	fi

	GWK_GPU_PER_NODE=`expr $GWK_PER_NODE \* $gwk_gpu_number`
	GWK_CPU_PER_NODE=`expr $GWK_PER_NODE \* $gwk_cpu_number`
	GWK_MEM_PER_NODE=`expr $GWK_PER_NODE \* $gwk_ramMB`

    # Dispatch cpu woker
    # Max cpu woker num per node
	local CWK_MIN_PER_NODE=`expr $cwk_number / ${GWK_NODE_NUM}`
	local CWK_MAX_PER_NODE=${CWK_MIN_PER_NODE}
	left=`expr $cwk_number % ${GWK_NODE_NUM}`
	if [ $left -ne 0 ]; then
		let "CWK_MAX_PER_NODE+=1"
	fi
	
	CWK_PER_NODE=${CWK_MAX_PER_NODE}
    # ===================================
    # only compute, not used?
	CWK_NODE_NUM=${GWK_NODE_NUM}
	if [ ${cwk_number} -lt ${GWK_NODE_NUM} ]; then
		CWK_NODE_NUM=${cwk_number}
	fi
	if [ ${CWK_MIN_PER_NODE} -eq ${CWK_MAX_PER_NODE} ]; then
		:
	fi

	CWK_MIN_CPU_PER_NODE=`expr ${CWK_MIN_PER_NODE} \* $cwk_cpu_number`
	MIN_CPUS=`expr $GWK_CPU_PER_NODE + $CWK_MIN_CPU_PER_NODE`
    # ===================================

    CWK_GPU_PER_NODE=`expr ${CWK_PER_NODE} \* $cwk_gpu_number`
	CWK_CPU_PER_NODE=`expr ${CWK_PER_NODE} \* $cwk_cpu_number`
	CWK_MEM_PER_NODE=`expr ${CWK_PER_NODE} \* $cwk_ramMB`

	CPU_PER_NODE=`expr ${CWK_CPU_PER_NODE} + ${GWK_CPU_PER_NODE}`
	GPU_PER_NODE=`expr ${CWK_GPU_PER_NODE} + ${GWK_GPU_PER_NODE}`
	MEM_PER_NODE=`expr ${CWK_MEM_PER_NODE} + ${GWK_MEM_PER_NODE}`

	GPU_NODE_NUM=${GWK_NODE_NUM}
	JOB_NODE_NUM=${GWK_NODE_NUM}

	if [ $DEBUG_MODE -eq 1 ]; then
        log_logfile ""
        log_logfile "TASK_PATH: $TASK_PATH"
        log_logfile "TOTAL_TASKS: $TOTAL_TASKS" 
        log_logfile "${cwk_role} TotalCPUs: $cwkTotalCPUs" 
        log_logfile "${gwk_role} TotalCPUs: $gwkTotalCPUs" 
        log_logfile "TOTAL_CPUS: $TOTAL_CPUS" 
        log_logfile "${cwk_role} TotalGPUS: $cwkTotalGPUS" 
        log_logfile "${gwk_role} TotalGPUS: $gwkTotalGPUS" 
        log_logfile "TOTAL_GPUS: $TOTAL_GPUS"
        log_logfile "MAX_GPUS_PER_NODE: $MAX_GPUS_PER_NODE"
        log_logfile ""
        log_logfile "==== Resource per ${gwk_role} ===="
        log_logfile "${gwk_role}_number: $gwk_number"
        log_logfile "${gwk_role}_gpu_enable: $gwk_gpu_enable"
        log_logfile "${gwk_role}_gpu_number: $gwk_gpu_number"
        log_logfile "${gwk_role}_cpu_number: $gwk_cpu_number"
        log_logfile "${gwk_role}_ramMB: $gwk_ramMB"
        log_logfile "==== Workers on Nodes ===="
        log_logfile "MAX_${gwk_role}_PER_NODE: $MAX_GWK_PER_NODE"
        log_logfile "${gwk_role}_PER_NODE: $GWK_PER_NODE"
        log_logfile "${gwk_role}_NODE_NUM: $GWK_NODE_NUM"
        log_logfile "${gwk_role}_GPU_PER_NODE: $GWK_GPU_PER_NODE"
        log_logfile "${gwk_role}_CPU_PER_NODE: $GWK_CPU_PER_NODE"
        log_logfile "${gwk_role}_MEM_PER_NODE: $GWK_MEM_PER_NODE"
        log_logfile ""
        log_logfile "==== Resource per $cwk_role ===="
        log_logfile "${cwk_role}_number: $cwk_number"
        log_logfile "${cwk_role}_gpu_enable: $cwk_gpu_enable"
        log_logfile "${cwk_role}_gpu_number: $cwk_gpu_number"
        log_logfile "${cwk_role}_cpu_number: $cwk_cpu_number"
        log_logfile "${cwk_role}_ramMB: $cwk_ramMB"
        log_logfile "==== $cwk_role s on Nodes ===="
        log_logfile "${cwk_role}_MIN_PER_NODE: $CWK_MIN_PER_NODE"
        log_logfile "${cwk_role}_MAX_PER_NODE: $CWK_MAX_PER_NODE"
        log_logfile "${cwk_role}_PER_NODE: $CWK_PER_NODE"
        log_logfile "${cwk_role}_NODE_NUM: $CWK_NODE_NUM"
        log_logfile "${cwk_role}_GPU_PER_NODE: $CWK_GPU_PER_NODE"
        log_logfile "${cwk_role}_CPU_PER_NODE: $CWK_CPU_PER_NODE"
        log_logfile "${cwk_role}_MEM_PER_NODE: $CWK_MEM_PER_NODE"
        log_logfile ""
        log_logfile "JOB_NODE_NUM: $JOB_NODE_NUM"
        log_logfile "GPU_PER_NODE: $GPU_PER_NODE"
        log_logfile "CPU_PER_NODE: $CPU_PER_NODE"
        log_logfile "MEM_PER_NODE: $MEM_PER_NODE"
        log_logfile "GPU_NODE_NUM: $GPU_NODE_NUM"
        log_logfile "MIN_CPUS: $MIN_CPUS"
        log_logfile ""
	fi
	return 0

}


# ******************************************
# ******************************************
# ******************************************

# =====================================================
# =====================================================
# =====================================================

#
# get task index list of specified node
# Usage: $0 <node index> <node total> <task total>
#        node index started with 1
# Output: [<Index1>,<Index2>,...<IndexX>]
#
function getTaskIndexsOfNodeX()
{

	if [ $# -ne 3 ]; then
		echo "Usage: $0 <node index> <node total> <task total>" >&2
		return 1
	fi

	local nodeIndex=$1
	local nodeTotal=$2
	local taskTotal=$3

	if [ ${nodeIndex} -gt ${nodeTotal} ]; then
		echo "node index is larger than node total (${nodeIndex} > ${nodeTotal})" >&2
		return 1
	fi

	# for ps, it is ok
#	if [ ${nodeTotal} -gt ${taskTotal} ]; then
#		echo "node total is larger than task total (${nodeTotal} > ${taskTotal})" >&2
#		return 1
#	fi

	local allIndex=""
	local startIndex=`expr ${nodeIndex} - 1`
	while true
	do
		if [ ${startIndex} -ge ${taskTotal} ]; then
			break
		fi
		if [ -z "$allIndex" ]; then
			allIndex="${startIndex}"
		else
			allIndex="${allIndex},${startIndex}"
		fi
		let "startIndex += nodeTotal" 
	done

	if [ ! -z "${LOGFILE}" ]; then
		echo "NodeIndex=${nodeIndex},NodeTotal=${nodeTotal},TaskTotal=${taskTotal}, Task allIndex: $allIndex" >> $LOGFILE
	fi
	echo "$allIndex"
	return 0

}

#
# check whether the docker list file has enough record lines
# Usage: $0 <file path> <line number>
#
function isDockerRecordFileReady()
{
	if [ $# -ne 2 ]; then
		echo "Usage: $0 <file path> <max lines>" >&2
		return 1
	fi
	local fileName=$1
	local lineMax=$2
	local nowNum=0

	for i in `seq 1 $MAX_WAIT_TIME`
	do
		nowNum=$(cat ${fileName} | wc -l)
		if [ $nowNum -ge $lineMax ]; then
			return 0
		fi
		if [ $DEBUG_MODE -eq 1 ]; then
			echo "  [$i] Waitting for ready of $fileName (Total:$lineMax, Now:$nowNum), ..."
		fi
		sleep 1
	done
	return 1
}

#
# get comma-separated addr list of dockers with specified role
#   Usage: getRoledDockerAllAddrs <role name> <docker record filepath>
#
function getRoledDockerAllAddrs()
{
	if [ $# -ne 3 ]; then
		echo "Usage: $0 <role> <filepath> <hvd>" >&2
		return 1
	fi
	
	local MATCHSTR="TYPE=$1"
	local ROLE_NUM=$(getRoledDockerNumber $1 $2)
	local hvd=$3
	# old record format: TYPE=worker,INDEX=0,NODE=gpunode1,NAME=736_gpunode1_worker_0,IPADDR=172.17.56.8,GPUIDS=0:1
    # new record format: TYPE=worker,INDEX=0,NODE=gpunode1,NAME=736_gpunode1_worker_0,GPUIDS=0:1,ID=38de4a3d8e23,IPADDR=172.17.56.8,STATUS=running,CPU=2,MEMORY=4,GPU=2
	if [ $hvd -eq 1 ];then
		port=22
	else
		port=2222
	fi
	ip_list=$(cat $2 | grep -o 'IPADDR=[^,]*' | cut -d'=' -f2)
  result=$(echo "$ip_list" | awk -v port="$port" 'BEGIN {FS=OFS=","} {for (i=1; i<=NF; i++) $i=$i":"port} 1' | paste -sd ',')
  echo $result
	return 0
}

#
# get number of dockers with specified role
#   Usage: getRoledDockerNumber <role name> <docker record filepath>
#
function getRoledDockerNumber()
{
	if [ $# -ne 2 ]; then
		echo "Usage: $0 <role> <filepath>" >&2
		return 1
	fi
	
	local MATCHSTR="TYPE=$1"
	total=$(cat $2 | grep -c $MATCHSTR)
  echo $total
	#cat $2 | awk -F, -v MATCH=$MATCHSTR 'BEGIN {total=0} {if ($1 == MATCH) {total+=1}} END{printf("%d\n",total)}'
	return 0
}

#
# get task id from record
# Usage: $0 <single record string>
# new record format: YPE=worker,INDEX=0,NODE=gpunode1,NAME=736_gpunode1_worker_0,GPUIDS=0:1,ID=38de4a3d8e23,IPADDR=172.17.56.8,STATUS=running,CPU=2,MEMORY=4,GPU=2
#
function getRecordTaskId()
{

	if [ $# -ne 1 ]; then
		echo "Usage: $0 <record string>" >&2
		return 1
	fi

	#echo $1 | awk -F, '{split($2,arr,"=");printf("%s\n",arr[2])}'
	echo $1 | grep -o 'INDEX=[^,]*' | cut -d'=' -f2
	return 0
}

#
# get docker node from record
# Usage: $0 <single record string>
# new record format: YPE=worker,INDEX=0,NODE=gpunode1,NAME=736_gpunode1_worker_0,GPUIDS=0:1,ID=38de4a3d8e23,IPADDR=172.17.56.8,STATUS=running,CPU=2,MEMORY=4,GPU=2
#
function getRecordDockerNode()
{

	if [ $# -ne 1 ]; then
		echo "Usage: $0 <record string>" >&2
		return 1
	fi

	#echo $1 | awk -F, '{split($3,arr,"=");printf("%s\n",arr[2])}'
	echo $1 | grep -o 'NODE=[^,]*' | cut -d'=' -f2
	return 0
}

#
# get docker name from record
# Usage: $0 <single record string>
# new record format: YPE=worker,INDEX=0,NODE=gpunode1,NAME=736_gpunode1_worker_0,GPUIDS=0:1,ID=38de4a3d8e23,IPADDR=172.17.56.8,STATUS=running,CPU=2,MEMORY=4,GPU=2
#
function getRecordDockerName()
{

	if [ $# -ne 1 ]; then
		echo "Usage: $0 <record string>" >&2
		return 1
	fi

	#echo $1 | awk -F, '{split($4,arr,"=");printf("%s\n",arr[2])}'
	echo $1 | grep -o 'NAME=[^,]*' | cut -d'=' -f2
	return 0
}

#
# get docker GPU IDs from record
# Usage: $0 <single record string>
# new record format: YPE=worker,INDEX=0,NODE=gpunode1,NAME=736_gpunode1_worker_0,GPUIDS=0:1,ID=38de4a3d8e23,IPADDR=172.17.56.8,STATUS=running,CPU=2,MEMORY=4,GPU=2
#
function getRecordDockerGPUIDs()
{

	if [ $# -ne 1 ]; then
		echo "Usage: $0 <record string>" >&2
		return 1
	fi

	#echo $1 | awk -F, '{split($5,arr,"=");printf("%s\n",arr[2])}'
	echo $1 | grep -o 'GPUIDS=[^,]*' | cut -d'=' -f2
	return 0
}

function getRecordDockerAddr()
{

	if [ $# -ne 1 ]; then
		echo "Usage: $0 <record string>" >&2
		return 1
	fi

	#echo $1 | awk -F, '{split($7,arr,"=");printf("%s\n",arr[2])}'
	echo $1 | grep -o 'IPADDR=[^,]*' | cut -d'=' -f2
	return 0
}
#
# filter specified role records to file from all records
# Usage: $0 <role name> <full record file> <roled record file>
#
function filterRecordsOfRole()
{
	if [ $# -ne 3 ]; then
		echo "Usage: $0 <role name> <full record file> <roled record file>" >&2
		return 1
	fi

	local MATCHSTR="TYPE=$1"
	#cat $2 | awk -F, -v MATCH=$MATCHSTR '{if ($1 == MATCH) {print $0}}' > $3
	cat $2 | grep  $MATCHSTR > $3
	return 0
}


#
# exec all roled docker remotely by ssh
# Usage: $0 <user name> <role name> <docker record filepath> <python code> <ps hosts> <worker hosts> <log dir> <job id> <python args> <work path> <horovod>
#
function RemoteExecRoledDocker()
{
	if [ $# -ne 14 ]; then
		echo "Usage: $0 <user name> <role name> <docker record filepath> <python code> <ps hosts> <worker hosts> <log dir> <job id> <python args> <work path> <horovod> <accelerator type> <elastic> <min np number>" >&2
		return 1
	fi

	local role_num=$(getRoledDockerNumber $2 $3)
	if [ $DEBUG_MODE -eq 1 ]; then
		echo "role_num=$role_num"
	fi
	local role_id_max=$(expr ${role_num} - 1)
	local userName=$1
	local rolename=$2
	local logdir=$7
	local jobid=$8
	local python_args="${9}"
	local work_path="${10}"
	local hvd="${11}"
	local elastic="${13}"
	local min_np_numbers="${14}"
	local hvd_args=""
	local accelerator_type="${12}"
	local linex=""
	local task_index=""
	local docker_node=""
	local docker_name=""
	local docker_gpuids=""
	local docker_logfile=""
	local docker_addr=""

	if [ "$docker_gpuids" = "" ];then
                docker_gpuids='-'
        fi
        if [ "$python_args" = "" ];then
                python_args='-'
        fi
	if [ $hvd -eq 1 ] && [ $elastic -eq 0 ];then
	#计算Horovod启动参数
	#===============================================================	
		local np=0
		local Hv=""
		local hvd_args=""
		for i in $(seq 1 ${role_num})
		do
			let "TOTALJOBS+=1"
                        if [ $DEBUG_MODE -eq 1 ]; then
                                echo
                                echo "TOTALJOBS updated to $TOTALJOBS"
                                echo
                        fi
			cal=1
			H=""
			linex=$(sed -n "${i}p" $3)
			task_index=$(getRecordTaskId $linex)
      docker_node=$(getRecordDockerNode $linex)
			docker_name=$(getRecordDockerName $linex)
			docker_logfile=$(getRoledTaskLogfile $logdir $jobid $rolename ${task_index})
			echo 'Horovod 输出在最后一个worker的log '>>${docker_logfile}
			docker_gpuids=$(getRecordDockerGPUIDs $linex)
			docker_gpuids=$(echo ${docker_gpuids} | sed 's/:/,/g')
			docker_addr=$(getRecordDockerAddr $linex)
			#calculate number of GPUs per docker
            length=${#docker_gpuids}
			for ((i=0; i<length; i++))
				do
				substr="${docker_gpuids:i:i}"
				if [ "$substr" = "," ];then
					cal=$((${cal} + 1))
				fi
			done
                	np=$(($np + $cal))
                	#put the addr into hvd_args.
                	Hv=$Hv$docker_addr:$cal,
        	done
        	Hv=${Hv%%,}
        	hvd_args="-np $np -H $Hv"
	#=================================================================
				
		ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/tensorflow/tensorflow_run.sh ${docker_name} ${1} ${4} ${5} ${6} ${2} ${task_index} \"${docker_gpuids}\" \"${python_args}\" \"${work_path}\" \"${hvd_args}\" \"${hvd}\" \"${accelerator_type}\" >& ${docker_logfile} &
		#echo " debug --- ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/tensorflow/tensorflow_run.sh ${docker_name} ${1} ${4} ${5} ${6} ${2} ${task_index} ${docker_gpuids} ${python_args} ${work_path}  ${hvd_args} ${hvd} >& ${docker_logfile} &"
		exec_pid=$!
                if [ "${rolename}" == "${WORKER_ROLE_NAME}" ]; then
                        if [ -z "${WORKER_EXEC_PIDS}" ]; then
                                WORKER_EXEC_PIDS="${exec_pid}"
                        else
                                WORKER_EXEC_PIDS="${WORKER_EXEC_PIDS},${exec_pid}"
                        fi
                else
                        echo "unknown role name : ${rolename}"
                fi
                sleep 1
		  
	elif [ $hvd -eq 1 ] && [ $elastic -eq 1 ];then
	    local np=0
      	    local Hv=""
            local hvd_args=""
            for i in $(seq 1 ${role_num})
                do
                let "TOTALJOBS+=1"
                if [ $DEBUG_MODE -eq 1 ]; then
                    echo
                    echo "TOTALJOBS updated to $TOTALJOBS"
                    echo
                fi
        cal=1
        linex=$(sed -n "${i}p" $3)
        task_index=$(getRecordTaskId $linex)
        docker_node=$(getRecordDockerNode $linex)
        docker_name=$(getRecordDockerName $linex)
        docker_logfile=$(getRoledTaskLogfile $logdir $jobid $rolename ${task_index})
        echo 'Horovod 输出在最后一个worker的log '>>${docker_logfile}
        docker_gpuids=$(getRecordDockerGPUIDs $linex)
        docker_gpuids=$(echo ${docker_gpuids} | sed 's/:/,/g')
        docker_addr=$(getRecordDockerAddr $linex)
        #calculate number of GPUs per docker
        length=${docker_gpuids}
        for ((i=0; i<length; i++))
          do
          substr="${docker_gpuids:i:i}"
          if [ "$substr" = "," ];then
            cal=$((${cal} + 1))
          fi
        done
        np=$(($np + $cal))
      done
      hvd_args=" -np ${min_np_numbers} --min-np ${min_np_numbers} --max-np $np --host-discovery-script ${logdir}/slurm_discover_hosts.sh"
    #=================================================================

      ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/tensorflow/tensorflow_run.sh ${docker_name} ${1} ${4} ${5} ${6} ${2} ${task_index} \"${docker_gpuids}\" \"${python_args}\" \"${work_path}\" \"${hvd_args}\" \"${hvd}\" \"${accelerator_type}\" >& ${docker_logfile} &
      #echo " debug --- ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/tensorflow/tensorflow_run.sh ${docker_name} ${1} ${4} ${5} ${6} ${2} ${task_index} ${docker_gpuids} ${python_args} ${work_path}  ${hvd_args} ${hvd} >& ${docker_logfile} &"
      exec_pid=$!
                  if [ "${rolename}" == "${WORKER_ROLE_NAME}" ]; then
                          if [ -z "${WORKER_EXEC_PIDS}" ]; then
                                  WORKER_EXEC_PIDS="${exec_pid}"
                          else
                                  WORKER_EXEC_PIDS="${WORKER_EXEC_PIDS},${exec_pid}"
                          fi
                  else
                          echo "unknown role name : ${rolename}"
                  fi
                  sleep 1

	else
		for i in $(seq 1 ${role_num})
        	do
                	linex=$(sed -n "${i}p" $3)
                	task_index=$(getRecordTaskId $linex)
                	docker_node=$(getRecordDockerNode $linex)
                	docker_name=$(getRecordDockerName $linex)
                	docker_logfile=$(getRoledTaskLogfile $logdir $jobid $rolename ${task_index})
                	docker_gpuids=$(getRecordDockerGPUIDs $linex)
                	docker_gpuids=$(echo ${docker_gpuids} | sed 's/:/,/g')

        	if [ "$docker_gpuids" = "" ];then
            		docker_gpuids='-'
        	fi
		if [ "$hvd_args" = "" ];then
                        hvd_args='-'
                fi

                	if [ $DEBUG_MODE -eq 1 ]; then
                        	echo "task_index:$task_index"
                        	echo "docker_node:$docker_node"
                        	echo "docker_name:$docker_name"
                        	echo "docker_gpuids:$docker_gpuids"
                        	echo "docker_logfile:$docker_logfile"

                        # Arguments of tensorflow_run.sh: <docker name> <user name> <python code> <ps hosts> <worker hosts> <job name> <task id> <GPU IDs> <python_args>"
                        	echo "ssh $SOTHISAI_HOME/scripts/scheduler/slurm/tensorflow/tensorflow_run.sh ${docker_name} ${1} ${4} ${5} ${6} ${2} ${task_index} ${docker_gpuids} \"${python_args}\" \"${work_path}\" >& ${docker_logfile}"
                	fi
                	ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/tensorflow/tensorflow_run.sh ${docker_name} ${1} ${4} ${5} ${6} ${2} ${task_index} \"${docker_gpuids}\" \"${python_args}\" \"${work_path}\" \"${hvd_args}\" \"${hvd}\" \"${accelerator_type}\" >& ${docker_logfile} &

                	exec_pid=$!
                	if [ "${rolename}" == "${PS_ROLE_NAME}" ]; then
                        	if [ -z "${PS_EXEC_PIDS}" ]; then
                                	PS_EXEC_PIDS="${exec_pid}"
                        	else
                                	PS_EXEC_PIDS="${PS_EXEC_PIDS},${exec_pid}"
                        	fi
                	elif [ "${rolename}" == "${WORKER_ROLE_NAME}" ]; then
                        	if [ -z "${WORKER_EXEC_PIDS}" ]; then
                                	WORKER_EXEC_PIDS="${exec_pid}"
                        	else
                                	WORKER_EXEC_PIDS="${WORKER_EXEC_PIDS},${exec_pid}"
                        	fi
                	else
                        	echo "unknown role name : ${rolename}"
                	fi

                	let "TOTALJOBS+=1"
                	if [ $DEBUG_MODE -eq 1 ]; then
                        	echo
                        	echo "TOTALJOBS updated to $TOTALJOBS"
                        	echo
                	fi
                	sleep 1
        	done
	fi
	if [ $DEBUG_MODE -eq 1 ]; then
        #echo "$docker_logfile"
        if [ "$rolename" == "$PS_ROLE_NAME" ]; then
            echo "PS_EXEC_PIDS=$PS_EXEC_PIDS"
        elif [ "$rolename" == "$WORKER_ROLE_NAME" ]; then
            echo "WORKER_EXEC_PIDS=$WORKER_EXEC_PIDS"
        fi
    fi
}

#
# transform characters to upper type
# Usage: getUpperCharacter <strings>
#
function getUpperCharacter()
{
        if [ $# -ne 0 ]; then
        	if which tr > /dev/null 2>&1; then
        	        echo "$*" | tr 'a-z' 'A-Z'
        	else
        	        typeset -u mychar
        	        mychar="$*"
        	        echo $mychar
        	fi
        fi
        return 0
}

#
# chech whether non-running job exist
# Usage: $0 <job total> <file saved all jobs>
#
function hasJobExited()
{

	if [ $# -ne 2 ]; then
		echo "Usage: $0 <job total> <file saved all jobs>"
		return 1
	fi

	local jobTotal=${1}
	local jobExist=$(cat $2 | wc -l)

	if [ $DEBUG_MODE -eq 1 ]; then
		echo "jobTotal:$jobTotal"
		cat $2
		echo
	fi

	if [ ${jobExist} -lt ${jobTotal} ]; then
		if [ $DEBUG_MODE -eq 1 ]; then
			echo "some job has exist"
		fi
		return 0
	fi

	leftJobs=$(cat $2 | awk '{if ($2 != "Running") print $0}')
	if [ $DEBUG_MODE -eq 1 ]; then
		echo "leftJobs:"
		echo "$leftJobs"
	fi
	if [ ! -z "$leftJobs" ]; then
		if [ $DEBUG_MODE -eq 1 ]; then
			echo "find jobs not running"
		fi
		return 0
	fi
	return 1
}

# 
# Waitting for job to exit (not running or disappeared)
# Usage: $0 <total job number>
#
function waitForJobExit()
{
	if [ $# -ne 1 ]; then
		echo "Usage: <total job number>"
		return 1
	fi
	local jobTotal=$1
	local jobTemp=/tmp/bashjobs.$$
	while true
	do
		jobs > ${jobTemp}

		if hasJobExited $jobTotal $jobTemp; then
			break
		fi
		if [ $DEBUG_MODE -eq 1 ]; then
			echo "jobs not running:"
			echo "$leftJobs"
		fi
		sleep 1
	done
	test -f $jobTemp && rm -f $jobTemp
	return 0

}

#
# chech whether all procs exited
# Usage: $0 <comma-separated procs>
#
function allProcsExited()
{

	if [ $# -ne 1 ]; then
		echo "Usage: $0 <comma-separated procs>"
		return 1
	fi

	local allpids=$(echo $1 | sed 's/,/ /g')
	echo "allProcsExited, all pids: $allpids"
	for pidx in $allpids
	do
		if kill -0 ${pidx} 2>/dev/null; then
			if [ $DEBUG_MODE -eq 1 ]; then
				echo "    Proc ${pidx} is running"
			fi
			return 1
		fi
	done
	if [ $DEBUG_MODE -eq 1 ]; then
		echo "allProcsExited, all procs exited"
	fi
	return 0
}

#
# chech whether procs had exited
# Usage: $0 <comma-separated procs>
#
function hasProcsExited()
{

	if [ $# -ne 1 ]; then
		echo "Usage: $0 <comma-separated procs>"
		return 1
	fi

	local allpids=$(echo $1 | sed 's/,/ /g')
	if [ $DEBUG_MODE -eq 1 ]; then
		echo "hasProcsExited, all pids: $allpids"
	fi
	for pidx in $allpids
	do
		if ! kill -0 ${pidx} 2>/dev/null; then
			if [ $DEBUG_MODE -eq 1 ]; then
				echo "  Proc ${pidx} is exited"
			fi
			return 0
		fi
	done
	if [ $DEBUG_MODE -eq 1 ]; then
		echo "hasProcsExited, all procs is running"
	fi
	return 1
}

# 
# Waitting for TF job to exit
# Usage: $0 <comma-separated ps procs> <comma-separated worker procs>
#
function waitForTFExit()
{
	if [ $# -ne 3 ]; then
		echo "Usage:  <comma-separated ps procs> <comma-separated worker procs> <hvd>"
		return 1
	fi
	local psProcs=$1
	local workerProcs=$2
	local hvd=$3
	if [ $hvd -eq 0 ]; then
		while true
		do
			#if hasProcsExited ${psProcs} || allProcsExited ${workerProcs}; then
			if hasProcsExited ${psProcs} || hasProcsExited ${workerProcs}; then
				break
			fi
			if [ $DEBUG_MODE -eq 1 ]; then
				echo "TF should continue to run."
			fi
			sleep 5
		done
	else
		while true
		do
			if hasProcsExited ${workerProcs}; then
				break
			fi
			if [ $DEBUG_MODE -eq 1 ]; then
                        	echo "TF should continue to run."
                	fi
			sleep 5
		done
	fi
	echo "TF should be stopped."
	return 0

}

#
# exec all dockers in specified docker record file remotely by ssh
# Usage: $0 <SLURM_JOB_USER> <job id> <docker record filepath> <comma-sperated role list> <python code> <log dir> <python args>
#
function RemoteExecAllDocker()
{
	#echo "==============, all args: $*"
	if [ $# -ne 12 ]; then
                echo "Usage: $0 <user name> <job id> <docker record filepath> <comma-sperated role list> <python code> <log dir> <python args> <work path> <is horovod> <accelerator type> <elastic> <min np number>" >&2
                return 1
        fi

	local jobid=${2}
	local docker_file=${3}
	#echo "===========, docker_file=${docker_file}"
	local hvd="${9}"
	local elastic="${11}"
	local min_np_numbers="${12}"
	local PSMATCH="ps"
	local PSHOSTS=$(getRoledDockerAllAddrs ${PSMATCH} ${docker_file} ${hvd} )
	if [ "$PSHOSTS" = "" ]; then
		PSHOSTS="-"
	fi
	local WKMATCH="worker"
	local WKHOSTS=$(getRoledDockerAllAddrs ${WKMATCH} ${docker_file} ${hvd} )

	if [ $DEBUG_MODE -eq 1 ]; then
		echo "PSHOSTS:$PSHOSTS"
		echo "WKHOSTS:$WKHOSTS"
	fi

	local PS_EXEC_PIDS=""
	local WORKER_EXEC_PIDS=""
	local python_code=${5}
	local logdir=${6}
	local python_args="${7}"
	local work_path="${8}"
	local accelerator_type="${10}"

	export TOTALJOBS=0

	for rolex in `echo $4 | sed 's/,/ /g'`
	do
		if [ $DEBUG_MODE -eq 1 ]; then
			echo "  -> rolex: $rolex"
		fi

		TEMP_FILE=${logdir}/${rolex}.${jobid}

		filterRecordsOfRole ${rolex} ${docker_file} ${TEMP_FILE}
		RemoteExecRoledDocker ${1} ${rolex} ${TEMP_FILE} ${python_code} ${PSHOSTS} ${WKHOSTS} ${logdir} ${jobid} "${python_args}" "${work_path}" "${hvd}" "${accelerator_type}" "${elastic}" "${min_np_numbers}"
	done

	if [ $DEBUG_MODE -eq 1 ]; then
		echo "final PS_EXEC_PIDS=$PS_EXEC_PIDS"
		echo "final WORKER_EXEC_PIDS=$WORKER_EXEC_PIDS"
	fi
	
        if [[ -z $PS_EXEC_PIDS ]]; then
                PS_EXEC_PIDS="-"
        fi
	waitForTFExit ${PS_EXEC_PIDS} ${WORKER_EXEC_PIDS} ${hvd}
	return 0

}


#
# exec all dockers in specified docker record file remotely by ssh
# Usage: $0 <SLURM_JOB_USER> <job id> <docker record filepath> <comma-sperated role list> <python code> <log dir> <python args>
#
function RemoteExecTFTuningDocker()
{
	if [ $# -ne 9 ]; then
		echo "Usage: $0 <user name> <job id> <docker record filepath> <comma-sperated role list> <python code> <log dir> <mongo db name> <work path> <accelerator type>" >&2
		return 1
	fi

    local username=${1}
	local jobid=${2}
	local docker_file=${3}
    local rolelist=${4}
	local MASTER_EXEC_PIDS=""
	local WORKER_EXEC_PIDS=""
	local python_code=${5}
	local logdir=${6}
	local mongo_db_name="${7}"
    local work_path="${8}"
	export TOTALJOBS=0

	for rolex in `echo $rolelist | sed 's/,/ /g'`
	do
		if [ $DEBUG_MODE -eq 1 ]; then
			echo "  -> rolex: $rolex"
		fi
		TEMP_FILE=${logdir}/${rolex}.${jobid}
		filterRecordsOfRole ${rolex} ${docker_file} ${TEMP_FILE}
        
        
        testLogfile2=/tmp/test2.log
        echo "RemoteExecTFTuningRoledDocker ${username} ${rolex} ${TEMP_FILE} ${python_code} ${logdir} ${jobid} \"${mongo_db_name}\" \"${work_path}\"" > $testLogfile2
		RemoteExecTFTuningRoledDocker ${username} ${rolex} ${TEMP_FILE} ${python_code} ${logdir} ${jobid} "${mongo_db_name}" "${work_path}" "${accelerator_type}"
	done

	if [ $DEBUG_MODE -eq 1 ]; then
		echo "final MASTER_EXEC_PIDS=$MASTER_EXEC_PIDS"
		echo "final WORKER_EXEC_PIDS=$WORKER_EXEC_PIDS"
	fi

	waitForTFExit ${MASTER_EXEC_PIDS} ${WORKER_EXEC_PIDS} 0
	return 0
}

#
# exec all roled docker remotely by ssh
# Usage: $0 <user name> <role name> <docker record filepath> <python code> <ps hosts> <worker hosts> <log dir> <job id> <python args>
#
function RemoteExecTFTuningRoledDocker()
{
	if [ $# -ne 9 ]; then
		echo "Usage: $0 <user name> <role name> <docker record filepath> <python code> <log dir> <job id> <mongo db name> <work path> <accelerator type>" >&2
		return 1
	fi
	local role_num=$(getRoledDockerNumber $2 $3)
	if [ $DEBUG_MODE -eq 1 ]; then
		echo "role_num=$role_num"
	fi
	local role_id_max=$(expr ${role_num} - 1)
	local userName=$1
	local rolename=$2
    local python_code="${4}"
	local logdir=$5
	local jobid=$6
	local mongo_db_name="${7}"
    local work_path="${8}"

	local linex=""
	local task_index=""
	local docker_node=""
	local docker_name=""
	local docker_gpuids=""
	local docker_logfile=""

	for i in $(seq 1 ${role_num})
	do
		linex=$(sed -n "${i}p" $3)
		#echo "linex:$linex"

		task_index=$(getRecordTaskId $linex)
		docker_node=$(getRecordDockerNode $linex)
		docker_name=$(getRecordDockerName $linex)
		docker_logfile=$(getRoledTaskLogfile $logdir $jobid $rolename ${task_index})

		docker_gpuids=$(getRecordDockerGPUIDs $linex)
		docker_gpuids=$(echo ${docker_gpuids} | sed 's/:/,/g')

        if [ "$docker_gpuids" = "" ];then
            docker_gpuids='-'
        fi
        
		if [ $DEBUG_MODE -eq 1 ]; then
			echo "task_index:$task_index"
			echo "docker_node:$docker_node"
			echo "docker_name:$docker_name"
			echo "docker_gpuids:$docker_gpuids"
			echo "docker_logfile:$docker_logfile"
			# Arguments of tensorflow_run.sh: <docker name> <user name> <python code> <ps hosts> <worker hosts> <job name> <task id> <GPU IDs> <mongo_db_name>"
			echo "ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/tensorflow/tensorflow_tuning_run.sh ${docker_name} ${docker_gpuids} \"${mongo_db_name}\" \"${work_path}\" \"${TASK_PATH}\" >& ${docker_logfile}"
		fi
		ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/tensorflow/tensorflow_tuning_run.sh ${docker_name} ${docker_gpuids} \"${mongo_db_name}\" \"${work_path}\" \"${TASK_PATH}\" \"${userName}\"  \"${accelerator_type}\" >& ${docker_logfile} &

		exec_pid=$!
		if [ "${rolename}" == "${MASTER_ROLE_NAME}" ]; then
			if [ -z "${MASTER_EXEC_PIDS}" ]; then
				MASTER_EXEC_PIDS="${exec_pid}"
			else
				MASTER_EXEC_PIDS="${MASTER_EXEC_PIDS},${exec_pid}"
			fi
		elif [ "${rolename}" == "${WORKER_ROLE_NAME}" ]; then
			if [ -z "${WORKER_EXEC_PIDS}" ]; then
				WORKER_EXEC_PIDS="${exec_pid}"
			else
				WORKER_EXEC_PIDS="${WORKER_EXEC_PIDS},${exec_pid}"
			fi
		else
			echo "unknown role name : ${rolename}"
		fi

		let "TOTALJOBS+=1"
		if [ $DEBUG_MODE -eq 1 ]; then
			echo
			echo "TOTALJOBS updated to $TOTALJOBS"
			echo
		fi
		sleep 1
	
	done

	if [ $DEBUG_MODE -eq 1 ]; then
		if [ "$rolename" == "$MASTER_ROLE_NAME" ]; then
			echo "MASTER_EXEC_PIDS=$MASTER_EXEC_PIDS"
		elif [ "$rolename" == "$WORKER_ROLE_NAME" ]; then
			echo "WORKER_EXEC_PIDS=$WORKER_EXEC_PIDS"
		fi
	fi
}


function tf_main_test()
{

	ps_number=3
	ps_cpu_number=2
	ps_gpu_enable=0
	ps_gpu_number=0
	ps_ramMB=4096

	worker_number=4
	worker_cpu_number=1
	worker_gpu_enable=1
	worker_gpu_number=1
	worker_ramMB=2048

	initDistributeEnv

	echo "haha, nodes=$GPU_NODE_NUM"
	for i in `seq 1 $GPU_NODE_NUM`
	do
		echo "======== $i ========"
		allTaskIndex=$(getTaskIndexsOfNodeX $i $GPU_NODE_NUM $ps_number)
		echo "node[$i] ps allIndex=${allTaskIndex}"
		allTaskIndex=$(getTaskIndexsOfNodeX $i $GPU_NODE_NUM $worker_number)
		echo "node[$i] worker allIndex=${allTaskIndex}"
	done
}


# ----------------------------------------
#tf_main_test
#exit 0




