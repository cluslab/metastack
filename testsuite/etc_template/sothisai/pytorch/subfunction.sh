#! /bin/bash
SLURM_INSTALL_PATH=/opt/gridview/slurm
source $SLURM_INSTALL_PATH/etc/sothisai/basic_env.sh

#
# move variable below to basic_env
#
# max gpu configured per node
#export MAX_GPUS_PER_NODE=2

# max time to wait file is 5 min
export MAX_WAIT_TIME=600

# name of parameter server
export WORKER_ROLE_NAME="worker"

function log_logfile(){
    if [ ! -z "${LOGFILE}" ];then
        echo -e "$1" | tee -ia  $LOGFILE
    else
        echo -e "$1"
    fi
}

function initDistributeEnv()
{

	#
	# We recommand that
	# each worker must use 1 GPU
	#
	if [ $worker_gpu_enable -ge 1 ] && [ $worker_gpu_number -gt $MAX_GPUS_PER_NODE ]; then
		log_logfile "****** GPUs of worker is too large (Req: $worker_gpu_number, Configured: $MAX_GPUS_PER_NODE)" >> ${LOGFILE}
		return 1
	fi

	TOTAL_TASKS=${worker_number}

	local wkTotalCPUs=`expr $worker_number \* $worker_cpu_number`
	TOTAL_CPUS=${wkTotalCPUs}

    local wkTotalGPUS=0
	if [ $worker_gpu_enable -ge 1 ]; then
		wkTotalGPUS=`expr $worker_number \* $worker_gpu_number`
	fi
	TOTAL_GPUS=${wkTotalGPUS}
    # ============================
    # Dispatch
	local num=0
	local left=0

	MAX_WORKER_PER_NODE=`expr $MAX_GPUS_PER_NODE / $worker_gpu_number`
	if [ $worker_number -le $MAX_WORKER_PER_NODE ]; then
		WORKER_PER_NODE=$worker_number
		WORKER_NODE_NUM=1
	else
		iii=$MAX_WORKER_PER_NODE
		while((iii>=1))
		do
		#	echo "$iii"
			num=`expr $worker_number / $iii`
			left=`expr $worker_number % $iii`
		#	echo "  -> number: $num, left: $left"
			if [ $left -eq 0 ]; then
				WORKER_PER_NODE=$iii
				WORKER_NODE_NUM=$num
				break
			fi
			let "iii-=1"
		done
	fi

	WK_GPU_PER_NODE=`expr $WORKER_PER_NODE \* $worker_gpu_number`
	WK_CPU_PER_NODE=`expr $WORKER_PER_NODE \* $worker_cpu_number`
	WK_MEM_PER_NODE=`expr $WORKER_PER_NODE \* $worker_ramMB`



	MIN_CPUS=${WK_CPU_PER_NODE}

	CPU_PER_NODE=${WK_CPU_PER_NODE}
	GPU_PER_NODE=${WK_GPU_PER_NODE}
	MEM_PER_NODE=${WK_MEM_PER_NODE}

	GPU_NODE_NUM=${WORKER_NODE_NUM}
	JOB_NODE_NUM=${WORKER_NODE_NUM}

	if [ $DEBUG_MODE -eq 1 ]; then
        log_logfile ""
        log_logfile "TASK_PATH: $TASK_PATH"
        log_logfile "TOTAL_TASKS: $TOTAL_TASKS"
        log_logfile "wkTotalCPUs: $wkTotalCPUs"
        log_logfile "TOTAL_CPUS: $TOTAL_CPUS"
        log_logfile "wkTotalGPUS: $wkTotalGPUS"
        log_logfile "TOTAL_GPUS: $TOTAL_GPUS"
        log_logfile "MAX_GPUS_PER_NODE: $MAX_GPUS_PER_NODE"
        log_logfile ""
        log_logfile "==== Resource per worker ===="
        log_logfile "worker_number: $worker_number"
        log_logfile "worker_gpu_enable: $worker_gpu_enable"
        log_logfile "worker_gpu_number: $worker_gpu_number"
        log_logfile "worker_cpu_number: $worker_cpu_number"
        log_logfile "worker_ramMB: $worker_ramMB"

        log_logfile "MAX_WORKER_PER_NODE: $MAX_WORKER_PER_NODE"
        log_logfile "WORKER_PER_NODE: $WORKER_PER_NODE"
        log_logfile "WORKER_NODE_NUM: $WORKER_NODE_NUM"
        log_logfile "WK_GPU_PER_NODE: $WK_GPU_PER_NODE"
        log_logfile "WK_CPU_PER_NODE: $WK_CPU_PER_NODE"
        log_logfile "WK_MEM_PER_NODE: $WK_MEM_PER_NODE"
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
        local addrlist=$(cat $2 | awk -F, -v MATCH=$MATCHSTR -v PORT=$port -v TOTAL=${ROLE_NUM} '{
if ($1 == MATCH)
{
        split($2,arr,"=");
        myindex=arr[2]
        split($7,arr,"=");
        addr_port[myindex]=sprintf("%s:%d",arr[2],PORT);
}
} END {
        all_addrs="";
        iii=0;
        while (iii < TOTAL)
        {
                loopindex=sprintf("%s", iii);
                if (length(all_addrs)==0)
                {
                        all_addrs=sprintf("%s", addr_port[loopindex]);
                }
                else
                {
                        all_addrs=sprintf("%s,%s", all_addrs, addr_port[loopindex]);
                }
                iii++
        }
        printf("%s\n", all_addrs)
}')

        echo $addrlist | sed 's/ /,/g'

        return 0
}




function getWorker0Addr()
{
	if [ $# -ne 1 ]; then
		echo "Usage: $0 <docker file >" >&2
		return 1
	fi

    #record format: TYPE=worker,INDEX=0,NODE=gpunode1,NAME=736_gpunode1_worker_0,GPUIDS=0:1,ID=38de4a3d8e23,IPADDR=172.17.56.8,STATUS=running,CPU=2,MEMORY=4,GPU=2
	#local addrlist=$(cat $1 | grep 'INDEX=0' | awk -F',' '{print $7}' | awk -F'=' '{print $2}')
  local addrlist=$(cat $1 | grep 'INDEX=0' | grep -oP 'IPADDR=\K[^,]+')
	echo $addrlist | sed 's/ /,/g'

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
	
	#local MATCHSTR="TYPE=$1"
	total=$(cat $2 | grep -c "TYPE=$1")
	#cat $2 | awk -F, -v MATCH=$MATCHSTR 'BEGIN {total=0} {if ($1 == MATCH) {total+=1}} END{printf("%d\n",total)}'
	echo $total
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
# Usage: $0 <user name> <role name> <docker record filepath> <python code> <worker hosts> <log dir> <job id> <python args>
#
function RemoteExecRoledDocker()
{
	if [ $# -ne 12 ]; then
		echo "Usage: $0 <user name> <role name> <docker record filepath> <python code> <worker0 addr> <log dir> <job id> <python args> <work path> <hvd> <elastic> <min np number>" >&2
		return 1
	fi

	local role_num=$(getRoledDockerNumber $2 $3)
	if [ $DEBUG_MODE -eq 1 ]; then
		echo "role_num=$role_num"
	fi
	local role_id_max=$(expr ${role_num} - 1)
	local userName=$1
	local rolename=$2
	local python_code=$4
	local wk0_addr=$5
	local logdir=$6
	local jobid=$7
	local python_args="${8}"
	local work_path="${9}"
	local hvd=${10}
	local elastic="${11}"
	local min_np_numbers="${12}"
	local hvd_args=""
	
	local linex=""
	local rank=""
	local docker_node=""
	local docker_name=""
	local docker_logfile=""
	local docker_gpuids=""
	local docker_addr=""

	if [ "$docker_gpuids" = "" ];then
                docker_gpuids='-'
        fi
        if [ "$python_args" = "" ];then
                python_args='-'
        fi
	if [ ${hvd} -eq 1 ] && [ ${elastic} -eq 0 ];then
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
        hvd_args="-np $np -p 22 -H $Hv"
		echo ${hvd_args}
		ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/pytorch/pytorch_run.sh ${docker_name} ${python_code} \"${python_args}\" ${task_index} ${role_num} ${wk0_addr} \"${work_path}\" ${userName} \"${hvd_args}\" ${hvd} >& ${docker_logfile} &

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
  elif [ ${hvd} -eq 1 ] && [ ${elastic} -eq 1 ];then
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
            length=${docker_gpuids}
            for ((i=0; i<length; i++))
                do
                substr="${docker_gpuids:i:i}"
                if [ "$substr" = "," ];then
                  cal=$((${cal} + 1))
                fi
            done
                np=$(($np + $cal))
                #put the addr into hvd_args.
            done
            hvd_args=" -np $min_np_numbers --min_np_numbers $min_np_numbers --max_np_numbers $np --host-discovery-script ${logdir}/slurm_discover_hosts.sh"
		echo ${hvd_args}
		ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/pytorch/pytorch_run.sh ${docker_name} ${python_code} \"${python_args}\" ${task_index} ${role_num} ${wk0_addr} \"${work_path}\" ${userName} \"${hvd_args}\" ${hvd} >& ${docker_logfile} &

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
	else
	#first start work0
		local w0_is_start=false
		hvd_args=""
		for i in $(seq 1 ${role_num})
		do
			#linex=$(sed -n "${i}p" $3)
			linex=""
			if [ "$w0_is_start" = false ];then
				linex=$(grep 'INDEX=0' $3)
				w0_is_start=true
			else
				j=$(expr $i - 1)
				linex=$(grep "INDEX=${j}," $3)
			fi
			echo "linex:$linex"
			rank=$(getRecordTaskId $linex)
			docker_node=$(getRecordDockerNode $linex)
			docker_name=$(getRecordDockerName $linex)
			docker_logfile=$(getRoledTaskLogfile $logdir $jobid $rolename ${rank})

			if [ $DEBUG_MODE -eq 1 ]; then
				echo "rank:$rank"
				echo "docker_node:$docker_node"
				echo "docker_name:$docker_name"
				echo "docker_logfile:$docker_logfile"

				echo "ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/pytorch/pytorch_run.sh ${docker_name} ${python_code} ${python_args} ${rank} ${role_num} ${wk0_addr} ${work_path} ${hvd_args} ${hvd} >& ${docker_logfile}"
			fi
			ssh $docker_node $SOTHISAI_HOME/scripts/scheduler/slurm/pytorch/pytorch_run.sh ${docker_name} ${python_code} \"${python_args}\" ${rank} ${role_num} ${wk0_addr} \"${work_path}\" ${userName} \"${hvd_args}\" ${hvd} >& ${docker_logfile} &

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
		if [ "$rolename" == "$WORKER_ROLE_NAME" ]; then
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
# Waitting for Distributed job to exit
# Usage: $0 <comma-separated worker procs>
#
function waitForDisTaskExit()
{
	if [ $# -ne 1 ]; then
		echo "Usage:  <comma-separated worker procs>"
		return 1
	fi
	local workerProcs=$1
	while true
	do
		if hasProcsExited ${workerProcs}; then
			break
		fi
		if [ $DEBUG_MODE -eq 1 ]; then
			echo "Pytorch should continue to run."
		fi
		sleep 5
	done
	echo "Pytorch should be stopped."
	return 0

}

#
# exec all dockers in specified docker record file remotely by ssh
# Usage: $0 <SLURM_JOB_USER> <job id> <docker record filepath> <comma-sperated role list> <python code> <log dir> <python args>
#
function RemoteExecAllDocker()
{
	if [ $# -ne 11 ]; then
		echo "Usage: $0 <user name> <job id> <docker record filepath> <comma-sperated role list> <python code> <log dir> <python args> <work path> <hvd> <elastic> <min_np_numbers>" >&2
		return 1
	fi

    	local user_name=${1}
	local jobid=${2}
	local docker_file=${3}
	local hvd=${9}
	local elastic="${10}"
  local min_np_numbers="${11}"
	echo " "first time" ${hvd}"
	local WK0ADDR=$(getWorker0Addr ${docker_file})
	if [ $DEBUG_MODE -eq 1 ]; then
		echo "WKHOSTS:$WKHOSTS"
	fi
	local WKMATCH="worker"
	#local WKHOSTS=$(getRoledDockerAllAddrs ${WKMATCH} ${docker_file} ${hvd} )
	local WORKER_EXEC_PIDS=""
	local python_code=${5}
	local logdir=${6}
	local python_args="${7}"
   	local work_path="${8}"
	export TOTALJOBS=0

	for rolex in `echo $4 | sed 's/,/ /g'`
	do
		if [ $DEBUG_MODE -eq 1 ]; then
			echo "  -> rolex: $rolex"
		fi
		TEMP_FILE=${logdir}/${rolex}.${jobid}
		filterRecordsOfRole ${rolex} ${docker_file} ${TEMP_FILE}
		RemoteExecRoledDocker ${user_name} ${rolex} ${TEMP_FILE} ${python_code} ${WK0ADDR} ${logdir} ${jobid} "${python_args}" "${work_path}" "${hvd}" "${elastic}" "${min_np_numbers}"

	done

	if [ $DEBUG_MODE -eq 1 ]; then
		echo "final WORKER_EXEC_PIDS=$WORKER_EXEC_PIDS"
	fi

	waitForDisTaskExit ${WORKER_EXEC_PIDS}
	return 0

}





