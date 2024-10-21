#!/bin/bash
function init_distribute_env()
{
	local wk_gpu_enable=$1
	local wk_gpu_number=$2
	local wk_cpu_number=$3
	local wk_ram_size=$4
	local wk_number=$5
	local partition=$6

	#获取队列单节点的最大配置
	for arg in `sinfo -p $partition -N --noheade | awk '{print $1}' | head -n1 | xargs scontrol show node | grep CfgTRES | sed 's/,/ /g'` ;
	do
		if   [[ "$arg" =~ "cpu" ]];then
			MAX_CPUS_PER_NODE=`echo $arg | awk -F= '{print $NF}'`
			#echo $MAX_CPUS_PER_NODE
		elif [[ "$arg" =~ "mem" ]];then
			MAX_MEMS_PER_NODE=`echo $arg | awk -F= '{print $NF}' | awk -FM '{print $1}'`
			#echo $MAX_MEMS_PER_NODE
		elif [[ "$arg" =~ "gres" ]];then
			MAX_GPUS_PER_NODE=`echo $arg | awk -F= '{print $NF}'`
			#echo $MAX_GPUS_PER_NODE
		fi
	done
	
	#获取单节点最大的容器数量
	MAX_WK_PER_NODE_OF_GPU=0
	MAX_WK_PER_NODE_OF_CPU=0
	MAX_WK_PER_NODE_OF_MEM=0
	if [[ ! -z $MAX_GPUS_PER_NODE ]] && [[ $wk_gpu_enable == 'true' ]];then
		MAX_WK_PER_NODE_OF_GPU=`expr $MAX_GPUS_PER_NODE / $wk_gpu_number`
	fi
	#echo "$MAX_WK_PER_NODE_OF_GPU"
	MAX_WK_PER_NODE_OF_CPU=`expr $MAX_CPUS_PER_NODE / $wk_cpu_number`
	#echo "$MAX_WK_PER_NODE_OF_CPU"
	MAX_WK_PER_NODE_OF_MEM=`expr $MAX_MEMS_PER_NODE / $wk_ram_size`
	#echo "$MAX_WK_PER_NODE_OF_MEM"
	if [[ $wk_gpu_enable == 'true' ]];then
		MAX_WK_PER_NODE=`echo -e "${MAX_WK_PER_NODE_OF_GPU}\n${MAX_WK_PER_NODE_OF_CPU}\n${MAX_WK_PER_NODE_OF_MEM}" | sort -nr | tail -n 1`
	else
		MAX_WK_PER_NODE=`echo -e "${MAX_WK_PER_NODE_OF_CPU}\n${MAX_WK_PER_NODE_OF_MEM}" | sort -nr | tail -n 1`
	fi
	if [[ "$MAX_WK_PER_NODE" -lt 1 ]]; then
    # 如果是，则将变量设置为1
    MAX_WK_PER_NODE=1
  fi
	#echo "$MAX_WK_PER_NODE"

	#获取单节点申请的资源配置
	if [[ $MAX_WK_PER_NODE -gt $wk_number ]];then
		MAX_WK_PER_NODE=$wk_number
	fi
	APPLY_CPUS_PER_NODE=`expr $MAX_WK_PER_NODE \* $wk_cpu_number`
	APPLY_GPUS_PER_NODE=`expr $MAX_WK_PER_NODE \* $wk_gpu_number`
	APPLY_MEMS_PER_NODE=`expr $MAX_WK_PER_NODE \* $wk_ram_size`
	
 	#获取申请的节点数量
	DIVIDED_NUM=`expr $wk_number / $MAX_WK_PER_NODE`
	DIVIDED_LEFT=`expr $wk_number % $MAX_WK_PER_NODE`
	if [[ $DIVIDED_LEFT -eq 0 ]];then
		APPLY_NODE_NUM=$DIVIDED_NUM
	else
		APPLY_NODE_NUM=`expr $DIVIDED_NUM + 1`
	fi
	APPLY_TOTAL_CPUS=`expr $wk_number \* $wk_cpu_number`
	WK_TOTAL_NUMBER=${wk_number}
}


function get_task_indexs_of_nodex()
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

        echo "$allIndex"
        return 0
}



