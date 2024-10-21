#! /bin/bash
SLURM_INSTALL_PATH=/opt/gridview/slurm
source $SLURM_INSTALL_PATH/etc/sothisai/basic_env.sh
GRES_CONF=$SLURM_INSTALL_PATH/etc/gres.conf
# not used
export JOB_NODEMEM=""
export JOB_MINMEMNODE=""
export JOB_MINMEMCPU=""
export JOB_NODEGPUS=""
export JOB_NODECPUS=""
# =========

export DOCKER_CPUIDS=""
export DOCKER_GPUIDS=""
export DOCKER_CTRMEM=""

export DOCKER_TMP_PASSWD=""
export DOCKER_TMP_GROUP=""
export DOCKER_TMP_SHADOW=""

#
# debug
#
function ddebug()
{
    if [ $# -ne 2 ]; then
        echo "Usage: ddebug $DEBUG_MODE xxx."
    elif [ $# -eq 2 ]; then 
        if [ $1 -eq 1 ]; then
            echo "Debug:$2"
        fi
    fi
}

function getContainerFile(){
	echo "$1/_container_$2"
    return 0
}

function getHeaderNode(){
    local filter_header_nodes=`echo $SLURM_NODELIST | awk -F'[,]' '{print $1}'`
    local header_nodes_title=`echo $filter_header_nodes | awk -F '[' '{print $1}'`
    local header_nodes_index=`echo $filter_header_nodes | awk -F '[' '{print $2}'`
    local header_node_index=`echo $header_nodes_index | awk -F '[-]' '{print $1}'`
    local header_node="$header_nodes_title$header_node_index"
    echo $header_node
}

function removeDockerInstanceFilePath()
{
    rm -rf  $1/_dockerlist_$2
}


function collectContainerInfoToDockerFile(){
	local total_containers_number=$1
	local task_path=$2
	local task_id=$3
	for((i=1;i<=240;i++));
	do
		distributed_container_number=`ls $task_path | grep ^_container_ | wc -l`
		if [ $distributed_container_number -eq $total_containers_number ];then
			cat $task_path/_container_* >> $task_path/_dockerlist_$task_id
			rm -rf $task_path/_container_*
			break
		else
			sleep 5
		fi
	done	
}
#
# check and make dir as specified user
# Usage: $0 <user name> <dir path>
#        if dir path started with "/", dir is a absolute path. otherwise, it will be maked at user home
#
function checkAndMakeDirOfUser()
{
    if [ $# -ne 2 ]; then
        echo "Usage:  <user name> <dir path>" >&2
        return 1
    fi
    
}

#
# query user home by root
#
function getUserHome()
{
    su - ${1} -c pwd
}

#
# query user home from /etc/passwd
#
function getUserHomeFromPasswd()
{
    #cat /etc/passwd | awk -F: -v UNAME=$1 '{if ($1 == UNAME)print $6}'
    userName=$1
    ( getent passwd "${userName}" || getent passwd | grep "^$userName:") | head -n1 | awk -F: '{print $6}'
}

#
# query user home from /etc/passwd
#
function getTaskPath()
{
    local ushome=`getUserHomeFromPasswd $1`
    if [ -z "$ushome" ]; then
        return 1
    fi

    echo "$ushome/$2/$3/$4/$5"
}

# get appName
function getAppName()
{
    appName=$(echo "$1" | tr 'a-z' 'A-Z' | sed 's/_/-/g' )
    echo "${appName}"
}


#
# query user home from /etc/passwd
#
function getSlurmJobFile()
{
    echo "$1/job_$2_`date +'%Y%m%d_%H%M%S'`"
}


#
# get file env.prolog file related with specified jobid at userhome 
# Usage: getEnvFilePath <jobid> <userhome>
#
function getEnvFilePath()
{
    #grep -rw "JOBID=$1" $2/env.prolog_* | awk -F: '{print $1}'
    local allenv=`ls -t $2/job_env/env.prolog_* 2>/dev/null`
    if [ $? -eq 0 ]; then
        for i in $allenv
        do
            if grep -rw "JOBID=$1" $i >/dev/null 2>&1; then
                echo "$i"
                exit 0
            else
            #    echo "$iii not match"
                :
            fi
        done
    fi
    exit 1
}

#
# get get Instance User Env
# Usage: getInstanceUserEnv <user_name>
#
function getInstanceUserEnv()
{
    userName=$1
    (getent passwd | grep "^${userName}:"  || getent passwd "${userName}") | head -n1 | awk -F: '{print " -e NB_UID="$3, "-e NB_GID="$4}'
}

#
# generate file path of docker instance list
# Usage: getDockerInstanceFilePath <TASK_PATH> <SLURM_JOB_ID>
#
function getDockerInstanceFilePath()
{
    echo "$1/_dockerlist_$2"
    return 0
}

#
# generate file path of docker instance list
# Usage: getSlurmPrologFilePath <TASK_PATH> <SLURM_JOB_ID> <SLURMD_NODENAME>
#
function getSlurmPrologFilePath()
{
    echo "$1/prolog.env.${2}.${3}"
    return 0
}

#
# generate file path of task message
# Usage: getMessagePath <TASK_PATH>
#
function getTaskMessagePath(){
    echo "$1/task.message"
    return 0
}

function getPrepareContainerLogPath()
{
   echo "$1/prepare_container"
   return 0
}


function log_message {
  echo "$1" |tee -ia $MESSAGEPATH
}

#
# get docker instance name for single node job
#     used by slurm.prolog/slurm.epilog/slurm.job
#
function getDockerName()
{
    if [ ! -z "$SLURMD_NODENAME" ] && [ ! -z "$SLURM_JOB_ID" ]; then
        echo "${SLURM_JOB_ID}_${SLURMD_NODENAME}"
        return 0
    elif [ $# -eq 2 ]; then
        echo "${1}_${2}"
        return 0
    fi
    return 1
}

#
# get docker instance name for distribute job
#     used by slurm.prolog/slurm.epilog/slurm.job
#     Usage: getDistributeDockerName <jobid> <nodename> <rolename> <taskindex>
#
function getDistributeDockerName()
{
    if [ $# -eq 4 ]; then
        echo "${1}_${2}_${3}_${4}"
        return 0
    fi
    return 1
}

#
# geet jobss exechost
#     Usage: getJobExecNodes <jobid>
#     Output: gv[21,139],node1
#
function getJobExecNodes()
{
        local RET=1
        if [ $# -eq 1 ]; then
                local squelog=/tmp/.squeue.log.$$
                local squeerr=/tmp/.squeue.err.$$
                if $SQUEUE --nohead -j $1 2>$squeerr > $squelog; then
                        if [ -s $squelog ]; then
                                cat $squelog | awk '{print $NF}'
                                RET=0
                        fi
                fi
        test -f $squelog && rm -f $squelog
        test -f $squeerr && rm -f $squeerr
        fi
        return $RET
}

#
# get node index at all job nodeslit
#     used by slurm.prolog/slurm.epilog/slurm.job
# Usage: getNodeIndexOfList <jobid> <nodename>
#
function getNodeIndexOfList()
{
        if [ $# -eq 2 ]; then
                if allhosts=`getJobExecNodes $1`; then
                        $SCONTROL show hostnames $allhosts | awk -v tnode=$2 '{if ($1 == tnode) print NR}'
                        return 0
                fi
        fi
        return 0
}

#
# get {job}_{node} string
#     used by slurm.prolog/slurm.epilog/slurm.job
#
function getJobNodeStr()
{
    if [ ! -z "$SLURMD_NODENAME" ] && [ ! -z "$SLURM_JOB_ID" ]; then
        echo "${SLURM_JOB_ID}_${SLURMD_NODENAME}"
        return 0
    elif [ $# -eq 2 ]; then
        echo "${1}_${2}"
        return 0
    fi
    return 1
}

#
# get docker IP address by docker name
#     used by slurm.prolog/slurm.epilog/slurm.job
#
function getDockerIPAddr()
{
    local ipaddr="null"
    local dcinsp=$SPOOL_DIR/ipaddr.$$.$RANDOM
    if [ $# -ne 1 ]; then
        echo "Docker name not specified"
        return 1
    fi
    if /usr/bin/sudo ai_docker inspect $1 > $dcinsp; then
        sed -i '1d' $dcinsp
        sed -i '$d' $dcinsp
        ipaddr=`cat $dcinsp | jq -r .NetworkSettings.Networks.bridge.IPAddress`
        if [ "$ipaddr" != "null" ]; then
            test -f $dcinsp && rm -f $dcinsp
            echo $ipaddr
            return 0
        fi
    fi
    test -f $dcinsp && rm -f $dcinsp
    echo $ipaddr
    return 1
}

#
# get docker Id by docker name
#     used by slurm.prolog/slurm.epilog/slurm.job
#


function getDockerStatus()
{
    dockerstatus="null"
#    local dcinsp=$SPOOL_DIR/dockerinsp.$$.$RANDOM
    dockerstatus=`/usr/bin/sudo ai_docker inspect $1 | jq -r '.[] | .State.Status'`
    if [ "$dockerstatus" != "null" ];then
        echo $dockerstatus
    fi
    return 0
}

# ===========passwd group shadow for container =================
# pubic
function getTempPasswdFile()
{
	local home_dir=$1
	if [ -z $home_dor ];then
		home_dir=`getUserHomeFromPasswd "$SLURM_JOB_USER"`
	fi
        local user_info_dir=$home_dir/.ai_user_info
        echo "$user_info_dir/ai_passwd"
        return 0
}
# pubic
function getTempShadowFile()
{
	local home_dir=$1
        if [ -z $home_dor ];then
                home_dir=`getUserHomeFromPasswd "$SLURM_JOB_USER"`
        fi
        local user_info_dir=$home_dir/.ai_user_info
        echo "$user_info_dir/ai_shadow"
        return 0
}

# pubic
function getTempGroupFile()
{
	local home_dir=$1
        if [ -z $home_dor ];then
                home_dir=`getUserHomeFromPasswd "$SLURM_JOB_USER"`
        fi
        local user_info_dir=$home_dir/.ai_user_info
        echo "$user_info_dir/ai_group"
        return 0
}
#public
function getTempSudoFile()
{
	local home_dir=$1
        if [ -z $home_dor ];then
                home_dir=`getUserHomeFromPasswd "$SLURM_JOB_USER"`
        fi
        local user_info_dir=$home_dir/.ai_user_info
        echo "$user_info_dir/ai_sudoer"
        return 0
}

# pubic
function createTempPasswdFile()
{
    checkAndCreateWorkdir
    DOCKER_TMP_PASSWD=$(getTempPasswdFile)
    if [ ! -f "${DOCKER_TMP_PASSWD}" ]; then
        (getent passwd "${SLURM_JOB_USER}" || getent passwd | grep "^${SLURM_JOB_USER}:") | head -n1 > $DOCKER_TMP_PASSWD
    fi
    return 0
}

# pubic
function createTempShadowFile()
{
    checkAndCreateWorkdir
    DOCKER_TMP_SHADOW=$(getTempShadowFile)
    if [ ! -f "${DOCKER_TMP_SHADOW}" ]; then
        getent shadow | grep "^${SLURM_JOB_USER}:" | head -n1 > $DOCKER_TMP_SHADOW
    fi
    return 0
}

#public
function createTempSudoerFile()
{
    checkAndCreateWorkdir
    DOCKER_TMP_SUDO=$(getTempSudoFile)
    if [ ! -f "${DOCKER_TMP_SUDO}" ]; then
        echo "${SLURM_JOB_USER} ALL=(ALL) NOPASSWD:ALL" > $DOCKER_TMP_SUDO
    fi
    return 0
}

# pubic
function createTempGroupFile()
{
    checkAndCreateWorkdir
    DOCKER_TMP_GROUP=$(getTempGroupFile)
    if [ ! -f "${DOCKER_TMP_GROUP}" ]; then
        items="`echo video render` `id -gn ${SLURM_JOB_USER}`"
        items="`echo "$items" | sort | uniq`"
	for item in $items ;do
		if [ "$item" = "video" ] || [ "$item" = "render" ];then
			username=$SLURM_JOB_USER ; getent group $item | awk -v var=$username -F: '{print $1":"$2":"$3":"var}' >> $DOCKER_TMP_GROUP
		else
			getent group | grep -e "^${item}:" >> $DOCKER_TMP_GROUP
		fi
	done
    fi
    return 0
}

#private
function getUserShadowPasswd()
{
    local dcname="$1"
    if [ ! -z "$SLURM_JOB_USER" ]; then
        dcname=${SLURM_JOB_USER}
    fi
    if [ ! -z "$dcname" ]; then
        cat /etc/shadow | awk -F: -v careduser="$dcname" '
        #BEGIN{
        #    printf("careduser=%s\n",careduser);
        #}
        {
                           if(careduser==$1)
                           {
                print $2
                exit 0
                           }
        }'
        return 0
    fi
    return 1
}


# public
function prepareUserFileNameAndGroupFileName()
{
    if [ -z "${DOCKER_TMP_GROUP}" ]; then
        DOCKER_TMP_GROUP=$(getTempGroupFile)
    fi
    if [ -z "${DOCKER_TMP_PASSWD}" ]; then
        DOCKER_TMP_PASSWD=$(getTempPasswdFile)
    fi
    if [ -z "${DOCKER_TMP_SHADOW}" ]; then
        DOCKER_TMP_SHADOW=$(getTempShadowFile)
    fi
}

function prepareUserFileAndGroupFile()
{
    createTempGroupFile
    createTempPasswdFile
    createTempShadowFile
    createTempSudoerFile
}

function checkAndCreateWorkdir()
{
    local home_dir=`getUserHomeFromPasswd "$SLURM_JOB_USER"`
    local user_info_dir=$home_dir/.ai_user_info
    if [ ! -d $user_info_dir ];then
        mkdir -p $user_info_dir
        chmod 777 $user_info_dir
    fi
}


#
# match node name with line of "Nodes=gv[21,139] CPU_IDs=0-1 Mem=0 GRES_IDX=" or
#      Nodes=b02r1n06 CPU_IDs=6-11 Mem=24576 GRES=gpu(IDX:1)
#      Nodes=b02r1n[07-08] CPU_IDs=0-5 Mem=24576 GRES=gpu(IDX:0)
# $1 ->  file to save detailed job info
# $2 ->  nodename to search
#
# private
function getNodesByNodeName()
{
    if [ $# -eq 2 ] && [ -f $1 ]; then
        local lineno=0
        local matched=0
        local nodereg=`cat $1 | grep "^[[:space:]]*Nodes=" | awk '{print $1}' | awk -F= '{print $2}'`
        for i in $nodereg
        do
            let "lineno++"
            if $SCONTROL show hostnames $i | grep -w $2 > /dev/null; then
                matched=1
                break
            fi
        done
        if [ $matched -eq 1 ]; then
            cat $1 | grep "^[[:space:]]*Nodes=" | sed -n "${lineno}p"
            return 0
        fi
    fi
    return 1
}

#
# SchedulerParameters -> default_gbytes
# DefMemPerNode
# MaxMemPerNode
#
function getMinMemNode()
{
    if [ $# -eq 1 ] && [ -f $1 ]; then
        if cat $1 |grep MinMemoryNode > /dev/null; then
            cat $1 |grep MinMemoryNode | awk '{iii=1; while (iii++<NF) {if (index($iii,"MinMemoryNode")!=0) { split($iii,arr,"="); print arr[2]; break}}}'
            return 0
        fi
    fi
    return 1
}

function getNodeMem()
{
    local MinMN="0"
    if [ $# -eq 1 ] && [ -f $1 ]; then

        #if cat $1 |grep -w "Nodes=$SLURMD_NODENAME" > /dev/null; then
        #    cat $1 |grep -w "Nodes=$SLURMD_NODENAME" | awk '{iii=1; while (iii++<NF) {if (index($iii,"Mem")!=0) { split($iii,arr,"="); print arr[2]; break}}}'
        #    return 0
        #fi
        local nodestr=$(getNodesByNodeName $1 $SLURMD_NODENAME)
        if [ $? -eq 0 ]; then
            echo $nodestr |  awk '{iii=1; while (iii++<NF) {if (index($iii,"Mem")!=0) { split($iii,arr,"="); if (arr[2]~/[a-z]|[A-Z]/) { print arr[2] } else { print arr[2]"M" }; break}}}'
            return 0
        fi
    fi
    return 1
}

#
# NOTE: TODO
# 
# SchedulerParameters -> default_gbytes
# DefMemPerCPU
# MaxMemPerCPU
#
function getCpuMinMem()
{
    local MinMN="0"
    if [ $# -eq 1 ] && [ -f $1 ]; then
        if cat $1 |grep MinMemoryCPU > /dev/null; then
            cat $1 |grep MinMemoryCPU | awk '{iii=1; while (iii++<NF) {if (index($iii,"MinMemoryCPU")!=0) { split($iii,arr,"="); print arr[2]; break}}}'
            return 0
        fi
    fi
    return 1
}

# =============get resource from jobdetail===============
# public
function getFinalNodeGpus()
{
    #GRES_IDX=gpu:K2(IDX:0-1)

    if [ $# -eq 1 ] && [ -f $1 ]; then
        if JOB_NODEGPUS=$(getSpecifiedJobNodeGpus $1 $SLURMD_NODENAME); then
            idsFormat $JOB_NODEGPUS
            return $?
        fi
    fi
    return 1
}
# public
function getSpecifiedJobNodeGpuIDs(){
    # jobdetail_file=$1
    # nodename=$2
    if [ $# -eq 2 ] && [ -f $1 ]; then
        if JOB_NODEGPUS=$(getSpecifiedJobNodeGpus $1 $2); then
            idsFormat $JOB_NODEGPUS
            return $?
        fi
    fi
    return 1
}
# public
function getFinalNodeCpus()
{
    # Return CPU_IDs : 0-1,20-21
    # jobtail_file=$1
    if [ $# -eq 1 ] && [ -f $1 ]; then
        if JOB_NODECPUS=$(getSpecifiedJobNodeCpus $1 $SLURMD_NODENAME); then
            idsFormat $JOB_NODECPUS
            return $?
        fi
    fi
    return 1
}
# public
function getSpecifiedJobNodeCpuIDs()
{
    # Return CPU_IDs : 0-1,20-21
    # jobdetail_file=$1
    # nodename=$2
    if [ $# -eq 2 ] && [ -f $1 ]; then
        if JOB_NODECPUS=$(getSpecifiedJobNodeCpus $1 $2); then
            idsFormat $JOB_NODECPUS
            return $?
        fi
    fi
    return 1
}
# public
function getFinalNodeMem()
{
    if [ $# -eq 1 ] && [ -f $1 ]; then
        DOCKER_CTRMEM=$(getNodeMem $1)
        if [ ! -z "$DOCKER_CTRMEM" ] && [ "$DOCKER_CTRMEM" != "0" ]; then
            echo "$DOCKER_CTRMEM"
            return 0
        fi
        DOCKER_CTRMEM=$(getMinMemNode $1)
        if [ ! -z "$DOCKER_CTRMEM" ] && [ "$DOCKER_CTRMEM" != "0" ]; then
            echo "$DOCKER_CTRMEM"
            return 0
        fi
        DOCKER_CTRMEM=$(calcNodeMemFromCpuMem $1)
        if [ ! -z "$DOCKER_CTRMEM" ] && [ "$DOCKER_CTRMEM" != "0" ]; then
            echo "$DOCKER_CTRMEM"
            return 0
        fi
    fi
    return 1
}

# used by  getFinalNodeMem
# private
function calcNodeMemFromCpuMem()
{
    if [ ! -z "$DOCKER_CTRMEM" ] && [ "$DOCKER_CTRMEM" != "0" ]; then
        echo "$DOCKER_CTRMEM"
        return 0
    fi
    
    if [ -z "$DOCKER_CPUIDS" ]; then
        DOCKER_CPUIDS=$(getFinalNodeCpus $1)
    fi
    
    if [ -z "$DOCKER_CPUIDS" ]; then
        return 1
    fi

    local proc_no=$(echo "$DOCKER_CPUIDS" | awk -F, '{print NF}')

    if [ -z "$JOB_MINMEMCPU" ]; then
        JOB_MINMEMCPU=$(getCpuMinMem $1)
    fi
    
    if [ -z "$JOB_MINMEMCPU" ]; then
        return 1
    fi

    #echo "DOCKER_CPUIDS=$DOCKER_CPUIDS"
    #echo "JOB_MINMEMCPU=$JOB_MINMEMCPU"
    #echo "proc_no=$proc_no"
    local tail_char=${JOB_MINMEMCPU: -1}
    #echo "tail_char=$tail_char"

    local val=${JOB_MINMEMCPU}
    case "$tail_char" in
    [KMGTkmgt])
        val=${JOB_MINMEMCPU%?}
        let "val=val*proc_no"
        DOCKER_CTRMEM="${val}${tail_char}"
        #echo "0 DOCKER_CTRMEM:$DOCKER_CTRMEM"
        ;;
        *)
        let "DOCKER_CTRMEM=val*proc_no"
        #echo "1 DOCKER_CTRMEM:$DOCKER_CTRMEM"
        ;;
        esac
    echo "$DOCKER_CTRMEM"
    return 0
}

# private
# Usage: job_detail_file job_compute_nodename
# return: echo 6-11
#     Nodes=b02r1n06 CPU_IDs=6-11 Mem=24576 GRES=gpu(IDX:1)
#     Nodes=b02r1n[07-08] CPU_IDs=0-5 Mem=24576 GRES=gpu(IDX:0)
function getSpecifiedJobNodeCpus(){
    if [ $# -eq 2 ] && [ -f $1 ]; then
        local job_detail_file=$1
        local nodename=$2
        local node_res_str=$(getNodesByNodeName $job_detail_file $nodename)
        if [ $? -eq 0 ]; then
            echo $node_res_str |  awk '{iii=1; while (iii++<NF) {if (index($iii,"CPU_IDs")!=0) { split($iii,arr,"="); print arr[2]; break}}}'
            return 0
        fi
    fi
    return 1
}

# private
# Usage: job_detail_file job_compute_nodename
# return: echo 6-11
#     Nodes=b02r1n06 CPU_IDs=6-11 Mem=24576 GRES=gpu(IDX:1)
#     Nodes=b02r1n[07-08] CPU_IDs=0-5 Mem=24576 GRES=gpu(IDX:0)
#GRES_IDX=gpu:K2(IDX:0-1)
function getSpecifiedJobNodeGpus()
{

    if [ $# -eq 2 ] && [ -f $1 ]; then
        local job_detail_file=$1
        local nodename=$2
        local node_res_str=$(getNodesByNodeName $job_detail_file $nodename)
        if [ $? -eq 0 ]; then
            echo $node_res_str | awk '{iii=1; while (iii++<NF) {if ((index($iii,"GRES_IDX")!=0)||(index($iii,"GRES")!=0)) { split($iii,arr,"="); len=split(arr[2],idarr,":");split(idarr[len],ids,")"); print ids[1]; break}}}'
            return 0
        fi
    fi
    return 1
}
# private
function idsFormat()
{
    # 1-3 -> node[1-3] -> mulitple lines: 1 2 3 -> 1,2,3
    if [ $# -ne 0 ]; then
        $SCONTROL show hostname node[$1] | sed 's/node//g' | sed ":a;N;s/\n/,/g;ta"
            return 0
    else
            return 1
    fi
}

# =================================================


function getJobNodelist()
{
    if [ $# -ne 1 ]; then
        return 1
    fi
    local jobid=$1
    local nodes=`$SQUEUE -t R -j $jobid | awk 'NR>1{print $8}'`
    if [ -z "$nodes" ]; then
        return 1
    fi
    local nodestr=`$SCONTROL show hostnames $nodes`
    echo "$nodestr"
    return 0
}
# =============================================================
# ==== not used? ====
#
# SchedulerParameters -> default_gbytes
#
function getJobDetailInfo()
{
    if [ $# -eq 2 ]; then
        $SCONTROL --detail show jobs $1 > $2
    fi
    return 1
}

# ==== not used? ====
function getJobNodeResources()
{
    if [ $# -ne 1 ]; then
        return 1
    fi
    local jobid=$1
    local tmpfile=/tmp/jobdetail.${jobid}.$$
    if [ $# -eq 1 ]; then
        getJobDetailInfo $jobid $tmpfile
    fi
    local nodes=$(getJobNodelist $jobid)
    if [  -z "$nodes" ]; then
        return 1
    fi

    local jobnodes=""

    local cpuids=""
    local cpunum=""
    local memnum=""

    for nodex in `echo $nodes`
    do
        SLURMD_NODENAME=$nodex
        cpuids=$(getFinalNodeCpus $tmpfile) 
        cpunum=`echo -n "$cpuids" | awk -F, '{print NF}'`
        memnum=$(getFinalNodeMem $tmpfile)
        if [ -z "$jobnodes" ]; then
            jobnodes="${SLURMD_NODENAME}:${cpunum}:${memnum}"
        else
            jobnodes="${jobnodes},${SLURMD_NODENAME}:${cpunum}:${memnum}"
        fi
    done

    echo "${jobid},${jobnodes}"
    test -f $tmpfile && rm -f $tmpfile
    return 0
}
# =============================================================

#
# get tasks log file depending on logdir/jobid/rolename/index
# Usage:  $0 <logdir> <job id> <role name> <task id>
#
function getRoledTaskLogfile()
{
	if [ $# -ne 4 ]; then
		echo "Usage: $0 <logdir> <job id> <role name> <task id>" >&2
		return 1
	fi
	local logdir=$1
	local jobid=$2
	local rolename=$3
	local index=$4
	echo "${logdir}/${jobid}.${rolename}_${index}.log"
	return 0
}

#
# remove Env Prolog
#     used by epilog
#
function rmEnvProlog()
{
    userhome=$(getUserHomeFromPasswd ${SLURM_JOB_USER})
    echo "userhome=$userhome"
    envprolog=$(getEnvFilePath ${SLURM_JOB_ID} ${userhome})
    echo "envprolog=$envprolog"
    if [ -f ${envprolog} ];then
        rm -f ${envprolog}
    fi
}

function getGpuMemSize()
{
    gpu_mem_size=`echo $(nvidia-smi --query-gpu=gpu_uuid,utilization.gpu,memory.total,memory.used --format=csv 2>/dev/null | sed -n '2p' | awk -F, '{print $3}' | awk -F' ' '{print $1}')`
    if [ ! -z "$gpu_mem_size" ]; then
        gpu_mem_size=${gpu_mem_size}"m"
        echo "$gpu_mem_size"
    fi
    return 0
}

function getMluMemSize()
{
    mlu_mem_size=`echo $(cnmon info  --memory | grep -A1 Physical  | grep Total | head -n1 | awk '{print$3}')`
	if [ ! -z "$mlu_mem_size" ]; then
        mlu_mem_size=${mlu_mem_size}"m"
        echo "$mlu_mem_size"
    fi
    return 0
}

function getDcuMemSize()
{
    smi_command=$(determine_smi_command "rocm-smi" "hy-smi")
    dcu_mem_size=$(echo $($smi_command --showmeminfo vram 2>/dev/null | grep -i 'total memory' | head -n 1 | awk -F: '{print $3/1024/1024}'))
    if [ ! -z "$dcu_mem_size" ]; then
        dcu_mem_size=${dcu_mem_size}"m"
        echo "$dcu_mem_size"
    fi
    return 0
}

# 定义函数以确定使用哪个smi命令
function determine_smi_command() {
    local command_name="$1"  # 传入的命令名称，例如 "rocm-smi"
    local alternative_command="$2"  # 替代命令名称，例如 "hy-smi"

    # 检查传入的命令是否存在
    if command -v "$command_name" &> /dev/null; then
        echo "$command_name"  # 如果存在，返回该命令名称
    else
        echo "$alternative_command"  # 如果不存在，返回替代命令名称
    fi
}



# ++++++++++++++++++++++++++++++++++++++++++++++++++++

# --------------- docker function -------------------
# ******************************************

#
# format Docker Dcu Devices
#     used by prolog
#
#private
function formatDockerDcuDevices()
{
    DC_DEVICES=""
    if [ $# -ne 1 ]; then
        echo ""
    else
        #0,1,2,3
        local dcuidx=$1

        if [ -z "$dcuidx" ];then
            #--device=/dev/kfd --device=/dev/dri/ --env HIP_VISIBLE_DEVICES=-1
            echo ""
        elif [ "$dcuidx" == "-" ];then
            echo ""
        else
            hip_index=0
            hip_indexs=""
            carindexs=""
            for index in `echo $dcuidx | sed 's/,/ /g'`
            do
                   local carindex=`expr $index + 1`
                   local rederDindex=`expr $index + 128`
                   DC_DEVICES=$DC_DEVICES" --device=/dev/dri/card""$carindex"" --device=/dev/dri/renderD""$rederDindex"
                   hip_indexs=$hip_indexs","$hip_index
                   hip_index=$(($hip_index+1))
                   carindexs=$carindexs","$carindex
            done
            hip_indexs=`echo "$hip_indexs" | sed 's/.//'`
            carindexs=`echo "$carindexs" | sed 's/.//'`
            DC_DEVICES=$DC_DEVICES" --env HIP_VISIBLE_DEVICES=$hip_indexs --env HIP_VISIBLE_DEVICES_IDX=$carindexs --device=/dev/kfd --security-opt seccomp=unconfined -v /opt/hyhal:/opt/hyhal "
	    if [ -e /dev/mkfd ];then
                   DC_DEVICES=$DC_DEVICES" --device=/dev/mkfd "
            fi
            echo $DC_DEVICES
        fi
    fi
}

function formatDockerMluDevices()
{
    DC_DEVICES=""
    if [ $# -ne 1 ]; then
        echo ""
    else
        local mluidx=$1
        if [ -z "$mluidx" ];then
            echo ""
        elif [ "$mluidx" == "-" ];then
            echo ""
        else
            DC_DEVICES="-v /usr/bin/cnmon:/usr/bin/cnmon --privileged"
            echo $DC_DEVICES
        fi
    fi
}
function log_prolog_detail()
{
    echo "$1" |tee -ia $LOGFILE
}

#public
##get CUDA_VISIBLE_DEVICE from log file, return a string consists of all GPUIDs visible from CUDA
## if CUDA_VISIBLE_DEVICE=0,1, then string is 0 1
## In mig single case, if CUDA_VISIBLE_DEVICE=66,77, then string is 0:0 0:1
## In mig mix case, if CUDA_VISIBLE_DEVICE=0,1,66,77, then string is 0 1 0:0 0:1
## return 5, if no cuda_visible_dev found or if it's not a mig env
function getCUDAGPUIDs(){
    gpu_ids=""
    ##env
    cuda_devices=`env | grep CUDA_VISIBLE_DEVICE | awk -F= '{print $2}' | sed 's/,/ /g'`
    #return 5, if no cuda_visible_dev found
    if [ -z "$cuda_devices" ]; then
        return 5
    fi
    arr=("$cuda_devices")
    for device_idx in ${arr[@]}
    do
        if [[ -n `cat $GRES_CONF | grep -w "nvidia-cap$device_idx"` ]];then
            ##grep "nvidia-cap$device_idx" from /gres.conf file, if match, search the line above, and print out third and fifth field,ie:GPUID:MIGID
            #for example, suppose gres.conf look like this:
            # GPU 0 MIG 3 /proc/driver/nvidia/capabilities/gpu0/mig/gi11/access
            #NodeName=node01 Name=gpu Type=1g.5gb File=/dev/nvidia-caps/nvidia-cap102
            #
            #if we find the line that matches "nvidia-cap$device_idx" and NodeName="${SLURMD_NODENAME}",
            #then lookup its previous line and find 0:3 as its gpuid
            ids=`cat $GRES_CONF | grep -B1 -w "nvidia-cap$device_idx" | grep -B1 "NodeName=${SLURMD_NODENAME}" | awk  'NR==1 {print $3":"$5}'`
        else
            #return 5, if it's not a mig env
            return 5
        fi
        ##return a string called gpu_ids seperated by spaces
        if [ $device_idx -eq ${arr[${#arr[@]}-1]} ];then
            gpu_ids=$gpu_ids$ids
        else
            gpu_ids=$gpu_ids$ids" "
        fi
    done
    echo "$gpu_ids"
  }
#public
##split CUDA__ARRAY into individual pieces according to the formatted task_index, ie index after function getIndexFromArr
## to use this func, pass your array as the third variable
function splitCUDAGPUIDs(){
    local task_idx_format=$1
    local gpu_used=$2
    local Gpu_arr=($3)
    declare -a arr_new
    local new_idx=0
    local gpu_idx
    #split by formatted task_index,for example, if formatted task_idx is 1 ,and gpu used in this container is 2, then
    #splitted array we want to return is Gpu_arr[2-3]
    for (( gpu_idx=$[ $task_idx_format * $gpu_used ]; gpu_idx<$[ $task_idx_format * $gpu_used + $gpu_used ]; gpu_idx++ ))
    do
        arr_new[new_idx]=${Gpu_arr[gpu_idx]}
        new_idx+=1
    done
    echo ${arr_new[*]}
}
##function returns an array index of an input element
##TASK_INDEXS_arr=(1 3 5)  $(getIndexFromArr 3)  will return 1
function getIndexFromArr(){
    local arr_idx
    local task_idx=$1
    local TASK_INDEXS_arr=($2)
    local TASK_INDEXS_length=$3
    if [ -z "$task_idx" ]; then
        echo "-1"
    fi
    for (( arr_idx=0; arr_idx<TASK_INDEXS_length; arr_idx++ ))
    do
       if [ "${TASK_INDEXS_arr[arr_idx]}" = "$task_idx" ]; then
           echo "$arr_idx"
       fi
    done
}

#private
function formatGeneralMounts()
{
    docker_general_mounts=" -v ${DOCKER_TMP_SUDO}:/etc/sudoers.d/ai_sudoer -v ${USERHOME}:${USERHOME} -v ${sharing_path}:${sharing_path}:ro"
    echo "$docker_general_mounts"
}

#private
function formatLocalTimeMount()
{
    docker_time_mount=" -v /etc/localtime:/etc/localtime:ro "
    echo "$docker_time_mount"
}

#private
function createDockerResLimits(){
    DC_RESOURCE_LIMITS=""
    RESOURCE_LIMITS=""

    echo "add cpuids ..."
    # --cpu-period=100000 --cpu-quota=20000
    RESOURCE_LIMITS="$RESOURCE_LIMITS --cpuset-cpus=$DC_CPU_IDS"
    echo "RESOURCE_LIMITS=$RESOURCE_LIMITS" >> $LOGFILE

    echo "add memory"
    #RESOURCE_LIMITS="$RESOURCE_LIMITS --memory=$finalCtrMem --memory-swappiness=0"
    RESOURCE_LIMITS="$RESOURCE_LIMITS --memory=$DC_MEM"
    echo "RESOURCE_LIMITS=$RESOURCE_LIMITS" >> $LOGFILE
    if [ ! -z "$docker_shmsize" ]; then
        RESOURCE_LIMITS="$RESOURCE_LIMITS --shm-size=$docker_shmsize"
	fi
	echo "finally, RESOURCE_LIMITS=$RESOURCE_LIMITS" >> $LOGFILE
    DC_RESOURCE_LIMITS=$RESOURCE_LIMITS
}

function createDockerPortsMap(){
    DC_PORTS_MAP="$docker_framework_ports_map"
}
function createDockerMounts(){
    DC_MOUNTS=""
    DC_FRAMEWORK_MOUNTS=""
    #TF mounts
    docker_general_mounts=$(formatGeneralMounts)
    # docker_framework_mounts from prolog
    docker_mounts="$docker_general_mounts $docker_framework_mounts"
    echo "docker_mounts=$docker_mounts" >> $LOGFILE
    DC_MOUNTS=$docker_mounts
    DC_FRAMEWORK_MOUNTS=$docker_framework_mounts
}
function createDockerRunCmdArg(){
    UPDATE_USER_INFO_CMD="sed -i '/^${SLURM_JOB_USER}:/d' /etc/passwd ; cat ${DOCKER_TMP_PASSWD} >> /etc/passwd ; cat ${DOCKER_TMP_GROUP} >> /etc/group ; cat ${DOCKER_TMP_SHADOW} >> /etc/shadow ;"
    DC_CMD_ARG="-c \\\"$UPDATE_USER_INFO_CMD $docker_cmd_arg\\\""
}

function createDockerEnv(){
    #env_prolog
    # docker_framework_env from prolog
    DC_ENV="$docker_framework_env $docker_env"
}
function createDockerEntrypoint(){
    # docker_framework_entrypoint from prolog
    DC_ENTRYPOINT="$docker_framework_entrypoint"
}
function getUid(){
    user_name=$1
    (getent passwd | grep "^${user_name}:" || getent passwd "${user_name}") | head -n1 | awk -F: '{print $3}'
}
function getGid(){
    user_name=$1
    (getent passwd | grep "^${user_name}:" || getent passwd "${user_name}") | head -n1 | awk -F: '{print $4}'
}
function prepare_codeserver_env(){
    user_name=$1
    user_home=$2
    su - ${user_name} -c "mkdir -p ${user_home}/.config/code-server/ && mkdir -p ${user_home}/project"
    cat << EOF > ${user_home}/.config/code-server/config.yaml
bind-addr: 0.0.0.0:8080
auth: none
cert: false
EOF
}

#${source_paths} ${target_paths}
# instance
function getExtraMountInfo(){
	local source_paths=$1
	local target_paths=$2
  local target_path_permissions=$3
	local source_array=(${source_paths// / })
	local target_array=(${target_paths// / })
  local permission_array=(${target_path_permissions// / })
	local volumes=""
	local index=0
	for source_path in ${source_array[@]}
	do
		volumes=${volumes}" -v ${source_path}:${target_array[${index}]}:${permission_array[${index}]} "
		index=$(($index + 1))
	done
	echo ${volumes}
}
#${taskUser} ${taskImage} $dcName $dcCpuIDs ${taskMem} $dcGpuIDs $taskRole $taskx $taskNode $TASK_PATH
# public
function createRoleContainerParamsFile(){
    local TASK_USER=$1
    local DC_IMAGE=$2
    local DC_NAME=$3
    local DC_CPU_IDS=$4
    local DC_MEM=$5
    local DC_GPU_IDS=$6
    local TASK_ROLE=$7
    local TASK_ROLE_X=$8
    local TASK_NODE=$9
    local ACCELERATOR_TYPE=${10}
    if [ ! -z ${11} ];then
    	local ALIAS=${11}
    fi
    if [ ! -z $12 ];then
	local SHARED_HOSTS=${12}
    fi

    createDockerResLimits
    createDockerMounts
    createDockerRunCmdArg
    createDockerPortsMap
    createDockerEntrypoint
    if [ "$ACCELERATOR_TYPE" = "dcu" ];then
        formatDockerDcuDevices "$DC_GPU_IDS"
    elif [ "$ACCELERATOR_TYPE" = "mlu" ];then
        formatDockerMluDevices "$DC_GPU_IDS"
    fi
	
    createDockerEnv
    # file params
    cat << EOF > $TASK_PATH/$DC_NAME 
TASK_ROLE="$TASK_ROLE"
TASK_ROLE_X="$TASK_ROLE_X"
TASK_NODE="$TASK_NODE"
TASK_TYPE="$TASK_TYPE"

USERHOME="$USERHOME"
ACCELERATOR_TYPE="$ACCELERATOR_TYPE"

DC_NAME="$DC_NAME"
DC_GPUIDS="$DC_GPU_IDS"
ALIAS="$ALIAS"
SHARED_HOSTS="$SHARED_HOSTS"

# env.prolog

DC_HOST_PORT="$host_port"
DC_CONTAINER_PORT="$container_port"
SHARING_PATH="$sharing_path"

#
DC_RESOURCE_LIMITS="$DC_RESOURCE_LIMITS"
DC_PORTS_MAP="$DC_PORTS_MAP"
DC_MOUNTS="$DC_MOUNTS"
DC_FRAMEWORK_MOUNTS="$DC_FRAMEWORK_MOUNTS"
DC_DEVICES="$DC_DEVICES"
DC_ENV="$DC_ENV"
DC_ENTRYPOINT="${DC_ENTRYPOINT}"
DC_IMAGENAME="$DC_IMAGE"
DC_CMD_ARG="$DC_CMD_ARG"
EOF
}

# batch change spank env to normal env
# eg.$SPANK_AI_product_name to $product_name
function getSpankEnv()
{
    pre=SPANK_AI_
    OLDIFS="$IFS"
    IFS=$'\n'
    for i in $(export -p | grep $pre)
    do
        env_old_key=${i%%=*}
        env_old_key=${env_old_key/export /}
        env_old_key=${env_old_key/declare -x /}
        env_key=${env_old_key/$pre/}
        export $env_key=${!env_old_key}
    done
    IFS="$OLDIFS"
}

# get uverbs number
function getUverbsNum() {
    local count=$(ls /dev/infiniband/uverbs* 2>/dev/null | wc -l)
    echo $count
}

# get rocm number
function getRocmNum() {
    local count=$(ls /dev/infiniband/rdma_cm 2>/dev/null | wc -l)
    echo $count
}

# ++++++++++++++++++++++++++++++++++++++++++++++++++++
# ******************************************


#
# Common test function
#
function common_main_test()
{

    #getJobNodeResources $1
    #getUserShadowPasswd $*
    
    #CUDA_VISIBLE_DEVICES=NoDevFiles

    #echo "Final MinMemNode:"
    #getMinMemNode $1 && echo "SUCCEED"
    #echo "Final NodeMem:"
    #getNodeMem $1 && echo "SUCCEED"
    #echo "Final MinMemCPU:"
    #getCpuMinMem $1 && echo "SUCCEED"
    
    #echo "Final CPUS:"
    #getFinalNodeCpus $1 && echo "SUCCEED"
    #echo "Final GPUS:"
    #getFinalNodeGpus $1 && echo "SUCCEED"
    #calcNodeMemFromCpuMem $1
    
    #echo
    
    #DOCKER_CPUIDS=$(getFinalNodeCpus $1) && echo "DOCKER_CPUIDS=$DOCKER_CPUIDS"
    #DOCKER_DOCKER_GPUIDS=$(getFinalNodeGpus $1) && echo "DOCKER_GPUIDS=$DOCKER_GPUIDS"
    #DOCKER_DOCKER_CTRMEM=$(getFinalNodeMem $1) && echo "DOCKER_CTRMEM=$DOCKER_CTRMEM"
    #DOCKER_
    #DOCKER_echo "DOCKER_CTRMEM=$DOCKER_CTRMEM"
    #DOCKER_echo "DOCKER_CPUIDS=$DOCKER_CPUIDS"
    #DOCKER_echo "DOCKER_GPUIDS=$DOCKER_GPUIDS"
    
    getDockerIPAddr $1
}

#common_main_test $*



