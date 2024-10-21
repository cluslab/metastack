#!/bin/sh
#
# This script will kill any user processes on a node when the last
# SLURM job there ends. For example, if a user directly logs into
# an allocated node SLURM will not kill that process without this
# script being executed as an epilog.
#
# SLURM_BIN can be used for testing with private version of SLURM
#SLURM_BIN="/usr/bin/"
#
if [ x$SLURM_UID = "x" ] ; then 
	exit 0
fi
if [ x$SLURM_JOB_ID = "x" ] ; then 
	exit 0
fi
SLURM_INSTALL_PATH=/opt/gridview/slurm
# source tool functions and configuration
source $SLURM_INSTALL_PATH/etc/sothisai/function.sh

# SLURM Prolog log
prolog_info=$SLURM_INSTALL_PATH/log/prolog_info_`date +%Y%m%d`
function log_prolog_info(){
    echo -e "[`date`] $1" | tee -ia  $prolog_info
}

#==== ENVPROLOG ====
log_prolog_info "SLURM_JOB_USER=$SLURM_JOB_USER"
export USERHOME=$(getUserHomeFromPasswd ${SLURM_JOB_USER})
log_prolog_info "USERHOME=$USERHOME"

getSpankEnv
if [ ! "$container_flag" ]; then
	log_prolog_info "JOB:$SLURM_JOB_ID ENVPROLOG not found, exit"
	exit 0
fi

# TASK_PATH in ENVPROLOG
if [ -z "$TASK_PATH" ];then
	log_prolog_info "JOB:$SLURM_JOB_ID TASK_PATH in null,ignore prolog!"
	exit 0
fi 
#
# ============= ===============
# detail logfile
export USER_NAME=$SLURM_JOB_USER
#task.message
export MESSAGEPATH=`getTaskMessagePath $TASK_PATH`
#prepare_container
export PREPAREPATH=`getPrepareContainerLogPath $TASK_PATH`
# prolog.env.$SLURM_JOB_ID.$SLURMD_NODENAME
export LOGFILE=`getSlurmPrologFilePath $TASK_PATH $SLURM_JOB_ID $SLURMD_NODENAME`

logfile_dir=`dirname $LOGFILE`
if [ ! -d "$logfile_dir" ];then
	su $USER_NAME -c "mkdir -p $logfile_dir"
fi

date | tee -ia $LOGFILE
su $USER_NAME -c "touch $MESSAGEPATH $LOGFILE $PREPAREPATH"

iptables -P FORWARD ACCEPT

function log_prolog_message(){
    echo "$1" | tee -ia $MESSAGEPATH
}
function log_prepare_message(){
    echo "$1" | tee -ia $PREPAREPATH
}
function log_prolog_detail(){
    echo -e "[`date`] $1" | tee -ia $LOGFILE
}

export JOBDETAIL=${TASK_PATH}/detail.job.$SLURM_JOB_ID
log_prolog_detail "detailed file for ${SLURM_JOB_ID}: ${JOBDETAIL}"
header_node=`getHeaderNode`
#header should clean old config and touch new task detail file.
if [ $header_node == $SLURMD_NODENAME ];then
    log_prolog_detail "header node.clean old config and touch new task detail file."
    removeDockerInstanceFilePath $TASK_PATH $SLURM_JOB_ID
    su $USER_NAME -c "touch $JOBDETAIL"
    $SCONTROL --detail show jobs $SLURM_JOB_ID > $JOBDETAIL 2>>$LOGFILE
fi
# ====== ======
log_prepare_message "======校验作业信息======"
log_prepare_message "`date`"
log_prepare_message "用户身份 : $USER_NAME"
log_prepare_message "主目录 : $USERHOME"
log_prepare_message "作业类型 : $dl_framework"
log_prepare_message "作业路径 : $TASK_PATH"
log_prepare_message "======================="

# debug info
if [ $DEBUG_MODE -eq 1 ]; then
	log_prolog_detail "================= env begin =================="
	env | tee -ia $LOGFILE
	log_prolog_detail "================= env over =================="
	log_prolog_detail ""
fi

# Params in ENVPROLOG
export IMAGENAME="${image_version}"
if [ $DEBUG_MODE -eq 1 ]; then
	log_prolog_detail "IMAGENAME=$IMAGENAME"
fi

export DLFRAMEWORK="${dl_framework}"
if [ $DEBUG_MODE -eq 1 ]; then
	log_prolog_detail "DLFRAMEWORK=$DLFRAMEWORK"
fi

# ==== maybe can be remove ====
if [ $DEBUG_MODE -eq 1 ]; then
	log_prolog_detail "run dir is: `pwd`"
	log_prolog_detail "current script: $0"
	log_prolog_detail "SPOOL_DIR=$SPOOL_DIR"
fi
checkAndCreateWorkdir
# ==== ====

DOCKER_TMP_PASSWD=`getTempPasswdFile $USERHOME`
DOCKER_TMP_SHADOW=`getTempShadowFile $USERHOME`
DOCKER_TMP_GROUP=`getTempGroupFile $USERHOME`
DOCKER_TMP_SUDO=`getTempSudoFile $USERHOME`

#log_prolog_detail "DOCKER_TMP_PASSWD=$DOCKER_TMP_PASSWD"
#log_prolog_detail "DOCKER_TMP_SHADOW=$DOCKER_TMP_SHADOW"
#log_prolog_detail "DOCKER_TMP_GROUP=$DOCKER_TMP_GROUP"
#log_prolog_detail "DOCKER_TMP_SUDO=$DOCKER_TMP_SUDO"

# duplication
# create passwd group shadow file
# prepareUserFileNameAndGroupFileName
prepareUserFileAndGroupFile


# ==== execute Prolog for dl framework ====
PROLOG_HOME=`dirname $0`
if [ $DEBUG_MODE -eq 1 ]; then
	log_prolog_detail "PROLOG_HOME=$PROLOG_HOME"
fi
FRAMEWORK_HOME="$PROLOG_HOME/$DLFRAMEWORK"
if [ $DEBUG_MODE -eq 1 ]; then
	log_prolog_detail "FRAMEWORK_HOME=$FRAMEWORK_HOME"
fi
if [ "$DLFRAMEWORK" = "instance" ]; then
	TASK_PROLOG="$FRAMEWORK_HOME"
	if [ "$task_type" = "job" ];then
		TASK_PROLOG="$FRAMEWORK_HOME/$task_type"
	fi
else
	TASK_PROLOG="$FRAMEWORK_HOME/$task_type"
fi

FRAMEWORK_PROLOG="$TASK_PROLOG/prolog"

if [ $DEBUG_MODE -eq 1 ]; then
	log_prolog_detail "FRAMEWORK_PROLOG=$FRAMEWORK_PROLOG"
fi

if [ ! -f "${FRAMEWORK_PROLOG}" ]; then
	log_prolog_detail "Prolog for $DLFRAMEWORK ($FRAMEWORK_PROLOG) not found"
	exit 0
fi
# exec FRAMEWORK_PROLOG
source $FRAMEWORK_PROLOG
# ==== ==== ==== ====
date | tee -ia $LOGFILE
exit 0

# ======= export params list =====================
# USERHOME
# ENVPROLOG
# USER_NAME
# MESSAGEPATH
# LOGFILE
# JOBDETAIL
# IMAGENAME
# DLFRAMEWORK



