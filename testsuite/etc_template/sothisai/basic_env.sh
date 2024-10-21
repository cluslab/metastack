#! /bin/bash
#SothisAI env
export SOTHISAI_HOME=/opt/gridview
#SLURM install path
export SLURM_INSTALL_PATH=/opt/gridview/slurm

#SothisAI Accelerator(nv_gpu|dcu)
export SOTHISAI_ACCELERATOR=dcu

# print debug msg
export DEBUG_MODE=1

# make pwd to non-root user
export ROOT_PASSWD_UPDATE=1

# commands of slurm
export SCONTROL=$SLURM_INSTALL_PATH/bin/scontrol
export SQUEUE=$SLURM_INSTALL_PATH/bin/squeue
export SBATCH=$SLURM_INSTALL_PATH/bin/sbatch
export SCANCEL=$SLURM_INSTALL_PATH/bin/scancel

# dir to touch temperary file
export SPOOL_DIR="/tmp"

# dir to be shared
export USER_SHARE_SPOOL_DIR=".AITemp"



