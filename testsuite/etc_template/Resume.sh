#!/bin/bash

echo "Resume start: $(date)" >> /opt/gridview/slurm/log/power.log
hosts=`/opt/gridview/slurm/bin/scontrol show hostname $1`
for host in $hosts
do
    ssh $host "systemctl restart slurmd"
done
echo "Resume end: $(date)" >> /opt/gridview/slurm/log/power.log
echo "------------------------------------------------------------" >> /opt/gridview/slurm/log/power.log
