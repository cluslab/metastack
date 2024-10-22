# 基于metastack-22.05-2.2.0版本的“作业执行时间预测”功能



## 功能说明

​		基于metastack-22.05-2.2.0版本的“作业执行时间预测”功能，旨在用户提交作业时，为用户预测作业的执行时间。并让带有预测标记的作业，可以按照时间预测值进行回填。

​		本功能当前版本不予用户交互，用户侧无感体验时间预测功能。

​		相较之前版本，作业有了更精确的执行时间，按照时间预测值回填，可以提升回填调度中作业的回填机率，作业可以更早开始运行，从而提高集群整体的资源利用率。


# 安装部署

###### 			依赖环境安装

​		功能涉及AI回归模型，需前置安装部分依赖库，具体如下：

Python 3.9.19、pandas、numpy、sklearnjoblib、os、sys、argparse、warnings

​		若环境为联网环境，推荐使用Anaconda3安装虚拟环境并安装对应版本依赖库。

​		若环境为离线环境，请正确配置下面配置相关中python 3.9 执行路径。

###### 			时间预测工具安装

​		产品包内有安装脚本install.sh，执行安装脚本安装时间预测工具。

​		将job_submit.lua脚本cp到metastack的etc目录下，与luaconfig路径一致。

​		若安装路径与默认不一致，请手动更改工具文件中变量路径，确保后续预测工具可正常使用。

# 配置使用

​		安装包中contribs\predictor下有configuration文件，其中包含所有开关及路径的配置项。

​		其中包含配置相关：

###### 			时间预测功能配置相关

​			#预测功能开关

​			predictionFunction=0关闭预测流程

​			predictionFunction=1开启预测流程只适用于白名单用户

​			predictionFunction=2开启预测流程适用全部用户



​			#预测使用的方法

​			predictionMethod=0 均值法

​			predictionMethod=1 AI随机森林算法



​			#数据集获取范围,单位为月

​			period=3



###### 			时间预测工具路径相关

​			注意：请确保以下安装路径正确。路径不对会影响功能的正常使用。

​			#sacct命令路径

​			sacct_cmd=/opt/gridview/slurm/bin/sacct



​			#slurm/etc 配置路径

​			etc_path=/opt/gridview/slurm/etc



​			#预测工具路径

​			predictor_path=/opt/gridview/slurm/etc/luaconfig/predictor



​			#python 3.9 执行路径

​			python_executable=/opt/gridview/slurm/etc/luaconfig/predictor/AIenvironment/CentOS-7.6/envs/AIpredict/bin/python



​			#AI随机森林工具路径

​			sklearn_path=/opt/gridview/slurm/etc/luaconfig/predictor/prediction_methods/sklearn



###### 			历史作业更新

​			首次安装需执行updateJobHistory脚本获取历史作业集，及生成对应的AI预测模型。

​			若历史作业集需频繁更新，建议将updateJobHistory脚本放置在系统级crontab定时任务中，作业集和AI预测模型会对应更新。
