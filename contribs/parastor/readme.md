# Slurm Burst Buffer 自动化清理工具 (slurm_bb_cleanup)
这是一个基于 Python 2 开发的运维辅助脚本，主要用于管理 Slurm 与 ParaStor (Burst Buffer) 存储系统之间的缓存资源。它能够识别并清理残留、过期或指定的作业缓存数据，确保存储空间的有效循环利用。

1. 核心功能
自动扫描：对比 Slurm squeue 队列，自动识别已结束作业残留的 j{job_id}n{x} 格式缓存组。

安全回收：调用 ParaStor REST API 触发数据集回收（Recycle）任务，并实时监控任务状态。

彻底清理：在回收完成后，自动删除数据集（Dataset）及缓存组（Cache Group）。

精确指定：支持通过 Job ID 强制清理特定作业的关联资源。

2. 安装与环境准备
运行环境

语言: Python 2.x 

依赖命令: curl, squeue, scontrol, scancel

权限: 必须以 root 用户运行（用于读取 slurm.conf 路径及执行清理命令）。

设置快捷指令 (Alias)

为了方便在终端任何位置调用，建议将脚本路径写入 ~/.bashrc：

Bash
(1)假设脚本存放路径为 /usr/local/bin/slurm_bb_cleanup.py
echo "alias bb-clean='python2 /usr/local/bin/slurm_bb_cleanup.py'" >> ~/.bashrc

(2)刷新配置
source ~/.bashrc

3. 使用说明
场景 A：全量自动清理

检查当前系统所有已不在队列中的 Job 缓存并进行清理。

Bash
sudo bb-clean


场景 B：清理特定作业资源

通过 -j 参数指定特定的 Job ID。此模式下，脚本不会检查该 Job 是否在 squeue 中，直接执行清理逻辑。

Bash
# 清理 Job ID 为 6005 的缓存资源
sudo bb-clean -j 6005

4. 内部逻辑流程
解析配置：通过 scontrol 定位 slurm.conf 所在目录，并读取同级目录下的 burst_buffer.conf 获取存储 API 地址与凭据。

API 认证：对密码进行偏移量加密及 Base64 编码，调用 /restLogin 获取会话 Token。

状态监控：

发送回收指令后，脚本会进入 轮询监控 状态。

默认每 10 秒查询一次任务进度，直到状态变为 COMPLETED。

若 600 秒（10分钟）内未完成，脚本将报错退出以保护系统。

资源释放：依次执行 Delete Dataset -> Delete Cache Group -> scancel -H {job_id}。

5. 参数参考
参数	短指令	说明
--job	-j	指定需要清理的 Slurm Job ID
--help	-h	显示帮助信息

6. 注意事项

配置文件：请确保 burst_buffer.conf 中 ParaStorAddr 等字段填写准确。

网络连通性：运行环境需能通过 HTTPS 访问存储管理节点的 API 端口（默认为配置中的端口）。

脚本版本：本脚本针对 Python 2 环境编写，若在 Python 3 环境运行需进行语法调整。