# metastack简介
metastack 是基于 Slurm 的功能增强和性能优化的版本。我们团队根据多个超算，智算中心稳定运行的经验，并结合开源社区的最佳理念和实践，在保留Slurm原生功能的基础上，对Slurm进行了改进，提供了一些特色功能，例如分区并行调度，分区可见性等，解决了用户在使用Slurm中的痛点。目前metastack仅在 Linux 下进行了测试。

metastack可轻松创建计算集群，帮助您在高性能计算集群中有效地管理和调度作业。

metastack源码地址：[https://github.com/cluslab/metastack](https://github.com/cluslab/metastack)


# 整体架构
![image](https://github.com/cluslab/metastack/assets/150906851/fabbe9e3-288f-41b3-8720-33085ee1cdcb)

# 亮点

1. 大规模稳定性
   1. 支持在14000节点规模、18000用户和6万个以上并发作业量的长期稳定运行
   2. 支持PMIx启动模式下超大规模作业(2000以上节点)异常的服务端容错处理
2. 高吞吐量
   1. 支持每秒2000+作业并发入队
   2. 单线程串行调度场景下，支持每秒1000+的出队效率
   3. 多线程并发调度场景下，支持每秒10000+的出队效率
3. 调度策略
   1. 对无节点交叉的独立分区，使用单独的线程进行作业调度
   2. 对同一个用户下重复且无法运行的作业进行快速调度，节省调度时间
4. 信息采集
   1. 支持实时获取作业步CPU利用率
   2. 支持提供用户作业级别负载异常检测
5. 商业证书调度
   1. 支持证书资源调度、证书跨集群共享、证书资源抢占等
6. 容器支持
   1. 提供对Docker、Singularity和Enroot容器作业的调度运行支持
7. 绿色节能调度
   1. 节能开启后，支持队列动态保持部分idle的节点，方便作业的快速启动
   2. 支持排除特定状态的节点进入节能模式

# 快速安装
请参阅快速入门指南立即开始使用 MetaStack

https://www.cluslab.cn/thread/57
# 获取帮助
[https://www.cluslab.cn/cate/13/seq/0](https://www.cluslab.cn/cate/13/seq/0)

[https://github.com/cluslab/metastack/issues](https://github.com/cluslab/metastack/issues)
# 联系我们
如果您有任何疑问，请随时通过以下方式与我们联系：

https://www.cluslab.cn

