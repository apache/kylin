---
layout: docs-cn
title:  "集群模式部署"
categories: install
permalink: /cn/docs24/install/kylin_cluster.html
---


### Kylin 服务器模式

Kylin 实例是无状态的。其运行时状态存于存储在 HBase (由 `conf/kylin.properties` 中的 `kylin.metadata.url` 指定) 中的 metadata 中。出于负载均衡的考虑，建议运行多个共享一个 metadata 存储的 Kylin 实例，因此他们在表结构中共享同一个状态，job 状态, Cube 状态, 等等。

每一个 Kylin 实例在 `conf/kylin.properties` 中都有一个 "kylin.server.mode" entry，指定了运行时的模式，有 3 个选项: 

 *  **job** : 在实例中运行 job engine; Kylin job engine 管理集群 的 jobs;
 *  **query** : 只运行 query engine; Kylin query engine 接收和回应你的 SQL 查询;
 *  **all** : 在实例中既运行 job engine 也运行 query engines。 

注意只有一个实例可以运行 job engine ("all" 或 "job" 模式), 其他必须是 "query" 模式。 

下图中描绘了一个典型场景:

![]( /images/install/kylin_server_modes.png)

### 配置多个 Kylin 服务器

当您在拥有多个 Kylin 服务器实例的集群运行 Kylin 时, 请确保您为每一个实例在 `conf/kylin.properties` 中正确的配置了以下属性。

 *  `kylin.server.cluster-servers`
	使用中的服务器列表, 当事件变化时，让一个实例去通知其他服务器。例如: 

```
kylin.rest.servers=host1:7070,host2:7070
```

 *  `kylin.server.mode`


默认情况下，只有一个实例的 `kylin.server.mode` 设置为 "all" 或 "job", 其余的为 "query"。

```
kylin.server.mode=all
```

也即默认情况下，只有一个节点用于调度构建任务的执行。如果您需要配置多个节点同时执行任务构建，以满足高可用和高并发的需求，请参考 "启用多个任务引擎" 的内容，在 [高级设置](advance_settings.html) 页.

### 安装负载均衡器

为确保 Kylin 服务器的高可用性, 您需要在这些服务器之前安装负载均衡器, 让其将传入的请求路由至集群。客户端和负载均衡器通信代替和特定的 Kylin 实例通信。安装负载均衡器超出了范围，您可以选择像 Nginx, F5 或 cloud LB 服务这样的实现。
	
### 读／写分离的双集群配置

Kylin 可以连接两个集群以获得更好的稳定性和性能：

 * 一个 Hadoop 集群用作 Cube 构建; 这个集群可以是一个大的、与其它应用共享的集群；
 * 一个 HBase 集群用作 SQL 查询；通常这个集群是专门为 Kylin 配置的，节点数不用像 Hadoop 集群那么多。HBase 的配置可以更加针对 Kylin Cube 只读的特性而进行优化。  

这种部署策略已经被很多大企业所采纳并得到验证。它是迄今我们知道适合生产环境的最佳部署方案。关于如何配置这种架构，请参考 [Deploy Apache Kylin with Standalone HBase Cluster](/blog/2016/06/10/standalone-hbase-cluster/)