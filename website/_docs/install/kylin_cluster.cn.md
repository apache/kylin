---
layout: docs-cn
title:  "集群模式部署"
categories: install
permalink: /cn/docs/install/kylin_cluster.html
---

Kylin 实例是无状态的服务，运行时的状态信息存储在 HBase metastore 中。 出于负载均衡的考虑，您可以启用多个共享一个 metastore 的 Kylin 实例，使得各个节点分担查询压力且互为备份，从而提高服务的可用性。下图描绘了 Kylin 集群模式部署的一个典型场景：
![](/images/install/kylin_server_modes.png)



### Kylin 节点配置

如果您需要将多个 Kylin 节点组成集群，请确保他们使用同一个 Hadoop 集群、HBase 集群。然后在每个节点的配置文件 `$KYLIN_HOME/conf/kylin.properties` 中执行下述操作：

1. 配置相同的 `kylin.metadata.url` 值，即配置所有的 Kylin 节点使用同一个 HBase metastore。
2. 配置 Kylin 节点列表 `kylin.server.cluster-servers`，包括所有节点（包括当前节点），当事件变化时，接收变化的节点需要通知其他所有节点（包括当前节点）。
3. 配置 Kylin 节点的运行模式 `kylin.server.mode`，参数值可选 `all`, `job`, `query` 中的一个，默认为 `all`。
`job` 模式代表该服务仅用于任务调度，不用于查询；`query` 模式代表该服务仅用于查询，不用于构建任务的调度；`all` 模式代表该服务同时用于任务调度和 SQL 查询。
> **注意：**默认情况下只有**一个实例**用于构建任务的调度 （即 `kylin.server.mode` 设置为 `all` 或者 `job` 模式），如果您需要配置多个节点进行任务构建，以满足高可用和高并发的需求，请参考 [Kylin 设置](/docs/install/configuration.html) 页中的**任务引擎高可用**的内容。



### 安装负载均衡器

为了将查询请求发送给集群而非单个节点，您可以部署一个负载均衡器，如 [Nginx](http://nginx.org/en/)， [F5](https://www.f5.com/) 或 [cloudlb](https://rubygems.org/gems/cloudlb/) 等，使得客户端和负载均衡器通信代替和特定的 Kylin 实例通信。



### 读写分离部署

为了达到更好的稳定性和最佳的性能，建议进行读写分离部署，将 Kylin 部署在两个集群上，如下：

* 一个 Hadoop 集群用作 **Cube 构建**，这个集群可以是一个大的、与其它应用共享的集群；
* 一个 HBase 集群用作 **SQL 查询**，通常这个集群是专门为 Kylin 配置的，节点数不用像 Hadoop 集群那么多，HBase 的配置可以针对 Kylin Cube 只读的特性而进行优化。

这种部署策略是适合生产环境的最佳部署方案，关于如何进行读写分离部署，请参考 [Deploy Apache Kylin with Standalone HBase Cluster](/blog/2016/06/10/standalone-hbase-cluster/)。