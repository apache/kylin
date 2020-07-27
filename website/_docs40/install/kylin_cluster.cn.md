---
layout: docs40-cn
title:  "集群模式部署"
categories: install
permalink: /cn/docs40/install/kylin_cluster.html
---

Kylin 实例是无状态的服务，运行时的状态信息存储在 Mysql metastore 中。 出于负载均衡的考虑，您可以启用多个共享一个 metastore 的 Kylin 实例，使得各个节点分担查询压力且互为备份，从而提高服务的可用性。下图描绘了 Kylin 集群模式部署的一个典型场景：
![](/images/install/kylin_server_modes.4.x.png)



### Kylin 集群模式部署

如果您需要将多个 Kylin 节点组成集群，请确保他们使用同一个 Hadoop 集群。然后在每个节点的配置文件 `$KYLIN_HOME/conf/kylin.properties` 中执行下述操作：

1. 配置相同的 `kylin.metadata.url` 值，即配置所有的 Kylin 节点使用同一个 Mysql metastore。
2. 配置 Kylin 节点列表 `kylin.server.cluster-servers`，包括所有节点（包括当前节点），当事件变化时，接收变化的节点需要通知其他所有节点（包括当前节点）。
3. 配置 Kylin 节点的运行模式 `kylin.server.mode`，参数值可选 `all`, `job`, `query` 中的一个，默认值为 `all`。
`job` 模式代表该服务仅用于任务调度，不用于查询；`query` 模式代表该服务仅用于查询，不用于构建任务的调度；`all` 模式代表该服务同时用于任务调度和 SQL 查询。

> **注意：**默认情况下只有**一个实例**用于构建任务的调度 （即 `kylin.server.mode` 设置为 `all` 或者 `job` 模式）。



### 任务引擎高可用

从 v2.0 开始, Kylin 支持多个任务引擎一起运行，相比于默认单任务引擎的配置，多引擎可以保证任务构建的高可用。

使用多任务引擎，你可以在多个 Kylin 节点上配置它的角色为 `job` 或 `all`。

为了避免它们之间产生竞争，需要在`kylin.properties`中配置任务调度器为分布式调度：

```properties
kylin.job.scheduler.default=2
kylin.job.lock=org.apache.kylin.job.lock.zookeeper.ZookeeperJobLock
```
然后将所有任务和查询节点的地址注册到 `kylin.server.cluster-servers`。

### 配置`CuratorScheculer`进行任务调度

从 v3.0.0-alpha 开始，kylin引入基于Curator的主从模式多任务引擎调度器，用户可以修改如下配置来启用CuratorScheculer：

```properties
kylin.job.scheduler.default=100
kylin.server.self-discovery-enabled=true
```
更多关于kylin任务调度器的细节可以参考[Apache Kylin Wiki](https://cwiki.apache.org/confluence/display/KYLIN/Comparison+of+Kylin+Job+scheduler).



### 安装负载均衡器

为了将查询请求发送给集群而非单个节点，您可以部署一个负载均衡器，如 [Nginx](http://nginx.org/en/)， [F5](https://www.f5.com/) 或 [cloudlb](https://rubygems.org/gems/cloudlb/) 等，使得客户端和负载均衡器通信代替和特定的 Kylin 实例通信。

