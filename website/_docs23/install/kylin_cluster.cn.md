---
layout: docs23-cn
title:  "Cluster 模式下部署"
categories: install
permalink: /cn/docs23/install/kylin_cluster.html
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

 *  `kylin.rest.servers`
	使用中的服务器列表, 当事件变化时，让一个实例去通知其他服务器。例如: 

```
kylin.rest.servers=host1:7070,host2:7070
```

 *  `kylin.server.mode`
	确保只有一个实例的 `kylin.server.mode` 设置为 "all" 或 "job", 其余的应该为 "query"

```
kylin.server.mode=all
```

### 安装负载均衡器

为确保 Kylin 服务器的高可用性, 您需要在这些服务器之前安装负载均衡器, 让其将传入的请求路由至集群。客户端和负载均衡器通信代替和特定的 Kylin 实例通信。安装负载均衡器超出了范围，您可以选择像 Nginx, F5 或 cloud LB 服务这样的实现。
	
