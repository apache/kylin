---
layout: docs
title:  "Deploy in Cluster Mode"
categories: install
permalink: /docs/install/kylin_cluster.html
---


Kylin instances are stateless services, and runtime state information is stored in the Mysql metastore. For load balancing purposes, you can enable multiple Kylin instances that share a metastore, so that each node shares query pressure and backs up each other, improving service availability. The following figure depicts a typical scenario for Kylin cluster mode deployment:
![](/images/install/kylin_server_modes.4.x.png)



### Kylin Node Configuration

If you need to cluster multiple Kylin nodes, make sure they use the same Hadoop cluster. Then do the following steps in each node's configuration file `$KYLIN_HOME/conf/kylin.properties`:

1. Configure the same `kylin.metadata.url` value to configure all Kylin nodes to use the same Mysql metastore.
2. Configure the Kylin node list `kylin.server.cluster-servers`, including all nodes (the current node is also included). When the event changes, the node receiving the change needs to notify all other nodes (the current node is also included).
3. Configure the running mode `kylin.server.mode` of the Kylin node. Optional values include `all`, `job`, `query`. The default value is *all*.
The *job* mode means that the service is only used for job scheduling, not for queries; the *query* pattern means that the service is only used for queries, not for scheduling jobs; the *all* pattern represents the service for both job scheduling and queries.

> *Note*:  By default, only *one instance* can be used for the job scheduling (ie., `kylin.server.mode` is set to `all` or `job`).



### Enable Job Engine HA

Since v2.0, Kylin supports multiple job engines running together, which is more extensible, available and reliable than the default job scheduler.

To enable the distributed job scheduler, you need to set or update the configs in the `kylin.properties`, there are two configuration options:

```properties
kylin.job.scheduler.default=2
kylin.job.lock=org.apache.kylin.job.lock.zookeeper.ZookeeperJobLock
```

Then please add all job servers and query servers to the `kylin.server.cluster-servers`.

### Use `CuratorScheculer`

Since v3.0.0-alpha, kylin introduces the Leader/Follower mode multiple job engines scheduler based on Curator. Users can modify the following configuration to enable CuratorScheduler:

```properties
kylin.job.scheduler.default=100
kylin.server.self-discovery-enabled=true
```

For more details about the kylin job scheduler, please refer to [Apache Kylin Wiki](https://cwiki.apache.org/confluence/display/KYLIN/Comparison+of+Kylin+Job+scheduler).

### Installing a load balancer

To send query requests to a cluster instead of a single node, you can deploy a load balancer such as [Nginx](http://nginx.org/en/), [F5](https://www.f5.com/) or [cloudlb](https://rubygems.org/gems/cloudlb/), etc., so that the client and load balancer communication instead communicate with a specific Kylin instance.

### Read and write separation deployment
There are some differences between read and write separation deployment of kylin 4 and kylin 3, Please refer to : [Read Write Separation Deployment for Kylin 4](https://cwiki.apache.org/confluence/display/KYLIN/Read-Write+Separation+Deployment+for+Kylin+4.0)