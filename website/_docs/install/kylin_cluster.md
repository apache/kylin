---
layout: docs
title:  "Deploy in Cluster Mode"
categories: install
permalink: /docs/install/kylin_cluster.html
---


Kylin instances are stateless services, and runtime state information is stored in the HBase metastore. For load balancing purposes, you can enable multiple Kylin instances that share a metastore, so that each node shares query pressure and backs up each other, improving service availability. The following figure depicts a typical scenario for Kylin cluster mode deployment:
![](/images/install/kylin_server_modes.png)



### Kylin Node Configuration

If you need to cluster multiple Kylin nodes, make sure they use the same Hadoop cluster, HBase cluster. Then do the following steps in each node's configuration file `$KYLIN_HOME/conf/kylin.properties`:

1. Configure the same `kylin.metadata.url` value to configure all Kylin nodes to use the same HBase metastore.
2. Configure the Kylin node list `kylin.server.cluster-servers`, including all nodes (the current node is also included). When the event changes, the node receiving the change needs to notify all other nodes (the current node is also included).
3. Configure the running mode `kylin.server.mode` of the Kylin node. Optional values include `all`, `job`, `query`. The default value is *all*.
The *job* mode means that the service is only used for task scheduling, not for queries; the *query* pattern means that the service is only used for queries, not for scheduling tasks; the *all* pattern represents the service for both task scheduling and queries.
> *Note*:  By default, only *one instance* is used for the scheduling of the build job (ie `kylin.server.mode` is set to `all` or `job`), if you need to configure multiple Nodes as job mode to meet high-availability and high-concurrency requirements, please refer to the *Job Engine High Availability* section of the [Kylin Settings](/docs/install/configuration.html) page.



### Installing a load balancer

To send query requests to a cluster instead of a single node, you can deploy a load balancer such as [Nginx](http://nginx.org/en/), [F5](https://www.f5.com/) or [cloudlb](https://rubygems.org/gems/cloudlb/), etc., so that the client and load balancer communication instead communicate with a specific Kylin instance.



### Read and write separation deployment

For better stability and optimal performance, it is recommended to perform a read-write separation deployment, deploying Kylin on two clusters as follows:

* A Hadoop cluster used to *Cube build*, which can be a large cluster shared with other applications;
* An HBase cluster used to *SQL query*. Usually this cluster is configured for Kylin. The number of nodes does not need to be as many as Hadoop clusters. HBase configuration can be optimized for Kylin Cube read-only features.

This deployment strategy is the best deployment solution for the production environment. For how to perform read-write separation deployment, please refer to [Deploy Apache Kylin with Standalone HBase Cluster](/blog/2016/06/10/standalone-hbase-cluster/) .