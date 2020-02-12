---
layout: docs31
title:  "Deploy in Cluster Mode"
categories: install
permalink: /docs31/install/kylin_cluster.html
---


Kylin instances are stateless services, and runtime state information is stored in the HBase metastore. For load balancing purposes, you can enable multiple Kylin instances that share a metastore, so that each node shares query pressure and backs up each other, improving service availability. The following figure depicts a typical scenario for Kylin cluster mode deployment:
![](/images/install/kylin_server_modes.png)



### Kylin Node Configuration

If you need to cluster multiple Kylin nodes, make sure they use the same Hadoop cluster, HBase cluster. Then do the following steps in each node's configuration file `$KYLIN_HOME/conf/kylin.properties`:

1. Configure the same `kylin.metadata.url` value to configure all Kylin nodes to use the same HBase metastore.
2. Configure the Kylin node list `kylin.server.cluster-servers`, including all nodes (the current node is also included). When the event changes, the node receiving the change needs to notify all other nodes (the current node is also included).
3. Configure the running mode `kylin.server.mode` of the Kylin node. Optional values include `all`, `job`, `query`. The default value is *all*.
The *job* mode means that the service is only used for job scheduling, not for queries; the *query* pattern means that the service is only used for queries, not for scheduling jobs; the *all* pattern represents the service for both job scheduling and queries.

> *Note*:  By default, only *one instance* can be used for the job scheduling (ie., `kylin.server.mode` is set to `all` or `job`).



### Enable Job Engine HA

Since v2.0, Kylin supports multiple job engines running together, which is more extensible, available and reliable than the default job scheduler.

To enable the distributed job scheduler, you need to set or update the configs in the `kylin.properties`:

```properties
kylin.job.scheduler.default=2
kylin.job.lock=org.apache.kylin.storage.hbase.util.ZookeeperJobLock
```

Please add all job servers and query servers to the `kylin.server.cluster-servers`.



### Installing a load balancer

To send query requests to a cluster instead of a single node, you can deploy a load balancer such as [Nginx](http://nginx.org/en/), [F5](https://www.f5.com/) or [cloudlb](https://rubygems.org/gems/cloudlb/), etc., so that the client and load balancer communication instead communicate with a specific Kylin instance.



### Read and write separation deployment

For better stability and optimal performance, it is recommended to perform a read-write separation deployment, deploying Kylin on two clusters as follows:

* A Hadoop cluster used to *Cube build*, which can be a large cluster shared with other applications;
* An HBase cluster used to *SQL query*. Usually this cluster is configured for Kylin. The number of nodes does not need to be as many as Hadoop clusters. HBase configuration can be optimized for Kylin Cube read-only features.

This deployment strategy is the best deployment solution for the production environment. For how to perform read-write separation deployment, please refer to [Deploy Apache Kylin with Standalone HBase Cluster](/blog/2016/06/10/standalone-hbase-cluster/) .

#### Example

We know AWS EMR is a popular Hadoop on cloud solution. Let's take it as an example.

1. Create EMR Cluster

If you are using AWS EMR cluster, you may create two EMR cluster, the main cluster and hbase cluster.

The main cluster should include components like `Hadoop`, `Hive`, `Pig`, `Spark`, `Sqoop`, `Tez` etc. And this is where you choose to deploy Kylin.
The hbase cluster should include component `HBase`. 

2. Prepare client config

Let's choose to deploy Kylin on the Master Node of main cluster. In this node, you should check following things:

- Check accessibility of HDFS.

To do this, you can run a command `hadoop fs -fs $HOSTNAME_MASTER_NODE_OF_HBASE_CLUSTER:8020 -ls /` to make sure you can access HDFS of hbase cluster.

Here, you should replace $HOSTNAME_MASTER_NODE_OF_HBASE_CLUSTER with the hostname of master node of hbase cluster.

- Check accessibility of HBase.

At the beginning, when you run the command `hbase shell`, and execute somethings like `list_namespace`, you will got error message because there is no HBase in main cluster.

So you should copy every files under `/etc/hbase/conf` from master node of hbase cluster to master node of main cluster in the same place.

After that, you can access to HBase.

3. Update kylin.properties

```
kylin.storage.hbase.cluster-fs=$HOSTNAME_MASTER_NODE_OF_HBASE_CLUSTER:8020
```

4. Start kylin

Please refer to http://kylin.apache.org/docs/install/kylin_aws_emr.html to do some prepare job and start Kylin. 

```sh
sh bin/kylin.sh start
```