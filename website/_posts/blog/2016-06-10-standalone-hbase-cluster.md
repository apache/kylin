---
layout: post-blog
title:  Deploy Apache Kylin with Standalone HBase Cluster
date:   2016-06-10 17:30:00
author: Yerui Sun
categories: blog
---

## Introduction

Apache Kylin mainly use HBase to storage cube data. The performance of HBase cluster impacts on the query performance of Kylin directly. In common scenario, HBase is deployed with MR/Hive on one HDFS cluster, which makes that the resouces HBase used is limited, and the MR job affects the performance of HBase. These problems can be resolved with standalone HBase cluster, and Apache Kylin has support this deploy mode for now.

## Enviroment Requirements
To enable standalone HBase cluster supporting, check the basic enviroments at first:

 - Deploy the main cluster and hbase cluster, make sure both works normally
 - Make sure Kylin Server can access both clusters using hdfs shell with fully qualifiered path
 - Make sure Kylin Server can submit MR job to main cluster, and can use hive shell to access data warehouse, make sure the configurations of hadoop and hive points to main cluster
 - Make sure Kylin Server can access hbase cluster using hbase shell, make sure the configuration of hbase points to hbase cluster
 - Make sure the job on main cluster can access hbase cluster directly
 
## Configurations
Update the config `kylin.hbase.cluster.fs` in kylin.properties, with a value as the Namenode address of HBase Cluster, like `hdfs://hbase-cluster-nn01.example.com:8020`

Notice that the value should keep consistent with the Namenode address of `root.dir` on HBase Master node, to ensure bulkload into hbase.

## Enable NN HA
HDFS Namenode HA improved the availablity of cluster significantly, and maybe the HBase cluster enabled it.
To enable NN HA on HBase cluster, set `kylin.hbase.cluster.fs` as NN-HA format path of HBase cluster in kylin.properties, like `hdfs://hbase-cluster:8020`.

Since the MR Job of cube building access both main cluster and hbase cluster, exceptions may be threw during task running. The reason is task need to access HBase cluster with NN-HA related configs, which usually not deployed in the main cluster.

These exceptions could be resolved by configs updating. In details, merge NN-HA related configs of two clusters, including `dfs.nameservices`, `dfs.ha.namenodes.`, `dfs.client.failover.proxy.provider.` and other related. These configs should be updated into hdfs-site.xml in Kylin Server and Resource Manager of main cluster, also be added into kylin_job_conf.xml.

Howerver, it may be difficult to update configs in production environments. Here's another way which avoing configs updating:

 - Update HBase client in Kylin Server with patch HBASE-14347, and set `hbase.use.dynamic.jar=false` in hbase-site.xml
 - Update Kylin Server with patch KYLIN-1910, and set `kylin.hbase.cluster.hdfs.config.file=hbase.hdfs.xml` in kylin.properties. The `hbase.hdfs.xml` meaning the hdfs-site.xml of HBase cluster, put it in the same dir of kylin.properties
 - Update Hadoop of Kylin Server and Resource Manager with patch YARN-3021

## TroubleShooting

 - UnknownHostException occurs during Cube Building
   It usually occurs with HBase HA nameservice config, please refer the above section "Enable NN HA"
 - 'Error when open connection hbase' during Kylin Server startup
   HBase tries to mkdir tmp dirs on hdfs during connection setup, failed with lack of NN-HA related configs, refer "Enable NN HA"
 - Failed to submit cube building job
   HBase cluster path will be parsed during generate cube building job, failed with lack of NN-HA related configs, refer "Enable NN HA"
 - Cube Building Step 'Convert Cuboid Data to HFile' failed
   Usually occurred with Kerberos Authentication. Resource Manager tries to renew all tokens when job submitting, the renew of HBase cluster token will be failed, with lack of NN-HA related configs, refer "Enable NN HA"
 - HFile BulkLoading Stucks for long time
   Check the regionserver log, there should be lots of error log, with WrongFS exception. Make sure the namenode address in `kylin.properites/kylin.hbase.cluster.fs` and hbase master node `hbase-site.xml/root.dir` is same
