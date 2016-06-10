--
layout: post-blog
title:  Deploy Apache Kylin with Standalone HBase Cluster
date:   2016-06-10 17:30:00
author: Yerui Sun
categories: blog
--

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

## Using NN HA
HDFS Namenode HA improved the availablity of cluster significantly, and maybe the HBase cluster enabled it. Apache Kylin doesn't support the HA perfectly for now, and here's the workaroud:
 - Add all `dfs.nameservices` related configs of HBase Cluster into `hadoop/etc/hadoop/hdfs-site.xml` in Kylin Server, to make sure that can access HBase Cluster using hdfs shell with nameservice path
 - Add all `dfs.nameservices` related configs of both two clusters into `kylin_job_conf.xml`, to make sure that the MR job can access hbase cluster with nameservice path

## TroubleShooting
 - UnknownHostException occurs during Cube Building
   It usually occurs with HBase HA nameservice config, please refer the above section "Using NN HA"
 - HFile BulkLoading Stucks for long time
   Check the regionserver log, there should be lots of error log, with WrongFS exception. Make sure the namenode address in `kylin.properites/kylin.hbase.cluster.fs` and hbase master node `hbase-site.xml/root.dir` is same
