---
layout: docs24
title:  "Hadoop Environment"
categories: install
permalink: /docs24/install/hadoop_env.html
---

Kylin need run in a Hadoop node, to get better stability, we suggest you to deploy it a pure Hadoop client machine, on which  the command lines like `hive`, `hbase`, `hadoop`, `hdfs` already be installed and configured. The Linux account that running Kylin has got permission to the Hadoop cluster, including create/write hdfs, hive tables, hbase tables and submit MR jobs. 

## Software dependencies

* Hadoop: 2.7+
* Hive: 0.13 - 1.2.1+
* HBase: 1.1+
* Spark 2.1
* JDK: 1.7+
* OS: Linux only, CentOS 6.5+ or Ubuntu 16.0.4+

Tested with Hortonworks HDP 2.2 - 2.6, Cloudera CDH 5.7 - 5.11, AWS EMR 5.7 - 5.10, Azure HDInsight 3.5 - 3.6.

For trial and development purpose, we recommend you try Kylin with an all-in-one sandbox VM, like [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/), and give it 10 GB memory. To avoid permission issue in the sandbox, you can use its `root` account. We also suggest you using bridged mode instead of NAT mode in Virtual Box settings. Bridged mode will assign your sandbox an independent IP address so that you can avoid issues like [this](https://github.com/KylinOLAP/Kylin/issues/12).


 
