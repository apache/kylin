---
layout: docs
title:  "Installation Guide"
categories: install
permalink: /docs/install/index.html
---

### Software Requirements

* Hadoop: 2.7+, 3.1+ (since v2.5)
* Hive: 0.13 - 1.2.1+
* HBase: 1.1+, 2.0 (since v2.5)
* Spark (optional) 2.1.1+
* Kafka (optional) 1.0.0+ (since v2.5)
* JDK: 1.8+ (since v2.5)
* OS: Linux only, CentOS 6.5+ or Ubuntu 16.0.4+

Tests passed on Hortonworks HDP 2.2-2.6 and 3.0, Cloudera CDH 5.7-5.11 and 6.0, AWS EMR 5.7-5.10, Azure HDInsight 3.5-3.6.

We recommend you to try out Kylin or develop it using the integrated sandbox, such as [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/), and make sure it has at least 10 GB of memory. When configuring a sandbox, we recommend that you use the Bridged Adapter model instead of the NAT model.



### Hardware Requirements

The minimum configuration of a server running Kylin is 4 core CPU, 16 GB RAM and 100 GB disk. For high-load scenarios, a 24-core CPU, 64 GB RAM or higher is recommended.



### Hadoop Environment

Kylin relies on Hadoop clusters to handle large data sets. You need to prepare a Hadoop cluster with HDFS, YARN, MapReduce, Hive, HBase, Zookeeper and other services for Kylin to run.
Kylin can be launched on any node in a Hadoop cluster. For convenience, you can run Kylin on the master node. For better stability, it is recommended to deploy Kylin on a clean Hadoop client node with Hive, HBase, HDFS and other command lines installed and client configuration (such as `core-site.xml`, `hive-site.xml`, `hbase-site.xml` and others) are also reasonably configured and can be automatically synchronized with other nodes.

Linux accounts running Kylin must have access to the Hadoop cluster, including the permission to create/write HDFS folders, Hive tables, HBase tables, and submit MapReduce tasks.



### Kylin Installation

1. Download a binary package for your Hadoop version from the [Apache Kylin Download Site](https://kylin.apache.org/download/). For example, Kylin 2.5.0 for HBase 1.x can be downloaded from the following command line:

```shell
cd /usr/local/
wget http://mirror.bit.edu.cn/apache/kylin/apache-kylin-2.5.0/apache-kylin-2.5.0-bin-hbase1x.tar.gz
```

2. Unzip the tarball and configure the environment variable `$KYLIN_HOME` to the Kylin folder.

```shell
tar -zxvf apache-kylin-2.5.0-bin-hbase1x.tar.gz
cd apache-kylin-2.5.0-bin-hbase1x
export KYLIN_HOME=`pwd`
```



### Checking the operating environment

Kylin runs on a Hadoop cluster and has certain requirements for the version, access rights, and CLASSPATH of each component. To avoid various environmental problems, you can run the script, `$KYLIN_HOME/bin/check-env.sh` to have a test on your environment, if there are any problems with your environment, the script will print a detailed error message. If there is no error message, it means that your environment is suitable for Kylin to run.



### Start Kylin

Run the script, `$KYLIN_HOME/bin/kylin.sh start` , to start Kylin. The interface output is as follows:

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.5.0-bin-hbase1x
......
A new Kylin instance is started by root. To stop it, run 'kylin.sh stop'
Check the log at /usr/local/apache-kylin-2.5.0-bin-hbase1x/logs/kylin.log
Web UI is at http://<hostname>:7070/kylin
```



### Using Kylin

Once Kylin is launched, you can access it via the browser `http://<hostname>:7070/kylin` with
specifying `<hostname>` with IP address or domain name, and the default port is 7070.
The initial username and password are `ADMIN/KYLIN`.
After the server is started, you can view the runtime log, `$KYLIN_HOME/logs/kylin.log`.



### Stop Kylin

Run the `$KYLIN_HOME/bin/kylin.sh stop` script to stop Kylin. The console output is as follows:

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.5.0-bin-hbase1x
Stopping Kylin: 25964
Stopping in progress. Will check after 2 secs again...
Kylin with pid 25964 has been stopped.
```

You can run `ps -ef | grep kylin` to see if the Kylin process has stopped.