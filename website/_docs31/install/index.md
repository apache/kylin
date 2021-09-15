---
layout: docs31
title:  "Installation Guide"
categories: install
permalink: /docs31/install/index.html
---

### Software Requirements

* Hadoop: 2.7+, 3.1+ (since v2.5)
* Hive: 0.13 - 1.2.1+
* HBase: 1.1+, 2.0 (since v2.5)
* Spark (optional) 2.3.0+
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

- Download a binary package for your Hadoop version from the [Apache Kylin Download Site](https://kylin.apache.org/download/). For example, Kylin 2.5.0 for HBase 1.x can be downloaded from the following command line:

```shell
cd /usr/local/
wget http://mirror.bit.edu.cn/apache/kylin/apache-kylin-2.5.0/apache-kylin-2.5.0-bin-hbase1x.tar.gz
```

- Unzip the tarball and configure the environment variable `$KYLIN_HOME` to the Kylin folder.

```shell
tar -zxvf apache-kylin-2.5.0-bin-hbase1x.tar.gz
cd apache-kylin-2.5.0-bin-hbase1x
export KYLIN_HOME=`pwd`
```

- From v2.6.1, Kylin will not ship Spark binary anymore; You need to install Spark seperately, and then point `SPARK_HOME` system environment variable to it: 

```shell
export SPARK_HOME=/path/to/spark
```

or run the script to download it:

```shell
$KYLIN_HOME/bin/download-spark.sh
```

### Kylin tarball structure
* `bin`: shell scripts to start/stop Kylin service, backup/restore metadata, as well as some utility scripts.
* `conf`: XML configuration files. The function of these xml files can be found in [configuration page](/docs31/install/configuration.html)
* `lib`: Kylin jar files for external use, like the Hadoop job jar, JDBC driver, HBase coprocessor jar, etc.
* `meta_backups`: default backup folder when run "bin/metastore.sh backup";
* `sample_cube`: files to create the sample cube and its tables.
* `spark`: the default spark binary that built with Kylin.
* `tomcat` the tomcat web server that run Kylin application. 
* `tool`: the jar file for running utility CLI. 

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

**Note**: If you encounter the following error when starting kylin (possible in Hadoop3 environment)：

```
Caused by: java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(ZLjava/lang/String;Ljava/lang/Object;)V
	at org.apache.hadoop.conf.Configuration.set(Configuration.java:1358)
	at org.apache.hadoop.conf.Configuration.set(Configuration.java:1339)
	at org.apache.kylin.common.util.HadoopUtil.healSickConfig(HadoopUtil.java:77)
	at org.apache.kylin.common.util.HadoopUtil.getCurrentConfiguration(HadoopUtil.java:63)
	at org.apache.kylin.storage.hbase.HBaseConnection.newHBaseConfiguration(HBaseConnection.java:170)
	at org.apache.kylin.storage.hbase.HBaseConnection.get(HBaseConnection.java:259)
	at org.apache.kylin.storage.hbase.HBaseResourceStore.getConnection(HBaseResourceStore.java:96)
	at org.apache.kylin.storage.hbase.HBaseResourceStore.createHTableIfNeeded(HBaseResourceStore.java:119)
	at org.apache.kylin.storage.hbase.HBaseResourceStore.<init>(HBaseResourceStore.java:89)
	... 8 more
```

You can try download [guava-28.0-jre.jar](https://repo1.maven.org/maven2/com/google/guava/guava/28.0-jre/guava-28.0-jre.jar), put it into `$KYLIN_HOME/tool/` and `$KYLIN_HOME/tomcat/lib/` and restart kylin `bin/kylin.sh restart`.

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


### HDFS folder structure
Kylin will generate files on HDFS. The root folder is "/kylin/", but will have the second level folder for each Kylin cluster, named with the metadata table name, by default it is "kylin_metadata" (can be customized in `conf/kylin.properties`).

Usually, there are at least these four kind of directories under `/kylin/kylin_metadata`: `cardinality`, `coprocessor`, `kylin-job_id`, `resources`. 
1. `cardinality`: the output folder of the cardinality calculation job when Kylin loads a Hive table. It can be cleaned when there is no job running;
2. `coprocessor`: the folder that Kylin puts HBase coprocessor jar file. Please do not delete it. 
3. `kylin-job_id`: the cubing job's output folder. Please keep them; if need a cleanup, follow the [storage cleanup guide](/docs31/howto/howto_cleanup_storage.html). 
4. `resources`: the metadata entries that too big to persisted in HBase (e.g, a dictionary or table snapshot); Please do not delete it; if need a cleanup, follow the [cleanup resources from metadata](/docs31/howto/howto_backup_metadata.html) 
5. `jdbc-resources`: similar as `resources`, only appeared when using MySQL as the metadata storage。
