---
layout: docs
title:  "Installation Guide"
categories: install
permalink: /docs/install/index.html
---

### Software Requirements

* Hadoop: cdh5.x, cdh6.x, hdp2.x, EMR5.x, EMR6.x, HDI4.x
* Hive: 0.13 - 1.2.1+
* Spark: 2.4.7
* Mysql: 5.1.17及以上
* JDK: 1.8+
* OS: Linux only, CentOS 6.5+ or Ubuntu 16.0.4+

Tests passed on Hortonworks HDP2.4, Cloudera CDH 5.7 and 6.3.2, AWS EMR 5.31 and 6.0, Azure HDInsight 4.0.

We recommend you to try out Kylin or develop it using the integrated sandbox, such as [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/), and make sure it has at least 10 GB of memory. When configuring a sandbox, we recommend that you use the Bridged Adapter model instead of the NAT model.



### Hardware Requirements

The minimum configuration of a server running Kylin is 4 core CPU, 16 GB RAM and 100 GB disk. For high-load scenarios, a 24-core CPU, 64 GB RAM or higher is recommended.



### Hadoop Environment

Kylin relies on Hadoop clusters to handle large data sets. You need to prepare a Hadoop cluster with HDFS, YARN, Hive, Zookeeper and other services for Kylin to run.
Kylin can be launched on any node in a Hadoop cluster. For convenience, you can run Kylin on the master node. For better stability, it is recommended to deploy Kylin on a clean Hadoop client node with Hive, HDFS and other command lines installed and client configuration (such as `core-site.xml`, `hive-site.xml`and others) are also reasonably configured and can be automatically synchronized with other nodes.

Linux accounts running Kylin must have access to the Hadoop cluster, including the permission to create/write HDFS folders, Hive tables.



### Kylin Installation

- Download a Apache kylin 4.0.0 binary package from the [Apache Kylin Download Site](https://kylin.apache.org/download/). For example, the following command line can be used:

```shell
cd /usr/local/
wget http://mirror.bit.edu.cn/apache/kylin/apache-kylin-4.0.0/apache-kylin-4.0.0-bin.tar.gz
```

- Unzip the tarball and configure the environment variable `$KYLIN_HOME` to the Kylin folder.

```shell
tar -zxvf apache-kylin-4.0.0-bin.tar.gz
cd apache-kylin-4.0.0-bin
export KYLIN_HOME=`pwd`
```

- Run the script to download spark:

```shell
$KYLIN_HOME/bin/download-spark.sh
```

Or configure SPARK_HOME points to the path of spark2.4.7/3.1.1 in the environment.

- Configure MySQL metastore

Kylin 4.0 uses MySQL as metadata storage, make the following configuration in `kylin.properties`:

```shell
kylin.metadata.url=kylin_metadata@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql//localhost:3306/kylin_test,username=,password=
kylin.env.zookeeper-connect-string=ip:2181
```

You need to change the Mysql user name and password, as well as the database and table where the metadata is stored. And put mysql jdbc connector into `$KYLIN_HOME/ext/`, if there is no such directory, please create it.
Please refer to [配置 Mysql 为 Metastore](/_docs40/tutorial/mysql_metastore.html) learn about the detailed configuration of MySQL as a Metastore.

### Kylin tarball structure
* `bin`: shell scripts to start/stop Kylin service, backup/restore metadata, as well as some utility scripts.
* `conf`: XML configuration files. The function of these xml files can be found in [configuration page](/docs/install/configuration.html)
* `lib`: Kylin jar files for external use, like the Hadoop job jar, JDBC driver, HBase coprocessor jar, etc.
* `meta_backups`: default backup folder when run "bin/metastore.sh backup";
* `sample_cube`: files to create the sample cube and its tables.
* `spark`: Spark by $KYLIN_HOME/bin/download.sh download.
* `tomcat` the tomcat web server that run Kylin application. 
* `tool`: the jar file for running utility CLI. 

### Perform additional steps for some environments
For Hadoop environment of CDH6.X, EMR5.X, EMR6.X, you need to perform some additional steps before starting kylin.
For CDH6.X environment, please check the document: [Deploy kylin4.0 on CDH6](https://cwiki.apache.org/confluence/display/KYLIN/Deploy+Kylin+4+on+CDH+6)
For EMR environment, please check the document: [Deploy kylin4.0 on EMR](https://cwiki.apache.org/confluence/display/KYLIN/Deploy+Kylin+4+on+AWS+EMR)

### Checking the operating environment

Kylin runs on a Hadoop cluster and has certain requirements for the version, access rights, and CLASSPATH of each component. To avoid various environmental problems, you can run the script, `$KYLIN_HOME/bin/check-env.sh` to have a test on your environment, if there are any problems with your environment, the script will print a detailed error message. If there is no error message, it means that your environment is suitable for Kylin to run.


### Start Kylin

Run the script, `$KYLIN_HOME/bin/kylin.sh start` , to start Kylin. The interface output is as follows:

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-4.0.0-bin
......
A new Kylin instance is started by root. To stop it, run 'kylin.sh stop'
Check the log at /usr/local/apache-kylin-4.0.0-bin/logs/kylin.log
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
KYLIN_HOME is set to /usr/local/apache-kylin-4.0.0-bin
Stopping Kylin: 25964
Stopping in progress. Will check after 2 secs again...
Kylin with pid 25964 has been stopped.
```

You can run `ps -ef | grep kylin` to see if the Kylin process has stopped.


### HDFS folder structure
Kylin will generate files on HDFS. The default root directory is "kylin/", and then the metadata table name of kylin cluster will be used as the second layer directory name, and the default is "kylin_metadata"(can be customized in `conf/kylin.properties`)

Generally, `/kylin/kylin_metadata` directory stores data according to different projects, such as data directory of "learn_kylin" project is `/kylin/kylin_metadata/learn_kylin`, which usually includes the following subdirectories:
1.`job_tmp`: store temporary files generated during the execution of tasks.
2.`parquet`: the cuboid file of each cube.
3.`table_snapshot`: stores the dimension table snapshot.
