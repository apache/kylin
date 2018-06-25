---
layout: docs
title:  "Installation Guide"
categories: install
permalink: /docs/install/index.html
---

## Software requirements

* Hadoop: 2.7+
* Hive: 0.13 - 1.2.1+
* HBase: 1.1+
* Spark 2.1.1+
* JDK: 1.7+
* OS: Linux only, CentOS 6.5+ or Ubuntu 16.0.4+

Tested with Hortonworks HDP 2.2 - 2.6, Cloudera CDH 5.7 - 5.11, AWS EMR 5.7 - 5.10, Azure HDInsight 3.5 - 3.6.

For trial and development purpose, we recommend you try Kylin with an all-in-one sandbox VM, like [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/), and give it 10 GB memory. We suggest you using bridged mode instead of NAT mode in Virtual Box settings. 

## Hardware requirements

The server to run Kylin need 4 core CPU, 16 GB memory and 100 GB disk as the minimal configuration. For high workload scenario, 24 core CPU, 64 GB memory or more is recommended.


## Hadoop Environment

Kylin depends on Hadoop cluster to process the massive data set. You need prepare a well configured Hadoop cluster for Kylin to run, with the common services includes HDFS, YARN, MapReduce, Hive, HBase, Zookeeper and other services. It is most common to install Kylin on a Hadoop client machine, from which Kylin can talk with the Hadoop cluster via command lines including `hive`, `hbase`, `hadoop`, etc. 

Kylin itself can be started in any node of the Hadoop cluster. For simplity, you can run it in the master node. But to get better stability, we suggest you to deploy it a pure Hadoop client node, on which the command lines like `hive`, `hbase`, `hadoop`, `hdfs` already be installed and the client congfigurations (core-site.xml, hive-site.xml, hbase-site.xml, etc) are properly configured and will be automatically syned with other nodes. The Linux account that running Kylin has the permission to access the Hadoop cluster, including create/write HDFS folders, hive tables, hbase tables and submit MR jobs. 

## Installation Kylin

 * Download a version of Kylin binaries for your Hadoop version from a closer Apache download site. For example, Kylin 2.3.1 for HBase 1.x from US:
{% highlight Groff markup %}
cd /usr/local
wget http://www-us.apache.org/dist/kylin/apache-kylin-2.3.1/apache-kylin-2.3.1-hbase1x-bin.tar.gz
{% endhighlight %}
 * Uncompress the tarball and then export KYLIN_HOME pointing to the Kylin folder
{% highlight Groff markup %}
tar -zxvf apache-kylin-2.3.1-hbase1x-bin.tar.gz
cd apache-kylin-2.3.1-bin
export KYLIN_HOME=`pwd`
{% endhighlight %}
 * Make sure the user has the privilege to run hadoop, hive and hbase cmd in shell. If you are not so sure, you can run `$KYLIN_HOME/bin/check-env.sh`, it will print out the detail information if you have some environment issues. If no error, that means the environment is ready.
{% highlight Groff markup %}
-bash-4.1# $KYLIN_HOME/bin/check-env.sh
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.3.1-bin
-bash-4.1#
{% endhighlight %}
 * Start Kylin, run `$KYLIN_HOME/bin/kylin.sh start`, after the server starts, you can watch `$KYLIN_HOME/logs/kylin.log` for runtime logs;
{% highlight Groff markup %}
-bash-4.1# $KYLIN_HOME/bin/kylin.sh start
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.3.1-bin
Retrieving hive dependency...
Retrieving hbase dependency...
Retrieving hadoop conf dir...
Retrieving kafka dependency...
Retrieving Spark dependency...
...
A new Kylin instance is started by root. To stop it, run 'kylin.sh stop'
Check the log at /usr/local/apache-kylin-2.3.1-bin/logs/kylin.log
Web UI is at http://<hostname>:7070/kylin
-bash-4.1#
{% endhighlight %}
 * After Kylin started you can visit <http://hostname:7070/kylin> in your web browser. The initial username/password is ADMIN/KYLIN. 
 * To stop Kylin, run `$KYLIN_HOME/bin/kylin.sh stop`
{% highlight Groff markup %}
-bash-4.1# $KYLIN_HOME/bin/kylin.sh stop
Retrieving hadoop conf dir... 
KYLIN_HOME is set to /usr/local/apache-kylin-2.3.1-bin
Stopping Kylin: 7014
Kylin with pid 7014 has been stopped.
{% endhighlight %}


