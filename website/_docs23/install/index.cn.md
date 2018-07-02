---
layout: docs23-cn
title:  "安装指南"
categories: install
permalink: /cn/docs23/install/index.html
---

## 软件要求

* Hadoop: 2.7+
* Hive: 0.13 - 1.2.1+
* HBase: 1.1+
* Spark 2.1.1+
* JDK: 1.7+
* OS: Linux only, CentOS 6.5+ or Ubuntu 16.0.4+

用 Hortonworks HDP 2.2 - 2.6, Cloudera CDH 5.7 - 5.11, AWS EMR 5.7 - 5.10, Azure HDInsight 3.5 - 3.6 进行测试。

出于试用和开发的目的，我们建议您使用集成的 sandbox 来试用 Kylin，比如 [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/)，且其要保证 10 GB memory。我们建议您在 Virtual Box settings 中使用桥接模式代替 NAT 模式。 

## 硬件要求

运行 Kylin 的服务器的最低的配置为 4 core CPU, 16 GB memory 和 100 GB disk。 对于高负载的场景，建议使用 24 core CPU, 64 GB memory 或更高的配置。


## Hadoop 环境

Kylin 依赖于 Hadoop 集群处理大量的数据集。您需要准备一个配置好 HDFS, YARN, MapReduce, Hive, Hbase, Zookeeper 和其他服务的 Hadoop 集群供 Kylin 运行。最常见的是在 Hadoop client machine 上安装 Kylin，这样 Kylin 可以通过（`hive`, `hbase`, `hadoop`, 以及其他的）命令行与 Hadoop 进行通信。 

Kylin 可以在 Hadoop 集群的任意节点上启动。方便起见，您可以在 master 节点上运行 Kylin。但为了更好的稳定性，我们建议您将其部署在一个干净的 Hadoop client 节点上，该节点上 `hive`, `hbase`, `hadoop`, `hdfs` 命令行已安装好且 client 配置如（core-site.xml, hive-site.xml, hbase-site.xml, 及其他）也已经合理的配置且其可以自动和其它节点同步。运行 Kylin 的 Linux 账户要有访问 Hadoop 集群的权限，包括 create/write HDFS 文件夹, hive 表, hbase 表 和 提交 MR jobs 的权限。 

## Kylin 安装

 * 从最新的 Apache 下载网站下载一个适用于您 Hadoop 版本的 Kylin binaries 文件。例如，来源于 US 适用于 HBase 1.x 的 Kylin 2.3.1:
{% highlight Groff markup %}
cd /usr/local
wget http://www-us.apache.org/dist/kylin/apache-kylin-2.3.1/apache-kylin-2.3.1-hbase1x-bin.tar.gz
{% endhighlight %}
 * 解压 tar 包，然后配置环境变量 KYLIN_HOME 指向 Kylin 文件夹
{% highlight Groff markup %}
tar -zxvf apache-kylin-2.3.1-hbase1x-bin.tar.gz
cd apache-kylin-2.3.1-bin
export KYLIN_HOME=`pwd`
{% endhighlight %}
 * 确保用户有权限在 shell 中运行 hadoop, hive 和 hbase cmd。如果您不确定，您可以运行 `$KYLIN_HOME/bin/check-env.sh` 脚本，如果您的环境有任何的问题，它会将打印出详细的信息。如果没有 error，意味着环境没问题。
{% highlight Groff markup %}
-bash-4.1# $KYLIN_HOME/bin/check-env.sh
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.3.1-bin
-bash-4.1#
{% endhighlight %}
 * 运行 `$KYLIN_HOME/bin/kylin.sh start` 脚本来启动 Kylin，服务器启动后，您可以通过查看 `$KYLIN_HOME/logs/kylin.log` 获得运行时日志。
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
 * Kylin 启动后您可以通过浏览器 <http://hostname:7070/kylin> 查看。初始用户名和密码是 ADMIN/KYLIN。
 * 运行 `$KYLIN_HOME/bin/kylin.sh stop` 脚本，停止 Kylin。
{% highlight Groff markup %}
-bash-4.1# $KYLIN_HOME/bin/kylin.sh stop
Retrieving hadoop conf dir... 
KYLIN_HOME is set to /usr/local/apache-kylin-2.3.1-bin
Stopping Kylin: 7014
Kylin with pid 7014 has been stopped.
{% endhighlight %}


