---
layout: docs-cn
title:  "安装指南"
categories: install
permalink: /cn/docs/install/index.html
---

### 软件要求

* Hadoop: 2.7+, 3.1+ (since v2.5)
* Hive: 0.13 - 1.2.1+
* HBase: 1.1+, 2.0 (since v2.5)
* Spark (可选) 2.3.0+
* Kafka (可选) 1.0.0+ (since v2.5)
* JDK: 1.8+ (since v2.5)
* OS: Linux only, CentOS 6.5+ or Ubuntu 16.0.4+

在 Hortonworks HDP 2.2-2.6 and 3.0, Cloudera CDH 5.7-5.11 and 6.0, AWS EMR 5.7-5.10, Azure HDInsight 3.5-3.6 上测试通过。

我们建议您使用集成的 sandbox 来试用 Kylin 或进行开发，比如 [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/)，且要保证其有至少 10 GB 内存。在配置沙箱时，我们推荐您使用 Bridged Adapter 模型替代 NAT 模型。



### 硬件要求

运行 Kylin 的服务器的最低配置为 4 core CPU，16 GB 内存和 100 GB 磁盘。 对于高负载的场景，建议使用 24 core CPU，64 GB 内存或更高的配置。

### Hadoop 环境

Kylin 依赖于 Hadoop 集群处理大量的数据集。您需要准备一个配置好 HDFS，YARN，MapReduce，Hive， HBase，Zookeeper 和其他服务的 Hadoop 集群供 Kylin 运行。
Kylin 可以在 Hadoop 集群的任意节点上启动。方便起见，您可以在 master 节点上运行 Kylin。但为了更好的稳定性，我们建议您将 Kylin 部署在一个干净的 Hadoop client 节点上，该节点上 Hive，HBase，HDFS 等命令行已安装好且 client 配置（如 `core-site.xml`，`hive-site.xml`，`hbase-site.xml`及其他）也已经合理的配置且其可以自动和其它节点同步。

运行 Kylin 的 Linux 账户要有访问 Hadoop 集群的权限，包括创建/写入 HDFS 文件夹，Hive 表， HBase 表和提交 MapReduce 任务的权限。 



### Kylin 安装

1. 从 [Apache Kylin下载网站](https://kylin.apache.org/download/) 下载一个适用于您 Hadoop 版本的二进制文件。例如，适用于 HBase 1.x 的 Kylin 2.5.0 可通过如下命令行下载得到：

```shell
cd /usr/local/
wget http://mirror.bit.edu.cn/apache/kylin/apache-kylin-2.5.0/apache-kylin-2.5.0-bin-hbase1x.tar.gz
```

2. 解压 tar 包，配置环境变量 `$KYLIN_HOME` 指向 Kylin 文件夹。

```shell
tar -zxvf apache-kylin-2.5.0-bin-hbase1x.tar.gz
cd apache-kylin-2.5.0-bin-hbase1x
export KYLIN_HOME=`pwd`
```

从 v2.6.1 开始， Kylin 不再包含 Spark 二进制包; 您需要另外下载 Spark，然后设置 `SPARK_HOME` 系统变量到 Spark 安装目录： 

```shell
export SPARK_HOME=/path/to/spark
```

或者使用脚本下载:

```shell
$KYLIN_HOME/bin/download-spark.sh
```

### Kylin tarball 目录
* `bin`: shell 脚本，用于启动／停止 Kylin，备份／恢复 Kylin 元数据，以及一些检查端口、获取 Hive/HBase 依赖的方法等；
* `conf`: Hadoop 任务的 XML 配置文件，这些文件的作用可参考[配置页面](/docs/install/configuration.html)
* `lib`: 供外面应用使用的 jar 文件，例如 Hadoop 任务 jar, JDBC 驱动, HBase coprocessor 等.
* `meta_backups`: 执行 `bin/metastore.sh backup` 后的默认的备份目录;
* `sample_cube` 用于创建样例 Cube 和表的文件。
* `spark`: 自带的 spark。
* `tomcat`: 自带的 tomcat，用于启动 Kylin 服务。
* `tool`: 用于执行一些命令行的jar文件。


### 检查运行环境

Kylin 运行在 Hadoop 集群上，对各个组件的版本、访问权限及 CLASSPATH 等都有一定的要求，为了避免遇到各种环境问题，您可以运行 `$KYLIN_HOME/bin/check-env.sh` 脚本来进行环境检测，如果您的环境存在任何的问题，脚本将打印出详细报错信息。如果没有报错信息，代表您的环境适合 Kylin 运行。



### 启动 Kylin

运行 `$KYLIN_HOME/bin/kylin.sh start` 脚本来启动 Kylin，界面输出如下：

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.5.0-bin-hbase1x
......
A new Kylin instance is started by root. To stop it, run 'kylin.sh stop'
Check the log at /usr/local/apache-kylin-2.5.0-bin-hbase1x/logs/kylin.log
Web UI is at http://<hostname>:7070/kylin
```



### 使用 Kylin

Kylin 启动后您可以通过浏览器 `http://<hostname>:7070/kylin` 进行访问。
其中 `<hostname>` 为具体的机器名、IP 地址或域名，默认端口为 7070。
初始用户名和密码是 `ADMIN/KYLIN`。
服务器启动后，您可以通过查看 `$KYLIN_HOME/logs/kylin.log` 获得运行时日志。


### 停止 Kylin

运行 `$KYLIN_HOME/bin/kylin.sh stop` 脚本来停止 Kylin，界面输出如下：

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-2.5.0-bin-hbase1x
Stopping Kylin: 25964
Stopping in progress. Will check after 2 secs again...
Kylin with pid 25964 has been stopped.
```

您可以运行 `ps -ef | grep kylin` 来查看 Kylin 进程是否已停止。

### HDFS 目录结构
Kylin 会在 HDFS 上生成文件，根目录是 "/kylin/", 然后会使用 Kylin 集群的元数据表名作为第二层目录名，默认为 "kylin_metadata" (可以在`conf/kylin.properties`中定制).

通常, `/kylin/kylin_metadata` 目录下会有这么几种子目录：`cardinality`, `coprocessor`, `kylin-job_id`, `resources`, `jdbc-resources`. 
1. `cardinality`: Kylin 加载 Hive 表时，会启动一个 MR 任务来计算各个列的基数，输出结果会暂存在此目录。此目录可以安全清除。
2. `coprocessor`: Kylin 用于存放 HBase coprocessor jar 的目录；请勿删除。
3. `kylin-job_id`: Cube 计算过程的数据存储目录，请勿删除。 如需要清理，请遵循 [storage cleanup guide](/docs/howto/howto_cleanup_storage.html). 
4. `resources`: Kylin 默认会将元数据存放在 HBase，但对于太大的文件（如字典或快照），会转存到 HDFS 的该目录下，请勿删除。如需要清理，请遵循 [cleanup resources from metadata](/docs/howto/howto_backup_metadata.html) 
5. `jdbc-resources`：性质同上，只在使用 MySQL 做元数据存储时候出现。
