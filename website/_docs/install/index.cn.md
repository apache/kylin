---
layout: docs-cn
title:  "安装指南"
categories: install
permalink: /cn/docs/install/index.html
---

### 软件要求

* Hadoop: cdh5.x, cdh6.x, hdp2.x, EMR5.x, EMR6.x, HDI4.x
* Hive: 0.13 - 1.2.1+
* Spark: 2.4.7/3.1.1
* Mysql: 5.1.17 及以上
* JDK: 1.8+
* OS: Linux only, CentOS 6.5+ or Ubuntu 16.0.4+

在 Hortonworks HDP2.4, Cloudera CDH 5.7 and 6.3.2, AWS EMR 5.31 and 6.0, Azure HDInsight 4.0 上测试通过。

我们建议您使用集成的 sandbox 来试用 Kylin 或进行开发，比如 [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/)，且要保证其有至少 10 GB 内存。在配置沙箱时，我们推荐您使用 Bridged Adapter 模型替代 NAT 模型。


### 硬件要求

运行 Kylin 的服务器的最低配置为 4 core CPU，16 GB 内存和 100 GB 磁盘。 对于高负载的场景，建议使用 24 core CPU，64 GB 内存或更高的配置。

### Hadoop 环境

Kylin 依赖于 Hadoop 集群处理大量的数据集。您需要准备一个配置好 HDFS, YARN, Hive, Zookeeper, Spark以及你可能需要的其他服务的 Hadoop 集群供 Kylin 运行。
Kylin 可以在 Hadoop 集群的任意节点上启动。方便起见，您可以在 master 节点上运行 Kylin。但为了更好的稳定性，我们建议您将 Kylin 部署在一个干净的 Hadoop client 节点上，该节点上 Hive，HDFS 等命令行已安装好且 client 配置（如 `core-site.xml`，`hive-site.xml`及其他）也已经合理的配置且其可以自动和其它节点同步。

运行 Kylin 的 Linux 账户要有访问 Hadoop 集群的权限，包括创建/写入 HDFS 文件夹，Hive 表的权限。 


### Kylin 安装

- 从 [Apache Kylin下载网站](https://kylin.apache.org/download/) 下载一个 Apache Kylin 4.0 的二进制文件。可通过如下命令行下载得到：

```shell
cd /usr/local/
wget http://mirror.bit.edu.cn/apache/kylin/apache-kylin-4.0.0/apache-kylin-4.0.0-bin.tar.gz
```

- 解压 tar 包，配置环境变量 `$KYLIN_HOME` 指向 Kylin 文件夹。

```shell
tar -zxvf apache-kylin-4.0.0-bin.tar.gz
cd apache-kylin-4.0.0-bin
export KYLIN_HOME=`pwd`
```

- 使用脚本下载spark:

```shell
$KYLIN_HOME/bin/download-spark.sh
```

download-spark.sh 脚本只能下载 spark2.4.7, 如果您使用的 kylin 二进制包以 spark3 为后缀，您需要从[Spark 官方网站](https://spark.apache.org/)下载 spark3.1.1 的二进制包。
建议将 spark 二进制包解压后放置在 ${KYLIN_HOME} 目录下，并重命名为 spark，以避免兼容性问题。详情请查看：[Refactor hive and hadoop dependency](https://cwiki.apache.org/confluence/display/KYLIN/KIP+10+refactor+hive+and+hadoop+dependency)
如果您自定义配置了 ${SPARK_HOME} 指向环境中的 spark2.4.7/spark3.1.1，请保证环境中的 spark 是可以正常提交以及执行任务的。

- 配置 Mysql 元数据

Kylin 4.0 使用 Mysql 作为元数据存储，需要在 kylin.properties 中做如下配置：

```shell
kylin.metadata.url=kylin_metadata@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://localhost:3306/kylin_test,username=,password=
kylin.env.zookeeper-connect-string=ip
```

你需要修改其中的 Mysql 用户名和密码，以及存储元数据的 database 和 table。并将 mysql jdbc connector 放在 `$KYLIN_HOME/ext` 目录下，没有该目录时请自行创建。
请参考 [配置 Mysql 为 Metastore](/_docs40/tutorial/mysql_metastore.html)  了解 Mysql 作为 Metastore 的详细配置。

### Kylin tarball 目录
* `bin`: shell 脚本，用于启动／停止 Kylin，备份／恢复 Kylin 元数据，以及一些检查端口、获取 Hive/HBase 依赖的方法等；
* `conf`: Hadoop 任务的 XML 配置文件，这些文件的作用可参考[配置页面](/docs/install/configuration.html)
* `lib`: 供外面应用使用的 jar 文件，例如 Hadoop 任务 jar.
* `meta_backups`: 执行 `bin/metastore.sh backup` 后的默认的备份目录;
* `sample_cube` 用于创建样例 Cube 和表的文件。
* `spark`: 使用kylin脚本下载得到的 spark。
* `tomcat`: 自带的 tomcat，用于启动 Kylin 服务。
* `tool`: 用于执行一些命令行的jar文件。

### 为某些环境执行额外步骤
对于 CDH6.x, EMR5.x, EMR6.x 的 hadoop 环境，在启动 kylin 之前需要执行一些额外的步骤。
CDH6.x 环境请查看文档: [Deploy Kylin4 on CDH6](https://cwiki.apache.org/confluence/display/KYLIN/Deploy+Kylin+4+on+CDH+6)
EMR 环境请查看文档: [Deploy Kylin4 on AWS EMR](https://cwiki.apache.org/confluence/display/KYLIN/Deploy+Kylin+4+on+AWS+EMR)

### 检查运行环境

Kylin 运行在 Hadoop 集群上，对各个组件的版本、访问权限及 CLASSPATH 等都有一定的要求，为了避免遇到各种环境问题，您可以运行 `$KYLIN_HOME/bin/check-env.sh` 脚本来进行环境检测，如果您的环境存在任何的问题，脚本将打印出详细报错信息。如果没有报错信息，代表您的环境适合 Kylin 运行。


### 启动 Kylin

运行 `$KYLIN_HOME/bin/kylin.sh start` 脚本来启动 Kylin，界面输出如下：

```
Retrieving hadoop conf dir...
KYLIN_HOME is set to /usr/local/apache-kylin-4.0.0-bin
......
A new Kylin instance is started by root. To stop it, run 'kylin.sh stop'
Check the log at /usr/local/apache-kylin-4.0.0-bin/logs/kylin.log
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
KYLIN_HOME is set to /usr/local/apache-kylin-4.0.0-bin
Stopping Kylin: 25964
Stopping in progress. Will check after 2 secs again...
Kylin with pid 25964 has been stopped.
```

您可以运行 `ps -ef | grep kylin` 来查看 Kylin 进程是否已停止。

### HDFS 目录结构
Kylin 会在 HDFS 上生成文件，默认根目录是 "/kylin/", 然后会使用 Kylin 集群的元数据表名作为第二层目录名，默认为 "kylin_metadata" (可以在`conf/kylin.properties`中定制).

通常, `/kylin/kylin_metadata` 目录下按照不同的 project 存放数据，比如 learn_kylin 项目的数据目录为 `/kylin/kylin_metadata/learn_kylin`, 该目录下通常包括以下子目录：
1.`job_tmp`: 存放执行任务过程中生成的临时文件。
2.`parquet`: 存放各个 cube 的 cuboid 文件。
3.`table_snapshot`: 存放维度表快照。
