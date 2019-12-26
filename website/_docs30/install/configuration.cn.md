---
layout: docs30-cn
title:  "Kylin 配置"
categories: install
permalink: /cn/docs30/install/configuration.html
---



- [配置文件及参数重写](#kylin-config)
    - [Kylin 配置文件](#kylin-config)
	- [配置重写](#config-override)
		- [项目级别配置重写](#project-config-override)
		- [Cube 级别配置重写](#cube-config-override)
		- [重写 MapReduce 参数](#mr-config-override)
		- [重写 Hive 参数](#hive-config-override)
		- [重写 Spark 参数](#spark-config-override)
- [部署配置](#kylin-deploy)
    - [部署 Kylin](#deploy-config)
	- [分配更多内存给 Kylin 实例](#kylin-jvm-settings)
	- [任务引擎高可用](#job-engine-ha)
	- [任务引擎安全模式](#job-engine-safemode)
	- [读写分离配置](#rw-deploy)
	- [RESTful Webservice](#rest-config)
- [Metastore 配置](#kylin_metastore)
    - [元数据相关](#metadata)
    - [基于 MySQL 的 Metastore (测试)](#mysql-metastore)
- [构建配置](#kylin-build)
    - [Hive 客户端 & SparkSQL](#hive-client-and-sparksql)
    - [配置 JDBC 数据源](#jdbc-datasource)
    - [数据类型精度](#precision-config)
    - [Cube 设计](#cube-config)
    - [Cube 大小估计](#cube-estimate)
	- [Cube 构建算法](#cube-algorithm)
	- [自动合并](#auto-merge)
	- [维表快照](#snapshot)
	- [Cube 构建](#cube-build)
	- [字典相关](#dict-config)
	- [超高基维度的处理](#uhc-config)
	- [Spark 构建引擎](#spark-cubing)
	- [通过 Livy 提交 Spark 任务](#livy-submit-spark-job)
	- [Spark 资源动态分配](#dynamic-allocation)
	- [任务相关](#job-config)
	- [启用邮件通知](#email-notification)
	- [启用 Cube Planner](#cube-planner)
    - [任务输出](#job-output)
    - [启用压缩](#compress-config)
    - [实时 OLAP](#realtime-olap)
- [清理存储配置](#storage-clean-up-configuration)
    - [存储清理相关](#storage-clean-up-config)
- [查询配置](#kylin-query)
    - [查询相关](#query-config)
    - [模糊查询](#fuzzy)
	- [查询缓存](#cache-config)
	- [查询限制](#query-limit)
	- [坏查询](#bad-query)
	- [查询下压](#query-pushdown)
	- [查询改写](#convert-sql)
	- [收集查询指标到 JMX](#jmx-metrics)
	- [收集查询指标到 dropwizard](#dropwizard-metrics)
- [安全配置](#kylin-security)
	- [集成 LDAP 实现单点登录](#ldap-sso)
	- [集成 Apache Ranger](#ranger)
	- [启用 ZooKeeper ACL](#zookeeper-acl)
- [启用 Memcached 做分布式查询缓存](#distributed-cache)


### 配置文件及参数重写 {#kylin-config}

本小节介绍 Kylin 的配置文件和如何进行配置重写。



### Kylin 配置文件	 {#kylin-config-file}

Kylin 会自动从环境中读取 Hadoop 配置（`core-site.xml`），Hive 配置（`hive-site.xml`）和 HBase 配置（`hbase-site.xml`），另外，Kylin 的配置文件在 `$KYLIN_HOME/conf/` 目录下，如下：

- `kylin_hive_conf.xml`：该文件包含了 Hive 任务的配置项。
- `kylin_job_conf.xml` & `kylin_job_conf_inmem.xml`：该文件包含了 MapReduce 任务的配置项。当执行 **In-mem Cubing** 任务时，需要在 `kylin_job_conf_inmem.xml` 中为 mapper 申请更多的内存
- `kylin-kafka-consumer.xml`：该文件包含了 Kafka 任务的配置项。
- `kylin-server-log4j.properties`：该文件包含了 Kylin 服务器的日志配置项。
- `kylin-tools-log4j.properties`：该文件包含了 Kylin 命令行的日志配置项。
- `setenv.sh` ：该文件是用于设置环境变量的 shell 脚本，可以通过 `KYLIN_JVM_SETTINGS` 调整 Kylin JVM 栈的大小，且可以设置 `KAFKA_HOME` 等其他环境变量。
- `kylin.properties`：该文件是 Kylin 使用的全局配置文件。



### 配置重写	{#config-override}

`$KYLIN_HOME/conf/` 中有部分配置项可以在 Web UI 界面进行重写，配置重写分为**项目级别配置重写**和 **Cube 级别配置重写**。配置重写的优先级关系为：Cube 级别配置重写 > 项目级别配置重写 > 全局配置文件。



### 项目级别配置重写	{#project-config-override}

在 Web UI 界面点击 **Manage Project** ，选中某个项目，点击 **Edit** -> **Project Config** -> **+ Property**，进行项目级别的配置重写，如下图所示：
![](/images/install/override_config_project.png)



### Cube 级别配置重写		{#cube-config-override}

在设计 Cube （**Cube Designer**）的 **Configuration Overwrites** 步骤可以添加配置项，进行 Cube 级别的配置重写，如下图所示：
![](/images/install/override_config_cube.png)

以下参数可以在 Cube 级别重写：

- `kylin.cube.size-estimate*`
- `kylin.cube.algorithm*`
- `kylin.cube.aggrgroup*`
- `kylin.metadata.dimension-encoding-max-length`
- `kylin.cube.max-building-segments`
- `kylin.cube.is-automerge-enabled`
- `kylin.job.allow-empty-segment`
- `kylin.job.sampling-percentage`
- `kylin.source.hive.redistribute-flat-table`
- `kylin.engine.spark*`
- `kylin.query.skip-empty-segments`



### MapReduce 任务配置重写	{#mr-config-override}

Kylin 支持在项目和 Cube 级别重写 `kylin_job_conf.xml` 和 `kylin_job_conf_inmem.xml` 中参数，以键值对的性质，按照如下格式替换：
`kylin.engine.mr.config-override.<key> = <value>`
 * 如果用户希望任务从 Yarn 获得更多内存，可以这样设置：
 `kylin.engine.mr.config-override.mapreduce.map.java.opts=-Xmx7g` 和 `kylin.engine.mr.config-override.mapreduce.map.memory.mb=8192`
 * 如果用户希望 Cube 的构建任务使用不同的 YARN resource queue，可以设置：
 `kylin.engine.mr.config-override.mapreduce.job.queuename={queueName}`


### Hive 任务配置重写  {#hive-config-override}

Kylin 支持在项目和 Cube 级别重写 `kylin_hive_conf.xml` 中参数，以键值对的性质，按照如下格式替换：
`kylin.source.hive.config-override.<key> = <value>`
如果用户希望 Hive 使用不同的 YARN resource queue，可以设置：
`kylin.source.hive.config-override.mapreduce.job.queuename={queueName}` 



### Spark 任务配置重写   {#spark-config-override}

Kylin 支持在项目和 Cube 级别重写 `kylin.properties` 中的 Spark 参数，以键值对的性质，按照如下格式替换：
`kylin.engine.spark-conf.<key> = <value>`
如果用户希望 Spark 使用不同的 YARN resource queue，可以设置：
`kylin.engine.spark-conf.spark.yarn.queue={queueName}`



### 部署配置 {#kylin-deploy}

本小节介绍部署 Kylin 相关的配置。



### 部署 Kylin  {#deploy-config}

- `kylin.env.hdfs-working-dir`：指定 Kylin 服务所用的 HDFS 路径，默认值为 `/kylin`，请确保启动 Kylin 实例的用户有读写该目录的权限
- `kylin.env`：指定 Kylin 部署的用途，参数值可选 `DEV`，`QA`， `PROD`，默认值为 `DEV`，在 DEV 模式下一些开发者功能将被启用
- `kylin.env.zookeeper-base-path`：指定 Kylin 服务所用的 ZooKeeper 路径，默认值为 `/kylin`
- `kylin.env.zookeeper-connect-string`：指定 ZooKeeper 连接字符串，如果为空，使用 HBase 的 ZooKeeper
- `kylin.env.hadoop-conf-dir`：指定 Hadoop 配置文件目录，如果不指定的话，获取环境中的 `HADOOP_CONF_DIR`
- `kylin.server.mode`：指定 Kylin 实例的运行模式，参数值可选 `all`， `job`， `query`，默认值为 `all`，job 模式代表该服务仅用于任务调度，不用于查询；query 模式代表该服务仅用于查询，不用于构建任务的调度；all 模式代表该服务同时用于任务调度和 SQL 查询。
- `kylin.server.cluster-name`：指定集群名称



### 分配更多内存给 Kylin 实例		{#kylin-jvm-settings}

在 `$KYLIN_HOME/conf/setenv.sh` 中存在对 `KYLIN_JVM_SETTINGS` 的两种示例配置。
默认配置使用的内存较少，用户可以根据自己的实际情况，注释掉默认配置并取消另一配置前的注释符号以启用另一配置，从而为 Kylin 实例分配更多的内存资源，该项配置的默认值如下：

```shell
export KYLIN_JVM_SETTINGS="-Xms1024M -Xmx4096M -Xss1024K -XX`MaxPermSize=512M -verbose`gc -XX`+PrintGCDetails -XX`+PrintGCDateStamps -Xloggc`$KYLIN_HOME/logs/kylin.gc.$$ -XX`+UseGCLogFileRotation -XX`NumberOfGCLogFiles=10 -XX`GCLogFileSize=64M"
# export KYLIN_JVM_SETTINGS="-Xms16g -Xmx16g -XX`MaxPermSize=512m -XX`NewSize=3g -XX`MaxNewSize=3g -XX`SurvivorRatio=4 -XX`+CMSClassUnloadingEnabled -XX`+CMSParallelRemarkEnabled -XX`+UseConcMarkSweepGC -XX`+CMSIncrementalMode -XX`CMSInitiatingOccupancyFraction=70 -XX`+UseCMSInitiatingOccupancyOnly -XX`+DisableExplicitGC -XX`+HeapDumpOnOutOfMemoryError -verbose`gc -XX`+PrintGCDetails -XX`+PrintGCDateStamps -Xloggc`$KYLIN_HOME/logs/kylin.gc.$$ -XX`+UseGCLogFileRotation -XX`NumberOfGCLogFiles=10 -XX`GCLogFileSize=64M"
```



### 任务引擎高可用  {#job-engine-ha}

- `kylin.job.scheduler.default=2`：启用分布式任务调度器
- `kylin.job.lock=org.apache.kylin.storage.hbase.util.ZookeeperJobLock`：开启分布式任务锁

> 提示：更多信息请参考 [集群模式部署](/cn/docs/install/kylin_cluster.html) 中的**任务引擎高可用**部分。


### 任务引擎安全模式   {#job-engine-safemode}

安全模式仅在默认调度器中生效

- `kylin.job.scheduler.safemode=TRUE`: 启用安全模式，新提交的任务不会被执行。
- `kylin.job.scheduler.safemode.runable-projects=project1,project2`: 安全模式下仍然可以执行的项目列表，支持设置多个。

### 读写分离配置   {#rw-deploy}

- `kylin.storage.hbase.cluster-fs`：指明 HBase 集群的 HDFS 文件系统
- `kylin.storage.hbase.cluster-hdfs-config-file`：指向 HBase 集群的 HDFS 配置文件

> 提示：更多信息请参考 [Deploy Apache Kylin with Standalone HBase Cluster](http://kylin.apache.org/blog/2016/06/10/standalone-hbase-cluster/)



### RESTful Webservice  {#rest-config}

- `kylin.web.timezone`：指定 Kylin 的 REST 服务所使用的时区，默认值为 GMT+8
- `kylin.web.cross-domain-enabled`：是否支持跨域访问，默认值为 TRUE
- `kylin.web.export-allow-admin`：是否支持管理员用户导出信息，默认值为 TRUE
- `kylin.web.export-allow-other`：是否支持其他用户导出信息，默认值为 TRUE
- `kylin.web.dashboard-enabled`：是否启用 Dashboard，默认值为 FALSE



### Metastore 配置 {#kylin_metastore}

本小节介绍 Kylin Metastore 相关的配置。



### 元数据相关 {#metadata}

- `kylin.metadata.url`：指定元数据库路径，默认值为 kylin_metadata@hbase
- `kylin.metadata.sync-retries`：指定元数据同步重试次数，默认值为 3 
- `kylin.metadata.sync-error-handler`：默认值为 `DefaultSyncErrorHandler`
- `kylin.metadata.check-copy-on-write`：清除元数据缓存，默认值为 `FALSE`
- `kylin.metadata.hbase-client-scanner-timeout-period`：表示 HBase 客户端发起一次 scan 操作的 RPC 调用至得到响应之间总的超时时间，默认值为 10000(ms)
- `kylin.metadata.hbase-rpc-timeout`：指定 HBase 执行 RPC 操作的超时时间，默认值为 5000(ms)
- `kylin.metadata.hbase-client-retries-number`：指定 HBase 重试次数，默认值为 1（次）
- `kylin.metadata.resource-store-provider.jdbc`：指定 JDBC 使用的类，默认值为 `org.apache.kylin.common.persistence.JDBCResourceStore`



### 基于 MySQL 的 Metastore (测试) {#mysql-metastore}

> **注意**：该功能还在测试中，建议用户谨慎使用。

- `kylin.metadata.url`：指定元数据路径
- `kylin.metadata.jdbc.dialect`：指定 JDBC 方言
- `kylin.metadata.jdbc.json-always-small-cell`：默认值为 TRUE
- `kylin.metadata.jdbc.small-cell-meta-size-warning-threshold`：默认值为 100(MB)
- `kylin.metadata.jdbc.small-cell-meta-size-error-threshold`：默认值为 1(GB)
- `kylin.metadata.jdbc.max-cell-size`：默认值为 1(MB)
- `kylin.metadata.resource-store-provider.jdbc`：指定 JDBC 使用的类，默认值为 org.apache.kylin.common.persistence.JDBCResourceStore

> 提示：更多信息请参考[基于 MySQL 的 Metastore 配置](/cn/docs/tutorial/mysql_metastore.html)



### 构建配置 {#kylin-build}

本小节介绍 Kylin 数据建模及构建相关的配置。



### Hive 客户端 & SparkSQL {#hive-client-and-sparksql}

- `kylin.source.hive.client`：指定 Hive 命令行类型，参数值可选 cli 或 beeline，默认值为 cli
- `kylin.source.hive.beeline-shell`：指定 Beeline shell 的绝对路径，默认值为 beeline
- `kylin.source.hive.beeline-params`：当使用 Beeline 做为 Hive 的 Client 工具时，需要配置此参数，以提供更多信息给 Beeline
- `kylin.source.hive.enable-sparksql-for-table-ops`：默认值为 FALSE，当使用 SparkSQL 时需要设置为 TRUE
- `kylin.source.hive.sparksql-beeline-shell`：当使用 SparkSQL Beeline 做为 Hive 的 Client 工具时，需要配置此参数为 /path/to/spark-client/bin/beeline
- `kylin.source.hive.sparksql-beeline-params`：当使用 SparkSQL Beeline 做为 Hive 的 Client 工具时，需要配置此参数




### 配置 JDBC 数据源  {#jdbc-datasource}

- `kylin.source.default`：JDBC 使用的数据源种类
- `kylin.source.jdbc.connection-url`：JDBC 连接字符串
- `kylin.source.jdbc.driver`：JDBC 驱动类名
- `kylin.source.jdbc.dialect`：JDBC方言，默认值为 default
- `kylin.source.jdbc.user`：JDBC 连接用户名
- `kylin.source.jdbc.pass`：JDBC 连接密码 
- `kylin.source.jdbc.sqoop-home`：Sqoop 安装路径
- `kylin.source.jdbc.sqoop-mapper-num`：指定应该分为多少个切片，Sqoop 将为每一个切片运行一个 mapper，默认值为 4
- `kylin.source.jdbc.field-delimiter`：指定字段分隔符， 默认值为 \

> 提示：更多信息请参考[建立 JDBC 数据源](/cn/docs/tutorial/setup_jdbc_datasource.html)。




### 数据类型精度 {#precision-config}

- `kylin.source.hive.default-varchar-precision`：指定 varchar 字段的最大长度，默认值为256
- `kylin.source.hive.default-char-precision`：指定 char 字段的最大长度，默认值为 255
- `kylin.source.hive.default-decimal-precision`：指定 decimal 字段的精度，默认值为 19
- `kylin.source.hive.default-decimal-scale`：指定 decimal 字段的范围，默认值为 4



### Cube 设计 {#cube-config}

- `kylin.cube.ignore-signature-inconsistency`：Cube desc 中的 signature 信息能保证 Cube 不被更改为损坏状态，默认值为 FALSE
- `kylin.cube.aggrgroup.max-combination`：指定一个 Cube 的聚合组 Cuboid 上限，默认值为 32768
- `kylin.cube.aggrgroup.is-mandatory-only-valid`：是否允许 Cube 只包含 Base Cuboid，默认值为 FALSE，当使用 Spark Cubing 时需设置为 TRUE
- `kylin.cube.rowkey.max-size`：指定可以设置为 Rowkeys 的最大列数，默认值为 63，且最大不能超过 63
- `kylin.cube.allow-appear-in-multiple-projects`：是否允许一个 Cube 出现在多个项目中
- `kylin.cube.gtscanrequest-serialization-level`：默认值为 1
- `kylin.metadata.dimension-encoding-max-length`：指定维度作为 Rowkeys 时使用 fix_length 编码时的最大长度，默认值为 256
- `kylin.web.hide-measures`: 隐藏一些可能不需要的度量，默认值是RAW




### Cube 大小估计 {#cube-estimate}

Kylin 和 HBase 都在写入磁盘时使用压缩，因此，Kylin 将在其原来的大小上乘以比率来估计 Cube 大小。

- `kylin.cube.size-estimate-ratio`：普通的 Cube，默认值为 0.25
- `kylin.cube.size-estimate-memhungry-ratio`：已废弃，默认值为 0.05
- `kylin.cube.size-estimate-countdistinct-ratio`：包含精确去重度量的 Cube 大小估计，默认值为 0.5
- `kylin.cube.size-estimate-topn-ratio`：包含 TopN 度量的 Cube 大小估计，默认值为 0.5 



### Cube 构建算法 {#cube-algorithm}

- `kylin.cube.algorithm`：指定 Cube 构建的算法，参数值可选 `auto`，`layer` 和 `inmem`， 默认值为 auto，即 Kylin 会通过采集数据动态地选择一个算法 (layer or inmem)，如果用户很了解 Kylin 和自身的数据、集群，可以直接设置喜欢的算法
- `kylin.cube.algorithm.layer-or-inmem-threshold`：默认值为 7
- `kylin.cube.algorithm.inmem-split-limit`：默认值为 500
- `kylin.cube.algorithm.inmem-concurrent-threads`：默认值为 1
- `kylin.job.sampling-percentage`：指定数据采样百分比，默认值为 100



### 自动合并 {#auto-merge}

- `kylin.cube.is-automerge-enabled`：是否启用自动合并，默认值为 TRUE，将该参数设置为 FALSE 时，自动合并功能会被关闭，即使 Cube 设置中开启了自动合并、设置了自动合并阈值，也不会触发合并任务。
 


### 维表快照   {#snapshot}

- `kylin.snapshot.max-mb`：允许维表的快照大小的上限，默认值为 300(M)
- `kylin.snapshot.max-cache-entry`：缓存中最多可以存储的 snapshot 数量，默认值为 500
- `kylin.snapshot.ext.shard-mb`：设置存储维表快照的 HBase 分片大小，默认值为 500(M)
- `kylin.snapshot.ext.local.cache.path`：本地缓存路径，默认值为 lookup_cache 
- `kylin.snapshot.ext.local.cache.max-size-gb`：本地维表快照缓存大小，默认值为 200(M)



### Cube 构建 {#cube-build}

- `kylin.storage.default`：指定默认的构建引擎，默认值为 2，即 HBase 
- `kylin.source.hive.keep-flat-table`：是否在构建完成后保留 Hive 中间表，默认值为 FALSE
- `kylin.source.hive.database-for-flat-table`：指定存放 Hive 中间表的 Hive 数据库名字，默认值为 default，请确保启动 Kylin 实例的用户有操作该数据库的权限
- `kylin.source.hive.flat-table-storage-format`：指定 Hive 中间表的存储格式，默认值为 SEQUENCEFILE
- `kylin.source.hive.flat-table-field-delimiter`：指定 Hive 中间表的分隔符，默认值为  \u001F 
- `kylin.source.hive.intermediate-table-prefix`：指定 Hive 中间表的表名前缀，默认值为  kylin\_intermediate\_ 
- `kylin.source.hive.redistribute-flat-table`：是否重分配 Hive 平表，默认值为 TRUE
- `kylin.source.hive.redistribute-column-count`：重分配列的数量，默认值为 3
- `kylin.source.hive.table-dir-create-first`：默认值为 FALSE
- `kylin.storage.partition.aggr-spill-enabled`：默认值为 TRUE
- `kylin.engine.mr.lib-dir`：指定 MapReduce 任务所使用的 jar 包的路径 
- `kylin.engine.mr.reduce-input-mb`：MapReduce 任务启动前会依据输入预估 Reducer 接收数据的总量，再除以该参数得出 Reducer 的数目，默认值为 500（MB）                
- `kylin.engine.mr.reduce-count-ratio`：用于估算 Reducer 数目，默认值为 1.0
- `kylin.engine.mr.min-reducer-number`：MapReduce 任务中 Reducer 数目的最小值，默认值为 1
- `kylin.engine.mr.max-reducer-number`：MapReduce 任务中 Reducer 数目的最大值，默认值为 500
- `kylin.engine.mr.mapper-input-rows`：每个 Mapper 可以处理的行数，默认值为 1000000，如果将这个值调小，会起更多的 Mapper
- `kylin.engine.mr.max-cuboid-stats-calculator-number`：用于计算 Cube 统计数据的线程数量，默认值为 1
- `kylin.engine.mr.build-dict-in-reducer`：是否在构建任务 **Extract Fact Table Distinct Columns** 的 Reduce 阶段构建字典，默认值为 TRUE
- `kylin.engine.mr.yarn-check-interval-seconds`：构建引擎间隔多久检查 Hadoop 任务的状态，默认值为 10（s）    
- `kylin.engine.mr.use-local-classpath`: 是否使用本地 mapreduce 应用的 classpath。默认值为 TRUE    



### 字典相关  {#dict-config}

- `kylin.dictionary.use-forest-trie`：默认值为 TRUE 
- `kylin.dictionary.forest-trie-max-mb`：默认值为 500
- `kylin.dictionary.max-cache-entry`：默认值为 3000
- `kylin.dictionary.growing-enabled`：默认值为 FALSE
- `kylin.dictionary.append-entry-size`：默认值为 10000000
- `kylin.dictionary.append-max-versions`：默认值为 3
- `kylin.dictionary.append-version-ttl`：默认值为 259200000
- `kylin.dictionary.resuable`：是否重用字典，默认值为 FALSE
- `kylin.dictionary.shrunken-from-global-enabled`：是否缩小全局字典，默认值为 TRUE



### 超高基维度的处理 {#uhc-config}

Cube 构建默认在 **Extract Fact Table Distinct Column** 这一步为每一列分配一个 Reducer，对于超高基维度，可以通过以下参数增加 Reducer 个数

- `kylin.engine.mr.build-uhc-dict-in-additional-step`：默认值为 FALSE，设置为 TRUE
- `kylin.engine.mr.uhc-reducer-count`：默认值为 1，可以设置为 5，即为每个超高基的列分配 5 个 Reducer。



### Spark 构建引擎  {#spark-cubing}

- `kylin.engine.spark-conf.spark.master`：指定 Spark 运行模式，默认值为 yarn
- `kylin.engine.spark-conf.spark.submit.deployMode`：指定 Spark on YARN 的部署模式，默认值为 cluster
- `kylin.engine.spark-conf.spark.yarn.queue`：指定 Spark 资源队列，默认值为 default
- `kylin.engine.spark-conf.spark.driver.memory`：指定 Spark Driver 内存大小，默认值为 2G
- `kylin.engine.spark-conf.spark.executor.memory`：指定 Spark Executor 内存大小，默认值为 4G
- `kylin.engine.spark-conf.spark.yarn.executor.memoryOverhead`：指定 Spark Executor 堆外内存大小，默认值为 1024(MB)
- `kylin.engine.spark-conf.spark.executor.cores`：指定单个 Spark Executor可用核心数，默认值为 1
- `kylin.engine.spark-conf.spark.network.timeout`：指定 Spark 网络超时时间，600
- `kylin.engine.spark-conf.spark.executor.instances`：指定一个 Application 拥有的 Spark Executor 数量，默认值为 1
- `kylin.engine.spark-conf.spark.eventLog.enabled`：是否记录 Spark 时间，默认值为 TRUE
- `kylin.engine.spark-conf.spark.hadoop.dfs.replication`：HDFS 的副本数，默认值为 2
- `kylin.engine.spark-conf.spark.hadoop.mapreduce.output.fileoutputformat.compress`：是否压缩输出，默认值为 TRUE
- `kylin.engine.spark-conf.spark.hadoop.mapreduce.output.fileoutputformat.compress.codec`：输出所用压缩，默认值为 org.apache.hadoop.io.compress.DefaultCodec
- `kylin.engine.spark.rdd-partition-cut-mb`：Kylin 用该参数的大小来分割 partition，默认值为 10(MB)，可以在 Cube 级别重写这个参数，调整至更大，来减少分区数
- `kylin.engine.spark.min-partition`：最小分区数，默认值为 1
- `kylin.engine.spark.max-partition`：最大分区数，默认值为 5000
- `kylin.engine.spark.storage-level`：RDD 分区数据缓存级别，默认值为 MEMORY_AND_DISK_SER
- `kylin.engine.spark-conf-mergedict.spark.executor.memory`：为合并字典申请更多的内存，默认值为 6G
- `kylin.engine.spark-conf-mergedict.spark.memory.fraction`：给系统预留的内存百分比，默认值为 0.2

> 提示：更多信息请参考 [用 Spark 构建 Cube](/cn/docs/tutorial/cube_spark.html)。



### 通过 Livy 提交 Spark 任务 {#livy-submit-spark-job}

- `kylin.engine.livy-conf.livy-enabled`：是否开启 Livy 进行 Spark 任务的提交。默认值为 *FALSE*
- `kylin.engine.livy-conf.livy-url`：指定了 Livy 的 URL。例如 *http://127.0.0.1:8998*
- `kylin.engine.livy-conf.livy-key.*`：指定了 Livy 的 name-key 配置。例如 *kylin.engine.livy-conf.livy-key.name=kylin-livy-1*
- `kylin.engine.livy-conf.livy-arr.*`：指定了 Livy 数组类型的配置。以逗号分隔。例如 *kylin.engine.livy-conf.livy-arr.jars=hdfs://your_self_path/hbase-common-1.4.8.jar,hdfs://your_self_path/hbase-server-1.4.8.jar,hdfs://your_self_path/hbase-client-1.4.8.jar*
- `kylin.engine.livy-conf.livy-map.*`：指定了 Spark 配置。例如 *kylin.engine.livy-conf.livy-map.spark.executor.instances=10*

> 提示：更多信息请参考 [Apache Livy Rest API](http://livy.incubator.apache.org/docs/latest/rest-api.html)。


### Spark 资源动态分配 {#dynamic-allocation}

- `kylin.engine.spark-conf.spark.shuffle.service.enabled`：是否开启 shuffle service
- `kylin.engine.spark-conf.spark.dynamicAllocation.enabled`：是否启用 Spark 资源动态分配
- `kylin.engine.spark-conf.spark.dynamicAllocation.initialExecutors`：如果所有的 Executor 都移除了，重新请求启动时初始 Executor 数量
- `kylin.engine.spark-conf.spark.dynamicAllocation.minExecutors`：最少保留的 Executor 数量
- `kylin.engine.spark-conf.spark.dynamicAllocation.maxExecutors`：最多申请的 Executor 数量
- `kylin.engine.spark-conf.spark.dynamicAllocation.executorIdleTimeout`：Executor 空闲时间超过设置的值后，除非有缓存数据，不然会被移除，默认值为 60(s)

> 提示：更多信息请参考 [Dynamic Resource Allocation](http://spark.apache.org/docs/1.6.2/job-scheduling.html#dynamic-resource-allocation)。



### 任务相关 {#job-config}

- `kylin.job.log-dir`：默认值为 /tmp/kylin/logs
- `kylin.job.allow-empty-segment`：是否容忍数据源为空，默认值为 TRUE
- `kylin.job.max-concurrent-jobs`：最大构建并发数，默认值为 10
- `kylin.job.retry`：构建任务失败后的重试次数，默认值为 0
- `kylin.job.retry-interval`: 每次重试的间隔毫秒数。默认值为 30000
- `kylin.job.scheduler.priority-considered`：是否考虑任务优先级，默认值为 FALSE
- `kylin.job.scheduler.priority-bar-fetch-from-queue`：指定从优先级队列中获取任务的时间间隔，默认值为 20(s)
- `kylin.job.scheduler.poll-interval-second`：从队列中获取任务的时间间隔，默认值为 30(s)
- `kylin.job.error-record-threshold`：指定任务抛出错误信息的阈值，默认值为 0
- `kylin.job.cube-auto-ready-enabled`：是否在构建完成后自动启用 Cube，默认值为 TRUE
- `kylin.cube.max-building-segments`：指定对同一个 Cube 的最大构建数量，默认值为 10



### 启用邮件通知		{#email-notification}

- `kylin.job.notification-enabled`：是否在任务成功或者失败时进行邮件通知，默认值为 FALSE
- `kylin.job.notification-mail-enable-starttls`：# 是否启用 starttls，默认值为 FALSE
- `kylin.job.notification-mail-host`：指定邮件的 SMTP 服务器地址
- `kylin.job.notification-mail-port`：指定邮件的 SMTP 服务器端口，默认值为 25
- `kylin.job.notification-mail-username`：指定邮件的登录用户名
- `kylin.job.notification-mail-password`：指定邮件的用户名密码
- `kylin.job.notification-mail-sender`：指定邮件的发送邮箱地址
- `kylin.job.notification-admin-emails`：指定邮件通知的管理员邮箱



### 启用 Cube Planner {#cube-planner}

- `kylin.cube.cubeplanner.enabled`：默认值为 TRUE
- `kylin.server.query-metrics2-enabled`：默认值为 TRUE
- `kylin.metrics.reporter-query-enabled`：默认值为 TRUE
- `kylin.metrics.reporter-job-enabled`：默认值为 TRUE
- `kylin.metrics.monitor-enabled`：默认值为 TRUE
- `kylin.cube.cubeplanner.enabled`：是否启用 Cube Planner，默认值为 TRUE
- `kylin.cube.cubeplanner.enabled-for-existing-cube`：是否对已有的 Cube 启用 Cube Planner，默认值为 TRUE
- `kylin.cube.cubeplanner.algorithm-threshold-greedy`：默认值为 8
- `kylin.cube.cubeplanner.expansion-threshold`：默认值为 15.0
- `kylin.cube.cubeplanner.recommend-cache-max-size`：默认值为 200
- `kylin.cube.cubeplanner.query-uncertainty-ratio`：默认值为 0.1
- `kylin.cube.cubeplanner.bpus-min-benefit-ratio`：默认值为 0.01
- `kylin.cube.cubeplanner.algorithm-threshold-genetic`：默认值为 23


> 提示：更多信息请参考 [使用 Cube Planner](/cn/docs/tutorial/use_cube_planner.html)。



### HBase 存储   {#hbase-config}

- `kylin.storage.hbase.table-name-prefix`：默认值为 KYLIN\_ 
- `kylin.storage.hbase.namespace`：指定 HBase 存储默认的 namespace，默认值为 default
- `kylin.storage.hbase.coprocessor-local-jar`：指向 HBase 协处理器有关 jar 包
- `kylin.storage.hbase.coprocessor-mem-gb`：设置 HBase 协处理器内存大小，默认值为 3.0(GB)
- `kylin.storage.hbase.run-local-coprocessor`：是否运行本地 HBase 协处理器，默认值为 FALSE
- `kylin.storage.hbase.coprocessor-timeout-seconds`：设置超时时间，默认值为 0
- `kylin.storage.hbase.region-cut-gb`：单个 Region 的大小，默认值为 5.0
- `kylin.storage.hbase.min-region-count`：指定最小 Region 个数，默认值为 1
- `kylin.storage.hbase.max-region-count`：指定最大 Region 个数，默认值为 500 
- `kylin.storage.hbase.hfile-size-gb`：指定 HFile 大小，默认值为 2.0(GB)
- `kylin.storage.hbase.max-scan-result-bytes`：指定扫描返回结果的最大值，默认值为 5242880(byte)，即 5(MB)
- `kylin.storage.hbase.compression-codec`：是否压缩，默认值为 none，即不开启压缩
- `kylin.storage.hbase.rowkey-encoding`：指定 Rowkey 的编码方式，默认值为 FAST_DIFF
- `kylin.storage.hbase.block-size-bytes`：默认值为 1048576
- `kylin.storage.hbase.small-family-block-size-bytes`：指定 Block 大小，默认值为 65536(byte)，即 64(KB)
- `kylin.storage.hbase.owner-tag`：指定 Kylin 平台的所属人，默认值为 whoami@kylin.apache.org
- `kylin.storage.hbase.endpoint-compress-result`：是否返回压缩结果，默认值为 TRUE
- `kylin.storage.hbase.max-hconnection-threads`：指定连接线程数量的最大值，默认值为 2048
- `kylin.storage.hbase.core-hconnection-threads`：指定核心连接线程的数量，默认值为 2048
- `kylin.storage.hbase.hconnection-threads-alive-seconds`：指定线程存活时间，默认值为 60 
- `kylin.storage.hbase.replication-scope`：指定集群复制范围，默认值为 0
- `kylin.storage.hbase.scan-cache-rows`：指定扫描缓存行数，默认值为 1024



### 启用压缩		{#compress-config}

Kylin 在默认状态下不会启用压缩，不支持的压缩算法会阻碍 Kylin 的构建任务，但是一个合适的压缩算法可以减少存储开销和网络开销，提高整体系统运行效率。
Kylin 可以使用三种类型的压缩，分别是 HBase 表压缩，Hive 输出压缩 和 MapReduce 任务输出压缩。 

> **注意**：压缩设置只有在重启 Kylin 实例后才会生效。

* HBase 表压缩

该项压缩通过 `kyiln.properties` 中的 `kylin.storage.hbase.compression-codec` 进行配置，参数值可选 `none`，`snappy`， `lzo`， `gzip`， `lz4`），默认值为 none，即不压缩数据。

> **注意**：在修改压缩算法前，请确保用户的 HBase 集群支持所选压缩算法。


* Hive 输出压缩

该项压缩通过 `kylin_hive_conf.xml` 进行配置，默认配置为空，即直接使用了 Hive 的默认配置。如果想重写配置，请在 `kylin_hive_conf.xml` 中添加 (或替换) 下列属性。以 SNAPPY 压缩为例：

```xml
<property>
	<name>mapreduce.map.output.compress.codec</name>
	<value>org.apache.hadoop.io.compress.SnappyCodec</value>
	<description></description>
</property>
<property>
	<name>mapreduce.output.fileoutputformat.compress.codec</name>
	<value>org.apache.hadoop.io.compress.SnappyCodec</value>
	<description></description>
</property>
```

* MapReduce 任务输出压缩

该项压缩通过 `kylin_job_conf.xml` 和 `kylin_job_conf_inmem.xml` 进行配置。默认值为空，即使用 MapReduce 的默认配置。如果想重写配置，请在 `kylin_job_conf.xml` 和 `kylin_job_conf_inmem.xml` 中添加 (或替换) 下列属性。以 SNAPPY 压缩为例：

```xml
<property>
	<name>mapreduce.map.output.compress.codec</name>
	<value>org.apache.hadoop.io.compress.SnappyCodec</value>
	<description></description>
</property>
<property>
	<name>mapreduce.output.fileoutputformat.compress.codec</name>
	<value>org.apache.hadoop.io.compress.SnappyCodec</value>
	<description></description>
</property>
```



### 实时 OLAP    {#realtime-olap}
- `kylin.stream.job.dfs.block.size`：指定了流式构建 Base Cuboid 任务所需 HDFS 块的大小。默认值为 *16M*。
- `kylin.stream.index.path`：指定了本地 segment 缓存的位置。默认值为 *stream_index*。
- `kylin.stream.cube-num-of-consumer-tasks`：指定了共享同一个 topic 分区的 replica set 数量，影响着不同 replica set 分配的分区数量。默认值为 *3*。
- `kylin.stream.cube.window`：指定了每个 segment 的持续时长，以秒为单位。默认值为 *3600*。
- `kylin.stream.cube.duration`：指定了 segment 从 active 状态变为 IMMUTABLE 状态的等待时间，以秒为单位。默认值为 *7200*。
- `kylin.stream.cube.duration.max`：segment 的 active 状态的最长持续时间，以秒为单位。默认值为 *43200*。
- `kylin.stream.checkpoint.file.max.num`：指定了每个 Cube 包含的 checkpoint 文件数的最大值。默认值为 *5*。
- `kylin.stream.index.checkpoint.intervals`：指定了两个 checkpoint 设置的时间间隔。默认值为 *300*。
- `kylin.stream.index.maxrows`：指定了缓存在堆/内存中的事件数的最大值。默认值为 *50000*。
- `kylin.stream.immutable.segments.max.num`：指定了当前 receiver 里每个 Cube 中状态为 IMMUTABLE 的 segment 的最大数值，如果超过最大值，当前 topic 的消费将会被暂停。默认值为 *100*。
- `kylin.stream.consume.offsets.latest`：是否从最近的偏移量开始消费。默认值为 *true*。
- `kylin.stream.node`：指定了 coordinator/receiver 的节点。形如 host:port。默认值为 *null*。
- `kylin.stream.metadata.store.type`：指定了元数据存储的位置。默认值为 *zk*。
- `kylin.stream.segment.retention.policy`：指定了当 segment 变为 IMMUTABLE 状态时，本地 segment 缓存的处理策略。参数值可选 `purge` 和 `fullBuild`。`purge` 意味着当 segment 的状态变为 IMMUTABLE，本地缓存的 segment 数据将被删除。`fullBuild` 意味着当 segment 的状态变为 IMMUTABLE，本地缓存的 segment 数据将被上传到 HDFS。默认值为 *fullBuild*。
- `kylin.stream.assigner`：指定了用于将 topic 分区分配给不同 replica set 的实现类。该类实现了 `org.apache.kylin.stream.coordinator.assign.Assigner` 类。默认值为 *DefaultAssigner*。
- `kylin.stream.coordinator.client.timeout.millsecond`：指定了连接 coordinator 客户端的超时时间。默认值为 *5000*。
- `kylin.stream.receiver.client.timeout.millsecond`：指定了连接 receiver 客户端的超时时间。默认值为 *5000*。
- `kylin.stream.receiver.http.max.threads`：指定了连接 receiver 的最大线程数。默认值为 *200*。
- `kylin.stream.receiver.http.min.threads`：指定了连接 receiver 的最小线程数。默认值为 *10*。
- `kylin.stream.receiver.query-core-threads`：指定了当前 receiver 用于查询的线程数。默认值为 *50*。
- `kylin.stream.receiver.query-max-threads`：指定了当前 receiver 用于查询的最大线程数。默认值为 *200*。
- `kylin.stream.receiver.use-threads-per-query`：指定了每个查询使用的线程数。默认值为 *8*。
- `kylin.stream.build.additional.cuboids`：是否构建除 Base Cuboid 外的 cuboids。除 Base Cuboid 外的 cuboids 指的是在 Cube 的 Advanced Setting 页面选择的强制维度的聚合。默认值为 *false*。默认只构建 Base Cuboid。
- `kylin.stream.segment-max-fragments`：指定了每个 segment 保存的最大 fragment 数。默认值为 *50*。
- `kylin.stream.segment-min-fragments`：指定了每个 segment 保存的最小 fragment 数。默认值为 *15*。
- `kylin.stream.max-fragment-size-mb`：指定了每个 fragment 文件的最大尺寸。默认值为 *300*。
- `kylin.stream.fragments-auto-merge-enable`：是否开启 fragment 文件自动合并的功能。默认值为 *true*。

> 提示：更多信息请参考 [Real-time OLAP](http://kylin.apache.org/docs30/tutorial/real_time_olap.html)。



### 存储清理配置  {#storage-clean-up-configuration}

本小节介绍 Kylin 存储清理有关的配置。



### 存储清理相关 {#storage-clean-up-config}

- `kylin.storage.clean-after-delete-operation`: 是否清理 HBase 和 HDFS 中的 segment 数据。默认值为 FALSE。



### 查询配置    {#kylin-query}

本小节介绍 Kylin 查询有关的配置。



### 查询相关   {#query-config}

- `kylin.query.skip-empty-segments`：查询是否跳过数据量为 0 的 segment，默认值为 TRUE
- `kylin.query.large-query-threshold`：指定最大返回行数，默认值为 1000000 
- `kylin.query.security-enabled`：是否在查询时检查 ACL，默认值为 TRUE 
- `kylin.query.security.table-acl-enabled`：是否在查询时检查对应表的 ACL，默认值为 TRUE 
- `kylin.query.calcite.extras-props.conformance`：是否严格解析，默认值为 LENIENT
- `kylin.query.calcite.extras-props.caseSensitive`：是否大小写敏感，默认值为 TRUE
- `kylin.query.calcite.extras-props.unquotedCasing`：是否需要将查询语句进行大小写转换，参数值可选 `UNCHANGED`， `TO_UPPER`， `TO_LOWER` ，默认值为 `TO_UPPER`，即全部大写
- `kylin.query.calcite.extras-props.quoting`：是否添加引号，参数值可选 `DOUBLE_QUOTE`， `BACK_TICK`，`BRACKET`，默认值为 `DOUBLE_QUOTE`
- `kylin.query.statement-cache-max-num`：缓存的 PreparedStatement 的最大条数，默认值为 50000
- `kylin.query.statement-cache-max-num-per-key`：每个键缓存的 PreparedStatement 的最大条数，默认值为 50 
- `kylin.query.enable-dict-enumerator`：是否启用字典枚举器，默认值为 FALSE
- `kylin.query.enable-dynamic-column`：是否启用动态列，默认值为 FALSE，设置为 TRUE 后可以查询一列中不包含 NULL 的行数



### 模糊查询 {#fuzzy}

- `kylin.storage.hbase.max-fuzzykey-scan`：设置扫描的模糊键的阈值，超过该参数值便不再扫描模糊键，默认值为 200 
- `kylin.storage.hbase.max-fuzzykey-scan-split`：分割大模糊键集来减少每次扫描中模糊键的数量，默认值为 1
- `kylin.storage.hbase.max-visit-scanrange`：默认值为 1000000 



### 查询缓存 {#cache-config}

- `kylin.query.cache-enabled`：是否启用缓存，默认值为 TRUE
- `kylin.query.cache-threshold-duration`：查询延时超过阈值则保存进缓存，默认值为 2000(ms)
- `kylin.query.cache-threshold-scan-count`：查询所扫描的数据行数超过阈值则保存进缓存，默认值为 10240(rows)
- `kylin.query.cache-threshold-scan-bytes`：查询所扫描的数据字节数超过阈值则保存进缓存，默认值为 1048576(byte)



### 查询限制 {#query-limit}

- `kylin.query.timeout-seconds`：设置查询超时时间，默认值为 0，即没有限制，如果设置的值小于 60，会被强制替换成 60 秒
- `kylin.query.timeout-seconds-coefficient`：设置查询超时秒数的系数，默认值为 0.5
- `kylin.query.max-scan-bytes`：设置查询扫描字节的上限，默认值为 0，即没有限制
- `kylin.storage.partition.max-scan-bytes`：设置查询扫描的最大字节数，默认值为 3221225472(bytes)，即 3GB
- `kylin.query.max-return-rows`：指定查询返回行数的上限，默认值为 5000000



### 坏查询		{#bad-query}

`kylin.query.timeout-seconds` 的值为大于 60 或为 0，`kylin.query.timeout-seconds-coefficient` 其最大值为 double 的上限。这两个参数的乘积为坏查询检查的间隔时间，如果为 0，那么会设为 60 秒，最长秒数是 int 的最大值。

- `kylin.query.badquery-stacktrace-depth`：设置堆栈追踪的深度，默认值为 10
- `kylin.query.badquery-history-number`：设置要展示的历史坏查询的数量，默认为 50
- `kylin.query.badquery-alerting-seconds`：默认为 90，如果运行时间大于这个值，那么首先就会打出该查询的日志信息，包括（时长、项目、线程、用户、查询 id）。至于是否保存最近的查询，取决于另一个参数。然后记录 Stack 日志信息，记录的深度由另一个参数指定，方便后续问题分析
- `kylin.query.badquery-persistent-enabled`：默认为 true，会保存最近的一些坏查询，而且不可在 Cube 级别进行覆盖




### 查询下压		{#query-pushdown}

- `kylin.query.pushdown.runner-class-name=org.apache.kylin.query.adhoc.PushDownRunnerJdbcImpl`：如果需要启用查询下压，需要移除这句配置的注释
- `kylin.query.pushdown.jdbc.url`：JDBC 的 URL
- `kylin.query.pushdown.jdbc.driver`：JDBC 的 driver 类名，默认值为 `org.apache.hive.jdbc.HiveDriver`
- `kylin.query.pushdown.jdbc.username`：JDBC 对应数据库的用户名，默认值为 `hive`
- `kylin.query.pushdown.jdbc.password`：JDBC 对应数据库的密码
- `kylin.query.pushdown.jdbc.pool-max-total`：JDBC 连接池的最大连接数，默认值为8
- `kylin.query.pushdown.jdbc.pool-max-idle`：JDBC 连接池的最大等待连接数，默认值为8
- `kylin.query.pushdown.jdbc.pool-min-idle`：默认值为 0
- `kylin.query.pushdown.update-enabled`：指定是否在查询下压中开启 update，默认值为 FALSE
- `kylin.query.pushdown.cache-enabled`：是否开启下压查询的缓存来提高相同查询语句的查询效率，默认值为 FALSE

> 提示：更多信息请参考[查询下压](/cn/docs/tutorial/query_pushdown.html)



### 查询改写 {#convert-sql}

- `kylin.query.force-limit`：该参数通过为 select * 语句强制添加 LIMIT 分句，达到缩短数据返回时间的目的，该参数默认值为 -1，将该参数值设置为正整数，如 1000，该值会被应用到 LIMIT 分句，查询语句最终会被转化成 select * from fact_table limit 1000
- `kylin.storage.limit-push-down-enabled`: 默认值为 *TRUE*，设置为 *FALSE* 意味着关闭存储层的 limit-pushdown 
- `kylin.query.flat-filter-max-children`：指定打平 filter 时 filter 的最大值。默认值为 500000 



### 收集查询指标到 JMX {#jmx-metrics}

- `kylin.server.query-metrics-enabled`：默认值为 FALSE，设为 TRUE 来将查询指标收集到 JMX

> 提示：更多信息请参考 [JMX](https://www.oracle.com/technetwork/java/javase/tech/javamanagement-140525.html)



### 收集查询指标到 dropwizard {#dropwizard-metrics}

- `kylin.server.query-metrics2-enabled`：默认值为 FALSE，设为 TRUE 来将查询指标收集到 dropwizard

> 提示：更多信息请参考 [dropwizard](https://metrics.dropwizard.io/4.0.0/)



### 安全配置 {#kylin-security}

本小节介绍 Kylin 安全有关的配置。



### 集成 LDAP 实现单点登录	{#ldap-sso}

- `kylin.security.profile`：安全认证的方式，参数值可选 `ldap`，`testing`，`saml`。集成 LDAP 实现单点登录时应设置为 `ldap`
- `kylin.security.ldap.connection-server`：LDAP 服务器，如 ldap://ldap_server:389
- `kylin.security.ldap.connection-username`：LDAP 用户名
- `kylin.security.ldap.connection-password`：LDAP 密码
- `kylin.security.ldap.user-search-base`：定义同步到 Kylin 的用户的范围
- `kylin.security.ldap.user-search-pattern`：定义登录验证匹配的用户名
- `kylin.security.ldap.user-group-search-base`：定义同步到 Kylin 的用户组的范围
- `kylin.security.ldap.user-group-search-filter`：定义同步到 Kylin 的用户的类型
- `kylin.security.ldap.service-search-base`：当需要服务账户可以访问 Kylin 时需要定义
- `kylin.security.ldap.service-search-pattern`：当需要服务账户可以访问 Kylin 时需要定义
- `kylin.security.ldap.service-group-search-base`：当需要服务账户可以访问 Kylin 时需要定义
- `kylin.security.acl.admin-role`：将一个 LDAP 群组映射成管理员角色（组名大小写敏感）
- `kylin.server.auth-user-cache.expire-seconds`：LDAP 用户信息缓存时间，默认值为 300(s)
- `kylin.server.auth-user-cache.max-entries`：LDAP 用户数目最大缓存，默认值为 100



### 集成 Apache Ranger {#ranger}

- `kylin.server.external-acl-provider=org.apache.ranger.authorization.kylin.authorizer.RangerKylinAuthorizer`

> 提示：更多信息请参考[Ranger 的安装文档之如何集成 Kylin 插件](https://cwiki.apache.org/confluence/display/RANGER/Kylin+Plugin)



### 启用 ZooKeeper ACL {#zookeeper-acl}

- `kylin.env.zookeeper-acl-enabled`：启用 ZooKeeper ACL 以阻止未经授权的用户访问 Znode 或降低由此导致的不良操作的风险，默认值为 `FALSE`
- `kylin.env.zookeeper.zk-auth`：使用 用户名：密码 作为 ACL 标识，默认值为 `digest:ADMIN:KYLIN`
- `kylin.env.zookeeper.zk-acl`：使用单个 ID 作为 ACL 标识，默认值为 `world:anyone:rwcda`，`anyone` 表示任何人

### 使用 Memcached 作为 Kylin 查询缓存 {#distributed-cache}

从 v2.6.0，Kylin 可以使用 Memcached 作为查询缓存，一起引入的还有一系列缓存增强 ([KYLIN-2895](https://issues.apache.org/jira/browse/KYLIN-2895))。想要启用该功能，您需要执行以下步骤：

1. 在一个或多个节点上安装 Memcached (最新稳定版 v1.5.12); 如果资源够的话，可以在每个安装 Kylin 的节点上安装 Memcached。

2. 按照如下所示方式修改 $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/classes 目录下的 applicationContext.xml 的内容：

注释如下代码：
{% highlight Groff markup %}
<bean id="ehcache"
      class="org.springframework.cache.ehcache.EhCacheManagerFactoryBean"
      p:configLocation="classpath:ehcache-test.xml" p:shared="true"/>

<bean id="cacheManager" class="org.springframework.cache.ehcache.EhCacheCacheManager"
      p:cacheManager-ref="ehcache"/>
{% endhighlight %}
取消如下代码的注释：
{% highlight Groff markup %}
<bean id="ehcache" class="org.springframework.cache.ehcache.EhCacheManagerFactoryBean"
      p:configLocation="classpath:ehcache-test.xml" p:shared="true"/>

<bean id="remoteCacheManager" class="org.apache.kylin.cache.cachemanager.MemcachedCacheManager" />
<bean id="localCacheManager" class="org.apache.kylin.cache.cachemanager.InstrumentedEhCacheCacheManager"
      p:cacheManager-ref="ehcache"/>
<bean id="cacheManager" class="org.apache.kylin.cache.cachemanager.RemoteLocalFailOverCacheManager" />

<bean id="memcachedCacheConfig" class="org.apache.kylin.cache.memcached.MemcachedCacheConfig">
    <property name="timeout" value="500" />
    <property name="hosts" value="${kylin.cache.memcached.hosts}" />
</bean>
{% endhighlight %}
applicationContext.xml 中 `${kylin.cache.memcached.hosts}` 的值就是在 conf/kylin.properties 中指定的 `kylin.cache.memcached.hosts` 的值。 

3.在 `conf/kylin.properties` 中添加如下参数：
{% highlight Groff markup %}
kylin.query.cache-enabled=true
kylin.query.lazy-query-enabled=true
kylin.query.cache-signature-enabled=true
kylin.query.segment-cache-enabled=true
kylin.cache.memcached.hosts=memcached1:11211,memcached2:11211,memcached3:11211
{% endhighlight %}

- `kylin.query.cache-enabled` 是否开启查询缓存的总开关，默认值为 `true`。
- `kylin.query.lazy-query-enabled` 是否为短时间内重复发送的查询，等待并重用前次查询的结果，默认为 `false`。  
- `kylin.query.cache-signature-enabled` 是否为缓存进行签名检查，依据签名变化来决定缓存的有效性。缓存的签名由项目中的 cube / hybrid 的状态以及它们的最后构建时间等来动态计算（在缓存被记录时）,默认为 `false`，高度推荐设置为 `true`。 
- `kylin.query.segment-cache-enabled` 是否在 segment 级别缓存从 存储引擎(HBase)返回的数据，默认为 `false`；设置为 `true`，且启用 Memcached 分布式缓存开启的时候，此功能才会生效。可为频繁构建的 cube （如 streaming cube）提升缓存命中率，从而提升性能。
- `kylin.cache.memcached.hosts` 指明了 memcached 的机器名和端口。