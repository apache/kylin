---
layout: docs-cn
title:  Kylin 升级
categories: howto
permalink: /cn/docs/howto/howto_upgrade.html
since: v1.5.1
---


Apache Kylin 的元数据和 Cube 的数据在 HBase 和 HDFS 中持久化，因此升级相对容易，用户无需担心数据丢失。



### 一般版本的升级

1.停止并确认没有正在运行的 Kylin 进程：

```sh
$KYLIN_HOME/bin/kylin.sh stop
ps -ef | grep kylin
```

2.备份 Kylin 元数据（可选），用来保障数据安全性：

```sh
$KYLIN_HOME/bin/metastore.sh backup
```

3.解压缩新版本的 Kylin 安装包，更新 `KYLIN_HOME` 环境变量：

```sh
tar -zxvf apache-kylin-{version}-bin-{env}.tar.gz
export KYLIN_HOME={path_to_your_installation_folder}
```

4.更新配置文件，将旧配置文件（如 `$KYLIN_HOME/conf/*`、`$KYLIN_HOME/tomcat/conf/*`）合并到新配置文件中。

5.启动 Kylin 进程：

```sh
$KYLIN_HOME/bin/kylin.sh start
```

6.升级协处理器，请参考[升级协处理器](/cn/docs/howto/howto_upgrade_coprocessor.html)。

7.检查 Web UI 是否成功启动、构建和查询等是否一切正常工作。

8.确认升级完成后，之前备份的元数据可以安全删除。



### 特定版本的升级

- **从 v2.4 升级到 v2.5.0**

1.Kylin 2.5 需要 Java 8; 如果您使用的是 Java 7，请升级Java。

2.Kylin 元数据兼容 v2.4 和 v2.5，不需要迁移。

3.Spark 引擎会将更多步骤从 MR 移动到 Spark，升级后用户可能会看到同一个 Cube 的性能差异。

4.如果用户使用 RDBMS 作为数据源，需要配置参数 `kylin.source.jdbc.sqoop-home` 需要指向用户环境中 Sqoop 的安装路径，而不是它的 `bin` 子文件夹，

5.默认启用 Cube Planner，新的 Cube 将在首次构建时通过它进行优化，系统 Cube 和仪表板需要手动启用。



- **从 v2.1.0 升级到 v2.2.0**

Kylin v2.2.0 Cube 元数据与 v2.1.0兼容，用户需要了解以下更改：

1.Cube ACL 被项目级 ACL 替代，用户需要手动配置项目级 ACL 以迁移现有的 Cube ACL。请参考 [项目级 ACL](/cn/docs/tutorial/project_level_acl.html)。
2.更新 HBase 协处理器，现有 Cube 的 HBase 表需要更新协处理器。请参考 [升级协处理器](/cn/docs/howto/howto_upgrade_coprocessor.html)。



- **从 v2.0.0 升级到 v2.1.0**

Kylin v2.1.0 Cube 元数据与 v2.0.0兼容，用户需要了解以下更改：

1.在之前版本，Kylin 使用两个 HBase 表 `kylin_metadata_user` 和 `kylin_metadata_acl` 来持久保存用户和 ACL 信息。从 v2.1开始，Kylin 将所有信息合并到一个表 `kylin_metadata` 中，这将使备份/恢复和维护更加容易。当启动 Kylin v2.1.0 时，系统会检测是否需要进行元数据迁移，如果为 `true`，用户需要按照提示执行命令进行迁移：

```
ERROR: Legacy ACL metadata detected. Please migrate ACL metadata first. Run command 'bin/kylin.sh org.apache.kylin.tool.AclTableMigrationCLI MIGRATE'.
```

迁移完成后，用户可以从 HBase 中删除旧的 `kylin_metadata_user` 和 `kylin_metadata_acl` 表。


2.从 v2.1 开始，Kylin 隐藏了 `kylin.properties` 中的默认设置，用户只需要取消注释或在其中添加自定义配置。

3.Spark 从 v1.6.3 升级到 v2.1.1，如果用户在 `kylin.properties` 中自定义了 Spark 配置，请参考 [Spark 文档](https://spark.apache.org/docs/2.1.0/)进行升级。

4.如果用户部署了读写分离集群，则需要将大的元数据文件（在 HDFS 中而不是 HBase 中）从 Hadoop 集群复制到 HBase 集群。

```sh
hadoop distcp hdfs://compute-cluster:8020/kylin/kylin_metadata/resources hdfs://query-cluster:8020/kylin/kylin_metadata/resources
```



- **从 v1.6.0 升级到 v2.0.0**

Kylin v2.0.0 可以直接使用 v1.6.0 元数据，请参考**一般版本的升级**一节中的步骤进行升级，用户需要了解以下更改：
`kylin.properties` 中的配置名称自 v2.0.0 以后进行修改。虽然旧的配置名称仍然有效，但建议使用新的配置名称，因为它们遵循 [编码和命名惯例](/cn/development/coding_naming_convention.html) ，并且更容易理解。请参考 [从旧属性名称到新属性名称的映射](https://github.com/apache/kylin/blob/2.0.x/core-common/src/main/resources/kylin-backward-compatibility.properties)。


- **从 v1.5.2 升级到 v1.5.3**

Kylin v1.5.3 元数据与 v1.5.2 兼容，用户需要做如下操作：

1.升级 HBase 协处理器

2.更新 `$KYLIN_HOME/conf/kylin_hive_conf.xml`

从 v1.5.3 开始，Kylin不再需要 Hive 合并小文件；对于从先前版本复制 `$KYLIN_HOMT/conf/` 的用户，请删除 `kylin_hive_conf.xml` 中的 `merge` 相关的配置，包括 `hive.merge.mapfiles`，`hive.merge.mapredfiles` 和 `hive.merge.size.per.task`，这将节省从 Hive 中提取数据的时间。


- **从 v1.5.1 升级到 v1.5.2**

Kylin v1.5.2 元数据与 v1.5.1 兼容，用户需要做如下操作：

1.升级 HBase 协处理器

2.更新 `kylin.properties`，

在 v1.5.2 中，有一些属性被废弃，有一些新的参数被增加。

被废弃的参数如下：

* `kylin.hbase.region.cut.small`
* `kylin.hbase.region.cut.medium`
* `kylin.hbase.region.cut.large`

新参数如下：

* `kylin.hbase.region.cut` 
* `kylin.hbase.hfile.size.gb` 

这些新参数决定了如何分割 HBase Region

3.添加 `$KYLIN_HOME/conf/kylin_job_conf_inmem.xml` 配置文件：

正如 Kylin v1.5 引入了 `Fast Cubing` 算法，该算法旨在利用更多内存来进行内存聚合。当执行**In-mem Cubing**任务时，需要在 `kylin_job_conf_inmem.xml` 中为 mapper 申请更多的内存。
此外，如果用户为不同容量的 Cube 使用了单独的配置文件，例如 `kylin_job_conf_small.xml`，`kylin_job_conf_medium.xml` 和 `kylin_job_conf_large.xml`，请注意，他们现在已被弃用，只有 `kylin_job_conf.xml` 和 `kylin_job_conf_inmem.xml` 才能用于提交 Cube 任务，如果用户具有 Cube 级别的任务配置（例如使用不同的 Yarn 作业队列），可以参考 [Kylin 配置](/cn/docs/install/configuration.html) 中的 **Cube 配置重写** 进行配置。