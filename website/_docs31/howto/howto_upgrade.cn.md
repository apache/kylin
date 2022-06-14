---
layout: docs31-cn
title:  "从旧版本升级"
categories: howto
permalink: /cn/docs31/howto/howto_upgrade.html
since: v1.5.1
---

Apache Kylin 是作为 Hadoop 的客户端运行，Apache Kylin 的元数据和 Cube 数据持久化在 Hadoop（HBase 和 HDFS）中，因此升级相对容易，用户无需担心数据丢失。可以通过以下步骤执行升级：

* 从 Kylin 下载页面选择对应的 Hadoop 版本下载新的 Apache Kylin 二进制包。
* 将新版本的 Kylin 包解压到新文件夹，例如 /usr/local/kylin/apache-kylin-2.1.0/ （不建议直接覆盖旧路径）。
* 将旧配置文件（`$KYLIN_HOME/conf/*`）合并到新配置文件中。不建议覆盖新的配置文件。如果修改了 tomcat 配置（$KYLIN_HOME/tomcat/conf/），请对其执行相同操作。
* 通过 `bin/kylin.sh stop` 停止当前的运行的 Kylin 实例。
* 将 `KYLIN_HOME` 环境变量设置为新 Kylin 安装的文件夹路径。如果在 `~/.bash_profile` 或其他脚本中设置了 `KYLIN_HOME`，记得更新它们。
* 通过 `$KYLIN_HOME/bin/kylin start` 启动新的 Kylin 实例。在新的 Kylin 启动之后，登录 Kylin web 页面检查元数据是否正确加载。
* [更新 coprocessor](howto_update_coprocessor.html) 确保 HBase region servers 使用最新的 Kylin coprocessor。
* 验证您的 SQL 查询是否可以成功执行。

以下是特定的版本升级指南：

## 从 Kylin 3.1.2 升级到 Kylin 3.1.3
1) 使用实时功能时，用户需要为 coordinator node 设置 `kylin.server.mode=stream_coordinator`，而不能将其设置为`kylin.server.mode=all`。
2) Kylin 用户可以在 kylin 3.1.3 中通过 config `kylin.security.encrypt.cipher.ivSpec` 自定义加密算法的 IV 值。
   1. 如果使用默认值，则不需要在 kylin.properties 中修改加密密码。
   2. 如果你改变了`kylin.security.encrypt.cipher.ivSpec`的值，原本加密后的密码需要进行重新加密。 
   3. 加密算法可用于加密 `kylin.metadata.url(mysql password)`, `kylin.security.ldap.connection-password` 等.

## 从 Kylin 3.0 升级到 Kylin 3.1.0
1) kylin 3.1.0 web 的`Set Config` 默认是关闭的。
2) [KYLIN-4478](https://issues.apache.org/jira/browse/KYLIN-4478) 修改了加密算法。如果在 kylin 中启用 LDAP，则需要重新加密 kylin.properties 中配置的 LDAP 服务器的密码：

```
cd $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib
java -classpath kylin-server-base-\<versioin\>.jar:kylin-core-common-\<versioin\>.jar:spring-beans-4.3.10.RELEASE.jar:spring-core-4.3.10.RELEASE.jar:commons-codec-1.7.jar org.apache.kylin.rest.security.PasswordPlaceholderConfigurer AES <your_password>
```

然后将重新加密的密码更新为`kylin.security.ldap.connection-password=<your_password_encrypted>`。

## 从 Kylin 2.4 升级到 Kylin 2.5.0
* Kylin 2.5 需要 Java 8；如果您使用的是 Java 7，请升级 Java。
* Kylin 元数据在 2.4 和 2.5 之间兼容。不需要迁移。
* Spark 引擎会将原先在 MR 上的步骤移至 Spark 上执行，升级后您可能会看到同一个 Cube 的性能差异。
* 属性 `kylin.source.jdbc.sqoop-home` 是 sqoop 安装的位置，而不是它的 “bin” 子文件夹，如果你使用RDBMS作为数据源，请修改它。
* 现在默认启用 Cube planner；新的多维数据集将在首次构建时就进行优化。System cube 和 dashboard 仍然需要手动启用。

## 从 Kylin 2.1.0 升级到 Kylin 2.2.0

Kylin 2.2.0 cube 元数据与 Kylin 2.1.0 兼容，但您需要注意以下变更：
* Cube ACL 已被移除，使用 Project Level ACL 进行代替。用户需要手动配置当前项目权限去适配迁移之前的 Cube 权限。请参考[Project Level ACL](/cn/docs24/tutorial/project_level_acl.html)。
* 更新 HBase coprocessor。现有 Cube 的 HBase tables 需要更新到最新的 coprocessor。按照 [更新 coprocessor](/cn/docs24/howto/howto_update_coprocessor.html) 进行更新。

## 从 Kylin 2.0.0 升级到 Kylin 2.1.0

Kylin 2.1.0 Cube 元数据与 Kylin 2.0.0 兼容，但需要注意以下的变更。

1）在之前的版本中，Kylin 使用了额外的两张 HBase 表 "kylin_metadata_user" 和 "kylin_metadata_acl" 来持久化用户和 ACL 信息。从 2.1 开始，Kylin 将所有信息合并到一张表中："kylin_metadata"。这将使备份/恢复和维护更加容易。启动 Kylin 2.1.0 时，会检测是否这两张表需要迁移；如果需要迁移，它将打印需要执行迁移的命令：

```
ERROR: Legacy ACL metadata detected. Please migrate ACL metadata first. Run command 'bin/kylin.sh org.apache.kylin.tool.AclTableMigrationCLI MIGRATE'.
```

迁移完成后，您可以从 HBase 中删除遗留的 "kylin_metadata_user" 和 "kylin_metadata_acl" 表。

2) 从 v2.1 开始, Kylin 注释了 "conf/kylin.properties" 中的默认配置; 您只需要取消注释或在其中添加自定义属性。

3) Spark 已经从 v1.6.3 升级到 v2.1.1，如果您在 kylin.properties 中自定义了 Spark 配置，请参考 [Spark 文档](https://spark.apache.org/docs/2.1.0/) 进行升级配置。

4）如果你在两个集群上运行Kylin（计算/查询分离），则需要将大元数据文件（保存在 HDFS 而不是 HBase）从 Hadoop 集群复制到 HBase 集群。

```
hadoop distcp hdfs://compute-cluster:8020/kylin/kylin_metadata/resources hdfs://query-cluster:8020/kylin/kylin_metadata/resources
```

## 从 Kylin 1.6.0 升级到 Kylin 2.0.0

Kylin v2.0.0 可以直接读取 v1.6.0 元数据。请按照上述常规升级步骤进行操作。

`kylin.properties` 中的配置名称从 2.0.0 版本开始更改。虽然旧的属性名称仍然有效，但建议使用新的属性名称，因为它们遵循 [命名约定](/cn/development/coding_naming_convention.html) 并且更容易理解。请参考[从旧属性到新属性的映射](https://github.com/apache/kylin/blob/2.0.x/core-common/src/main/resources/kylin-backward-compatibility.properties）。

## 从 Kylin 1.5.4 升级到 Kylin 1.6.0

Kylin 1.5.4 和 Kylin 1.6.0 在元数据上是兼容的。请按照上述常规升级步骤操作。

## 从 Kylin 1.5.3 升级到 Kylin 1.5.4

Kylin 1.5.3 和 Kylin 1.5.4 在元数据上是兼容的。请按照上述常规升级步骤操作。

## 从 Kylin 1.5.2 升级到 Kylin 1.5.3

Kylin 1.5.3 元数据与 Kylin 1.5.2 兼容，您的 Cube 不需要重建，像往常一样，需要执行一些操作：

#### 1. 更新 HBase coprocessor

现有 Cube 的 HBase 表需要更新到最新的 coprocessor；请按照[更新 coprocessor][howto_update_coprocessor.html]进行更新；

#### 2. 更新 conf/kylin_hive_conf.xml

从 Kylin 1.5.3 开始，Kylin 不再需要 Hive 来合并小文件；复制之前版本 conf/* 的用户，请去掉 kylin_hive_conf.xml 中的 "merge" 相关属性，包括 "hive.merge.mapfiles"、"hive.merge.mapredfiles"、"hive.merge.size.per.task"; 这将节省从 Hive 提取数据的时间。

## 从 Kylin 1.5.1 升级到 Kylin 1.5.2

Kylin v1.5.2 元数据与 v1.5.1 兼容，您的 Cube 不需要重建，但需要执行一些操作：

#### 1. 更新 HBase coprocessor

现有 Cube 的 HBase 表需要更新到最新的 coprocessor；请按照[更新 coprocessor][howto_update_coprocessor.html]进行更新；

#### 2. 更新 conf/kylin.properties

在 v1.5.2 中，不推荐使用几个属性，并添加了几个新属性：

已弃用:

* kylin.hbase.region.cut.small=5
* kylin.hbase.region.cut.medium=10
* kylin.hbase.region.cut.large=50

新增:

* kylin.hbase.region.cut=5
* kylin.hbase.hfile.size.gb=2

这些新参数决定了如何分割 HBase region；要使用不同的大小，您可以在 Cube 级别覆盖这些参数。

从旧的 kylin.properties 文件复制时，建议删除不推荐使用的并添加新的。

#### 3. 添加 conf/kylin_job_conf_inmem.xml

在 "conf" 文件夹中添加了一个名为 "conf/kylin_job_conf_inmem.xml" 的新作业配置文件；由于 Kylin 1.5 引入了“fast cubing”算法，该算法旨在利用更多内存来进行 in-mem 聚合； Kylin 将使用这个新的 conf 文件来提交 in-mem 的 cube build 作业，该作业与正常作业请求不同的内存；请根据您的集群容量适当调整。

此外，如果为不同容量的 Cube 使用了单独的配置文件，例如 "kylin_job_conf_small.xml", "kylin_job_conf_medium.xml" 和 "kylin_job_conf_large.xml"，请注意它们现在已被弃用；只有 "kylin_job_conf.xml" 和 "kylin_job_conf_inmem.xml" 用于提交cube job；如果你有 cube 级别的作业配置（比如使用不同的 Yarn 作业队列），你可以在 cube 级别自定义，查看 [KYLIN-1706](https://issues.apache.org/jira/browse/KYLIN-1706)。
