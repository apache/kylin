---
layout: dev-cn
title:  编码和命名惯例
categories: development
permalink: /cn/development/coding_naming_convention.html
---

## 编码惯例

团队合作中编码管理是非常重要的。它不仅使得代码整齐及整洁，它也节省了许多的工作。不同的代码惯例（自动格式化）将会导致不必要的代码改动使得在代码复审和合并时需要更多的努力。

对于 Java 代码，我们使用 Eclipse 默认的格式化配置，其中一个改动允许长行。

- 对于 Eclipse 开发者，无需手动设置。代码格式化配置 `.settings/org.eclipse.jdt.core.prefs` 在 git 仓库。当项目被引入时，您的 IDE 应该自动配置。
- 对于 intellij IDEA 开发者，您需要安装 "Eclipse Code Formatter" 并手动将 Eclipse 格式化配置加载到您的 IDE。详细内容请看[搭建开发环境](dev_env.html)。
- 我们在 maven 中有 *checkstyle plugin* 能强制进行规范检查。

对于 JavaScript，XML 和其它编码，请使用空格来缩进。作为常规，请保持代码格式与现有行一致。目前没有其他的强制措施。



## 配置命名惯例

Kylin 配置名称（在 `kylin.properties` 中的那些)

- 惯例是 `dot.separated.namespace.config-name-separated-by-dash`，所有的字符都是小写。
- 基本原理：点分隔前缀用于命名空间，如 java 包名。最后一级与类名相同，但要小写并以短划线分隔。结果与常见的 hadoop 配置名称一致，即。`dfs.namenode.servicerpc-bind-host`。
- 正面例子：`kylin.metadata.url`; `kylin.snapshot.max-mb`
- 反面例子：`kylin.cube.customEncodingFactories`，应该为 `kylin.cube.custom-encoding-factories`
- 名称的命名空间（或包）应映射来自配置中使用的 Java 项目和包。 下面是当前名称空间的列表。
  - kylin.env
  - kylin.metadata
  - kylin.snapshot
  - kylin.dictionary
  - kylin.cube
  - kylin.job
  - kylin.engine
  - kylin.engine.mr
  - kylin.engine.spark
  - kylin.source
  - kylin.source.hive
  - kylin.source.kafka
  - kylin.storage
  - kylin.storage.hbase
  - kylin.storage.partition
  - kylin.query
  - kylin.security
  - kylin.server
  - kylin.web



## 配置文件命名惯例

对于配置文件如日志配置，spring 配置，mapreduce job 配置等。

- 惯例是 `words-separated-by-dash.ext`，所有的字符要小写。
- 基本原理：要和 hadoop 配置文件命名一致，即。hdfs-site.xml
- 正面例子：`kylin-server-log4j.properties`
- 反面例子：`kylin_hive_conf.xml`，应该为 `kylin-hive-conf.xml`


