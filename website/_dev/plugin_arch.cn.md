---
layout: dev-cn
title:  插件架构
categories: development
permalink: /cn/development/plugin_arch.html
---

插件架构旨在使 Kylin 在计算框架，数据源和 cube 存储方面具有可扩展性。从 v1 开始，Kylin 与作为计算框架的 Hadoop MapReduce，作为数据源的 Hive，作为存储的 HBase 紧密结合。这样的问题出现了：Kylin 可以使用 Spark 作为 cube 引擎，或者可以使用像 Cassandra 那样不同的存储。我们希望对不同的选择持开放态度，并确保 Kylin 用最好的技术堆栈进化。这就是 Kylin v2 中引入插件架构的原因。

![Plugin Architecture Overview](/images/develop/plugin_arch_overview.png)

## 如何运行

cube 元数据定义了 cube 所依赖的引擎，源和存储的类型。工厂模式用于构造每个依赖项的实例。适配器模式用于将部件连接在一起。

例如一个 cube 描述可能包含:

- fact_table: `SOME_HIVE_TABLE`
- engine_type: `2` (MR Engine v2)
- storage_type: `2` (HBase Storage v2)

基于元数据，工厂创建 MR 引擎，Hive 数据源和 HBase 存储。

![Plugin Architecture Factory Pattern](/images/develop/plugin_arch_factory_pattern.png)

引擎就像一个主板，源和存储必须由输入和输出接口定义。数据源和存储必须适应接口，以便连接到引擎主板。

![Plugin Architecture Adaptor Pattern](/images/develop/plugin_arch_adaptor_pattern.png)

一旦上面的对象图被创建和连接，引擎就可以驱动 cube 构建过程。

## 插件架构的好处

- 自由
	- Zoo 打破了，不再与 Hadoop 绑定
	- 免费使用更好的引擎或存储
- 可扩展性
	- 接受任意输入，例如 Kafka
	- 拥抱下一代分布式平台，例如 Spark
- 灵活性
	- 为不同的数据集选择不同的引擎

