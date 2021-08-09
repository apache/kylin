---
layout: docs40-cn
title: Apache Kylin4 概述
categories: docs
permalink: /cn/docs40/index.html
---

欢迎来到 Apache Kylin™
------------  
> Analytical Data Warehouse for Big Data

Apache Kylin™是一个开源的、分布式的分析型数据仓库，提供 Hadoop 之上的 SQL 查询接口及多维分析（OLAP）能力以支持超大规模数据，最初由eBay Inc.开发并贡献至开源社区。

查看其它版本文档: 
* [v3.0 document](/cn/docs30/)
* [v2.4 document](/cn/docs24/)
* [归档](/archive/)

Apache Kylin4.0 是 Apache Kylin3.x 之后一次重大的版本更新，它采用了全新的 Spark 构建引擎和 Parquet 作为存储，同时使用 Spark 作为查询引擎。

Apache Kylin4.0 的第一个版本 kylin4.0.0-alpha 于 2020 年 7 月份发布，此后相继发布 kylin4.0.0-beta 以及正式版本。

为了方便用户对 Kylin4.x 有更全面更深层的了解，本篇文档会着重从 Kylin4.x 与之前版本有何异同的角度对 Kylin4.x 做全面概述。文章分为以下几个部分：

- 为什么选择 Parquet 替换 HBase
- 预计算结果在 Kylin4.0 中如何存储
- Kylin 4.0 的构建引擎
- Kylin 4.0 的查询引擎
- Kylin4.0 与 Kylin3.1 功能对比
- Kylin 4.0 性能表现
- Kylin 4.0 查询和构建调优
- Kylin 4.0 用户案例

## 为什么选择 Parquet 替换 HBase
在 3.x 以及之前的版本中，kylin 一直使用 HBase 作为存储引擎来保存 cube 构建后产生的预计算结果。HBase 作为 HDFS 之上面向列族的数据库，查询表现已经算是比较优秀，但是它仍然存在以下几个缺点：
1. HBase 不是真正的列式存储；
2. HBase 没有二级索引，Rowkey 是它唯一的索引；
3. HBase 没有对存储的数据进行编码，kylin 必须自己进行对数据编码的过程；
4. HBase 不适合云上部署和自动伸缩；
5. HBase 不同版本之间的 API 版本不同，存在兼容性问题（比如，0.98，1.0，1.1，2.0）；
6. HBase 存在不同的供应商版本，他们之间有兼容性问题。

针对以上问题，社区提出了对使用 Apache Parquet + Spark 来代替 HBase 的提议，理由如下：
1. Parquet 是一种开源并且已经成熟稳定的列式存储格式；
2. Parquet 对云更加友好，可以兼容各种文件系统，包括 HDFS、S3、Azure Blob store、Ali OSS 等；
3. Parquet 可以很好地与 Hadoop、Hive、Spark、Impala 等集成；
4. Parquet 支持自定义索引。

## 预计算结果在 Kylin4.0 中如何存储
在 Kylin4.x 中，预计算结果以 Parquet 格式存储在文件系统中，文件存储结构对于 I/O 优化很重要，提前对存储目录结构进行设计，就能够在查询时通过目录或者文件名过滤数据文件，避免不必要的扫描。
Kylin4 对 cube 进行构建得到的预计算结果的 Parquet 文件在文件系统中存储的目录结构如下：
- cube_name
  - SegmentA
    - Cuboid-111
      - part-0000-XXX.snappy.parquet
      - part-0001-XXX.snappy.parquet
      - ...
    - Cuboid-222
      - part-0000-XXX.snappy.parquet
      - part-0001-XXX.snappy.parquet
      - ...
  - SegmentB
      - Cuboid-111
        - part-0000-XXX.snappy.parquet
        - part-0001-XXX.snappy.parquet
        - ...
      - Cuboid-222
        - part-0000-XXX.snappy.parquet
        - part-0001-XXX.snappy.parquet
        - ...               

可以看出，与 HBase 相比，采用 Parquet 存储可以很方便地增删 cuboid 而不影响其他数据。利用这种特点，Kylin4 中实现了支持用户手动增删 cuboid 的功能，请参考：[How to update cuboid list for a cube](https://cwiki.apache.org/confluence/display/KYLIN/How+to+update+cuboid+list+for+a+cube)

## Kylin 4.0 的构建引擎
在 Kylin4 中，Spark Engine 是唯一的构建引擎，与之前版本中的构建引擎相比，存在如下特点：

1、Kylin4 的构建简化了很多步骤。比如在 Cube Build Job 中， kylin4 只需要资源探测和 cubing 两个步骤，就可以完成构建；
2、由于 Parquet 会对存储的数据进行编码，所以在 kylin4 中不再需要维度字典和对维度列编码的过程；
3、Kylin4 对全局字典做了全新的实现，更多细节请参考：[Kylin4 全局字典](https://cwiki.apache.org/confluence/display/KYLIN/Global+Dictionary+on+Spark+CN) ；
4、Kylin4 会根据集群资源、构建任务情况等对 Spark 进行自动调参；
5、Kylin4 提高了构建速度。

用户可以通过 `kylin.build.spark-conf` 开头的配置项手动修改构建相关的 Spark 配置，经过用户手动修改的 Spark 配置项不会再参与自动调参。

## Kylin 4.0 的查询引擎
Kylin4 的查询引擎 `Sparder(SparderContext)` 是由 spark application 后端实现的新型分布式查询引擎，相比于原来的查询引擎，Sparder 的优势体现在以下几点：
- 分布式的查询引擎，有效避免单点故障；
- 与构建所使用的计算引擎统一为 Spark；
- 对于复杂查询的性能有很大提高；
- 可以从 Spark 的新功能及其生态中获益。

在 Kylin4 中，Sparder 是作为一个 long-running 的 spark application 存在的。 Sparder 会根据 `kylin.query.spark-conf` 开头的配置项中配置的 Spark 参数来获取 Yarn 资源，如果配置的资源参数过大，可能会影响构建任务甚至无法成功启动 Sparder，如果 Sparder 没有成功启动，则所有查询任务都会失败，用户可以在 kylin WebUI 的 System 页面中检查 Sparder 状态。

默认情况下，用于查询的 spark 参数会设置的比较小，在生产环境中，大家可以适当把这些参数调大一些，以提升查询性能。
`kylin.query.auto-sparder-context` 参数用于控制是否在启动 kylin 的同时启动 Sparder，默认值为 false，即默认情况下会在执行第一条 SQL 的时候才启动 Sparder，由于这个原因，执行第一条 SQL 的时候的会花费较长时间。
如果你不希望第一条 SQL 的查询速度低于预期，可以设置 `kylin.query.auto-sparder-context` 为 `true`，此时 Sparder 会随 Kylin 一起启动。

## Kylin 4.0 与 Kylin 3.1 功能对比

| Feature                | Kylin 3.1.0                                  | Kylin 4.0                                      |
| ---------------------  | :------------------------------------------- | :----------------------------------------------|
| Storage                | HBase                                        | Parquet                                        |
| BuildEngine            | MapReduce/Spark/Flink                        | New Spark Engine                               |
| Metastore              | HBase(Default)/Mysql                         | Mysql(Default)                                 |
| DataSource             | Kafka/Hive/JDBC                              | Hive/CSV                                       |
| Global Dictionary      | Two implementation                           | New implementation                             |
| Cube Optimization Tool | Cube Planner                                 | Cube Planner phase1 and Optimize cube manually |
| Self-monitoring        | System cube and Dashboard                    | System cube and Dashboard                      |
| PushDown Engine        | Hive/JDBC                                    | Spark SQL                                      |
| Hadoop platform        | HDP2/HDP3/CDH5/CDH6/EMR5                     | HDP2/CDH5/CDH6/EMR5/EMR6/HDI                   |
| Deployment mode        | Single node/Cluster/Read and write separation| Single node/Cluster/Read and write separation  |

## Kylin 4.0 性能表现
为了测试 Kylin4.0 的性能，我们分别在 SSB 数据集和 TPC-H 数据集上做了 benchmark，与 Kylin3.1.0 进行对比。测试环境为 4 个节点的 CDH 集群，所使用的 yarn 队列分配了 400G 内存和 128 cpu cores。
性能测试对比结果如下：
- Comparison of build duration and result size（SSB）
![](/images/tutorial/4.0/overview/build_duration_ssb.png)  
![](/images/tutorial/4.0/overview/result_size_ssb.png)

测试结果可以体现以下两点：
- kylin4 的构建速度与 kylin3.1.0 的 Spark Engine 相比有明显提升；
- Kylin4 构建后得到的预计算结果 Parquet 文件大小与 HBase 相比有明显减小；

- Comparison of query response(SSB and TPC-H)
![](/images/tutorial/4.0/overview/query_response_ssb.png)
![](/images/tutorial/4.0/overview/query_response_tpch.png)

从查询结果对比中可以看出，对于***简单查询***，kylin3 与 Kylin4 不相上下，kylin4 略有不足；而对于***复杂查询***，kylin4 则体现出了明显的优势，查询速度比 kylin3 快很多。
并且，Kylin4 中的***简单查询***的性能还存在很大的优化空间。在有赞使用 Kylin4 的实践中，对于***简单查询***的性能可以优化到 1 秒以内。

## 如何升级
请参考文档：[How to migrate metadata to Kylin4](https://cwiki.apache.org/confluence/display/KYLIN/How+to+migrate+metadata+to+Kylin+4)

## Kylin 4.0 查询和构建调优
对于 Kylin4 的调优，请参考：[How to improve cube building and query performance](/docs40/howto/howto_optimize_build_and_query.html)

## Kylin 4.0 用户案例
[Why did Youzan choose Kylin4](/blog/2021/06/17/Why-did-Youzan-choose-Kylin4)

参考链接：
[Kylin Improvement Proposal 1: Parquet Storage](https://cwiki.apache.org/confluence/display/KYLIN/KIP-1%3A+Parquet+storage)






