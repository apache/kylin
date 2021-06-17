---
layout: docs40-cn
title:  优化构建和查询性能
categories: howto
permalink: /cn/docs40/howto/howto_optimize_build_and_query.html
since: v4.0.0
---

Apache kylin4.0 是继 Kylin3.0 之后一个重大的的架构升级版本，cube 构建 和查询都使用 spark 作为计算引擎，cube 数据存储在 parquet 文件中而不是 HBase。因此，Kylin4.0 的构建/查询性能调优与 [Kylin3.0 调优](http://kylin.apache.org/docs/howto/howto_optimize_build.html) 非常不同。

关于 Kylin4.0 的构建/查询性能调优经验，请参考：
[How to improve cube building and query performance of Apache Kylin4.0](https://cwiki.apache.org/confluence/display/KYLIN/How+to+improve+cube+building+and+query+performance).

同时提供视频讲解：
[How to optimize build performance in kylin 4.0](https://www.bilibili.com/video/BV1ry4y1z7Nt) 
[How to optimize query performance in kylin 4.0](https://www.bilibili.com/video/BV18K411G7k3)

以及 Kylin4.0 用户有赞的最佳实践博客：
[有赞为什么选择 kylin4.0](/cn_blog/2021/06/17/Why-did-Youzan-choose-Kylin4/) 