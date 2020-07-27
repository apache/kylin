---
layout: docs40-cn
title:  Use Cube Planner
categories: tutorial
permalink: /cn/docs40/tutorial/use_cube_planner.html
---

# Cube Planner

## 什么是 Cube Planner

OLAP 解决方案权衡了线上查询速度和线下 Cube build 花费（build Cube 的计算资源及保存 Cube 数据的存储资源）。资源效率是 OLAP engine 的最重要的能力。为了提高资源利用率，pre-build 最有价值的 cuboids 是至关重要的。

Cube Planner 使 Apache Kylin 变得更节约资源。其智能 build 部分 Cube 以最小化 building Cube 的花费且同时最大化服务终端用户查询的利益，然后从运行中的查询学习模式且相应的进行动态的推荐 cuboids。 

![CubePlanner](/images/CubePlanner/CubePlanner.png)

## 前提

为使得在 WebUI 上的 Dashboard 有效，您需要设置 **kylin.cube.cubeplanner.enabled=true** 以及 **kylin.properties** 中的其他属性。


{% highlight Groff markup %}
kylin.cube.cubeplanner.enabled=true
kylin.server.query-metrics2-enabled=true
kylin.metrics.reporter-query-enabled=true
kylin.metrics.reporter-job-enabled=true
kylin.metrics.monitor-enabled=true
{% endhighlight %}

## 如何使用

*注意：Cube planner 分为两个阶段。阶段 1 可以在构建 Cube 前基于估算的 cuboid 大小推荐 cuboid 列表，然而阶段 2 是根据统计信息为已存在的 Cube 推荐 cuboid 列表。优化前 Cube 应该在产品上线一段时间（如 3 个月）。因而 Kylin 平台从终端用户收集了足够真实的查询且使用他们优化 Cube。*  

在 Kylin4.0 中支持了 Cube planner 阶段 1, 请查看文档: [How to use Cube Planner in Kylin4.0](https://cwiki.apache.org/confluence/display/KYLIN/How+to+use+Cube+Planner+in+Kylin+4)

对于 Cube planner 阶段 2, kylin4.0 目前只做到了部分支持, 请查看文档: [How to update cuboid list for a cube in Kylin4.0](https://cwiki.apache.org/confluence/display/KYLIN/How+to+update+cuboid+list+for+a+cube)

