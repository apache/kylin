---
layout: docs31-cn
title:  使用 Cube Planner
categories: tutorial
permalink: /cn/docs31/tutorial/use_cube_planner.html
---

> 自 Apache Kylin v2.3.0 起使用

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

#### 步骤 1:

​	选择一个 Cube

#### 步骤 2:

1. 点击 '**Planner**' 按钮查看 Cube 的 '**Current Cuboid Distribution**'。

  应该确保 Cube 的状态是 '**READY**'

  如果 Cube 的状态是 '**DISABLED**'，将不能使用 Cube planner。

  如果 Cube 之前已经 build 过，需要通过 building 或 enabling 它来将其状态从 '**DISABLED**' 变为 '**READY**'。


#### 步骤 3:

a. 点击 '**Planner**' 按钮查看 Cube 的 '**Current Cuboid Distribution**'。

- 数据将会展示在 Sunburst Chart。

- 每一部分代表一个 cuboid，以不同的颜色展示，颜色取决于对 cuboid 查询的**频率**。

     ![CubePlanner](/images/CubePlanner/CP.png)


-  您可以移动鼠标经过图标然后它将会展示 cuboid 的详细信息。

   详细信息包含 5 属性，'**Name**'，'**ID**'，'**Query Count**'，'**Exactly Match Count**'，'**Row Count**' 和 '**Rollup Rate**'。

   Cuboid **Name** 由几个 '0' 或 '1' 组成。其意味着 dimensions 的结合。'0' 意味着 dimension 在这个组合中不存在，'1' 在这个组合中存在。所有的 dimensions 由在 advanced settings 中设置的 HBase 的 row keys 进行排序。 

   样例：

   ![CubePlanner](/images/CubePlanner/Leaf.png)

   "1111111110000000" 意味着 dimension 组合是 ["MONTH_BEG_DT","USER_CNTRY_SITE_CD","RPRTD_SGMNT_VAL","RPRTD_IND","SRVY_TYPE_ID","QSTN_ID","L1_L2_IND","PRNT_L1_ID","TRANCHE_ID"]，其基于 row key 排序。

   **ID** 是 cuboid 唯一的 id

   **Query Count** 是这个 cuboid 服务的查询总数，包括那些未预计算 cuboids 的查询，但是从线上 cuboid 中聚合出来。  

   **Exactly Match Count** 是针对 cuboid 的真实查询的查询数量统计。

   **Row Count** 是针对这个 cuboid 所有 segment 的总 row 统计。

   **Rollup Rate** = (Cuboid 的 Row 统计数 / 其父母 cuboid 的 Row 统计数) * 100%  

-  太阳图表的中心包含基本 cuboid 的组合信息。其 '**名字**' 由几个 '1' 组成。

至于一片叶子，其 '**名字**' 由几个 '0' 和 1' 组成。 

-    如果您想要指定一片叶子，点击它。视图将会自己变化。

     ![Leaf-Specify](/images/CubePlanner/Leaf-Specify.png)

-    如果您想要指定一片叶子的父叶子，点击**圆圈中心**（标记为黄色的部分）。

![Leaf-Specify-Parent](/images/CubePlanner/Leaf-Specify-Parent.png)

b. 点击 '**Recommend**' 按钮去查看 Cube 的 '**Recommend Cuboid Distribution**'。

如果 Cube 正在 building，Cube planner 的 '**Recommend**' 功能将不能被执行。请等待 cube build 完成。

-  数据将会被使用唯一的算法计算。看见这个窗口很正常。

   ![Recommending](/images/CubePlanner/Recommending.png)

-  数据将会被展示在太阳图表

   - 每一个部分以不同的颜色展示，颜色取决于**频率**。

![CubePlanner_Recomm](/images/CubePlanner/CPRecom.png)

- '**Recommend Cuboid Distribution**' 图表的具体操作和 '**Current Cuboid Distribution**' 图表的一样。
- 当鼠标悬停在太阳形图表上时，用户可以从 cuboid 中指出 dimension 名称，如下图所示
- 用户可以点击**导出**从现有的 Cube 中以 json 文件导出受欢迎的维度组合（TopN cuboid，当前包括 Top 10，Top 50，Top 100 选项），并将其下载到您的本地文件系统，用于在创建 Cube 时记录或将来导入 dimension 组合。

![export cuboids](/images/CubePlanner/export_cuboids.png)

c. 点击 '**Optimize**' 按钮优化 Cube。

- 一个确认窗口会弹出。点击 '**Yes**' 开始优化。点击 '**Cancel**' 取消优化。

- 用户可以在 Cube Planner 标签页了解 Cube 的最新优化时间。 

![column name+optimize time](/images/CubePlanner/column_name+optimize_time.png)

- 用户可以收到一个 Cube 优化 job 的邮件通知。

![optimize email](/images/CubePlanner/optimize_email.png)
