---
layout: docs31-cn
title:  使用 Dashboard
categories: tutorial
permalink: /cn/docs31/tutorial/use_dashboard.html
---

> 自 Apache Kylin v2.3.0 起使用

# Dashboard

作为 project 拥有者，想了解您 Cube 使用指标? 想要知道针对您 Cube 的每一天的查询? 什么是 AVG 查询延迟？您是否想知道每 GB 源数据的 AVG Cube build 时间，这对预测即将到来的 Cube build job 的时间成本非常有帮助？ 您可以从 Kylin Dashboard 找到所有信息。

Kylin Dashboard 展示有用的 Cube 使用数据，对用户非常重要。

## 前期

为了使 Dashboard 在 WebUI 中生效，您需要确保这些都设置了:
* 在 **kylin.properties** 中设置 **kylin.web.dashboard-enabled=true**。
* 根据 [toturial](setup_systemcube.html) 建立系统 Cubes.

## 如何使用

#### 步骤 1:

​	在导航条处点击 '**Dashboard**' 按钮。

​	此页面上有9个可供您操作的框。

​	每个框代表不同的属性，包括 '**Time Period**'，'**Total Cube Count**'，'**Avg Cube Expansion**'，'**Query Count**'，'**Average Query Latency**'，'**Job Count**'，'**Average Build Time per MB**'，'**Data grouped by Project**' 和 '**Data grouped by Time**'。

![Kylin Dashboard](/images/Dashboard/QueryCount.jpg)

#### 步骤 2:

您应该点击日历修改 '**Time Period**'。

![SelectPeriod](/images/Dashboard/SelectPeriod.png)

- '**Time period**' 默认为 **'Last 7 Days**'。

- 这里有 **2** 种方式修改 time period。一种是*使用 standard time period* 而另一种是 *定制您的 time period*。

  1. 如果您想要*使用 standard time periods*，您可以点击 '**Last 7 Days**' 只选择过去 7 天的数据，或点击 '**This Month**' 选择这个月的数据，或点击 '**Last Month**' 选择上个月的数据。 

  2. 如果您想要*定制 time period*，点击 '**Custom Range**'。

     这里有 **2** 种方式定制 time period，一种是*在文本框输入日期*而另一种是*从日历中选择日期*。

     1. 如果您想要*在文本框输入日期*，请确保两个日期都有效。
     2. 如果您想要*从日历中选择日期*，请确保点击了两个具体的日期。

- 您修改了 time period 后，点击 '**Apply**' 来使得改变生效，点击 '**Cancel**' 放弃修改。

#### 步骤 3:

现在数据分析将会在同一个页面改变和展示（重要的信息已经打上马赛克了）

- '**Total Cube Count**' 和 '**Avg Cube Expansion**' 的数量的颜色是**蓝色**。

  您可以在这两个框中点击 '**More Details**' 且您将会被引导至 '**Model**' 页面。

- '**Query Count**'，'**Average Query Latency**'，'**Job Count**' 和 '**Average Build Time per MB**' 的数量的颜色是**绿色**。

  您可以点击这个四个矩形获得关于你所选的数据的详细信息。详细信息将以图表的形式展示且展示在 '**Data grouped by Project**' 和 '**Data grouped by Time**' 框。

  1. '**Query Count**' 和 '**Average Query Latency**'

     您可以点击 '**Query Count**' 以获得详细信息。 

     ![QueryCount](/images/Dashboard/QueryCount.jpg)

     您可以点击 '**Average Query Latency**' 以获得详细信息。 

     ![AVG-Query-Latency](/images/Dashboard/AVGQueryLatency.jpg)

     您可以在这两个框中点击 '**More Details**' 并且将会引导至 '**Insight**' 页面。 

  2. '**Job Count**' 和 '**Average Build Time per MB**'

     您可以点击 '**Job Count**' 以获得详细信息。 

     ![Job-Count](/images/Dashboard/JobCount.jpg)

     您可以点击 '**Average Build Time per MB**' 以获得详细信息。 

     ![AVG-Build-Time](/images/Dashboard/AVGBuildTimePerMB.jpg)

     您可以在这两个框中点击 '**More Details**' 并且将会引导至 '**Monitor**' 页面。浏览器中看到 'Please wait...' 是常见的。

#### 步骤 4:

**Advanced Operations**

'**Data grouped by Project**' 和 '**Data grouped by Time**' 以图表的形式显示数据。

在 '**Data grouped by Project**' 中有一个单选按钮称为 '**showValue**'，您可以选择在图表中显示数字。

有一个单选的下拉框 '**Data grouped by Time**'，您可以选择在不同的时间线中显示图表。
