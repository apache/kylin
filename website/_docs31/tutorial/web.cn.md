---
layout: docs31-cn
title:  Web 界面
categories: 教程
permalink: /cn/docs31/tutorial/web.html
version: v1.2
---

> **支持的浏览器**
> 
> Windows: Google Chrome, FireFox
> 
> Mac: Google Chrome, FireFox, Safari

## 1. 访问 & 登陆
访问主机: http://hostname:7070
使用用户名登陆：ADMIN
使用密码登陆：KYLIN

![](/images/tutorial/1.5/Kylin-Web-Tutorial/1 login.png)

## 2. Kylin 中可用的 Hive 表
虽然 Kylin 使用 SQL 作为查询接口并利用 Hive 元数据，Kylin 不会让用户查询所有的 hive 表，因为到目前为止它是一个预构建 OLAP(MOLAP) 系统。为了使表在 Kylin 中可用，使用 "Sync" 方法能够方便地从 Hive 中同步表。

![](/images/tutorial/1.5/Kylin-Web-Tutorial/2 tables.png)

## 3. Kylin OLAP Cube
Kylin 的 OLAP Cube 是从星型模式的 Hive 表中获取的预计算数据集，这是供用户探索、管理所有 cube 的网页管理页面。由菜单栏进入 `Model` 页面，系统中所有可用的 cube 将被列出。

![](/images/tutorial/1.5/Kylin-Web-Tutorial/3 cubes.png)

探索更多关于 Cube 的详细信息

* Grid 视图:

   ![](/images/tutorial/1.5/Kylin-Web-Tutorial/4 grid-view.PNG)

* SQL 视图 (Hive 查询读取数据以生成 cube):

   ![](/images/tutorial/1.5/Kylin-Web-Tutorial/5 sql-view.png)

## 4. 在网页上编写和运行 SQL
Kylin 的网页版为用户提供了一个简单的查询工具来运行 SQL 以探索现存的 cube，验证结果并探索使用下一章中的 Pivot analysis 与可视化的结果集。

> **查询限制**
> 
> 1. 仅支持 SELECT 查询
> 
> 2. 支持聚合函数和 group by

由菜单栏进入 “Insight” 页面：

![](/images/tutorial/1.5/Kylin-Web-Tutorial/8 query.png)

* 源表：

   浏览器当前可用表（与 Hive 相同的结构和元数据）：
  
   ![](/images/tutorial/1.5/Kylin-Web-Tutorial/9 query-table.png)

* 新的查询：

   你可以编写和运行你的查询并探索结果。

   ![](/images/tutorial/1.5/Kylin-Web-Tutorial/10 query-result.png)

* 已保存的查询：

   与用户账号关联，你将能够从不同的浏览器甚至机器上获取已保存的查询。
   在结果区域点击 “Save”，将会弹出用来保存当前查询名字和描述：

   ![](/images/tutorial/1.5/Kylin-Web-Tutorial/11 save-query.png)

   点击 “Saved Queries” 浏览所有已保存的查询，你可以直接重新提交它或删除它：

* 查询历史：

   仅保存当前用户在当前浏览器中的查询历史，这将需要启用 cookie，并且如果你清理浏览器缓存将会丢失数据。点击 “Query History” 标签，你可以直接重新提交其中的任何一条并再次运行。

## 5. Pivot Analysis 与可视化
Kylin 的网页版提供一个简单的 Pivot 与可视化分析工具供用户探索他们的查询结果：

* 一般信息：

   当查询运行成功后，它将呈现一个成功指标与被访问的 cube 名字。
   同时它将会呈现这个查询在后台引擎运行了多久（不包括从 Kylin 服务器到浏览器的网络通信）：

   ![](/images/tutorial/1.5/Kylin-Web-Tutorial/12 general.png)

* 查询结果：

   能够方便地在一个列上排序。

   ![](/images/tutorial/1.5/Kylin-Web-Tutorial/13 results.png)

* 导出到 CSV 文件

   点击 “Export” 按钮以 CSV 文件格式保存当前结果。

* 可视化：

   同时，结果集将被方便地显示在 “可视化” 的不同图表中，总共有3种类型的图表：线性图、饼图和条形图

   注意：线形图仅当至少一个从 Hive 表中获取的维度有真实的 “Date” 数据类型列时才是可用的。

* 饼图：

   ![](/images/tutorial/1.5/Kylin-Web-Tutorial/15 bar-chart.png)

