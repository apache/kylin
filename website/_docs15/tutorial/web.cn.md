---
layout: docs15-cn
title:  Kylin网页版教程
categories: 教程
permalink: /cn/docs15/tutorial/web.html
version: v1.2
---

> **支持的浏览器**
> 
> Windows: Google Chrome, FireFox
> 
> Mac: Google Chrome, FireFox, Safari

## 1. 访问 & 登陆
访问主机: http://hostname:7070
使用用户名/密码登陆：ADMIN/KYLIN

![]( /images/Kylin-Web-Tutorial/1 login.png)

## 2. Kylin中可用的Hive表
虽然Kylin使用SQL作为查询接口并利用Hive元数据，Kylin不会让用户查询所有的hive表，因为到目前为止它是一个预构建OLAP(MOLAP)系统。为了使表在Kylin中可用，使用"Sync"方法能够方便地从Hive中同步表。

![]( /images/Kylin-Web-Tutorial/2 tables.png)

## 3. Kylin OLAP Cube
Kylin的OLAP Cube是从星型模式的Hive表中获取的预计算数据集，这是供用户探索、管理所有cube的网页管理页面。由菜单栏进入`Cubes`页面，系统中所有可用的cube将被列出。

![]( /images/Kylin-Web-Tutorial/3 cubes.png)

探索更多关于Cube的详细信息

* 表格视图:

   ![]( /images/Kylin-Web-Tutorial/4 form-view.png)

* SQL 视图 (Hive查询读取数据以生成cube):

   ![]( /images/Kylin-Web-Tutorial/5 sql-view.png)

* 可视化 (显示这个cube背后的星型模式):

   ![]( /images/Kylin-Web-Tutorial/6 visualization.png)

* 访问 (授予用户/角色权限，beta版中授予权限操作仅对管理员开放):

   ![]( /images/Kylin-Web-Tutorial/7 access.png)

## 4. 在网页上编写和运行SQL
Kelin的网页版为用户提供了一个简单的查询工具来运行SQL以探索现存的cube，验证结果并探索使用#5中的Pivot analysis与可视化分析的结果集。

> **查询限制**
> 
> 1. 仅支持SELECT查询
> 
> 2. 为了避免从服务器到客户端产生巨大的网络流量，beta版中的扫描范围阀值被设置为1,000,000。
> 
> 3. beta版中，SQL在cube中无法找到的数据将不会重定向到Hive

由菜单栏进入“Query”页面：

![]( /images/Kylin-Web-Tutorial/8 query.png)

* 源表：

   浏览器当前可用表（与Hive相同的结构和元数据）：
  
   ![]( /images/Kylin-Web-Tutorial/9 query-table.png)

* 新的查询：

   你可以编写和运行你的查询并探索结果。这里提供一个查询供你参考：

   ![]( /images/Kylin-Web-Tutorial/10 query-result.png)

* 已保存的查询：

   与用户账号关联，你将能够从不同的浏览器甚至机器上获取已保存的查询。
   在结果区域点击“Save”，将会弹出名字和描述来保存当前查询：

   ![]( /images/Kylin-Web-Tutorial/11 save-query.png)

   点击“Saved Queries”探索所有已保存的查询，你可以直接重新提交它来运行或删除它：

   ![]( /images/Kylin-Web-Tutorial/11 save-query-2.png)

* 查询历史：

   仅保存当前用户在当前浏览器中的查询历史，这将需要启用cookie，并且如果你清理浏览器缓存将会丢失数据。点击“Query History”标签，你可以直接重新提交其中的任何一条并再次运行。

## 5. Pivot Analysis与可视化
Kylin的网页版提供一个简单的Pivot与可视化分析工具供用户探索他们的查询结果：

* 一般信息：

   当查询运行成功后，它将呈现一个成功指标与被访问的cube名字。
   同时它将会呈现这个查询在后台引擎运行了多久（不包括从Kylin服务器到浏览器的网络通信）：

   ![]( /images/Kylin-Web-Tutorial/12 general.png)

* 查询结果：

   能够方便地在一个列上排序。

   ![]( /images/Kylin-Web-Tutorial/13 results.png)

* 导出到CSV文件

   点击“Export”按钮以CSV文件格式保存当前结果。

* Pivot表：

   将一个或多个列拖放到标头，结果将根据这些列的值分组：

   ![]( /images/Kylin-Web-Tutorial/14 drag.png)

* 可视化：

   同时，结果集将被方便地显示在“可视化”的不同图表中：

   注意：线形图仅当至少一个从Hive表中获取的维度有真实的“Date”数据类型列时才是可用的。

   * 条形图：

   ![]( /images/Kylin-Web-Tutorial/15 bar-chart.png)
   
   * 饼图：

   ![]( /images/Kylin-Web-Tutorial/16 pie-chart.png)

   * 线形图：

   ![]( /images/Kylin-Web-Tutorial/17 line-chart.png)

