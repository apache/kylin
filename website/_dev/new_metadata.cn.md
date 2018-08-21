---
layout: dev-cn
title:  "新的元数据模型"
categories: development
permalink: /cn/development/new_metadata.html
---

Kylin 正在进行循环代码重构，它将对元数据引入以下两个更改：

* 从 "cube_desc" 抽象一个 "model_desc" 层

定义一个 cube 前，用户将需要首先定义一个模型（"model_desc")；模型定义了哪个是事实表，哪些是维度表以及它们是如何 join 的；

在定义模型时，当用户定义 cube（“cube_desc”）时，他/她只需要为维度指定表/列名称，因为已经定义了 join 条件;

这种抽象是扩展元数据以实现非 cube 查询（即将推出）;

* 支持来自多个 hive 数据库的数据表；

用户有这样的场景：表来自多个 hive 数据库，表名可能相同；为了支持这种情况，Kylin 将使用数据库名称 + 表名作为表的唯一名称；并且当在 Kylin 中查询时，用户需要在 SQL 中指定数据库名称（如果它不是“default”）。 

这里有一个样例；事实表 "test_kylin_fact" 来自于默认的 hive 数据库，您不需要指定数据库名称；然而维度表来自于 "edw"，在查询中您需要使用 "edw.test_cal_dt"：

{% highlight Groff markup %}
select test_cal_dt.Week_Beg_Dt, sum(price) as c1, count(1) as c2 
 from test_kylin_fact
 inner JOIN edw.test_cal_dt as test_cal_dt
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 where test_kylin_fact.lstg_format_name='ABIN' 
{% endhighlight %}
