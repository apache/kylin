---
layout: docs31-cn
title: "优化 Cube 构建"
categories: tutorial
permalink: /cn/docs31/tutorial/cube_build_performance.html
---
 *本教程是关于如何一步步优化 cube build 的样例。* 
 
在这个场景中我们尝试优化一个简单的 Cube，用 1 张 fact 和 1 张 lookup 表 (日期 Dimension)。在真正的调整之前，请从 [优化 Cube Build](/docs20/howto/howto_optimize_build.html) 中大体了解关于 Cube build 的过程

![]( /images/tutorial/2.0/cube_build_performance/01.png)

基准是：

* 一个 Measure：平衡，总是计算 Max，Min 和 Count
* 所有 Dim_date (10 项) 会被用作 dimensions 
* 输入为 Hive CSV 外部表 
* 输出为 HBase 中未压缩的 Cube 

使用这些配置，结果为：13 分钟 build 一个 20 Mb 的 cube (Cube_01)

### Cube_02：减少组合
第一次提升，在 Dimensions 上使用 Joint 和 Hierarchy 来减少组合 (cuboids 的数量)。

使用月，周，工作日和季度的 Joint Dimension 将所有的 ID 和 Text 组合在一起

![]( /images/tutorial/2.0/cube_build_performance/02.png)

	
定义 Id_date 和 Year 作为 Hierarchy Dimension

这将其大小减至 0.72 MB 而时间减至 5 分钟

[Kylin 2149](https://issues.apache.org/jira/browse/KYLIN-2149)，理想情况下，这些 Hierarchies 也能够这样定义:
* Id_weekday > Id_date
* Id_Month > Id_date
* Id_Quarter > Id_date
* Id_week > Id_date

现在，还不能对同一 dimension 一起使用 Joint 和 Hierarchy。


### Cube_03：输出压缩
下一次提升，使用 Snappy 压缩 HBase Cube：

![alt text](/images/tutorial/2.0/cube_build_performance/03.png)

另一个选项为 Gzip：

![alt text](/images/tutorial/2.0/cube_build_performance/04.png)


压缩输出的结果为：

![alt text](/images/tutorial/2.0/cube_build_performance/05.png)

Snappy 和 Ggzip 的区别在时间上少于 1% 但是在大小上有 18% 差别


### Cube_04：压缩 Hive 表
时间分布如下：

![]( /images/tutorial/2.0/cube_build_performance/06.png)


按概念分组的详细信息 ：

![]( /images/tutorial/2.0/cube_build_performance/07.png)

67 % 用来 build / process flat 表且遵守 30% 用来 build cube

大量时间用在了第一步。

这种时间分布在有很少的 measures 和很少的 dim (或者是非常优化的) 的 cube 中是很典型的 


尝试在 Hive 输入表中使用 ORC 格式和压缩(Snappy)：

![]( /images/tutorial/2.0/cube_build_performance/08.png)


前三步 (Flat Table) 的时间已经提升了一半。

其他列式格式可以被测试：

![]( /images/tutorial/2.0/cube_build_performance/19.png)


* ORC
* 使用 Snappy 的 ORC 压缩

但结果比使用 Sequence 文件的效果差。

请看：[Shaofengshi in MailList](http://apache-kylin.74782.x6.nabble.com/Kylin-Performance-td6713.html#a6767) 关于这个的评论

第二步是重新分配 Flat Hive 表：

![]( /images/tutorial/2.0/cube_build_performance/20.png)

是一个简单的 row count，可以做出两个近似值
* 如果其不需要精确，fact 表的 row 可以被统计→ 这可以与步骤 1 并行执行 (且 99% 的时间将是精确的)

![]( /images/tutorial/2.0/cube_build_performance/21.png)


* 将来的版本中 (KYLIN-2165 v2.0)，这一步将使用 Hive 表数据实现。



### Cube_05：Hive 表 (失败) 分区
Rows 的分布为：

Table | Rows
--- | --- 
Fact Table | 3.900.00 
Dim Date | 2.100 

build flat 表的查询语句 (简单版本)：
{% highlight Groff markup %}
```sql
SELECT
,DIM_DATE.X
,DIM_DATE.y
,FACT_POSICIONES.BALANCE
FROM  FACT_POSICIONES  INNER JOIN DIM_DATE 
	ON  ID_FECHA = .ID_FECHA
WHERE (ID_DATE >= '2016-12-08' AND ID_DATE < '2016-12-23')
```
{% endhighlight %}

这里存在的问题是，Hive 只使用 1 个 Map 创建 Flat 表。重要的是我们要改变这种行为。解决方案是在同一列将 DIM 和 FACT 分区

* 选项 1：在 Hive 表中使用 id_date 作为分区列。这有一个大问题：Hive metastore 意味着几百个分区而不是几千个 (在 [Hive 9452](https://issues.apache.org/jira/browse/HIVE-9452) 中有一个解决该问题的方法但现在还未完成)
* 选项 2：生成一个新列如 Monthslot。

![]( /images/tutorial/2.0/cube_build_performance/09.png)


为 dim 和 fact 表添加同一个列

现在，用这个新的条件 join 表来更新数据模型

![]( /images/tutorial/2.0/cube_build_performance/10.png)

	
生成 flat 表的新查询类似于：
{% highlight Groff markup %}
```sql
SELECT *
	FROM  FACT_POSICIONES  **INNER JOIN** DIM_DATE 
		ON  ID_FECHA = .ID_FECHA    AND  MONTHSLOT=MONTHSLOT
```
{% endhighlight %}

用这个数据模型 rebuild 新 cube

结果，性能更糟了 :(。尝试了几种方法后，还是没找到解决方案

![]( /images/tutorial/2.0/cube_build_performance/11.png)


问题是分区没有被用来生成几个 Mappers

![]( /images/tutorial/2.0/cube_build_performance/12.png)

	
(我和 ShaoFeng Shi 检查了这个问题。他认为问题是这里只有很少的 rows 而且我们不是使用的真实的 Hadoop 集群。请看这个 [tech note](http://kylin.apache.org/docs16/howto/howto_optimize_build.html))。
	

### 结果摘要

![]( /images/tutorial/2.0/cube_build_performance/13.png)


调整进度如下：
* Hive 输入表压缩了
* HBase 输出压缩了
* 应用了 cardinality (Joint，Derived，Hierarchy 和 Mandatory) 减少的技术
* 为每一个 Dim 个性化 Dim 编码器并选择了 Dim 在 Row Key 中最好的顺序



现在，这里有三种类型的 cubes：
* 在 dimensions 中使用低 cardinality 的 Cubes（如 cube 4，大多数时间用在 flat 表这一步）
* 在 dimensions 中使用高 cardinality 的 Cubes（如 cube 6，大多数时间用于 Build cube，flat 表这一步少于 10%）
* 第三种类型，超高 cardinality (UHC) 其超出了本文的范围


### Cube 6：用高 cardinality Dimensions 的 Cube

![]( /images/tutorial/2.0/cube_build_performance/22.png)

在这个用例中 **72%** 的时间用来 build Cube

这一步是 MapReduce 任务，您可以在 ![alt text](/images/tutorial/2.0/cube_build_performance/23.png) > ![alt text](/images/tutorial/2.0/cube_build_performance/24.png) 看 YARN 中关于这一步的日志

Map – Reduce 的性能怎样能提升呢? 简单的方式是增加 Mappers 和 Reduces (等于增加了并行数) 的数量。


![]( /images/tutorial/2.0/cube_build_performance/25.png)


**注意：** YARN / MapReduce 有很多参数配置和适应您的系统。这里的重点只在于小部分。 

(在我的系统中我可以分配 12 – 14 GB 和 8 cores 给 YARN 资源)：

* yarn.nodemanager.resource.memory-mb = 15 GB
* yarn.scheduler.maximum-allocation-mb = 8 GB
* yarn.nodemanager.resource.cpu-vcores = 8 cores
有了这些配置我们并行列表的最大理论级别为 8。然而这里有一个问题：“3600 秒后超时了”

![]( /images/tutorial/2.0/cube_build_performance/26.png)


参数 mapreduce.task.timeout  (默认为 1 小时) 定义了 Application Master (AM) 在没有 ACK of Yarn Container 的情况下发生的最大时间。一旦这次通过了，AM 杀死 container 并重新尝试 4 次 (都是同一个结果)

问题在哪? 问题是 4 个 mappers 启动了，但每一个 mapper 需要超过 4 GB 完成

* 解决方案 1：增加 RAM 给 YARN 
* 解决方案 2：增加在 Mapper 步骤中使用的 vCores 数量来减少 RAM 使用
* 解决方案 3：您可以通过 node 为 YARN 使用最大的 RAM（yarn.nodemanager.resource.memory-mb) 并为每一个 container 使用最小的 RAM 进行实验（yarn.scheduler.minimum-allocation-mb）。如果您为每一个 container 增加了最小的 RAM，YARN 将会减少 Mappers 的数量。

![]( /images/tutorial/2.0/cube_build_performance/27.png)


在最后两个用例中结果是相同的：减少并行化的级别 ==> 
* 现在我们只启动 3 个 mappers 且同时启动，第四个必须等待空闲时间
* 3 个 mappers 将 ram 分散在它们之间，结果它们就会有足够的 ram 完成 task

一个正常的 “Build Cube” 步骤中您将会在 YARN 日志中看到相似的消息：

![]( /images/tutorial/2.0/cube_build_performance/28.png)


如果您没有周期性的看见这个，也许您在内存中遇到了瓶颈。



### Cube 7：提升 cube 响应时间
我们尝试使用不同 aggregations groups 来提升一些非常重要 Dim 或有高 cardinality 的 Dim 的查询性能。

在我们的用例中定义 3 个 Aggregations Groups：
1. “Normal cube”
2. 使用日期 Dim 和 Currency 的 Cube（就像 mandatory)
3. 使用日期 Dim 和 Carteras_Desc 的 Cube（就像 mandatory)

![]( /images/tutorial/2.0/cube_build_performance/29.png)


![]( /images/tutorial/2.0/cube_build_performance/30.png)


![]( /images/tutorial/2.0/cube_build_performance/31.png)



比较未使用 / 使用 AGGs：

![]( /images/tutorial/2.0/cube_build_performance/32.png)


使用多于 3% 的时间 build cube 以及 0.6% 的 space，使用 currency 或 Carteras_Desc 的查询会快很多。




