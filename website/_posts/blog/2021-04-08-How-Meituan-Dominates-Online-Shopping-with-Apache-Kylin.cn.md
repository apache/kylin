---
layout: post-blog
title:  Kylin 在美团到店餐饮的实践和优化
date:   2021-08-03 15:00:00
author: 岳庆
categories: cn_blog
---
从2016年开始，美团到店餐饮技术团队就开始使用Apache Kylin作为OLAP引擎，但是随着业务的高速发展，在构建和查询层面都出现了效率问题。于是，技术团队从原理解读开始，然后对过程进行层层拆解，并制定了由点及面的实施路线。本文总结了一些经验和心得，希望能够帮助业界更多的技术团队提高数据的产出效率。

## 背景

销售业务的特点是规模大、领域多、需求密。美团到店餐饮擎天销售系统（**以下简称“擎天”**）作为销售数据支持的主要载体，不仅涉及的范围较广，而且面临的技术场景也非常复杂（**多组织层级数据展示及鉴权、超过1/3的指标需要精准去重，峰值查询已经达到数万级别**）。在这样的业务背景下，建设稳定高效的OLAP引擎，协助分析人员快速决策，已经成为到餐擎天的核心目标。

Apache Kylin是一个基于Hadoop大数据平台打造的开源OLAP引擎，它采用了多维立方体预计算技术，利用空间换时间的方法，将查询速度提升至亚秒级别，极大地提高了数据分析的效率，并带来了便捷、灵活的查询功能。基于技术与业务匹配度，擎天于2016年采用Kylin作为OLAP引擎，接下来的几年里，这套系统高效地支撑了我们的数据分析体系。

2020年，美团到餐业务发展较快，数据指标也迅速增加。基于Kylin的这套系统，在构建和查询上均出现了严重的效率问题，从而影响到数据的分析决策，并给用户体验优化带来了很大的阻碍。技术团队经过半年左右的时间，对Kylin进行一系列的优化迭代，包括维度裁剪、模型设计以及资源适配等等等，帮助销售业绩数据SLA从90%提升至99.99%。基于这次实战，我们沉淀了一套涵盖了“原理解读”、“过程拆解”、“实施路线”的技术方案。希望这些经验与总结，能够帮助业界更多的技术团队提高数据产出与业务决策的效率。

## 问题与目标

销售作为衔接平台和商家的桥梁，包含销售到店和电话拜访两种业务模式，以战区、人力组织架构逐级管理，所有分析均需要按2套组织层级查看。在指标口径一致、数据产出及时等要求下，我们结合Kylin的预计算思想，进行了数据的架构设计。如下图所示：

![](/images/blog/meituan_cn/chart-01.png)

而Kylin计算维度组合的公式是2^N（**N为维度个数**），官方提供维度剪枝的方式，减少维度组合个数。但由于到餐业务的特殊性，单任务不可裁剪的组合个数仍高达1000+。在需求迭代以及人力、战区组织变动的场景下，需要回溯全部历史数据，会耗费大量的资源以及超高的构建时长。而基于业务划分的架构设计，虽能够极大地保证数据产出的解耦，保证指标口径的一致性，但是对Kylin构建产生了很大的压力，进而导致资源占用大、耗时长。基于以上业务现状，我们归纳了Kylin的MOLAP模式下存在的问题，具体如下：

- **效率问题命中难（实现原理）**：构建过程步骤多，各步骤之间强关联，仅从问题的表象很难发现问题的根本原因，无法行之有效地解决问题。
- **构建引擎未迭代（构建过程）**：历史任务仍采用MapReduce作为构建引擎，没有切换到构建效率更高的Spark。
- **资源利用不合理（构建过程）**：资源浪费、资源等待，默认平台动态资源适配方式，导致小任务申请了大量资源，数据切分不合理，产生了大量的小文件，从而造成资源浪费、大量任务等待。
- **核心任务耗时长（实施路线）**：擎天销售交易业绩数据指标的源表数据量大、维度组合多、膨胀率高，导致每天构建的时长超过2个小时。
- **SLA质量不达标（实施路线）**：SLA的整体达成率未能达到预期目标。

在认真分析完问题，并确定提效的大目标后，我们对Kylin的构建过程进行了分类，拆解出在构建过程中能提升效率的核心环节，通过“原理解读”、“层层拆解”、“由点及面”的手段，达成双向降低的目标。具体量化目标如下图所示：

![](/images/blog/meituan_cn/chart-02.png)

## 优化前提-原理解读

为了解决效率提升定位难、归因难的问题，我们解读了Kylin构建原理，包含了预计算思想以及By-layer逐层算法。

### 预计算

根据维度组合出所有可能的维度，对多维分析可能用到的指标进行预计算，将计算好的结果保存成Cube。假设我们有4个维度，这个Cube中每个节点（**称作Cuboid**）都是这4个维度的不同组合，每个组合定义了一组分析的维度（**如group by**），指标的聚合结果就保存在每个Cuboid上。查询时，我们根据SQL找到对应的Cuboid，读取指标的值，即可返回。如下图所示：

![](/images/blog/meituan_cn/chart-03.png)

### By-layer逐层算法

一个N维的Cube，是由1个N维子立方体、N个（N-1）维子立方体、N*(N-1)/2个(N-2)维子立方体、……N个1维子立方体和1个0维子立方体构成，总共有 2^N个子立方体。在逐层算法中，按照维度数逐层减少来计算，每个层级的计算（除了第一层，由原始数据聚合而来），是基于上一层级的计算结果来计算的。

例如：group by [A,B]的结果，可以基于group by [A,B,C]的结果，通过去掉C后聚合得来的，这样可以减少重复计算，当0维Cuboid计算出来的时候，整个Cube的计算也就完成了。如下图所示：

![](/images/blog/meituan_cn/chart-04.png)

## 过程分析-层层拆解

在了解完Kylin的底层原理后，我们将优化的方向锁定在“引擎选择”、“数据读取”、“构建字典”、“分层构建”、“文件转换”五个环节，再细化各阶段的问题、思路及目标后，我们终于做到了在降低计算资源的同时降低了耗时。详情如下表所示：

![](/images/blog/meituan_cn/chart-05.png)

### 构建引擎选择

目前，我们已经将构建引擎已逐步切换为Spark。擎天早在2016年就使用Kylin作为OLAP引擎，历史任务没有切换，仅仅针对MapReduce做了参数优化。其实在2017年，Kylin官网已启用Spark作为构建引擎（官网启用Spark构建引擎），构建效率相较MapReduce提升1至3倍，还可通过Cube设计选择切换，如下图所示：

![](/images/blog/meituan_cn/chart-06.png)

### 读取源数据

Kylin以外部表的方式读取Hive中的源数据，表中的数据文件（**存储在HDFS**）作为下一个子任务的输入，此过程可能存在小文件问题。当前，Kylin上游数据宽表文件数分布比较合理，无需在上游设置合并，如果强行合并反而会增加上游源表数据加工时间。

对于项目需求，要回刷历史数据或增加维度组合，需要重新构建全部的数据，通常采用按月构建的方式回刷历史，加载的分区过多出现小文件问题，导致此过程执行缓慢。在Kylin级别重写配置文件，对小文件进行合并，减少Map数量，可有效地提升读取效率。

**合并源表小文件**：合并Hive源表中小文件个数，控制每个Job并行的Task个数。调整参数如下表所示：

![](/images/blog/meituan_cn/chart-07.png)

**Kylin级别参数重写**：设置Map读取过程的文件大小。调整参数如下表所示：

![](/images/blog/meituan_cn/chart-08.png)

### 构建字典

Kylin通过计算Hive表出现的维度值，创建维度字典，将维度值映射成编码，并保存保存统计信息，节约HBase存储资源。每一种维度组合，称为一个Cuboid。理论上来说，一个N维的Cube，便有2^N种维度组合。

#### 组合数量查看

在对维度组合剪枝后，实际计算维度组合难以计算，可通过执行日志（**截图为提取事实表唯一列的步骤中，最后一个Reduce的日志**），查看具体的维度组合数量。如下图所示：

![](/images/blog/meituan_cn/chart-09.png)

#### 全局字典依赖

擎天有很多业务场景需要精确去重，当存在多个全局字典列时，可设置列依赖，例如：当同时存在“门店数量”、“在线门店数量”数据指标，可设置列依赖，减少对超高基维度的计算。如下图所示：

![](/images/blog/meituan_cn/chart-10.png)

#### 计算资源配置

当指标中存在多个精准去重指标时，可适当增加计算资源，提升对高基维度构建的效率。参数设置如下表所示：

![](/images/blog/meituan_cn/chart-11.png)

### 分层构建

此过程为Kylin构建的核心，切换Spark引擎后，默认只采用By-layer逐层算法，不再自动选择（By-layer逐层算法、快速算法）。Spark在实现By-layer逐层算法的过程中，从最底层的Cuboid一层一层地向上计算，直到计算出最顶层的Cuboid（相当于执行了一个不带group by的查询），将各层的结果数据缓存到内存中，跳过每次数据的读取过程，直接依赖上层的缓存数据，大大提高了执行效率。Spark执行过程具体内容如下。

#### Job阶段

Job个数为By-layer算法树的层数，Spark将每层结果数据的输出，作为一个Job。如下图所示：

![](/images/blog/meituan_cn/chart-12.png)

#### Stage阶段

每个Job对应两个Stage阶段，分为读取上层缓存数据和缓存该层计算后的结果数据。如下图所示：

![](/images/blog/meituan_cn/chart-13.png)

#### Task并行度设置

Kylin根据预估每层构建Cuboid组合数据的大小（**可通过维度剪枝的方式，减少维度组合的数量，降低Cuboid组合数据的大小，提升构建效率，本文暂不详细介绍**）和分割数据的参数值计算出任务并行度。计算公式如下：

- **Task个数计算公式**：Min(MapSize/cut-mb ，MaxPartition) ；Max(MapSize/cut-mb ，MinPartition)

- - **MapSize**：每层构建的Cuboid组合大小，即：Kylin对各层级维度组合大小的预估值。
  - **cut-mb**：分割数据大小，控制Task任务并行个数，可通过kylin.engine.spark.rdd-partition-cut-mb参数设置。
  - **MaxPartition**：最大分区，可通过kylin.engine.spark.max-partition参数设置。
  - **MinPartition**：最小分区，可通过kylin.engine.spark.min-partition参数设置。

- **输出文件个数计算**：每个Task任务将执行完成后的结果数据压缩，写入HDFS，作为文件转换过程的输入。文件个数即为：Task任务输出文件个数的汇总。

#### 资源申请计算

平台默认采用动态方式申请计算资源，单个Executor的计算能力包含：1个逻辑CPU（以下简称CPU）、6GB堆内内存、1GB的堆外内存。计算公式如下：

- **CPU** =  kylin.engine.spark-conf.spark.executor.cores * 实际申请的Executors个数。
- **内存** =（kylin.engine.spark-conf.spark.executor.memory + spark.yarn.executor.memoryOverhead）* 实际申请的Executors个数。
- **单个Executor的执行能力** = kylin.engine.spark-conf.spark.executor.memory / kylin.engine.spark-conf.spark.executor.cores，即：1个CPU执行过程中申请的内存大小。
- **最大Executors个数** = kylin.engine.spark-conf.spark.dynamicAllocation.maxExecutors，平台默认动态申请，该参数限制最大申请个数。

在资源充足的情况下，若单个Stage阶段申请1000个并行任务，则需要申请资源达到7000GB内存和1000个CPU，即：`CPU：1*1000=1000；内存：（6+1）*1000=7000GB`。

#### 资源合理化适配

由于By-layer逐层算法的特性，以及Spark在实际执行过程中的压缩机制，实际执行的Task任务加载的分区数据远远小于参数设置值，从而导致任务超高并行，占用大量资源，同时产生大量的小文件，影响下游文件转换过程。因此，合理的切分数据成为优化的关键点。通过Kylin构建日志，可查看各层级的Cuboid组合数据的预估大小，以及切分的分区个数（等于Stage阶段实际生成的Task个数）。如下图所示：

![](/images/blog/meituan_cn/chart-14.png)

结合Spark UI可查看实执行情况，调整内存的申请，满足执行所需要的资源即可，减少资源浪费。

1. 整体资源申请最小值大于Stage阶段Top1、Top2层级的缓存数据之和，保证缓存数据全部在内存。如下图所示：

![](/images/blog/meituan_cn/chart-15.png)

**计算公式**：Stage阶段Top1、Top2层级的缓存数据之和 < kylin.engine.spark-conf.spark.executor.memory * kylin.engine.spark-conf.spark.memory.fraction *  spark.memory.storageFraction *最大Executors个数

2. 单个Task实际所需要的内存和CPU（**1个Task执行使用1个CPU**）小于单个Executor的执行能力。如下图所示：

![](/images/blog/meituan_cn/chart-16.png)

**计算公式**：单个Task实际所需要的内存 < kylin.engine.spark-conf.spark.executor.memory * kylin.engine.spark-conf.spark.memory.fraction *  spark.memory.st·orageFraction / kylin.engine.spark-conf.spark.executor.cores。参数说明如下表所示：

![](/images/blog/meituan_cn/chart-17.png)

### 文件转换

Kylin将构建之后的Cuboid文件转换成HTable格式的Hfile文件，通过BulkLoad的方式将文件和HTable进行关联，大大降低了HBase的负载。此过程通过一个MapReduce任务完成，Map个数为分层构建阶段输出文件个数。日志如下：

![](/images/blog/meituan_cn/chart-18.png)

此阶段可根据实际输入的数据文件大小（**可通过MapReduce日志查看**），合理申请计算资源，避免资源浪费。

计算公式：Map阶段资源申请 = kylin.job.mr.config.override.mapreduce.map.memory.mb * 分层构建阶段输出文件个数。具体参数如下表所示：

![](/images/blog/meituan_cn/chart-19.png)

## 实施路线-由点及面

### 交易试点实践

我们通过对Kylin原理的解读以及构建过程的层层拆解，选取销售交易核心任务进行试点实践。如下图所示：

![](/images/blog/meituan_cn/chart-20.png)

### 实践结果对比

针对销售交易核心任务进行实践优化，对比调整前后资源实际使用情况和执行时长，最终达到双向降低的目标。如下图所示：

![](/images/blog/meituan_cn/chart-21.png)

## 成果展示

### 资源整体情况

擎天现有20+的Kylin任务，经过半年时间持续优化迭代，对比Kylin资源队列月均CU使用量和Pending任务CU使用量，在同等任务下资源消耗已明显降低。如下图所示：

![](/images/blog/meituan_cn/chart-23.png)

### SLA整体达成率

经过了由点及面的整体优化，擎天于2020年6月SLA达成率达到100%。如下图所示：

![](/images/blog/meituan_cn/chart-24.png)

## 展望

Apache Kylin在2015年11月正式成为Apache基金会的顶级项目。从开源到成为Apache顶级项目，只花了13个月的时间，而且它也是第一个由中国团队完整贡献到Apache的顶级项目。

目前，美团采用比较稳定的V2.0版本，经过近4年的使用与积累，到店餐饮技术团队在优化查询性能以及构建效率层面都积累了大量经验，本文主要阐述了在Spark构建过程的资源适配方法。值得一提的是，Kylin官方在2020年7月发布了V3.1版本，引入了Flink作为构建引擎，统一使用Flink构建核心过程，包含数据读取阶段、构建字典阶段、分层构建阶段、文件转换阶段，以上四部分占整体构建耗时的95%以上。此次版本的升级也大幅度提高了Kylin的构建效率。详情可查看：Flink Cube Build Engine。

回顾Kylin构建引擎的升级过程，从MapReduce到Spark，再到如今的Flink，构建工具的迭代始终向更加优秀的主流引擎在靠拢，而且Kylin社区有很多活跃的优秀代码贡献者，他们也在帮助扩大Kylin的生态，增加更多的新功能，非常值得大家学习。最后，美团到店餐饮技术团队再次表达对Apache Kylin项目团队的感谢。