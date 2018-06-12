---
layout: docs23-cn
title:  Kylin Cube 创建教程
categories: 教程
permalink: /cn/docs23/tutorial/create_cube.html
version: v1.2
since: v0.7.1
---
  
  
### I. 新建一个项目
1. 由顶部菜单栏进入 `Model` 页面，然后点击 `Manage Projects`。

   ![](/images/Kylin-Cube-Creation-Tutorial/1 manage-prject.png)

2. 点击 `+ Project` 按钮添加一个新的项目或者忽略第一步并且点击 `Add Project`（第一张图片中右边的按钮）。

   ![](/images/Kylin-Cube-Creation-Tutorial/2 %2Bproject.png)

3. 填写下列表单并点击 `submit` 按钮提交请求。

   ![](/images/Kylin-Cube-Creation-Tutorial/3 new-project.png)

4. 成功后，底部会显示通知。

   ![](/images/Kylin-Cube-Creation-Tutorial/3.1 pj-created.png)

### II. 同步一张表
1. 在顶部菜单栏点击 `Model`，然后点击左边的 `Data Source` 标签，它会列出所有加载进Kylin的表，点击 `Load Table` 按钮。

   ![](/images/Kylin-Cube-Creation-Tutorial/4 %2Btable.png)

2. 输入表名并点击 `Sync` 按钮提交请求。

   ![](/images/Kylin-Cube-Creation-Tutorial/5 hive-table.png)

3. 【可选】如果你想要浏览hive数据库来选择表，点击 `Load Table From Tree` 按钮。

   ![](/images/Kylin-Cube-Creation-Tutorial/5 hive-table.png)

4. 【可选】展开数据库节点，点击选择要加载的表，然后点击 `Sync` 按钮。

   ![](/images/Kylin-Cube-Creation-Tutorial/5 hive-table.png)

5. 成功的消息将会弹出，在左边的 `Tables` 部分，新加载的表已经被添加进来。点击表将会展开列。

   ![](/images/Kylin-Cube-Creation-Tutorial/5 hive-table.png)

6. 在后台，Kylin 将会执行 MapReduce 任务计算新 sync 表的 cardinality，任务完成后，刷新页面并点击表名，cardinality的值将会显示在表信息中。

   ![](/images/Kylin-Cube-Creation-Tutorial/5 hive-table.png)

### III. 新建一个 Data Model
创建 cube 前，需定义一个数据模型。数据模型定义为星形模型。一个模型可以被多个 cube 使用。

![](/images/Kylin-Cube-Creation-Tutorial/6 %2Bcube.png)

1. 点击顶部的 `Model` ，然后点击 `Models` 标签。点击 `+New` 按钮，在下拉框中选择 `New Model`。

2. 输入 model 的名字和可选的 description。

![](/images/Kylin-Cube-Creation-Tutorial/7 cube-info.png)

3. 在 `Fact Table` 中，为模型选择事实表。

    ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-factable.png)

4. 【可选】点击 `Add Lookup Table` 按钮添加一个 lookup 表。选择表名和 join 类型（内连接或左连接）

    ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-%2Bdim.png)

5. 【可选】点击 `New Join Condition` 按钮，左边选择事实表的外键，右边选择 lookup 表的主键。如果有多于一个 join 列重复执行。

    ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-typeA.png)

6. 点击 “OK”，重复4，5步来添加更多的 lookup 表。完成后，点击 “Next”。
   ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-edit.png)

7. `Dimensions` 页面允许选择在子 cube 中用作维度的列，然后点击 `Columns` 列，在下拉框中选择需要的列。

8. 点击 “Next” 到达 “Measures” 页面，选择作为 measure 的列，其只能从事实表中选择。

![](/images/Kylin-Cube-Creation-Tutorial/7 cube-info.png)

9. 点击 “Next” 到达 “Settings” 页面，如果事实表中的数据每日增长，选择 `Partition Date Column` 中相应的 date 列以及 date 格式，否则就将其留白。

    ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-factable.png)

10. 【可选】选择是否需要 “time of the day” 列，默认情况下为 `No`。如果选择 `Yes`, 选择 `Partition Time Column` 中相应的 time 列以及 time 格式

    ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-%2Bdim.png)

11. 【可选】如果一些 records 想从 cube 中移除出去，就像脏数据，可以在 `Filter` 中输入条件。

    ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-typeA.png)

12. 点击 `Save` 然后选择 `Yes` 来保存 data model。创建完成，data model 就会列在左边 `Models` 列表中。
   ![](/images/Kylin-Cube-Creation-Tutorial/8 dim-edit.png)

### III. 新建一个 Cube

创建完 data model，可以开始创建 cube。
点击顶部 `Model`，然后点击 `Models` 标签。点击 `+New` 按钮，在下拉框中选择 `New Cube`。

**步骤1. Cube 信息**

1. 选择 data model，输入 cube 名字；点击 `Next` 进行下一步。

cube 名字可以使用字母，数字和下划线（空格也是允许的）。`Notification Email List` 是运用来通知job执行成功或失败情况的邮箱列表。`Notification Events` 是触发事件的状态。

   ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-%2Bmeas.png)

**步骤2. 维度**

1. 点击 `Add Dimension`，在弹窗中显示的事实表和lookup表里勾选输入需要的列。Lookup 表的列有2个选项：“Normal” 和 “Derived”（默认）。“Normal” 添加一个 normal 独立的维度列，“Derived” 添加一个 derived 维度。阅读更多【如何优化 cube】（/docs15/howto/howto_optimize_cubes.html）。

2. 选择所有维度后点击 “Next”。

**步骤3. 度量**

1. 点击 `+Measure` 按钮添加一个新的度量。

   ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-%2Bmeas.png)

2. 根据它的表达式共有8种不同类型的度量：`SUM`、`MAX`、`MIN`、`COUNT`、`COUNT_DISTINCT` `TOP_N`, `EXTENDED_COLUMN` 和 `PERCENTILE`。请合理选择 `COUNT_DISTINCT` 和 `TOP_N` 返回类型，它与 cube 的大小相关。
   * SUM

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-sum.png)

   * MIN

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-min.png)

   * MAX

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-max.png)

   * COUNT

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-count.png)

   * DISTINCT_COUNT
   这个度量有两个实现：
   1）近似实现 HyperLogLog，选择可接受的错误率，低错误率需要更多存储；
   2）精确实现 bitmap（具体限制请看 https://issues.apache.org/jira/browse/KYLIN-1186）

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-distinct.png)
    
    注意：distinct 是一种非常重的数据类型，和其他度量相比构建和查询会更慢。
    
   * TOP_N
   TopN 度量在每个维度结合时预计算，它比未预计算的在查询时间上性能更好；需要两个参数：一是被用来作为 Top 记录的度量列；二是 literal ID，代表记录就像 seller_id；
   
   合理的选择返回类型，将决定多少 top 记录被监察：top 10, top 100, top 500, top 1000, top 5000 or top 10000。

     ![](/images/Kylin-Cube-Creation-Tutorial/9 meas-distinct.png)

   * EXTENDED_COLUMN
   Extended_Column 作为度量比作为维度更节省空间。一列和零一列可以生成新的列。

   * PERCENTILE
   Percentile 代表了百分比。值越大，错误就越少。100为最合适的值。

     ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 measure-percentile.png)

**步骤4. 更新设置**

这一步骤是为增量构建 cube 而设计的。

`Auto Merge Thresholds`: 自动合并小的 segments 到中等甚至更大的 segment。如果不想自动合并，删除默认2个选项。

`Volatile Range`: 默认为0，会自动合并所有可能的 cube segments，或者用 'Auto Merge' 将不会合并最新的 [Volatile Range] 天的 cube segments。

`Retention Threshold`: 只会保存 cube 过去几天的 segment，旧的 segment 将会自动从头部删除；0表示不启用这个功能。

`Partition Start Date`: cube 的开始日期.

![](/images/Kylin-Cube-Creation-Tutorial/11 refresh-setting2.png)

**步骤5. 高级设置**

`Aggregation Groups`: 默认 kylin 会把所有维度放在一个聚合组；如果你很好的了解你的查询模式，那么你可以创建多个聚合组。对于 "Mandatory Dimensions", "Hierarchy Dimensions" 和 "Joint Dimensions", 请阅读这个博客: [新的聚合组](/blog/2016/02/18/new-aggregation-group/)

`Rowkeys`: 是由维度编码值组成。"Dictionary" 是默认的编码方式; 如果维度和字典不符合（比如 cardinality > 1千万), 选择 "false" 然后为维度输入合适的长度，通常是那列的最大值; 如果超过最大值，他将会被截断。请注意，如果没有字典编码，cube 的大小将会变的非常大。

你可以拖拽维度列去调整其在 rowkey 中位置; 将 mandantory 维度放在开头, 然后是在过滤 ( where 条件)中起到很大作用的维度. 将高 cardinality 的维度放在低 cardinality 的维度前.

`Mandatory Cuboids`: 确保你想要构建的 cuboid 能顺利构建。

`Cube Engine`: cube 构建引擎。有两种类型：MapReduce and Spark。

`Advanced Dictionaries`: "Global Dictionary" 是准确计算不同度量的默认字典, 支持在所有 segments 汇总。

"Segment Dictionary" 是准确计算不同度量的专用字典, 是基于一个 segment 的并且不支持在所有 segments 汇总。特别地，如果你的 cube 不是分区的或者你能保证你的所有 SQL 按照 partition_column 进行 group by, 那么你应该使用 "Segment Dictionary" 而不是 "Global Dictionary"。

`Advanced Snapshot Table`: 为全局 lookup 表而设计，提供不同的存储类型。

`Advanced ColumnFamily`: 如果有超过一个超高的 cardinality 精确的计算不同的度量, 你可以将它们放在更多列簇中。

**步骤6. 重写配置**

Cube 级别的属性将会覆盖 kylin.properties 中的配置, 如果你没有要配置的，点击 `Next` 按钮。

![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/10 configuration.png)

**步骤7. 概览 & 保存**

你可以概览你的 cube 并返回之前的步骤进行修改。点击 `Save` 按钮完成 cube 创建。

![](/images/Kylin-Cube-Creation-Tutorial/13 overview.png)

恭喜，cube 创建好了，你可以去构建和玩它了。
