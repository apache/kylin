---
layout: docs31-cn
title:  使用Hive构建全局字典
categories: howto
permalink: /cn/docs31/howto/howto_use_hive_mr_dict.html
---

## Global Dictionary in Hive

### 背景介绍
Count distinct(bitmap) 度量对于许多场景来说都非常重要, 比如统计点击量, kylin从1.5.3版本开始支持精确去重.
Apache Kylin 实现了基于bitmap的精确去重, 并且使用全局字典将字符串类型编码为整数类型。
当前的全局字典是单线程构建的，对于高基列可能会占用大量的时间和内存。
Kylin v3.0.0 引入了第一版的 Hive global dictionary(KYLIN-3841). 这个功能使用Hive的分布式SQL引擎来构建全局字典。
为了进一步提升性能, kylin v3.1.0 引入了第二版的Hive global dictionary v2(KYLIN-4342), 这个版本在某些步骤使用MapReduce代替HQL进行全局字典的构建。

### 收益总结
1.使用分布式的方式来构建全局字典，节省时间。
2.Kylin集群中的Job Server可以做更少的工作, 因此会更加稳定。
3.OneID, Hive Global Dictionary在kylin之外仍然具有可读性，因此每个人都可以在公司其他场景中重用这个字典。

## 如何使用
如果你有一些count distinct(bitmap)的度量，并且该列的数据类型是string，你可以使用Hive Global Dictionary。
比如，如果列名为PV_ID和USER_ID，表名为USER_ACTION，则可以在cube级别添加配置`kylin.dictionary.mr-hive.columns=USER_ACTION_PV_ID,USER_ACTION_USER_ID`以启用这个功能。
请不要在integer类型的列使用Hive Global Dictionary，因为在falt table中这种列会被经过编码的整数类型替换，这样如果在同一列上有sum/max/min这些度量，聚合结果将会不正确。
Hive Global Dictionary功能与shrunken global dictionary(KYLIN-3491)是冲突的因为它们解决的是同一个问题，但是解决方式不同。

### 相关配置项

- `kylin.dictionary.mr-hive.columns` is used to specific which columns need to use Hive-MR dict, should be *TABLE1_COLUMN1,TABLE2_COLUMN2*. Better configured in cube level, default value is empty.
- `kylin.dictionary.mr-hive.database` is used to specific which database Hive-MR dict table located, default value is *default*.
- `kylin.hive.union.style` Sometime sql which used to build global dict table may have problem in union syntax, you may refer to Hive Doc for more detail. The default value is *UNION*, using lower version of Hive should change to *UNION ALL*.
- `kylin.dictionary.mr-hive.table.suffix` is used to specific suffix of global dict table, default value is *_global_dict*.
- `kylin.dictionary.mr-hive.intermediate.table.suffix` is used to specific suffix for distinct value table, default value is *_group_by*.
- `kylin.dictionary.mr-hive.columns.reduce.num` A key/value structure(or a map), which key is {TABLE_NAME}_{COLUMN_NAME}, and value is number for expected reducers in Build Segment Level Dictionary (MR job Parallel Part Build).
- `kylin.dictionary.mr-hive.ref.columns` To reuse other global dictionary(s), you can specific a list here, to refer to some existent global dictionary(s) built by another cube.

----

### Step by Step

#### 添加精确去重度量

![add_count_distinct_bitmap](/images/Hive-Global-Dictionary/add-count-distinct.png)

#### 在cube级别配置hive字典列

![set-hive-dict-column](/images/Hive-Global-Dictionary/set-hive-dict-cloumn.png)

#### 构建新的segment

![three-added-steps](/images/Hive-Global-Dictionary/new-added-step-1.png)

![three-added-steps](/images/Hive-Global-Dictionary/new-added-step-2.png)

关于这个功能的更多细节请参考 [Apache Kylin Wiki](https://cwiki.apache.org/confluence/display/KYLIN/Introduction+to+Hive+Global+Dictionary)

### 参考链接

- https://issues.apache.org/jira/browse/KYLIN-3491
- https://issues.apache.org/jira/browse/KYLIN-3841
- https://issues.apache.org/jira/browse/KYLIN-3905
- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Union
- http://kylin.apache.org/blog/2016/08/01/count-distinct-in-kylin/
- https://cwiki.apache.org/confluence/display/KYLIN/Introduction+to+Hive+Global+Dictionary