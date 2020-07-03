---
layout: docs
title:  Use Hive to build global dictionary
categories: howto
permalink: /docs/howto/howto_use_hive_mr_dict.html
---

## Global Dictionary in Hive

### Background

- Count distinct(bitmap) measure is very important for many scenario, such as PageView statistics, and Kylin support count distinct since 1.5.3 .
- Apache Kylin implements precisely count distinct measure based on bitmap, and use global dictionary to encode string value into integer.
- Currently we have to build global dictionary in single process/JVM, which may take a lot of time and memory for UHC.
- Kylin v3.0.0 introduce Hive global dictionary v1(KYLIN-3841). By this feature, we use Hive, a distributed SQL engine to build global dictionary.
- For improve performance, kylin v3.1.0 use MapReduce replace HQL in some steps, introduce Hive global dictionary v2(KYLIN-4342).

### Benefit Summary
1.Build Global Dictionary in distributed way, thus building job spent less time.
2.Job Server will do less job, thus be more stable. 
3.OneID, since the fact that Hive Global Dictionary is human-readable outside of Kylin, everyone can reuse this dictionary(Hive table) in the other scene across the company.

## How to use

If you have some count distinct(bitmap) measure, and data type of that column is String, you may need Hive Global Dictionary. Says columns name are PV_ID and USER_ID, and table name is USER_ACTION, you may add cube-level configuration `kylin.dictionary.mr-hive.columns=USER_ACTION_PV_ID,USER_ACTION_USER_ID` to enable this feature.

Please don't use hive global dictionary on integer type column, you have to know that the value will be replaced with encoded integer in flat hive table. If you have sum/max/min measure on the same column, you will get wrong result in these measures.

And you should know this feature is conflicted with shrunken global dictionary(KYLIN-3491) because they fix the same thing in different way.

### Configuration

- `kylin.dictionary.mr-hive.columns` is used to specific which columns need to use Hive-MR dict, should be *TABLE1_COLUMN1,TABLE2_COLUMN2*. Better configured in cube level, default value is empty.
- `kylin.dictionary.mr-hive.database` is used to specific which database Hive-MR dict table located, default value is *default*.
- `kylin.hive.union.style` Sometime sql which used to build global dict table may have problem in union syntax, you may refer to Hive Doc for more detail. The default value is *UNION*, using lower version of Hive should change to *UNION ALL*.
- `kylin.dictionary.mr-hive.table.suffix` is used to specific suffix of global dict table, default value is *_global_dict*.
- `kylin.dictionary.mr-hive.intermediate.table.suffix` is used to specific suffix for distinct value table, default value is *_group_by*.
- `kylin.dictionary.mr-hive.columns.reduce.num` A key/value structure(or a map), which key is {TABLE_NAME}_{COLUMN_NAME}, and value is number for expected reducers in Build Segment Level Dictionary (MR job Parallel Part Build).
- `kylin.dictionary.mr-hive.ref.columns` To reuse other global dictionary(s), you can specific a list here, to refer to some existent global dictionary(s) built by another cube.
- `kylin.source.hive.databasedir` The location of Hive table in HDFS.

----

## Step

#### Add count_distinct(bitmap) measure

![add_count_distinct_bitmap](/images/Hive-Global-Dictionary/add-count-distinct.png)

#### Set hive-dict-column in cube level config

![set-hive-dict-column](/images/Hive-Global-Dictionary/set-hive-dict-cloumn.png)

#### Build new segment

![three-added-steps](/images/Hive-Global-Dictionary/new-added-step-1.png)

![three-added-steps](/images/Hive-Global-Dictionary/new-added-step-2.png)

More detail about this feature please refer [Apache Kylin Wiki](https://cwiki.apache.org/confluence/display/KYLIN/Introduction+to+Hive+Global+Dictionary)

### Reference Link

- https://issues.apache.org/jira/browse/KYLIN-3491
- https://issues.apache.org/jira/browse/KYLIN-3841
- https://issues.apache.org/jira/browse/KYLIN-3905
- https://issues.apache.org/jira/browse/KYLIN-4342
- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Union
- https://kylin.apache.org/blog/2016/08/01/count-distinct-in-kylin/
- https://cwiki.apache.org/confluence/display/KYLIN/Introduction+to+Hive+Global+Dictionary