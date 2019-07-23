---
layout: docs30
title:  Use Hive to build global dictionary
categories: howto
permalink: /docs30/howto/howto_use_hive_mr_dict.html
---

### Global Dictionary 
Count distinct measure is very important for many scenario, such as PageView statistics, Kylin support count distinct since 1.5.3 (http://kylin.apache.org/blog/2016/08/01/count-distinct-in-kylin/). 
Apache Kylin implements precisely count distinct based on bitmap, and use global dictionary to encode string value into a Dict. 
Currently we have to build global dictionary in single process/JVM, which may take a lot of time and memory for UHC. By in this feature(KYLIN-3841), we use Hive, a distributed SQL engine to build global dictionary.

This will help to:
1. Reduce memory pressure of Kylin process, MapReduce will be used to build dict for Kylin
2. Make global dictionary reusable
3. Make global dictionary readable, you may use global dictionary outside Kylin, maybe useful in many scenario.

And this feature will add three steps if enabled.
1. Global Dict Mr/Hive extract dict_val from Data
2. Global Dict Mr/Hive build dict_val
3. Global Dict Mr/Hive replace dict_val to Data

### How to use
If you have a count distinct(bitmap) measure for a UHC. Says columns name are PV_ID and USER_ID, and table name is USER_ACTION, you may add cube-level configuration `kylin.dictionary.mr-hive.columns=USER_ACTION_PV_ID,USER_ACTION_USER_ID` to enable this feature.
You have to know that the value will be replaced into encoded integer in flat hive table, and this may cause failure of some query.

### Configuration
- `kylin.dictionary.mr-hive.columns` is used to specific which columns need to be Hive-MR dict.
- `kylin.dictionary.mr-hive.database` is used to specific which database Hive-MR dict located.
- `kylin.hive.union.style` Sometime sql which used to build global dict table may have syntax problem. This should be fixed by specific this entry with *UNION ALL*.
- `kylin.dictionary.mr-hive.table.suffix` is used to specific suffix of global dict table.