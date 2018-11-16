---
layout: docs
title:  Kylin Release 1.5.2
categories: releases
permalink: /docs/releases/kylin_release_1_5_2.html
---

_Tag:_ [kylin-1.5.2](https://github.com/apache/kylin/tree/kylin-1.5.2)


This version is backward compatible with v1.5.1. But after upgrade to v1.5.2 from v1.5.1, you need to update coprocessor, refer to [How to update coprocessor](/docs15/howto/howto_update_coprocessor.html).



### Highlights

* [KYLIN-1077] - Support Hive View as Lookup Table
* [KYLIN-1515] - Make Kylin run on MapR
* [KYLIN-1600] - Download diagnosis zip from GUI
* [KYLIN-1672] - support kylin on cdh 5.7



### New Feature

* [KYLIN-1016] - Count distinct on any dimension should work even not a predefined measure
* [KYLIN-1077] - Support Hive View as Lookup Table
* [KYLIN-1441] - Display time column as partition column
* [KYLIN-1515] - Make Kylin run on MapR
* [KYLIN-1600] - Download diagnosis zip from GUI
* [KYLIN-1672] - support kylin on cdh 5.7



### Improvement

* [KYLIN-869] - Enhance mail notification
* [KYLIN-955] - HiveColumnCardinalityJob should use configurations in conf/kylin_job_conf.xml
* [KYLIN-1313] - Enable deriving dimensions on non PK/FK
* [KYLIN-1323] - Improve performance of converting data to hfile
* [KYLIN-1340] - Tools to extract all cube/hybrid/project related metadata to facilitate diagnosing/debugging/* sharing
* [KYLIN-1381] - change RealizationCapacity from three profiles to specific numbers
* [KYLIN-1391] - quicker and better response to v2 storage engine's rpc timeout exception
* [KYLIN-1418] - Memory hungry cube should select LAYER and INMEM cubing smartly
* [KYLIN-1432] - For GUI, to add one option "yyyy-MM-dd HH:MM:ss" for Partition Date Column
* [KYLIN-1453] - cuboid sharding based on specific column
* [KYLIN-1487] - attach a hyperlink to introduce new aggregation group
* [KYLIN-1526] - Move query cache back to query controller level
* [KYLIN-1542] - Hfile owner is not hbase
* [KYLIN-1544] - Make hbase encoding and block size configurable just like hbase compression
* [KYLIN-1561] - Refactor storage engine(v2) to be extension friendly
* [KYLIN-1566] - Add and use a separate kylin_job_conf.xml for in-mem cubing
* [KYLIN-1567] - Front-end work for KYLIN-1557
* [KYLIN-1578] - Coprocessor thread voluntarily stop itself when it reaches timeout
* [KYLIN-1579] - IT preparation classes like BuildCubeWithEngine should exit with status code upon build * exception
* [KYLIN-1580] - Use 1 byte instead of 8 bytes as column indicator in fact distinct MR job
* [KYLIN-1584] - Specify region cut size in cubedesc and leave the RealizationCapacity in model as a hint
* [KYLIN-1585] - make MAX_HBASE_FUZZY_KEYS in GTScanRangePlanner configurable
* [KYLIN-1587] - show cube level configuration overwrites properties in CubeDesigner
* [KYLIN-1591] - enabling different block size setting for small column families
* [KYLIN-1599] - Add "isShardBy" flag in rowkey panel
* [KYLIN-1601] - Need not to shrink scan cache when hbase rows can be large
* [KYLIN-1602] - User could dump hbase usage for diagnosis
* [KYLIN-1614] - Bring more information in diagnosis tool
* [KYLIN-1621] - Use deflate level 1 to enable compression "on the fly"
* [KYLIN-1623] - Make the hll precision for data samping configurable
* [KYLIN-1624] - HyperLogLogPlusCounter will become inaccurate when there're billions of entries
* [KYLIN-1625] - GC log overwrites old one after restart Kylin service
* [KYLIN-1627] - add backdoor toggle to dump binary cube storage response for further analysis
* [KYLIN-1731] - allow non-admin user to edit 'Advenced Setting' step in CubeDesigner



### Bug fix

* [KYLIN-989] - column width is too narrow for timestamp field
* [KYLIN-1197] - cube data not updated after purge
* [KYLIN-1305] - Can not get more than one system admin email in config
* [KYLIN-1551] - Should check and ensure TopN measure has two parameters specified
* [KYLIN-1563] - Unsafe check of initiated in HybridInstance#init()
* [KYLIN-1569] - Select any column when adding a custom aggregation in GUI
* [KYLIN-1574] - Unclosed ResultSet in QueryService#getMetadata()
* [KYLIN-1581] - NPE in Job engine when execute MR job
* [KYLIN-1593] - Agg group info will be blank when trying to edit cube
* [KYLIN-1595] - columns in metric could also be in filter/groupby
* [KYLIN-1596] - UT fail, due to String encoding CharsetEncoder mismatch
* [KYLIN-1598] - cannot run complete UT at windows dev machine
* [KYLIN-1604] - Concurrent write issue on hdfs when deploy coprocessor
* [KYLIN-1612] - Cube is ready but insight tables not result
* [KYLIN-1615] - UT 'HiveCmdBuilderTest' fail on 'testBeeline'
* [KYLIN-1619] - Can't find any realization coursed by Top-N measure
* [KYLIN-1622] - sql not executed and report topN error
* [KYLIN-1631] - Web UI of TopN, "group by" column couldn't be a dimension column
* [KYLIN-1634] - Unclosed OutputStream in SSHClient#scpFileToLocal()
* [KYLIN-1637] - Sample cube build error
* [KYLIN-1638] - Unclosed HBaseAdmin in ToolUtil#getHBaseMetaStoreId()
* [KYLIN-1639] - Wrong logging of JobID in MapReduceExecutable.java
* [KYLIN-1643] - Kylin's hll counter count "NULL" as a value
* [KYLIN-1647] - Purge a cube, and then build again, the start date is not updated
* [KYLIN-1650] - java.io.IOException: Filesystem closed - in Cube Build Step 2 (MapR)
* [KYLIN-1655] - function name 'getKylinPropertiesAsInputSteam' misspelt
* [KYLIN-1660] - Streaming/Kafka config not match with table name
* [KYLIN-1662] - tableName got truncated during request mapping for /tables/tableName
* [KYLIN-1666] - Should check project selection before add a stream table
* [KYLIN-1667] - Streaming table name should allow enter "DB.TABLE" format
* [KYLIN-1673] - make sure metadata in 1.5.2 compatible with 1.5.1
* [KYLIN-1678] - MetaData clean just clean FINISHED and DISCARD jobs,but job correct status is SUCCEED
* [KYLIN-1685] - error happens while execute a sql contains '?' using Statement
* [KYLIN-1688] - Illegal char on result dataset table
* [KYLIN-1721] - KylinConfigExt lost base properties when store into file
* [KYLIN-1722] - IntegerDimEnc serialization exception inside coprocessor
