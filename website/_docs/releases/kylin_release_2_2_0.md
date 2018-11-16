---
layout: docs
title:  Kylin Release 2.2.0
categories: releases
permalink: /docs/releases/kylin_release_2_2_0.html
---

_Tag:_ [kylin-2.2.0](https://github.com/apache/kylin/tree/kylin-2.2.0)

This is a major release after 2.1, with more than 70 bug fixes and enhancements. 



### New Feature

* [KYLIN-2703] - Manage ACL through Apache Ranger
* [KYLIN-2752] - Make HTable name prefix configurable
* [KYLIN-2761] - Table Level ACL
* [KYLIN-2775] - Streaming Cube Sample



### Improvement

* [KYLIN-2535] - Use ResourceStore to manage ACL files
* [KYLIN-2604] - Use global dict as the default encoding for precise distinct count in web
* [KYLIN-2606] - Only return counter for precise count_distinct if query is exactAggregate
* [KYLIN-2622] - AppendTrieDictionary support not global
* [KYLIN-2623] - Move output(Hbase) related code from MR engine to outputside
* [KYLIN-2653] - Spark Cubing read metadata from HDFS
* [KYLIN-2717] - Move concept Table under Project
* [KYLIN-2790] - Add an extending point to support other types of column family
* [KYLIN-2795] - Improve REST API document, add get/list jobs
* [KYLIN-2803] - Pushdown non "select" query
* [KYLIN-2818] - Refactor dateRange & sourceOffset on CubeSegment
* [KYLIN-2819] - Add "kylin.env.zookeeper-base-path" for zk path
* [KYLIN-2823] - Trim TupleFilter after dictionary-based filter optimization
* [KYLIN-2844] - Override "max-visit-scanrange" and "max-fuzzykey-scan" at cube level
* [KYLIN-2854] - Remove duplicated controllers
* [KYLIN-2856] - Log pushdown query as a kind of BadQuery
* [KYLIN-2857] - MR configuration should be overwritten by user specified parameters when resuming MR jobs
* [KYLIN-2858] - Add retry in cache sync
* [KYLIN-2879] - Upgrade Spring & Spring Security to fix potential vulnerability
* [KYLIN-2891] - Upgrade Tomcat to 7.0.82.
* [KYLIN-2963] - Remove Beta for Spark Cubing



### Bug fix

* [KYLIN-1794] - Enable job list even some job metadata parsing failed
* [KYLIN-2600] - Incorrectly set the range start when filtering by the minimum value
* [KYLIN-2705] - Allow removing model's "partition_date_column" on web
* [KYLIN-2706] - Fix the bug for the comparator in SortedIteratorMergerWithLimit
* [KYLIN-2707] - Fix NPE in JobInfoConverter
* [KYLIN-2716] - Non-thread-safe WeakHashMap leading to high CPU
* [KYLIN-2718] - Overflow when calculating combination amount based on static rules
* [KYLIN-2753] - Job duration may become negative
* [KYLIN-2766] - Kylin uses default FS to put the coprocessor jar, instead of the working dir
* [KYLIN-2773] - Should not push down join condition related columns are compatible while not consistent
* [KYLIN-2781] - Make 'find-hadoop-conf-dir.sh' executable
* [KYLIN-2786] - Miss "org.apache.kylin.source.kafka.DateTimeParser"
* [KYLIN-2788] - HFile is not written to S3
* [KYLIN-2789] - Cube's last build time is wrong
* [KYLIN-2791] - Fix bug in readLong function in BytesUtil
* [KYLIN-2798] - Can't rearrange the order of rowkey columns though web UI
* [KYLIN-2799] - Building cube with percentile measure encounter with NullPointerException
* [KYLIN-2800] - All dictionaries should be built based on the flat hive table
* [KYLIN-2806] - Empty results from JDBC with Date filter in prepareStatement
* [KYLIN-2812] - Save to wrong database when loading Kafka Topic
* [KYLIN-2814] - HTTP connection may not be released in RestClient
* [KYLIN-2815] - Empty results with prepareStatement but OK with KylinStatement
* [KYLIN-2824] - Parse Boolean type in JDBC driver
* [KYLIN-2832] - Table meta missing from system diagnosis
* [KYLIN-2833] - Storage cleanup job could delete the intermediate hive table used by running jobs
* [KYLIN-2834] - Bug in metadata sync, Broadcaster lost listener after cache wipe
* [KYLIN-2838] - Should get storageType in changeHtableHost of CubeMigrationCLI
* [KYLIN-2862] - BasicClientConnManager in RestClient can't do well with syncing many query severs
* [KYLIN-2863] - Double caret bug in sample.sh for old version bash
* [KYLIN-2865] - Wrong fs when use two cluster
* [KYLIN-2868] - Include and exclude filters not work on ResourceTool
* [KYLIN-2870] - Shortcut key description is error at Kylin-Web
* [KYLIN-2871] - Ineffective null check in SegmentRange
* [KYLIN-2877] - Unclosed PreparedStatement in QueryService#execute()
* [KYLIN-2906] - Check model/cube name is duplicated when creating model/cube
* [KYLIN-2915] - Exception during query on lookup table
* [KYLIN-2920] - Failed to get streaming config on WebUI
* [KYLIN-2944] - HLLCSerializer, RawSerializer, PercentileSerializer returns shared object in serialize()
* [KYLIN-2949] - Couldn't get authorities with LDAP in RedHat Linux


### Task 

* [KYLIN-2782] - Replace DailyRollingFileAppender with RollingFileAppender to allow log retention
* [KYLIN-2925] - Provide document for Ranger security integration
* [KYLIN-2549] - Modify tools that related to ACL
* [KYLIN-2728] - Introduce a new cuboid scheduler based on cuboid tree rather than static rules
* [KYLIN-2729] - Introduce greedy algorithm for cube planner
* [KYLIN-2730] - Introduce genetic algorithm for cube planner
* [KYLIN-2802] - Enable cube planner phase one
* [KYLIN-2826] - Add basic support classes for cube planner algorithms
* [KYLIN-2961] - Provide user guide for Ranger Kylin Plugin