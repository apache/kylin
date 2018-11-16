---
layout: docs
title:  Kylin (incubating) Release 1.1.0
categories: releases
permalink: /docs/releases/kylin_release_1_1_0.html
---

_Tag:_ [kylin-1.1-incubating](https://github.com/apache/kylin/tree/kylin-1.1-incubating)



### New Feature

* [KYLIN-222] - Web UI to Display CubeInstance Information
* [KYLIN-906] - cube retention
* [KYLIN-910] - Allow user to enter "retention range" in days on Cube UI



### Improvement

* [KYLIN-343] - Enable timeout on query 
* [KYLIN-367] - automatically backup metadata everyday
* [KYLIN-589] - Cleanup Intermediate hive table after cube build
* [KYLIN-772] - Continue cube job when hive query return empty result set
* [KYLIN-858] - add snappy compression support
* [KYLIN-882] - check access to kylin.hdfs.working.dir
* [KYLIN-895] - Add "retention_range" attribute for cube instance, and automatically drop the oldest segment when exceeds retention
* [KYLIN-901] - Add tool for cleanup Kylin metadata storage
* [KYLIN-956] - Allow users to configure hbase compression algorithm in kylin.properties
* [KYLIN-957] - Support HBase in a separate cluster
* [KYLIN-965] - Allow user to configure the region split size for cube
* [KYLIN-971] - kylin display timezone on UI
* [KYLIN-987] - Rename 0.7-staging and 0.8 branch
* [KYLIN-998] - Finish the hive intermediate table clean up job in org.apache.kylin.job.hadoop.cube.StorageCleanupJob
* [KYLIN-999] - License check and cleanup for release
* [KYLIN-1013] - Make hbase client configurations like timeout configurable
* [KYLIN-1025] - Save cube change is very slow
* [KYLIN-1034] - Faster bitmap indexes with Roaring bitmaps
* [KYLIN-1035] - Validate [Project] before create Cube on UI
* [KYLIN-1037] - Remove hard-coded "hdp.version" from regression tests
* [KYLIN-1047] - Upgrade to Calcite 1.4
* [KYLIN-1048] - CPU and memory killer in Cuboid.findById()
* [KYLIN-1061] - "kylin.sh start" should check whether kylin has already been running
* [KYLIN-1048] - CPU and memory killer in Cuboid.findById()
* [KYLIN-1061] - "kylin.sh start" should check whether kylin has already been running



### Bug fix

* [KYLIN-457] - log4j error and dup lines in kylin.log
* [KYLIN-632] - "kylin.sh stop" doesn't check whether KYLIN_HOME was set
* [KYLIN-740] - Slowness with many IN() values
* [KYLIN-747] - bad query performance when IN clause contains a value doesn't exist in the dictionary
* [KYLIN-771] - query cache is not evicted when metadata changes
* [KYLIN-797] - Cuboid cache will cache massive invalid cuboid if existed many cubes which already be deleted 
* [KYLIN-847] - "select * from fact" does not work on 0.7 branch
* [KYLIN-913] - Cannot find rowkey column XXX in cube CubeDesc
* [KYLIN-918] - Calcite throws "java.lang.Float cannot be cast to java.lang.Double" error while executing SQL
* [KYLIN-944] - update doc about how to consume kylin API in JavaScript
* [KYLIN-950] - Web UI "Jobs" tab view the job reduplicated
* [KYLIN-952] - User can trigger a Refresh job on an non-existing cube segment via REST API
* [KYLIN-958] - update cube data model may fail and leave metadata in inconsistent state
* [KYLIN-961] - Can't get cube  source record count.
* [KYLIN-967] - Dump running queries on memory shortage
* [KYLIN-968] - CubeSegment.lastBuildJobID is null in new instance but used for rowkey_stats path
* [KYLIN-975] - change kylin.job.hive.database.for.intermediatetable cause job to fail
* [KYLIN-978] - GarbageCollectionStep dropped Hive Intermediate Table but didn't drop external hdfs path
* [KYLIN-982] - package.sh should grep out "Download\*" messages when determining version
* [KYLIN-983] - Query sql offset keyword bug
* [KYLIN-985] - Don't support aggregation AVG while executing SQL
* [KYLIN-1001] - Kylin generates wrong HDFS path in creating intermediate table
* [KYLIN-1004] - Dictionary with '' value cause cube merge to fail
* [KYLIN-1005] - fail to acquire ZookeeperJobLock when hbase.zookeeper.property.clientPort is configured other than 2181
* [KYLIN-1015] - Hive dependency jars appeared twice on job configuration
* [KYLIN-1020] - Although "kylin.query.scan.threshold" is set, it still be restricted to less than 4 million 
* [KYLIN-1026] - Error message for git check is not correct in package.sh