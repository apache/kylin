---
layout: docs
title:  Kylin Release 2.4.0
categories: releases
permalink: /docs/releases/kylin_release_2_4_0.html
---

_Tag:_ [kylin-2.4.0](https://github.com/apache/kylin/tree/kylin-2.4.0)

This is a major release after 2.3.x, with 8 new features and more than 30 bug fixes bug fixes and enhancement. 



### New Feature

* [KYLIN-2484] - Spark engine to support source from Kafka
* [KYLIN-3221] - Allow externalizing lookup table snapshot
* [KYLIN-3283] - Support values RelNode
* [KYLIN-3315] - Allow each project to set its own source at project level
* [KYLIN-3343] - Support JDBC source on UI
* [KYLIN-3358] - Support sum(case when...), sum(2\*price+1), count(column) and more
* [KYLIN-3378] - Support Kafka table join with Hive tables



### Improvement

* [KYLIN-3137] - Spark cubing without hive-site.xml
* [KYLIN-3174] - Default scheduler enhancement
* [KYLIN-3220] - Add manager for project ACL.
* [KYLIN-3234] - ResourceStore should add a API that can recursively list path.
* [KYLIN-3246] - Add manager for user.
* [KYLIN-3248] - Add batch grant API for project ACL.
* [KYLIN-3251] - Add a hook that can customer made test_case_data
* [KYLIN-3266] - Improve CI coverage
* [KYLIN-3267] - add override MR config at project/cube level only for mem-hungry build steps 
* [KYLIN-3271] - Optimize sub-path check of ResourceTool
* [KYLIN-3275] - Add unit test for StorageCleanupJob
* [KYLIN-3279] - Util Class for encryption and decryption
* [KYLIN-3284] - Refactor all OLAPRel computeSelfCost
* [KYLIN-3289] - Refactor the storage garbage clean up code
* [KYLIN-3294] - Remove HBaseMROutput.java, RangeKeyDistributionJob.java and other sunset classes
* [KYLIN-3314] - Refactor code for cube planner algorithm
* [KYLIN-3320] - CubeStatsReader cannot print stats properly for some cube 
* [KYLIN-3328] - Upgrade the metadata of sample cube to latest
* [KYLIN-3331] - Kylin start script hangs during retrieving hive dependencies
* [KYLIN-3345] - Use Apache Parent POM 19
* [KYLIN-3354] - KeywordDefaultDirtyHack cannot handle double-quoted defaultCatalog identifier
* [KYLIN-3369] - Reduce the data size sink from Kafka topic to HDFS
* [KYLIN-3380] - Allow to configure Sqoop for jdbc source with a kylin_sqoop_conf.xml like hive
* [KYLIN-3386] - TopN measure validate code refactor to make it more clear



### Bug fix

* [KYLIN-1768] - NDCuboidMapper throws ArrayIndexOutOfBoundsException when dimension is fixed length encoded to more than 256 bytes
* [KYLIN-1948] - IntegerDimEnc, does not encode -1 correctly
* [KYLIN-3115] - Incompatible RowKeySplitter initialize between build and merge job
* [KYLIN-3122] - Partition elimination algorithm seems to be inefficient and have serious issues with handling date/time ranges, can lead to very slow queries and OOM/Java heap dump conditions
* [KYLIN-3149] - Calcite's ReduceExpressionsRule.PROJECT_INSTANCE not working as expected
* [KYLIN-3168] - CubeHFileJob should use currentHBaseConfiguration but not new create hbase configuration
* [KYLIN-3257] - Useless call in FuzzyValueCombination
* [KYLIN-3277] - Override hiveconf settings when connecting to hive using jdbc
* [KYLIN-3281] - OLAPProjectRule can't normal working with  projectRel[input=sortRel]
* [KYLIN-3292] - The setting config dialog will cause NPE in Kylin server
* [KYLIN-3293] - FixedLenHexDimEnc return a wrong code length leads to cut bytes error.
* [KYLIN-3295] - Unused method SQLDigestUtil#appendTsFilterToExecute
* [KYLIN-3296] - When merge cube，get java.lang.ArrayIndexOutOfBoundsException at java.lang.System.arraycopy(Native Method)
* [KYLIN-3311] - Segments overlap error (refactor write conflict exception)
* [KYLIN-3324] - NegativeArraySizeException in CreateDictionaryJob$2.getDictionary()
* [KYLIN-3336] - java.lang.NoSuchMethodException: org.apache.kylin.tool.HBaseUsageExtractor.execute([Ljava.lang.String;)
* [KYLIN-3348] - "missing LastBuildJobID" error when building new cube segment
* [KYLIN-3352] - Segment pruning bug, e.g. date_col > "max_date+1"
* [KYLIN-3363] - Wrong partition condition appended in JDBC Source
* [KYLIN-3367] - Add the compatibility for new version of hbase
* [KYLIN-3368] - "/kylin/kylin_metadata/metadata/" has many garbage for spark cubing
* [KYLIN-3388] - Data may become not correct if mappers fail during the redistribute step, "distribute by rand()"
* [KYLIN-3396] - NPE throws when materialize lookup table to HBase
* [KYLIN-3398] - Inaccurate arithmetic operation in LookupTableToHFileJob#calculateShardNum
* [KYLIN-3400] - WipeCache and createCubeDesc causes deadlock
* [KYLIN-3401] - The current using zip compress tool has an arbitrary file write vulnerability
* [KYLIN-3404] - Last optimized time detail was not showing after cube optimization



### Task

* [KYLIN-3327] - Upgrade surefire version to 2.21.0
* [KYLIN-3372] - Upgrade jackson-databind version due to security concerns
* [KYLIN-3415] - Remove "external" module



### Sub-task

* [KYLIN-3359] - Support sum(expression) if possible
* [KYLIN-3362] - Support dynamic dimension push down
* [KYLIN-3364] - Make the behavior of BigDecimalSumAggregator consistent with hive
* [KYLIN-3373] - Some improvements for lookup table - UI part change
* [KYLIN-3374] - Some improvements for lookup table - metadata change
* [KYLIN-3375] - Some improvements for lookup table - build change
* [KYLIN-3376] - Some improvements for lookup table - query change
* [KYLIN-3377] - Some improvements for lookup table - snapshot management
