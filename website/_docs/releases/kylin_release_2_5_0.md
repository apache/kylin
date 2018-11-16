---
layout: docs
title:  Kylin Release 2.5.0
categories: releases
permalink: /docs/releases/kylin_release_2_5_0.html
---

*Tag*: [kylin-2.5.0](https://github.com/apache/kylin/tree/kylin-2.5.0)

This is a major release after 2.4, with 96 bug fixes and enhancement. 



### New Feature

* [KYLIN-2565] - Support Hadoop 3.0
* [KYLIN-3488] - Support MySQL as Kylin metadata storage
* [KYLIN-3366] - Configure automatic enabling of cubes after a build process


### Improvement

* [KYLIN-2998] - Kill spark application when cube job was discarded
* [KYLIN-3033] - Support HBase 2.0
* [KYLIN-3071] - Add configuration to reuse dict to reduce dict size
* [KYLIN-3094] - Upgrade ZooKeeper to 3.4.12
* [KYLIN-3146] - Response code and exception should be standardized for cube checking
* [KYLIN-3186] - Add support for partitioning columns that combine date and time (e.g. YYYYMMDDHHMISS)
* [KYLIN-3250] - Upgrade jetty version to 9.3.22
* [KYLIN-3259] - When a cube is deleted, remove it from the hybrid cube definition
* [KYLIN-3321] - Set MALLOC_ARENA_MAX in script
* [KYLIN-3355] - Improve the HTTP return code of Rest API
* [KYLIN-3370] - Enhance segment pruning
* [KYLIN-3384] - Allow setting REPLICATION_SCOPE on newly created tables
* [KYLIN-3414] - Optimize the cleanup of project L2 cache
* [KYLIN-3418] - User interface for hybrid model
* [KYLIN-3419] - Upgrade to Java 8
* [KYLIN-3421] - Improve job scheduler fetch performance
* [KYLIN-3423] - Performance improvement in FactDistinctColumnsMapper
* [KYLIN-3424] - Missing invoke addCubingGarbageCollectionSteps in the cleanup step for HBaseMROutput2Transition
* [KYLIN-3427] - Convert to HFile in Spark
* [KYLIN-3434] - Support prepare statement in Kylin server side
* [KYLIN-3441] - Merge cube segments in Spark
* [KYLIN-3442] - Fact distinct columns in Spark
* [KYLIN-3449] - Should allow deleting a segment in NEW status
* [KYLIN-3452] - Optimize spark cubing memory footprint
* [KYLIN-3453] - Improve cube size estimation for TOPN, COUNT DISTINCT
* [KYLIN-3454] - Fix potential thread-safe problem in ResourceTool
* [KYLIN-3457] - Distribute by multiple columns if not set shard-by column
* [KYLIN-3463] - Improve optimize job by avoiding creating empty output files on HDFS
* [KYLIN-3464] - Less user confirmation
* [KYLIN-3470] - Add cache for execute and execute_output to speed up list job api
* [KYLIN-3471] - Merge dictionary and statistics on Yarn
* [KYLIN-3472] - TopN merge in Spark engine performance tunning
* [KYLIN-3475] - Make calcite case handling and quoting method more configurable.
* [KYLIN-3478] - Enhance backwards compatibility
* [KYLIN-3479] - Model can save when Kafka partition date column not select
* [KYLIN-3480] - Change the conformance of calcite from default to lenient
* [KYLIN-3481] - Kylin JDBC: Shaded dependencies should not be transitive
* [KYLIN-3485] - Make unloading table more flexible
* [KYLIN-3489] - Improve the efficiency of enumerating dictionary values
* [KYLIN-3490] - For single column queries, only dictionaries are enough
* [KYLIN-3491] - Improve the cube building process when using global dictionary
* [KYLIN-3503] - Missing java.util.logging.config.file when starting kylin instance
* [KYLIN-3507] - Query NPE when project is not found
* [KYLIN-3509] - Allocate more memory for "Merge dictionary on yarn" step
* [KYLIN-3510] - Correct sqoopHome at 'createSqoopToFlatHiveStep'
* [KYLIN-3521] - Enable Cube Planner by default
* [KYLIN-3539] - Hybrid segment overlap not cover some case
* [KYLIN-3317] - Replace UUID.randomUUID with deterministic PRNG
* [KYLIN-3436] - Refactor code related to loading hive/stream table


### Bug fix

* [KYLIN-2522] - Compilation fails with Java 8 when upgrading to HBase 1.2.5
* [KYLIN-2662] - NegativeArraySizeException in "Extract Fact Table Distinct Columns"
* [KYLIN-2933] - Fix compilation against the Kafka 1.0.0 release
* [KYLIN-3025] - kylin ODBC error : {fn CONVERT} for bigint type in tableau 10.4
* [KYLIN-3255] - Cannot save cube
* [KYLIN-3258] - No check for duplicate cube name when creating a hybrid cube
* [KYLIN-3379] - timestampadd bug fix and add test
* [KYLIN-3382] - YARN job link wasn't displayed when job is running
* [KYLIN-3385] - Error when have sum(1) measure
* [KYLIN-3390] - QueryInterceptorUtil.queryInterceptors is not thread safe
* [KYLIN-3391] - BadQueryDetector only detect first query
* [KYLIN-3399] - Leaked lookup table in DictionaryGeneratorCLI#processSegment
* [KYLIN-3403] - Querying sample cube with filter "KYLIN_CAL_DT.WEEK_BEG_DT >= CAST('2001-09-09' AS DATE)" returns unexpected empty result set
* [KYLIN-3428] - java.lang.OutOfMemoryError: Requested array size exceeds VM limit
* [KYLIN-3438] - mapreduce.job.queuename does not work at 'Convert Cuboid Data to HFile' Step
* [KYLIN-3446] - Convert to HFile in spark reports ZK connection refused
* [KYLIN-3451] - Cloned cube doesn't have Mandatory Cuboids copied
* [KYLIN-3456] - Cube level's snapshot config does not work
* [KYLIN-3458] - Enabling config kylin.job.retry will cause log info incomplete
* [KYLIN-3461] - "metastore.sh refresh-cube-signature" not updating cube signature as expected
* [KYLIN-3462] - "dfs.replication=2" and compression not work in Spark cube engine
* [KYLIN-3476] - Fix TupleExpression verification when parsing sql
* [KYLIN-3477] - Spark job size not available when deployMode is cluster
* [KYLIN-3482] - Unclosed SetAndUnsetThreadLocalConfig in SparkCubingByLayer
* [KYLIN-3483] - Imprecise comparison between double and integer division
* [KYLIN-3492] - Wrong constant value in KylinConfigBase.getDefaultVarcharPrecision
* [KYLIN-3500] - kylin 2.4 use JDBC datasource :Unknown column 'A.A.CRT_DATE' in 'where clause'
* [KYLIN-3505] - DataType.getType wrong usage of cache
* [KYLIN-3516] - Job status not updated after job discarded
* [KYLIN-3517] - Couldn't update coprocessor on HBase 2.0
* [KYLIN-3518] - Coprocessor reports NPE when execute a query on HBase 2.0
* [KYLIN-3522] - PrepareStatement cache issue
* [KYLIN-3525] - kylin.source.hive.keep-flat-table=true will delete data
* [KYLIN-3529] - Prompt not friendly
* [KYLIN-3533] - Can not save hybrid
* [KYLIN-3534] - Failed at update cube info step
* [KYLIN-3535] - "kylin-port-replace-util.sh" changed port but not uncomment it
* [KYLIN-3536] - PrepareStatement cache issue when there are new segments built
* [KYLIN-3538] - Automatic cube enabled functionality is not merged into 2.4.0
* [KYLIN-3547] - DimensionRangeInfo: Unsupported data type boolean
* [KYLIN-3550] - "kylin.source.hive.flat-table-field-delimiter" has extra "\"
* [KYLIN-3551] - Spark job failed with "FileNotFoundException"
* [KYLIN-3553] - Upgrade Tomcat to 7.0.90.
* [KYLIN-3554] - Spark job failed but Yarn shows SUCCEED, causing Kylin move to next step
* [KYLIN-3557] - PreparedStatement should be closed in JDBCResourceDAO#checkTableExists
