---
layout: docs
title:  Kylin Release 1.5.4
categories: releases
permalink: /docs/releases/kylin_release_1_5_4.html
---

_Tag:_ [kylin-1.5.4](https://github.com/apache/kylin/tree/kylin-1.5.4)

This version includes bug fixes/enhancements as well as new features; It is backward compatible with v1.5.3; While after upgrade, you still need update coprocessor, refer to [How to update coprocessor](/docs15/howto/howto_update_coprocessor.html).



### New Feature

* [KYLIN-1732] - Support Window Function
* [KYLIN-1767] - UI for TopN: specify encoding and multiple "group by"
* [KYLIN-1849] - Search cube by name in Web UI
* [KYLIN-1908] - Collect Metrics to JMX
* [KYLIN-1921] - Support Grouping Functions
* [KYLIN-1964] - Add a companion tool of CubeMetaExtractor for cube importing



### Improvement

* [KYLIN-672] - Add Env and Project Info in job email notification
* [KYLIN-1702] - The Key of the Snapshot to the related lookup table may be not informative
* [KYLIN-1855] - Should exclude those joins in whose related lookup tables no dimensions are used in cube
* [KYLIN-1858] - Remove all InvertedIndex(Streaming purpose) related codes and tests
* [KYLIN-1866] - Add tip for field at 'Add Streaming' table page.
* [KYLIN-1867] - Upgrade dependency libraries
* [KYLIN-1874] - Make roaring bitmap version determined
* [KYLIN-1898] - Upgrade to Avatica 1.8 or higher
* [KYLIN-1904] - WebUI for GlobalDictionary
* [KYLIN-1906] - Add more comments and default value for kylin.properties
* [KYLIN-1910] - Support Separate HBase Cluster with NN HA and Kerberos Authentication
* [KYLIN-1920] - Add view CubeInstance json function
* [KYLIN-1922] - Improve the logic to decide whether to pre aggregate on Region server
* [KYLIN-1923] - Add access controller to query
* [KYLIN-1924] - Region server metrics: replace int type for long type for scanned row count
* [KYLIN-1925] - Do not allow cross project clone for cube
* [KYLIN-1926] - Loosen the constraint on FK-PK data type matching
* [KYLIN-1936] - Improve enable limit logic (exactAggregation is too strict)
* [KYLIN-1940] - Add owner for DataModel
* [KYLIN-1941] - Show submitter for slow query
* [KYLIN-1954] - BuildInFunctionTransformer should be executed per CubeSegmentScanner
* [KYLIN-1963] - Delegate the loading of certain package (like slf4j) to tomcat's parent classloader
* [KYLIN-1965] - Check duplicated measure name
* [KYLIN-1966] - Refactor IJoinedFlatTableDesc
* [KYLIN-1979] - Move hackNoGroupByAggregation to cube-based storage implementations
* [KYLIN-1984] - Don't use compression in packaging configuration
* [KYLIN-1985] - SnapshotTable should only keep the columns described in tableDesc
* [KYLIN-1997] - Add pivot feature back in query result page
* [KYLIN-2004] - Make the creating intermediate hive table steps configurable (two options)



### Bug

* [KYLIN-962] - [UI] Cube Designer can't drag rowkey normally
* [KYLIN-1194] - Filter(CubeName) on Jobs/Monitor page works only once
* [KYLIN-1488] - When modifying a model, Save after deleting a lookup table. The internal error will pop up.
* [KYLIN-1760] - Save query hits org.apache.hadoop.hbase.TableNotFoundException: kylin_metadata_user
* [KYLIN-1808] - unload non existing table cause NPE
* [KYLIN-1834] - java.lang.IllegalArgumentException: Value not exists! - in Step 4 - Build Dimension Dictionary
* [KYLIN-1883] - Consensus Problem when running the tool, MetadataCleanupJob
* [KYLIN-1889] - Didn't deal with the failure of renaming folder in hdfs when running the tool CubeMigrationCLI
* [KYLIN-1929] - Error to load slow query in "Monitor" page for non-admin user
* [KYLIN-1933] - Deploy in cluster mode, the "query" node report "scheduler has not been started" every second
* [KYLIN-1934] - 'Value not exist' During Cube Merging Caused by Empty Dict
* [KYLIN-1939] - Linkage error while executing any queries
* [KYLIN-1942] - Models are missing after change project's name
* [KYLIN-1953] - Error handling for diagnosis
* [KYLIN-1956] - Can't query from child cube of a hybrid cube after its status changed from disabled to enabled
* [KYLIN-1961] - Project name is always constant instead of real project name in email notification
* [KYLIN-1970] - System Menu UI ACL issue
* [KYLIN-1972] - Access denied when query seek to hybrid
* [KYLIN-1973] - java.lang.NegativeArraySizeException when Build Dimension Dictionary
* [KYLIN-1982] - CubeMigrationCLI: associate model with project
* [KYLIN-1986] - CubeMigrationCLI: make global dictionary unique
* [KYLIN-1992] - Clear ThreadLocal Contexts when query failed before scaning HBase
* [KYLIN-1996] - Keep original column order when designing cube
* [KYLIN-1998] - Job engine lock is not release at shutdown
* [KYLIN-2003] - error start time at query result page
* [KYLIN-2005] - Move all storage side behavior hints to GTScanRequest
