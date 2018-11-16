---
layout: docs
title:  Kylin Release 2.1.0
categories: releases
permalink: /docs/releases/kylin_release_2_1_0.html
---

_Tag:_ [kylin-2.1.0](https://github.com/apache/kylin/tree/kylin-2.1.0)

This is a major release after 2.0, with more than 100 bug fixes and enhancements.



### New Feature

* [KYLIN-1351] - Support RDBMS as data source
* [KYLIN-2515] - Route unsupported query back to source
* [KYLIN-2646] - Project level query authorization
* [KYLIN-2665] - Add model JSON edit in web 



### Improvement

* [KYLIN-2506] - Refactor Global Dictionary
* [KYLIN-2562] - Allow configuring yarn app tracking URL pattern
* [KYLIN-2578] - Refactor DistributedLock
* [KYLIN-2579] - Improvement on subqueries: reorder subqueries joins with RelOptRule
* [KYLIN-2580] - Improvement on subqueries: allow grouping by columns from subquery
* [KYLIN-2586] - use random port for CacheServiceTest as fixed port 7777 might have been occupied
* [KYLIN-2596] - Enable generating multiple streaming messages with one input message in streaming parser
* [KYLIN-2597] - Deal with trivial expression in filters like x = 1 + 2
* [KYLIN-2598] - Should not translate filter to a in-clause filter with too many elements
* [KYLIN-2599] - select * in subquery fail due to bug in hackSelectStar 
* [KYLIN-2602] - Add optional job threshold arg for MetadataCleanupJob
* [KYLIN-2603] - Push 'having' filter down to storage
* [KYLIN-2607] - Add http timeout for RestClient
* [KYLIN-2610] - Optimize BuiltInFunctionTransformer performance
* [KYLIN-2616] - GUI for multiple column count distinct measure
* [KYLIN-2624] - Correct reporting of HBase errors
* [KYLIN-2627] - ResourceStore to support simple rollback
* [KYLIN-2628] - Remove synchronized modifier for reloadCubeLocalAt
* [KYLIN-2633] - Upgrade Spark to 2.1
* [KYLIN-2642] - Relax check in RowKeyColDesc to keep backward compatibility
* [KYLIN-2667] - Ignore whitespace when caching query
* [KYLIN-2668] - Support Calcites Properties in JDBC URL
* [KYLIN-2673] - Support change the fact table when the cube is disable
* [KYLIN-2676] - Keep UUID in metadata constant 
* [KYLIN-2677] - Add project configuration view page
* [KYLIN-2689] - Only dimension columns can join when create a model
* [KYLIN-2691] - Support delete broken cube
* [KYLIN-2695] - Allow override spark conf in cube
* [KYLIN-2696] - Check SQL injection in model filter condition
* [KYLIN-2700] - Allow override Kafka conf at cube level
* [KYLIN-2704] - StorageCleanupJob should deal with a new metadata path
* [KYLIN-2742] - Specify login page for Spring security 4.x
* [KYLIN-2757] - Get cube size when using Azure Data Lake Store
* [KYLIN-2783] - Refactor CuboidScheduler to be extensible
* [KYLIN-2784] - Set User-Agent for ODBC/JDBC Drivers
* [KYLIN-2793] - ODBC Driver - Bypass cert validation when connect to SSL service



### Bug fix

* [KYLIN-1668] - Rowkey column shouldn't allow delete and add
* [KYLIN-1683] - Row key could drag and drop in view state of cube - advanced settings tabpage
* [KYLIN-2472] - Support Unicode chars in kylin.properties
* [KYLIN-2493] - Fix BufferOverflowException in FactDistinctColumnsMapper when value exceeds 4096 bytes
* [KYLIN-2540] - concat cascading is not supported
* [KYLIN-2544] - Fix wrong left join type when editing lookup table
* [KYLIN-2557] - Fix creating HBase table conflict when multiple kylin instances are starting concurrently
* [KYLIN-2559] - Enhance check-env.sh to check 'kylin.env.hdfs-working-dir' to be mandatory
* [KYLIN-2563] - Fix preauthorize-annotation bugs in query authorization
* [KYLIN-2568] - 'kylin_port_replace_util.sh' should only modify the kylin port and keep other properties unchanged. 
* [KYLIN-2571] - Return correct driver version from kylin jdbc driver
* [KYLIN-2572] - Fix parsing 'hive_home' error in 'find-hive-dependency.sh'
* [KYLIN-2573] - Enhance 'kylin.sh stop' to terminate kylin process finally
* [KYLIN-2574] - RawQueryLastHacker should group by all possible dimensions
* [KYLIN-2581] - Fix deadlock bugs in broadcast sync
* [KYLIN-2582] - 'Server Config' should be refreshed automatically in web page 'System', after we update it successfully. 
* [KYLIN-2588] - Query failed when two top-n measure with order by count(\*) exists in one cube
* [KYLIN-2589] - Enhance thread-safe in Authentication
* [KYLIN-2592] - Fix distinct count measure build failed issue with spark cubing 
* [KYLIN-2593] - Fix NPE issue when querying with Ton-N by count(\*) 
* [KYLIN-2594] - After reloading metadata, the project list should refresh
* [KYLIN-2595] - Display column alias name when query with keyword 'As'
* [KYLIN-2601] - The return type of tinyint for sum measure should be bigint
* [KYLIN-2605] - Remove the hard-code sample data path in 'sample.sh'
* [KYLIN-2608] - Bubble sort bug in JoinDesc
* [KYLIN-2609] - Fix grant role access issue on project page.
* [KYLIN-2611] - Unclosed HBaseAdmin in AclTableMigrationTool#checkTableExist
* [KYLIN-2612] - Potential NPE accessing familyMap in AclTableMigrationTool#getAllAceInfo
* [KYLIN-2613] - Wrong variable is used in DimensionDesc#hashCode
* [KYLIN-2621] - Fix issue on mapping LDAP group to the admin group
* [KYLIN-2637] - Show tips after creating project successfully
* [KYLIN-2641] - The current selected project is incorrect after we delete a project.
* [KYLIN-2643] - PreparedStatement should be closed in QueryServiceV2#execute()
* [KYLIN-2644] - Fix "Add Project" after refreshing Insight page
* [KYLIN-2647] - Should get FileSystem from HBaseConfiguration in HBaseResourceStore
* [KYLIN-2648] - kylin.env.hdfs-working-dir should be qualified and absolute path
* [KYLIN-2652] - Make KylinConfig thread-safe in CubeVisitService
* [KYLIN-2655] - Fix wrong job duration issue when resuming the error or stopped job.
* [KYLIN-2657] - Fix Cube merge NPE whose TopN dictionary not found
* [KYLIN-2658] - Unclosed ResultSet in JdbcExplorer#loadTableMetadata()
* [KYLIN-2660] - Show error tips if load hive error occurs and can not be connected.
* [KYLIN-2661] - Fix Cube list page display issue when using MODELER or ANALYST
* [KYLIN-2664] - Fix Extended column bug in web
* [KYLIN-2670] - Fix CASE WHEN issue in orderby clause
* [KYLIN-2674] - Should not catch OutOfMemoryError in coprocessor
* [KYLIN-2678] - Fix minor issues in KylinConfigCLITest
* [KYLIN-2684] - Fix Object.class not registered in Kyro issue with spark cubing
* [KYLIN-2687] - When the model has a ready cube, should not allow user to edit model JSON in web.
* [KYLIN-2688] - When the model has a ready cube, should not allow user to edit model JSON in web.
* [KYLIN-2693] - Should use overrideHiveConfig for LookupHiveViewMaterialization and RedistributeFlatHiveTable
* [KYLIN-2694] - Fix ArrayIndexOutOfBoundsException in SparkCubingByLayer
* [KYLIN-2699] - Tomcat LinkageError for curator-client jar file conflict
* [KYLIN-2701] - Unclosed PreparedStatement in QueryService#getPrepareOnlySqlResponse
* [KYLIN-2702] - Ineffective null check in DataModelDesc#initComputedColumns()
* [KYLIN-2707] - Fix NPE in JobInfoConverter
* [KYLIN-2708] - cube merge operations can not execute success
* [KYLIN-2711] - NPE if job output is lost
* [KYLIN-2713] - Fix ITJdbcSourceTableLoaderTest.java and ITJdbcTableReaderTest.java missing license header
* [KYLIN-2719] - serviceStartTime of CubeVisitService should not be an attribute which may be shared by multi-thread
* [KYLIN-2743] - Potential corrupt TableDesc when loading an existing Hive table
* [KYLIN-2748] - Calcite code generation can not gc cause OOM
* [KYLIN-2754] - Fix Sync issue when reload existing hive table
* [KYLIN-2758] - Query pushdown should be able to skip database prefix
* [KYLIN-2762] - Get "Owner required" error on saving data model
* [KYLIN-2767] - 404 error on click "System" tab
* [KYLIN-2768] - Wrong UI for count distinct measure
* [KYLIN-2769] - Non-partitioned cube doesn't need show start/end time
* [KYLIN-2778] - Sample cube doesn't have ACL info
* [KYLIN-2780] - QueryController.getMetadata and CacheController.wipeCache may be deadlock



### Sub-task

* [KYLIN-2548] - Keep ACL information backward compatible