---
layout: docs15
title:  Apache Kylin™ Release Notes
categories: gettingstarted
permalink: /docs15/release_notes.html
---

To download latest release, please visit: [http://kylin.apache.org/download/](http://kylin.apache.org/download/), 
there are source code package, binary package, ODBC driver and installation guide avaliable.

Any problem or issue, please report to Apache Kylin JIRA project: [https://issues.apache.org/jira/browse/KYLIN](https://issues.apache.org/jira/browse/KYLIN)

or send to Apache Kylin mailing list:

* User relative: [user@kylin.apache.org](mailto:user@kylin.apache.org)
* Development relative: [dev@kylin.apache.org](mailto:dev@kylin.apache.org)

## v1.5.2.1 - 2016-06-07
_Tag:_ [kylin-1.5.2.1](https://github.com/apache/kylin/tree/kylin-1.5.2.1)

This is a hot-fix version on v1.5.2, no new feature introduced, please upgrade to this version;

__Bug__

* [KYLIN-1758] - createLookupHiveViewMaterializationStep will create intermediate table for fact table
* [KYLIN-1739] - kylin_job_conf_inmem.xml can impact non-inmem MR job


## v1.5.2 - 2016-05-26
_Tag:_ [kylin-1.5.2](https://github.com/apache/kylin/tree/kylin-1.5.2)

This version is backward compatiple with v1.5.1. But after upgrade to v1.5.2 from v1.5.1, you need to update coprocessor, refer to [How to update coprocessor](/docs15/howto/howto_update_coprocessor.html).

__Highlights__

* [KYLIN-1077] - Support Hive View as Lookup Table
* [KYLIN-1515] - Make Kylin run on MapR
* [KYLIN-1600] - Download diagnosis zip from GUI
* [KYLIN-1672] - support kylin on cdh 5.7

__New Feature__

* [KYLIN-1016] - Count distinct on any dimension should work even not a predefined measure
* [KYLIN-1077] - Support Hive View as Lookup Table
* [KYLIN-1441] - Display time column as partition column
* [KYLIN-1515] - Make Kylin run on MapR
* [KYLIN-1600] - Download diagnosis zip from GUI
* [KYLIN-1672] - support kylin on cdh 5.7

__Improvement__

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

__Bug__

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
* [KYLIN-1660] - Streaming/kafka config not match with table name
* [KYLIN-1662] - tableName got truncated during request mapping for /tables/tableName
* [KYLIN-1666] - Should check project selection before add a stream table
* [KYLIN-1667] - Streaming table name should allow enter "DB.TABLE" format
* [KYLIN-1673] - make sure metadata in 1.5.2 compatible with 1.5.1
* [KYLIN-1678] - MetaData clean just clean FINISHED and DISCARD jobs,but job correct status is SUCCEED
* [KYLIN-1685] - error happens while execute a sql contains '?' using Statement
* [KYLIN-1688] - Illegal char on result dataset table
* [KYLIN-1721] - KylinConfigExt lost base properties when store into file
* [KYLIN-1722] - IntegerDimEnc serialization exception inside coprocessor

## v1.5.1 - 2016-04-13
_Tag:_ [kylin-1.5.1](https://github.com/apache/kylin/tree/kylin-1.5.1)

This version is backward compatiple with v1.5.0. But after upgrade to v1.5.1 from v1.5.0, you need to update coprocessor, refer to [How to update coprocessor](/docs15/howto/howto_update_coprocessor.html).

__Highlights__

* [KYLIN-1122] - Kylin support detail data query from fact table
* [KYLIN-1492] - Custom dimension encoding
* [KYLIN-1495] - Metadata upgrade from 1.0~1.3 to 1.5, including metadata correction, relevant tools, etc.
* [KYLIN-1534] - Cube specific config, override global kylin.properties
* [KYLIN-1546] - Tool to dump information for diagnosis

__New Feature__

* [KYLIN-1122] - Kylin support detail data query from fact table
* [KYLIN-1378] - Add UI for TopN measure
* [KYLIN-1492] - Custom dimension encoding
* [KYLIN-1495] - Metadata upgrade from 1.0~1.3 to 1.5, including metadata correction, relevant tools, etc.
* [KYLIN-1501] - Run some classes at the beginning of kylin server startup
* [KYLIN-1503] - Print version information with kylin.sh
* [KYLIN-1531] - Add smoke test scripts
* [KYLIN-1534] - Cube specific config, override global kylin.properties
* [KYLIN-1540] - REST API for deleting segment
* [KYLIN-1541] - IntegerDimEnc, custom dimension encoding for integers
* [KYLIN-1546] - Tool to dump information for diagnosis
* [KYLIN-1550] - Persist some recent bad query

__Improvement__

* [KYLIN-1490] - Use InstallShield 2015 to generate ODBC Driver setup files
* [KYLIN-1498] - cube desc signature not calculated correctly
* [KYLIN-1500] - streaming_fillgap cause out of memory
* [KYLIN-1502] - When cube is not empty, only signature consistent cube desc updates are allowed
* [KYLIN-1504] - Use NavigableSet to store rowkey and use prefix filter to check resource path prefix instead String comparison on tomcat side
* [KYLIN-1505] - Combine guava filters with Predicates.and
* [KYLIN-1543] - GTFilterScanner performance tuning
* [KYLIN-1557] - Enhance the check on aggregation group dimension number

__Bug__

* [KYLIN-1373] - need to encode export query url to get right result in query page
* [KYLIN-1434] - Kylin Job Monitor API: /kylin/api/jobs is too slow in large kylin deployment
* [KYLIN-1472] - Export csv get error when there is a plus sign in the sql
* [KYLIN-1486] - java.lang.IllegalArgumentException: Too many digits for NumberDictionary
* [KYLIN-1491] - Should return base cuboid as valid cuboid if no aggregation group matches
* [KYLIN-1493] - make ExecutableManager.getInstance thread safe
* [KYLIN-1497] - Make three <class>.getInstance thread safe
* [KYLIN-1507] - Couldn't find hive dependency jar on some platform like CDH
* [KYLIN-1513] - Time partitioning doesn't work across multiple days
* [KYLIN-1514] - MD5 validation of Tomcat does not work when package tar
* [KYLIN-1521] - Couldn't refresh a cube segment whose start time is before 1970-01-01
* [KYLIN-1522] - HLLC is incorrect when result is feed from cache
* [KYLIN-1524] - Get "java.lang.Double cannot be cast to java.lang.Long" error when Top-N metris data type is BigInt
* [KYLIN-1527] - Columns with all NULL values can't be queried
* [KYLIN-1537] - Failed to create flat hive table, when name is too long
* [KYLIN-1538] - DoubleDeltaSerializer cause obvious error after deserialize and serialize
* [KYLIN-1553] - Cannot find rowkey column "COL_NAME" in cube CubeDesc
* [KYLIN-1564] - Unclosed table in BuildCubeWithEngine#checkHFilesInHBase()
* [KYLIN-1569] - Select any column when adding a custom aggregation in GUI

## v1.5.0 - 2016-03-12
_Tag:_ [kylin-1.5.0](https://github.com/apache/kylin/tree/kylin-1.5.0)

__This version is not backward compatible.__ The format of cube and metadata has been refactored in order to get times of performance improvement. We recommend this version, but does not suggest upgrade from previous deployment directly. A clean and new deployment of this version is strongly recommended. If you have to upgrade from previous deployment, an upgrade guide will be provided by community later.

__Highlights__

* [KYLIN-875] - A plugin-able architecture, to allow alternative cube engine / storage engine / data source.
* [KYLIN-1245] - A better MR cubing algorithm, about 1.5 times faster by comparing hundreds of jobs.
* [KYLIN-942] - A better storage engine, makes query roughly 2 times faster (especially for slow queries) by comparing tens of thousands sqls.
* [KYLIN-738] - Streaming cubing EXPERIMENTAL support, source from kafka, build cube in-mem at minutes interval.
* [KYLIN-242] - Redesign aggregation group, support of 20+ dimensions made easy.
* [KYLIN-976] - Custom aggregation types (or UDF in other words).
* [KYLIN-943] - TopN aggregation type.
* [KYLIN-1065] - ODBC compatible with Tableau 9.1, MS Excel, MS PowerBI.
* [KYLIN-1219] - Kylin support SSO with Spring SAML.

__New Feature__

* [KYLIN-528] - Build job flow for Inverted Index building
* [KYLIN-579] - Unload table from Kylin
* [KYLIN-596] - Support Excel and Power BI
* [KYLIN-599] - Near real-time support
* [KYLIN-607] - More efficient cube building
* [KYLIN-609] - Add Hybrid as a federation of Cube and Inverted-index realization
* [KYLIN-625] - Create GridTable, a data structure that abstracts vertical and horizontal partition of a table
* [KYLIN-728] - IGTStore implementation which use disk when memory runs short
* [KYLIN-738] - StreamingOLAP
* [KYLIN-749] - support timestamp type in II and cube
* [KYLIN-774] - Automatically merge cube segments
* [KYLIN-868] - add a metadata backup/restore script in bin folder
* [KYLIN-886] - Data Retention for streaming data
* [KYLIN-906] - cube retention
* [KYLIN-943] - Approximate TopN supported by Cube
* [KYLIN-986] - Generalize Streaming scripts and put them into code repository
* [KYLIN-1219] - Kylin support SSO with Spring SAML
* [KYLIN-1277] - Upgrade tool to put old-version cube and new-version cube into a hybrid model
* [KYLIN-1458] - Checking the consistency of cube segment host with the environment after cube migration
	
* [KYLIN-976] - Support Custom Aggregation Types
* [KYLIN-1054] - Support Hive client Beeline
* [KYLIN-1128] - Clone Cube Metadata
* [KYLIN-1186] - Support precise Count Distinct using bitmap (under limited conditions)
* [KYLIN-1458] - Checking the consistency of cube segment host with the environment after cube migration
* [KYLIN-1483] - Command tool to visualize all cuboids in a cube/segment

__Improvement__

* [KYLIN-225] - Support edit "cost" of cube
* [KYLIN-410] - table schema not expand when clicking the database text
* [KYLIN-589] - Cleanup Intermediate hive table after cube build
* [KYLIN-623] - update Kylin UI Style to latest AdminLTE
* [KYLIN-633] - Support Timestamp for cube partition
* [KYLIN-649] - move the cache layer from service tier back to storage tier
* [KYLIN-655] - Migrate cube storage (query side) to use GridTable API
* [KYLIN-663] - Push time condition down to ii endpoint
* [KYLIN-668] - Out of memory in mapper when building cube in mem
* [KYLIN-671] - Implement fine grained cache for cube and ii
* [KYLIN-674] - IIEndpoint return metrics as well
* [KYLIN-675] - cube&model designer refactor
* [KYLIN-678] - optimize RowKeyColumnIO
* [KYLIN-697] - Reorganize all test cases to unit test and integration tests
* [KYLIN-702] - When Kylin create the flat hive table, it generates large number of small files in HDFS
* [KYLIN-708] - replace BitSet for AggrKey
* [KYLIN-712] - some enhancement after code review
* [KYLIN-717] - optimize OLAPEnumerator.convertCurrentRow()
* [KYLIN-718] - replace aliasMap in storage context with a clear specified return column list
* [KYLIN-719] - bundle statistics info in endpoint response
* [KYLIN-720] - Optimize endpoint's response structure to suit with no-dictionary data
* [KYLIN-721] - streaming cli support third-party streammessage parser
* [KYLIN-726] - add remote cli port configuration for KylinConfig
* [KYLIN-729] - IIEndpoint eliminate the non-aggregate routine
* [KYLIN-734] - Push cache layer to each storage engine
* [KYLIN-752] - Improved IN clause performance
* [KYLIN-753] - Make the dependency on hbase-common to "provided"
* [KYLIN-755] - extract copying libs from prepare.sh so that it can be reused
* [KYLIN-760] - Improve the hasing performance in Sampling cuboid size
* [KYLIN-772] - Continue cube job when hive query return empty resultset
* [KYLIN-773] - performance is slow list jobs
* [KYLIN-783] - update hdp version in test cases to 2.2.4
* [KYLIN-796] - Add REST API to trigger storage cleanup/GC
* [KYLIN-809] - Streaming cubing allow multiple kafka clusters/topics
* [KYLIN-816] - Allow gap in cube segments, for streaming case
* [KYLIN-822] - list cube overview in one page
* [KYLIN-823] - replace fk on fact table on rowkey & aggregation group generate
* [KYLIN-838] - improve performance of job query
* [KYLIN-844] - add backdoor toggles to control query behavior
* [KYLIN-845] - Enable coprocessor even when there is memory hungry distinct count
* [KYLIN-858] - add snappy compression support
* [KYLIN-866] - Confirm with user when he selects empty segments to merge
* [KYLIN-869] - Enhance mail notification
* [KYLIN-870] - Speed up hbase segments info by caching
* [KYLIN-871] - growing dictionary for streaming case
* [KYLIN-874] - script for fill streaming gap automatically
* [KYLIN-875] - Decouple with Hadoop to allow alternative Input / Build Engine / Storage
* [KYLIN-879] - add a tool to collect orphan hbases
* [KYLIN-880] - Kylin should change the default folder from /tmp to user configurable destination
* [KYLIN-881] - Upgrade Calcite to 1.3.0
* [KYLIN-882] - check access to kylin.hdfs.working.dir
* [KYLIN-883] - Using configurable option for Hive intermediate tables created by Kylin job
* [KYLIN-893] - Remove the dependency on quartz and metrics
* [KYLIN-895] - Add "retention_range" attribute for cube instance, and automatically drop the oldest segment when exceeds retention
* [KYLIN-896] - Clean ODBC code, add them into main repository and write docs to help compiling
* [KYLIN-901] - Add tool for cleanup Kylin metadata storage
* [KYLIN-902] - move streaming related parameters into StreamingConfig
* [KYLIN-909] - Adapt GTStore to hbase endpoint
* [KYLIN-919] - more friendly UI for 0.8
* [KYLIN-922] - Enforce same code style for both intellij and eclipse user
* [KYLIN-926] - Make sure Kylin leaves no garbage files in local OS and HDFS/HBASE
* [KYLIN-927] - Real time cubes merging skipping gaps
* [KYLIN-933] - friendly UI to use data model
* [KYLIN-938] - add friendly tip to page when rest request failed
* [KYLIN-942] - Cube parallel scan on Hbase
* [KYLIN-956] - Allow users to configure hbase compression algorithm in kylin.properties
* [KYLIN-957] - Support HBase in a separate cluster
* [KYLIN-960] - Split storage module to core-storage and storage-hbase
* [KYLIN-973] - add a tool to analyse streaming output logs
* [KYLIN-984] - Behavior change in streaming data consuming
* [KYLIN-987] - Rename 0.7-staging and 0.8 branch
* [KYLIN-1014] - Support kerberos authentication while getting status from RM
* [KYLIN-1018] - make TimedJsonStreamParser default parser
* [KYLIN-1019] - Remove v1 cube model classes from code repository
* [KYLIN-1021] - upload dependent jars of kylin to HDFS and set tmpjars
* [KYLIN-1025] - Save cube change is very slow
* [KYLIN-1036] - Code Clean, remove code which never used at front end
* [KYLIN-1041] - ADD Streaming UI
* [KYLIN-1048] - CPU and memory killer in Cuboid.findById()
* [KYLIN-1058] - Remove "right join" during model creation
* [KYLIN-1061] - "kylin.sh start" should check whether kylin has already been running
* [KYLIN-1064] - restore disabled queries in KylinQueryTest.testVerifyQuery
* [KYLIN-1065] - ODBC driver support tableau 9.1
* [KYLIN-1068] - Optimize the memory footprint for TopN counter
* [KYLIN-1069] - update tip for 'Partition Column' on UI
* [KYLIN-1074] - Load hive tables with selecting mode
* [KYLIN-1095] - Update AdminLTE to latest version
* [KYLIN-1096] - Deprecate minicluster
* [KYLIN-1099] - Support dictionary of cardinality over 10 millions
* [KYLIN-1101] - Allow "YYYYMMDD" as a date partition column
* [KYLIN-1105] - Cache in AbstractRowKeyEncoder.createInstance() is useless
* [KYLIN-1116] - Use local dictionary for InvertedIndex batch building
* [KYLIN-1119] - refine find-hive-dependency.sh to correctly get hcatalog path
* [KYLIN-1126] - v2 storage(for parallel scan) backward compatibility with v1 storage
* [KYLIN-1135] - Pscan use share thread pool
* [KYLIN-1136] - Distinguish fast build mode and complete build mode
* [KYLIN-1139] - Hive job not starting due to error "conflicting lock present for default mode EXCLUSIVE "
* [KYLIN-1149] - When yarn return an incomplete job tracking URL, Kylin will fail to get job status
* [KYLIN-1154] - Load job page is very slow when there are a lot of history job
* [KYLIN-1157] - CubeMigrationCLI doesn't copy ACL
* [KYLIN-1160] - Set default logger appender of log4j for JDBC
* [KYLIN-1161] - Rest API /api/cubes?cubeName= is doing fuzzy match instead of exact match
* [KYLIN-1162] - Enhance HadoopStatusGetter to be compatible with YARN-2605
* [KYLIN-1190] - Make memory budget per query configurable
* [KYLIN-1211] - Add 'Enable Cache' button in System page
* [KYLIN-1234] - Cube ACL does not work
* [KYLIN-1235] - allow user to select dimension column as options when edit COUNT_DISTINCT measure
* [KYLIN-1237] - Revisit on cube size estimation
* [KYLIN-1239] - attribute each htable with team contact and owner name
* [KYLIN-1244] - In query window, enable fast copy&paste by double clicking tables/columns' names.
* [KYLIN-1245] - Switch between layer cubing and in-mem cubing according to stats
* [KYLIN-1246] - get cubes API update - offset,limit not required
* [KYLIN-1251] - add toggle event for tree label
* [KYLIN-1259] - Change font/background color of job progress
* [KYLIN-1265] - Make sure 1.4-rc query is no slower than 1.0
* [KYLIN-1266] - Tune release package size
* [KYLIN-1267] - Check Kryo performance when spilling aggregation cache
* [KYLIN-1268] - Fix 2 kylin logs
* [KYLIN-1270] - improve TimedJsonStreamParser to support month_start,quarter_start,year_start
* [KYLIN-1281] - Add "partition_date_end", and move "partition_date_start" into cube descriptor
* [KYLIN-1283] - Replace GTScanRequest's SerDer form Kryo to manual
* [KYLIN-1287] - UI update for streaming build action
* [KYLIN-1297] - Diagnose query performance issues in 1.4 branch
* [KYLIN-1301] - fix segment pruning failure
* [KYLIN-1308] - query storage v2 enable parallel cube visiting
* [KYLIN-1312] - Enhance DeployCoprocessorCLI to support Cube level filter
* [KYLIN-1317] - Kill underlying running hadoop job while discard a job
* [KYLIN-1318] - enable gc log for kylin server instance
* [KYLIN-1323] - Improve performance of converting data to hfile
* [KYLIN-1327] - Tool for batch updating host information of htables
* [KYLIN-1333] - Kylin Entity Permission Control
* [KYLIN-1334] - allow truncating string for fixed length dimensions
* [KYLIN-1341] - Display JSON of Data Model in the dialog
* [KYLIN-1350] - hbase Result.binarySearch is found to be problematic in concurrent environments
* [KYLIN-1365] - Kylin ACL enhancement
* [KYLIN-1368] - JDBC Driver is not generic to restAPI json result
* [KYLIN-1424] - Should support multiple selection in picking up dimension/measure column step in data model wizard
* [KYLIN-1438] - auto generate aggregation group
* [KYLIN-1474] - expose list, remove and cat in metastore.sh
* [KYLIN-1475] - Inject ehcache manager for any test case that will touch ehcache manager
	
* [KYLIN-242] - Redesign aggregation group
* [KYLIN-770] - optimize memory usage for GTSimpleMemStore GTAggregationScanner
* [KYLIN-955] - HiveColumnCardinalityJob should use configurations in conf/kylin_job_conf.xml
* [KYLIN-980] - FactDistinctColumnsJob to support high cardinality columns
* [KYLIN-1079] - Manager large number of entries in metadata store
* [KYLIN-1082] - Hive dependencies should be add to tmpjars
* [KYLIN-1201] - Enhance project level ACL
* [KYLIN-1222] - restore testing v1 query engine in case need it as a fallback for v2
* [KYLIN-1232] - Refine ODBC Connection UI
* [KYLIN-1237] - Revisit on cube size estimation
* [KYLIN-1239] - attribute each htable with team contact and owner name
* [KYLIN-1245] - Switch between layer cubing and in-mem cubing according to stats
* [KYLIN-1265] - Make sure 1.4-rc query is no slower than 1.0
* [KYLIN-1266] - Tune release package size
* [KYLIN-1270] - improve TimedJsonStreamParser to support month_start,quarter_start,year_start
* [KYLIN-1283] - Replace GTScanRequest's SerDer form Kryo to manual
* [KYLIN-1297] - Diagnose query performance issues in 1.4 branch
* [KYLIN-1301] - fix segment pruning failure
* [KYLIN-1308] - query storage v2 enable parallel cube visiting
* [KYLIN-1318] - enable gc log for kylin server instance
* [KYLIN-1327] - Tool for batch updating host information of htables
* [KYLIN-1343] - Upgrade calcite version to 1.6
* [KYLIN-1350] - hbase Result.binarySearch is found to be problematic in concurrent environments
* [KYLIN-1366] - Bind metadata version with release version
* [KYLIN-1389] - Formatting ODBC Drive C++ code
* [KYLIN-1405] - Aggregation group validation
* [KYLIN-1465] - Beautify kylin log to convenience both production trouble shooting and CI debuging
* [KYLIN-1475] - Inject ehcache manager for any test case that will touch ehcache manager

__Bug__

* [KYLIN-404] - Can't get cube source record size.
* [KYLIN-457] - log4j error and dup lines in kylin.log
* [KYLIN-521] - No verification even if join condition is invalid
* [KYLIN-632] - "kylin.sh stop" doesn't check whether KYLIN_HOME was set
* [KYLIN-635] - IN clause within CASE when is not working
* [KYLIN-656] - REST API get cube desc NullPointerException when cube is not exists
* [KYLIN-660] - Make configurable of dictionary cardinality cap
* [KYLIN-665] - buffer error while in mem cubing
* [KYLIN-688] - possible memory leak for segmentIterator
* [KYLIN-731] - Parallel stream build will throw OOM
* [KYLIN-740] - Slowness with many IN() values
* [KYLIN-747] - bad query performance when IN clause contains a value doesn't exist in the dictionary
* [KYLIN-748] - II returned result not correct when decimal omits precision and scal
* [KYLIN-751] - Max on negative double values is not working
* [KYLIN-766] - round BigDecimal according to the DataType scale
* [KYLIN-769] - empty segment build fail due to no dictionary
* [KYLIN-771] - query cache is not evicted when metadata changes
* [KYLIN-778] - can't build cube after package to binary
* [KYLIN-780] - Upgrade Calcite to 1.0
* [KYLIN-797] - Cuboid cache will cache massive invalid cuboid if existed many cubes which already be deleted
* [KYLIN-801] - fix remaining issues on query cache and storage cache
* [KYLIN-805] - Drop useless Hive intermediate table and HBase tables in the last step of cube build/merge
* [KYLIN-807] - Avoid write conflict between job engine and stream cube builder
* [KYLIN-817] - Support Extract() on timestamp column
* [KYLIN-824] - Cube Build fails if lookup table doesn't have any files under HDFS location
* [KYLIN-828] - kylin still use ldap profile when comment the line "kylin.sandbox=false" in kylin.properties
* [KYLIN-834] - optimize StreamingUtil binary search perf
* [KYLIN-837] - fix submit build type when refresh cube
* [KYLIN-873] - cancel button does not work when [resume][discard] job
* [KYLIN-889] - Support more than one HDFS files of lookup table
* [KYLIN-897] - Update CubeMigrationCLI to copy data model info
* [KYLIN-898] - "CUBOID_CACHE" in Cuboid.java never flushes
* [KYLIN-905] - Boolean type not supported
* [KYLIN-911] - NEW segments not DELETED when cancel BuildAndMerge Job
* [KYLIN-912] - $KYLIN_HOME/tomcat/temp folder takes much disk space after long run
* [KYLIN-913] - Cannot find rowkey column XXX in cube CubeDesc
* [KYLIN-914] - Scripts shebang should use /bin/bash
* [KYLIN-918] - Calcite throws "java.lang.Float cannot be cast to java.lang.Double" error while executing SQL
* [KYLIN-929] - can not sort cubes by [Source Records] at cubes list page
* [KYLIN-930] - can't see realizations under each project at project list page
* [KYLIN-934] - Negative number in SUM result and Kylin results not matching exactly Hive results
* [KYLIN-935] - always loading when try to view the log of the sub-step of cube build job
* [KYLIN-936] - can not see job step log
* [KYLIN-944] - update doc about how to consume kylin API in javascript
* [KYLIN-946] - [UI] refresh page show no results when Project selected as [--Select All--]
* [KYLIN-950] - Web UI "Jobs" tab view the job reduplicated
* [KYLIN-951] - Drop RowBlock concept from GridTable general API
* [KYLIN-952] - User can trigger a Refresh job on an non-existing cube segment via REST API
* [KYLIN-967] - Dump running queries on memory shortage
* [KYLIN-975] - change kylin.job.hive.database.for.intermediatetable cause job to fail
* [KYLIN-978] - GarbageCollectionStep dropped Hive Intermediate Table but didn't drop external hdfs path
* [KYLIN-982] - package.sh should grep out "Download*" messages when determining version
* [KYLIN-983] - Query sql offset keyword bug
* [KYLIN-985] - Don't suppoprt aggregation AVG while executing SQL
* [KYLIN-991] - StorageCleanupJob may clean a newly created HTable in streaming cube building
* [KYLIN-992] - ConcurrentModificationException when initializing ResourceStore
* [KYLIN-993] - implement substr support in kylin
* [KYLIN-1001] - Kylin generates wrong HDFS path in creating intermediate table
* [KYLIN-1004] - Dictionary with '' value cause cube merge to fail
* [KYLIN-1020] - Although "kylin.query.scan.threshold" is set, it still be restricted to less than 4 million
* [KYLIN-1026] - Error message for git check is not correct in package.sh
* [KYLIN-1027] - HBase Token not added after KYLIN-1007
* [KYLIN-1033] - Error when joining two sub-queries
* [KYLIN-1039] - Filter like (A or false) yields wrong result
* [KYLIN-1047] - Upgrade to Calcite 1.4
* [KYLIN-1066] - Only 1 reducer is started in the "Build cube" step of MR_Engine_V2
* [KYLIN-1067] - Support get MapReduce Job status for ResourceManager HA Env
* [KYLIN-1075] - select [MeasureCol] from [FactTbl] is not supported
* [KYLIN-1093] - Consolidate getCurrentHBaseConfiguration() and newHBaseConfiguration() in HadoopUtil
* [KYLIN-1106] - Can not send email caused by Build Base Cuboid Data step failed
* [KYLIN-1108] - Return Type Empty When Measure-> Count In Cube Design
* [KYLIN-1113] - Support TopN query in v2/CubeStorageQuery.java
* [KYLIN-1115] - Clean up ODBC driver code
* [KYLIN-1121] - ResourceTool download/upload does not work in binary package
* [KYLIN-1127] - Refactor CacheService
* [KYLIN-1137] - TopN measure need support dictionary merge
* [KYLIN-1138] - Bad CubeDesc signature cause segment be delete when enable a cube
* [KYLIN-1140] - Kylin's sample cube "kylin_sales_cube" couldn't be saved.
* [KYLIN-1151] - Menu items should be aligned when create new model
* [KYLIN-1152] - ResourceStore should read content and timestamp in one go
* [KYLIN-1153] - Upgrade is needed for cubedesc metadata from 1.3 to 1.4
* [KYLIN-1171] - KylinConfig truncate bug
* [KYLIN-1179] - Cannot use String as partition column
* [KYLIN-1180] - Some NPE in Dictionary
* [KYLIN-1181] - Split metadata size exceeded when data got huge in one segment
* [KYLIN-1182] - DataModelDesc needs to be updated from v1.x to v2.0
* [KYLIN-1192] - Cannot edit data model desc without name change
* [KYLIN-1205] - hbase RpcClient java.io.IOException: Unexpected closed connection
* [KYLIN-1216] - Can't parse DateFormat like 'YYYYMMDD' correctly in query
* [KYLIN-1218] - java.lang.NullPointerException in MeasureTypeFactory when sync hive table
* [KYLIN-1220] - JsonMappingException: Can not deserialize instance of java.lang.String out of START_ARRAY
* [KYLIN-1225] - Only 15 cubes listed in the /models page
* [KYLIN-1226] - InMemCubeBuilder throw OOM for multiple HLLC measures
* [KYLIN-1230] - When CubeMigrationCLI copied ACL from one env to another, it may not work
* [KYLIN-1236] - redirect to home page when input invalid url
* [KYLIN-1250] - Got NPE when discarding a job
* [KYLIN-1260] - Job status labels are not in same style
* [KYLIN-1269] - Can not get last error message in email
* [KYLIN-1271] - Create streaming table layer will disappear if click on outside
* [KYLIN-1274] - Query from JDBC is partial results by default
* [KYLIN-1282] - Comparison filter on Date/Time column not work for query
* [KYLIN-1289] - Click on subsequent wizard steps doesn't work when editing existing cube or model
* [KYLIN-1303] - Error when in-mem cubing on empty data source which has boolean columns
* [KYLIN-1306] - Null strings are not applied during fast cubing
* [KYLIN-1314] - Display issue for aggression groups
* [KYLIN-1315] - UI: Cannot add normal dimension when creating new cube
* [KYLIN-1316] - Wrong label in Dialog CUBE REFRESH CONFIRM
* [KYLIN-1328] - "UnsupportedOperationException" is thrown when remove a data model
* [KYLIN-1330] - UI create model: Press enter will go back to pre step
* [KYLIN-1336] - 404 errors of model page and api 'access/DataModelDesc' in console
* [KYLIN-1337] - Sort cube name doesn't work well
* [KYLIN-1346] - IllegalStateException happens in SparkCubing
* [KYLIN-1347] - UI: cannot place cursor in front of the last dimension
* [KYLIN-1349] - 'undefined' is logged in console when adding lookup table
* [KYLIN-1352] - 'Cache already exists' exception in high-concurrency query situation
* [KYLIN-1356] - use exec-maven-plugin for IT environment provision
* [KYLIN-1357] - Cloned cube has build time information
* [KYLIN-1372] - Query using PrepareStatement failed with multi OR clause
* [KYLIN-1382] - CubeMigrationCLI reports error when migrate cube
* [KYLIN-1387] - Streaming cubing doesn't generate cuboids files on HDFS, cause cube merge failure
* [KYLIN-1396] - minor bug in BigDecimalSerializer - avoidVerbose should be incremented each time when input scale is larger than given scale
* [KYLIN-1400] - kylin.metadata.url with hbase namespace problem
* [KYLIN-1402] - StringIndexOutOfBoundsException in Kylin Hive Column Cardinality Job
* [KYLIN-1412] - Widget width of "Partition date column" is too small to select
* [KYLIN-1413] - Row key column's sequence is wrong after saving the cube
* [KYLIN-1414] - Couldn't drag and drop rowkey, js error is thrown in browser console
* [KYLIN-1417] - TimedJsonStreamParser is case sensitive for message's property name
* [KYLIN-1419] - NullPointerException occurs when query from subqueries with order by
* [KYLIN-1420] - Query returns empty result on partition column's boundary condition
* [KYLIN-1421] - Cube "source record" is always zero for streaming
* [KYLIN-1423] - HBase size precision issue
* [KYLIN-1430] - Not add "STREAMING_" prefix when import a streaming table
* [KYLIN-1443] - For setting Auto Merge Time Ranges, before sending them to backend, the related time ranges should be sorted increasingly
* [KYLIN-1456] - Shouldn't use "1970-01-01" as the default end date
* [KYLIN-1471] - LIMIT after having clause should not be pushed down to storage context
* 
* [KYLIN-1104] - Long dimension value cause ArrayIndexOutOfBoundsException
* [KYLIN-1331] - UI Delete Aggregation Groups: cursor disappeared after delete 1 dimension
* [KYLIN-1344] - Bitmap measure defined after TopN measure can cause merge to fail
* [KYLIN-1356] - use exec-maven-plugin for IT environment provision
* [KYLIN-1386] - Duplicated projects appear in connection dialog after clicking CONNECT button multiple times
* [KYLIN-1396] - minor bug in BigDecimalSerializer - avoidVerbose should be incremented each time when input scale is larger than given scale
* [KYLIN-1419] - NullPointerException occurs when query from subqueries with order by
* [KYLIN-1445] - Kylin should throw error if HIVE_CONF dir cannot be found
* [KYLIN-1466] - Some environment variables are not used in bin/kylin.sh <RUNNABLE_CLASS_NAME>
* [KYLIN-1469] - Hive dependency jars are hard coded in test
* [KYLIN-1471] - LIMIT after having clause should not be pushed down to storage context
* [KYLIN-1473] - Cannot have comments in the end of New Query textbox

__Task__

* [KYLIN-529] - Migrate ODBC source code to Apache Git
* [KYLIN-650] - Move all document from github wiki to code repository (using md file)
* [KYLIN-762] - remove quartz dependency
* [KYLIN-763] - remove author name
* [KYLIN-820] - support streaming cube of exact timestamp range
* [KYLIN-907] - Improve Kylin community development experience
* [KYLIN-1112] - Reorganize InvertedIndex source codes into plug-in architecture
	
* [KYLIN-808] - streaming cubing support split by data timestamp
* [KYLIN-1427] - Enable partition date column to support date and hour as separate columns for increment cube build

__Test__

* [KYLIN-677] - benchmark for Endpoint without dictionary
* [KYLIN-826] - create new test case for streaming building & queries


## v1.3.0 - 2016-03-14
_Tag:_ [kylin-1.3.0](https://github.com/apache/kylin/tree/kylin-1.3.0)

__New Feature__

* [KYLIN-579] - Unload table from Kylin
* [KYLIN-976] - Support Custom Aggregation Types
* [KYLIN-1054] - Support Hive client Beeline
* [KYLIN-1128] - Clone Cube Metadata
* [KYLIN-1186] - Support precise Count Distinct using bitmap (under limited conditions)

__Improvement__

* [KYLIN-955] - HiveColumnCardinalityJob should use configurations in conf/kylin_job_conf.xml
* [KYLIN-1014] - Support kerberos authentication while getting status from RM
* [KYLIN-1074] - Load hive tables with selecting mode
* [KYLIN-1082] - Hive dependencies should be add to tmpjars
* [KYLIN-1132] - make filtering input easier in creating cube
* [KYLIN-1201] - Enhance project level ACL
* [KYLIN-1211] - Add 'Enable Cache' button in System page
* [KYLIN-1234] - Cube ACL does not work
* [KYLIN-1240] - Fix link and typo in README
* [KYLIN-1244] - In query window, enable fast copy&paste by double clicking tables/columns' names.
* [KYLIN-1246] - get cubes API update - offset,limit not required
* [KYLIN-1251] - add toggle event for tree label
* [KYLIN-1259] - Change font/background color of job progress
* [KYLIN-1312] - Enhance DeployCoprocessorCLI to support Cube level filter
* [KYLIN-1317] - Kill underlying running hadoop job while discard a job
* [KYLIN-1323] - Improve performance of converting data to hfile
* [KYLIN-1333] - Kylin Entity Permission Control 
* [KYLIN-1343] - Upgrade calcite version to 1.6
* [KYLIN-1365] - Kylin ACL enhancement
* [KYLIN-1368] - JDBC Driver is not generic to restAPI json result

__Bug__

* [KYLIN-918] - Calcite throws "java.lang.Float cannot be cast to java.lang.Double" error while executing SQL
* [KYLIN-1075] - select [MeasureCol] from [FactTbl] is not supported
* [KYLIN-1078] - Cannot have comments in the end of New Query textbox
* [KYLIN-1104] - Long dimension value cause ArrayIndexOutOfBoundsException
* [KYLIN-1110] - can not see project options after clear brower cookie and cache
* [KYLIN-1159] - problem about kylin web UI
* [KYLIN-1214] - Remove "Back to My Cubes" link in non-edit mode
* [KYLIN-1215] - minor, update website member's info on community page
* [KYLIN-1230] - When CubeMigrationCLI copied ACL from one env to another, it may not work
* [KYLIN-1236] - redirect to home page when input invalid url
* [KYLIN-1250] - Got NPE when discarding a job
* [KYLIN-1254] - cube model will be overridden while creating a new cube with the same name
* [KYLIN-1260] - Job status labels are not in same style
* [KYLIN-1274] - Query from JDBC is partial results by default
* [KYLIN-1316] - Wrong label in Dialog CUBE REFRESH CONFIRM
* [KYLIN-1330] - UI create model: Press enter will go back to pre step
* [KYLIN-1331] - UI Delete Aggregation Groups: cursor disappeared after delete 1 dimension
* [KYLIN-1342] - Typo in doc
* [KYLIN-1354] - Couldn't edit a cube if it has no "partition date" set
* [KYLIN-1372] - Query using PrepareStatement failed with multi OR clause
* [KYLIN-1396] - minor bug in BigDecimalSerializer - avoidVerbose should be incremented each time when input scale is larger than given scale 
* [KYLIN-1400] - kylin.metadata.url with hbase namespace problem
* [KYLIN-1402] - StringIndexOutOfBoundsException in Kylin Hive Column Cardinality Job
* [KYLIN-1412] - Widget width of "Partition date column"  is too small to select
* [KYLIN-1419] - NullPointerException occurs when query from subqueries with order by
* [KYLIN-1423] - HBase size precision issue
* [KYLIN-1443] - For setting Auto Merge Time Ranges, before sending them to backend, the related time ranges should be sorted increasingly
* [KYLIN-1445] - Kylin should throw error if HIVE_CONF dir cannot be found
* [KYLIN-1456] - Shouldn't use "1970-01-01" as the default end date
* [KYLIN-1466] - Some environment variables are not used in bin/kylin.sh <RUNNABLE_CLASS_NAME>
* [KYLIN-1469] - Hive dependency jars are hard coded in test

__Test__

* [KYLIN-1335] - Disable PrintResult in KylinQueryTest


## v1.2 - 2015-12-15
_Tag:_ [kylin-1.2](https://github.com/apache/kylin/tree/kylin-1.2)

__New Feature__

* [KYLIN-596] - Support Excel and Power BI
    
__Improvement__

* [KYLIN-389] - Can't edit cube name for existing cubes
* [KYLIN-702] - When Kylin create the flat hive table, it generates large number of small files in HDFS 
* [KYLIN-1021] - upload dependent jars of kylin to HDFS and set tmpjars
* [KYLIN-1058] - Remove "right join" during model creation
* [KYLIN-1064] - restore disabled queries in KylinQueryTest.testVerifyQuery
* [KYLIN-1065] - ODBC driver support tableau 9.1
* [KYLIN-1069] - update tip for 'Partition Column' on UI
* [KYLIN-1081] - ./bin/find-hive-dependency.sh may not find hive-hcatalog-core.jar
* [KYLIN-1095] - Update AdminLTE to latest version
* [KYLIN-1099] - Support dictionary of cardinality over 10 millions
* [KYLIN-1101] - Allow "YYYYMMDD" as a date partition column
* [KYLIN-1105] - Cache in AbstractRowKeyEncoder.createInstance() is useless
* [KYLIN-1119] - refine find-hive-dependency.sh to correctly get hcatalog path
* [KYLIN-1139] - Hive job not starting due to error "conflicting lock present for default mode EXCLUSIVE "
* [KYLIN-1149] - When yarn return an incomplete job tracking URL, Kylin will fail to get job status
* [KYLIN-1154] - Load job page is very slow when there are a lot of history job
* [KYLIN-1157] - CubeMigrationCLI doesn't copy ACL
* [KYLIN-1160] - Set default logger appender of log4j for JDBC
* [KYLIN-1161] - Rest API /api/cubes?cubeName=  is doing fuzzy match instead of exact match
* [KYLIN-1162] - Enhance HadoopStatusGetter to be compatible with YARN-2605
* [KYLIN-1166] - CubeMigrationCLI should disable and purge the cube in source store after be migrated
* [KYLIN-1168] - Couldn't save cube after doing some modification, get "Update data model is not allowed! Please create a new cube if needed" error
* [KYLIN-1190] - Make memory budget per query configurable

__Bug__

* [KYLIN-693] - Couldn't change a cube's name after it be created
* [KYLIN-930] - can't see realizations under each project at project list page
* [KYLIN-966] - When user creates a cube, if enter a name which already exists, Kylin will thrown expection on last step
* [KYLIN-1033] - Error when joining two sub-queries
* [KYLIN-1039] - Filter like (A or false) yields wrong result
* [KYLIN-1067] - Support get MapReduce Job status for ResourceManager HA Env
* [KYLIN-1070] - changing  case in table name in  model desc
* [KYLIN-1093] - Consolidate getCurrentHBaseConfiguration() and newHBaseConfiguration() in HadoopUtil
* [KYLIN-1098] - two "kylin.hbase.region.count.min" in conf/kylin.properties
* [KYLIN-1106] - Can not send email caused by Build Base Cuboid Data step failed
* [KYLIN-1108] - Return Type Empty When Measure-> Count In Cube Design
* [KYLIN-1120] - MapReduce job read local meta issue
* [KYLIN-1121] - ResourceTool download/upload does not work in binary package
* [KYLIN-1140] - Kylin's sample cube "kylin_sales_cube" couldn't be saved.
* [KYLIN-1148] - Edit project's name and cancel edit, project's name still modified
* [KYLIN-1152] - ResourceStore should read content and timestamp in one go
* [KYLIN-1155] - unit test with minicluster doesn't work on 1.x
* [KYLIN-1203] - Cannot save cube after correcting the configuration mistake
* [KYLIN-1205] - hbase RpcClient java.io.IOException: Unexpected closed connection
* [KYLIN-1216] - Can't parse DateFormat like 'YYYYMMDD' correctly in query

__Task__

* [KYLIN-1170] - Update website and status files to TLP


## v1.1.1-incubating - 2015-11-04
_Tag:_ [kylin-1.1.1-incubating](https://github.com/apache/kylin/tree/kylin-1.1.1-incubating)

__Improvement__

* [KYLIN-999] - License check and cleanup for release

## v1.1-incubating - 2015-10-25
_Tag:_ [kylin-1.1-incubating](https://github.com/apache/kylin/tree/kylin-1.1-incubating)

__New Feature__

* [KYLIN-222] - Web UI to Display CubeInstance Information
* [KYLIN-906] - cube retention
* [KYLIN-910] - Allow user to enter "retention range" in days on Cube UI

__Bug__

* [KYLIN-457] - log4j error and dup lines in kylin.log
* [KYLIN-632] - "kylin.sh stop" doesn't check whether KYLIN_HOME was set
* [KYLIN-740] - Slowness with many IN() values
* [KYLIN-747] - bad query performance when IN clause contains a value doesn't exist in the dictionary
* [KYLIN-771] - query cache is not evicted when metadata changes
* [KYLIN-797] - Cuboid cache will cache massive invalid cuboid if existed many cubes which already be deleted 
* [KYLIN-847] - "select * from fact" does not work on 0.7 branch
* [KYLIN-913] - Cannot find rowkey column XXX in cube CubeDesc
* [KYLIN-918] - Calcite throws "java.lang.Float cannot be cast to java.lang.Double" error while executing SQL
* [KYLIN-944] - update doc about how to consume kylin API in javascript
* [KYLIN-950] - Web UI "Jobs" tab view the job reduplicated
* [KYLIN-952] - User can trigger a Refresh job on an non-existing cube segment via REST API
* [KYLIN-958] - update cube data model may fail and leave metadata in inconsistent state
* [KYLIN-961] - Can't get cube  source record count.
* [KYLIN-967] - Dump running queries on memory shortage
* [KYLIN-968] - CubeSegment.lastBuildJobID is null in new instance but used for rowkey_stats path
* [KYLIN-975] - change kylin.job.hive.database.for.intermediatetable cause job to fail
* [KYLIN-978] - GarbageCollectionStep dropped Hive Intermediate Table but didn't drop external hdfs path
* [KYLIN-982] - package.sh should grep out "Download*" messages when determining version
* [KYLIN-983] - Query sql offset keyword bug
* [KYLIN-985] - Don't suppoprt aggregation AVG while executing SQL
* [KYLIN-1001] - Kylin generates wrong HDFS path in creating intermediate table
* [KYLIN-1004] - Dictionary with '' value cause cube merge to fail
* [KYLIN-1005] - fail to acquire ZookeeperJobLock when hbase.zookeeper.property.clientPort is configured other than 2181
* [KYLIN-1015] - Hive dependency jars appeared twice on job configuration
* [KYLIN-1020] - Although "kylin.query.scan.threshold" is set, it still be restricted to less than 4 million 
* [KYLIN-1026] - Error message for git check is not correct in package.sh

__Improvement__

* [KYLIN-343] - Enable timeout on query 
* [KYLIN-367] - automatically backup metadata everyday
* [KYLIN-589] - Cleanup Intermediate hive table after cube build
* [KYLIN-772] - Continue cube job when hive query return empty resultset
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
* [KYLIN-1037] - Remove hardcoded "hdp.version" from regression tests
* [KYLIN-1047] - Upgrade to Calcite 1.4
* [KYLIN-1048] - CPU and memory killer in Cuboid.findById()
* [KYLIN-1061] - "kylin.sh start" should check whether kylin has already been running
* [KYLIN-1048] - CPU and memory killer in Cuboid.findById()
* [KYLIN-1061] - "kylin.sh start" should check whether kylin has already been running


## v1.0-incubating - 2015-09-06
_Tag:_ [kylin-1.0-incubating](https://github.com/apache/kylin/tree/kylin-1.0-incubating)

__New Feature__

* [KYLIN-591] - Leverage Zeppelin to interactive with Kylin

__Bug__

* [KYLIN-404] - Can't get cube source record size.
* [KYLIN-626] - JDBC error for float and double values
* [KYLIN-751] - Max on negative double values is not working
* [KYLIN-757] - Cache wasn't flushed in cluster mode
* [KYLIN-780] - Upgrade Calcite to 1.0
* [KYLIN-805] - Drop useless Hive intermediate table and HBase tables in the last step of cube build/merge
* [KYLIN-889] - Support more than one HDFS files of lookup table
* [KYLIN-897] - Update CubeMigrationCLI to copy data model info
* [KYLIN-898] - "CUBOID_CACHE" in Cuboid.java never flushes
* [KYLIN-911] - NEW segments not DELETED when cancel BuildAndMerge Job
* [KYLIN-912] - $KYLIN_HOME/tomcat/temp folder takes much disk space after long run
* [KYLIN-914] - Scripts shebang should use /bin/bash
* [KYLIN-915] - appendDBName in CubeMetadataUpgrade will return null
* [KYLIN-921] - Dimension with all nulls cause BuildDimensionDictionary failed due to FileNotFoundException
* [KYLIN-923] - FetcherRunner will never run again if encountered exception during running
* [KYLIN-929] - can not sort cubes by [Source Records] at cubes list page
* [KYLIN-934] - Negative number in SUM result and Kylin results not matching exactly Hive results
* [KYLIN-935] - always loading when try to view the log of the sub-step of cube build job
* [KYLIN-936] - can not see job step log 
* [KYLIN-940] - NPE when close the null resouce
* [KYLIN-945] - Kylin JDBC - Get Connection from DataSource results in NullPointerException
* [KYLIN-946] - [UI] refresh page show no results when Project selected as [--Select All--]
* [KYLIN-949] - Query cache doesn't work properly for prepareStatement queries

__Improvement__

* [KYLIN-568] - job support stop/suspend function so that users can manually resume a job
* [KYLIN-717] - optimize OLAPEnumerator.convertCurrentRow()
* [KYLIN-792] - kylin performance insight [dashboard]
* [KYLIN-838] - improve performance of job query
* [KYLIN-842] - Add version and commit id into binary package
* [KYLIN-844] - add backdoor toggles to control query behavior 
* [KYLIN-857] - backport coprocessor improvement in 0.8 to 0.7
* [KYLIN-866] - Confirm with user when he selects empty segments to merge
* [KYLIN-867] - Hybrid model for multiple realizations/cubes
* [KYLIN-880] -  Kylin should change the default folder from /tmp to user configurable destination
* [KYLIN-881] - Upgrade Calcite to 1.3.0
* [KYLIN-883] - Using configurable option for Hive intermediate tables created by Kylin job
* [KYLIN-893] - Remove the dependency on quartz and metrics
* [KYLIN-922] - Enforce same code style for both intellij and eclipse user
* [KYLIN-926] - Make sure Kylin leaves no garbage files in local OS and HDFS/HBASE
* [KYLIN-933] - friendly UI to use data model
* [KYLIN-938] - add friendly tip to page when rest request failed

__Task__

* [KYLIN-884] - Restructure docs and website
* [KYLIN-907] - Improve Kylin community development experience
* [KYLIN-954] - Release v1.0 (formerly v0.7.3)
* [KYLIN-863] - create empty segment when there is no data in one single streaming batch
* [KYLIN-908] - Help community developer to setup develop/debug environment
* [KYLIN-931] - Port KYLIN-921 to 0.8 branch

## v0.7.2-incubating - 2015-07-21
_Tag:_ [kylin-0.7.2-incubating](https://github.com/apache/kylin/tree/kylin-0.7.2-incubating)

__Main Changes:__  
Critical bug fixes after v0.7.1 release, please go with this version directly for new case and upgrade to this version for existing deployment.

__Bug__  

* [KYLIN-514] - Error message is not helpful to user when doing something in Jason Editor window
* [KYLIN-598] - Kylin detecting hive table delim failure
* [KYLIN-660] - Make configurable of dictionary cardinality cap
* [KYLIN-765] - When a cube job is failed, still be possible to submit a new job
* [KYLIN-814] - Duplicate columns error for subqueries on fact table
* [KYLIN-819] - Fix necessary ColumnMetaData order for Calcite (Optic)
* [KYLIN-824] - Cube Build fails if lookup table doesn't have any files under HDFS location
* [KYLIN-829] - Cube "Actions" shows "NA"; but after expand the "access" tab, the button shows up
* [KYLIN-830] - Cube merge failed after migrating from v0.6 to v0.7
* [KYLIN-831] - Kylin report "Column 'ABC' not found in table 'TABLE' while executing SQL", when that column is FK but not define as a dimension
* [KYLIN-840] - HBase table compress not enabled even LZO is installed
* [KYLIN-848] - Couldn't resume or discard a cube job
* [KYLIN-849] - Couldn't query metrics on lookup table PK
* [KYLIN-865] - Cube has been built but couldn't query; In log it said "Realization 'CUBE.CUBE_NAME' defined under project PROJECT_NAME is not found
* [KYLIN-873] - cancel button does not work when [resume][discard] job
* [KYLIN-888] - "Jobs" page only shows 15 job at max, the "Load more" button was disappeared

__Improvement__

* [KYLIN-159] - Metadata migrate tool 
* [KYLIN-199] - Validation Rule: Unique value of Lookup table's key columns
* [KYLIN-207] - Support SQL pagination
* [KYLIN-209] - Merge tail small MR jobs into one
* [KYLIN-210] - Split heavy MR job to more small jobs
* [KYLIN-221] - Convert cleanup and GC to job 
* [KYLIN-284] - add log for all Rest API Request
* [KYLIN-488] - Increase HDFS block size 1GB
* [KYLIN-600] - measure return type update
* [KYLIN-611] - Allow Implicit Joins
* [KYLIN-623] - update Kylin UI Style to latest AdminLTE
* [KYLIN-727] - Cube build in BuildCubeWithEngine does not cover incremental build/cube merge
* [KYLIN-752] - Improved IN clause performance
* [KYLIN-773] - performance is slow list jobs
* [KYLIN-839] - Optimize Snapshot table memory usage 

__New Feature__

* [KYLIN-211] - Bitmap Inverted Index
* [KYLIN-285] - Enhance alert program for whole system
* [KYLIN-467] - Validataion Rule: Check duplicate rows in lookup table
* [KYLIN-471] - Support "Copy" on grid result

__Task__

* [KYLIN-7] - Enable maven checkstyle plugin
* [KYLIN-885] - Release v0.7.2
* [KYLIN-812] - Upgrade to Calcite 0.9.2

## v0.7.1-incubating (First Apache Release) - 2015-06-10  
_Tag:_ [kylin-0.7.1-incubating](https://github.com/apache/kylin/tree/kylin-0.7.1-incubating)

Apache Kylin v0.7.1-incubating has rolled out on June 10, 2015. This is also the first Apache release after join incubating. 

__Main Changes:__

* Package renamed from com.kylinolap to org.apache.kylin
* Code cleaned up to apply Apache License policy
* Easy install and setup with bunch of scripts and automation
* Job engine refactor to be generic job manager for all jobs, and improved efficiency
* Support Hive database other than 'default'
* JDBC driver avaliable for client to interactive with Kylin server
* Binary pacakge avaliable download 

__New Feature__

* [KYLIN-327] - Binary distribution 
* [KYLIN-368] - Move MailService to Common module
* [KYLIN-540] - Data model upgrade for legacy cube descs
* [KYLIN-576] - Refactor expansion rate expression

__Task__

* [KYLIN-361] - Rename package name with Apache Kylin
* [KYLIN-531] - Rename package name to org.apache.kylin
* [KYLIN-533] - Job Engine Refactoring
* [KYLIN-585] - Simplify deployment
* [KYLIN-586] - Add Apache License header in each source file
* [KYLIN-587] - Remove hard copy of javascript libraries
* [KYLIN-624] - Add dimension and metric info into DataModel
* [KYLIN-650] - Move all document from github wiki to code repository (using md file)
* [KYLIN-669] - Release v0.7.1 as first apache release
* [KYLIN-670] - Update pom with "incubating" in version number
* [KYLIN-737] - Generate and sign release package for review and vote
* [KYLIN-795] - Release after success vote

__Bug__

* [KYLIN-132] - Job framework
* [KYLIN-194] - Dict & ColumnValueContainer does not support number comparison, they do string comparison right now
* [KYLIN-220] - Enable swap column of Rowkeys in Cube Designer
* [KYLIN-230] - Error when create HTable
* [KYLIN-255] - Error when a aggregated function appear twice in select clause
* [KYLIN-383] - Sample Hive EDW database name should be replaced by "default" in the sample
* [KYLIN-399] - refreshed segment not correctly published to cube
* [KYLIN-412] - No exception or message when sync up table which can't access
* [KYLIN-421] - Hive table metadata issue
* [KYLIN-436] - Can't sync Hive table metadata from other database rather than "default"
* [KYLIN-508] - Too high cardinality is not suitable for dictionary!
* [KYLIN-509] - Order by on fact table not works correctly
* [KYLIN-517] - Always delete the last one of Add Lookup page buttom even if deleting the first join condition
* [KYLIN-524] - Exception will throw out if dimension is created on a lookup table, then deleting the lookup table.
* [KYLIN-547] - Create cube failed if column dictionary sets false and column length value greater than 0
* [KYLIN-556] - error tip enhance when cube detail return empty
* [KYLIN-570] - Need not to call API before sending login request
* [KYLIN-571] - Dimensions lost when creating cube though Joson Editor
* [KYLIN-572] - HTable size is wrong
* [KYLIN-581] - unable to build cube
* [KYLIN-583] - Dependency of Hive conf/jar in II branch will affect auto deploy
* [KYLIN-588] - Error when run package.sh
* [KYLIN-593] - angular.min.js.map and angular-resource.min.js.map are missing in kylin.war
* [KYLIN-594] - Making changes in build and packaging with respect to apache release process
* [KYLIN-595] - Kylin JDBC driver should not assume Kylin server listen on either 80 or 443
* [KYLIN-605] - Issue when install Kylin on a CLI which does not have yarn Resource Manager
* [KYLIN-614] - find hive dependency shell fine is unable to set the hive dependency correctly
* [KYLIN-615] - Unable add measures in Kylin web UI
* [KYLIN-619] - Cube build fails with hive+tez
* [KYLIN-620] - Wrong duration number
* [KYLIN-621] - SecurityException when running MR job
* [KYLIN-627] - Hive tables' partition column was not sync into Kylin
* [KYLIN-628] - Couldn't build a new created cube
* [KYLIN-629] - Kylin failed to run mapreduce job if there is no mapreduce.application.classpath in mapred-site.xml
* [KYLIN-630] - ArrayIndexOutOfBoundsException when merge cube segments 
* [KYLIN-638] - kylin.sh stop not working
* [KYLIN-639] - Get "Table 'xxxx' not found while executing SQL" error after a cube be successfully built
* [KYLIN-640] - sum of float not working
* [KYLIN-642] - Couldn't refresh cube segment
* [KYLIN-643] - JDBC couldn't connect to Kylin: "java.sql.SQLException: Authentication Failed"
* [KYLIN-644] - join table as null error when build the cube
* [KYLIN-652] - Lookup table alias will be set to null
* [KYLIN-657] - JDBC Driver not register into DriverManager
* [KYLIN-658] - java.lang.IllegalArgumentException: Cannot find rowkey column XXX in cube CubeDesc
* [KYLIN-659] - Couldn't adjust the rowkey sequence when create cube
* [KYLIN-666] - Select float type column got class cast exception
* [KYLIN-681] - Failed to build dictionary if the rowkey's dictionary property is "date(yyyy-mm-dd)"
* [KYLIN-682] - Got "No aggregator for func 'MIN' and return type 'decimal(19,4)'" error when build cube
* [KYLIN-684] - Remove holistic distinct count and multiple column distinct count from sample cube
* [KYLIN-691] - update tomcat download address in download-tomcat.sh
* [KYLIN-696] - Dictionary couldn't recognize a value and throw IllegalArgumentException: "Not a valid value"
* [KYLIN-703] - UT failed due to unknown host issue
* [KYLIN-711] - UT failure in REST module
* [KYLIN-739] - Dimension as metrics does not work with PK-FK derived column
* [KYLIN-761] - Tables are not shown in the "Query" tab, and couldn't run SQL query after cube be built

__Improvement__

* [KYLIN-168] - Installation fails if multiple ZK
* [KYLIN-182] - Validation Rule: columns used in Join condition should have same datatype
* [KYLIN-204] - Kylin web not works properly in IE
* [KYLIN-217] - Enhance coprocessor with endpoints 
* [KYLIN-251] - job engine refactoring
* [KYLIN-261] - derived column validate when create cube
* [KYLIN-317] - note: grunt.json need to be configured when add new javascript or css file
* [KYLIN-324] - Refactor metadata to support InvertedIndex
* [KYLIN-407] - Validation: There's should no Hive table column using "binary" data type
* [KYLIN-445] - Rename cube_desc/cube folder
* [KYLIN-452] - Automatically create local cluster for running tests
* [KYLIN-498] - Merge metadata tables 
* [KYLIN-532] - Refactor data model in kylin front end
* [KYLIN-539] - use hbase command to launch tomcat
* [KYLIN-542] - add project property feature for cube
* [KYLIN-553] - From cube instance, couldn't easily find the project instance that it belongs to
* [KYLIN-563] - Wrap kylin start and stop with a script 
* [KYLIN-567] - More flexible validation of new segments
* [KYLIN-569] - Support increment+merge job
* [KYLIN-578] - add more generic configuration for ssh
* [KYLIN-601] - Extract content from kylin.tgz to "kylin" folder
* [KYLIN-616] - Validation Rule: partition date column should be in dimension columns
* [KYLIN-634] - Script to import sample data and cube metadata
* [KYLIN-636] - wiki/On-Hadoop-CLI-installation is not up to date
* [KYLIN-637] - add start&end date for hbase info in cubeDesigner
* [KYLIN-714] - Add Apache RAT to pom.xml
* [KYLIN-753] - Make the dependency on hbase-common to "provided"
* [KYLIN-758] - Updating port forwarding issue Hadoop Installation on Hortonworks Sandbox.
* [KYLIN-779] - [UI] jump to cube list after create cube
* [KYLIN-796] - Add REST API to trigger storage cleanup/GC

__Wish__

* [KYLIN-608] - Distinct count for ii storage

