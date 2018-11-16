---
layout: docs
title:  Kylin Release 1.5.0
categories: releases
permalink: /docs/releases/kylin_release_1_5_0.html
---

_Tag:_ [kylin-1.5.0](https://github.com/apache/kylin/tree/kylin-1.5.0)


__This version is not backward compatible.__ The format of Cube and metadata has been refactored in order to get times of performance improvement. We recommend this version, but does not suggest upgrade from previous deployment directly. A clean and new deployment of this version is strongly recommended. If you have to upgrade from previous deployment, an upgrade guide will be provided by community later.



### Highlights

* [KYLIN-875] - A plugin-able architecture, to allow alternative cube engine / storage engine / data source.
* [KYLIN-1245] - A better MR cubing algorithm, about 1.5 times faster by comparing hundreds of jobs.
* [KYLIN-942] - A better storage engine, makes query roughly 2 times faster (especially for slow queries) by comparing tens of thousands SQLs.
* [KYLIN-738] - Streaming cubing EXPERIMENTAL support, source from Kafka, build cube in-mem at minutes interval.
* [KYLIN-242] - Redesign aggregation group, support of 20+ dimensions made easy.
* [KYLIN-976] - Custom aggregation types (or UDF in other words).
* [KYLIN-943] - TopN aggregation type.
* [KYLIN-1065] - ODBC compatible with Tableau 9.1, MS Excel, MS PowerBI.
* [KYLIN-1219] - Kylin support SSO with Spring SAML.



### New Feature

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



### Improvement

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
* [KYLIN-721] - streaming cli support third-party stream-message parser
* [KYLIN-726] - add remote cli port configuration for KylinConfig
* [KYLIN-729] - IIEndpoint eliminate the non-aggregate routine
* [KYLIN-734] - Push cache layer to each storage engine
* [KYLIN-752] - Improved IN clause performance
* [KYLIN-753] - Make the dependency on hbase-common to "provided"
* [KYLIN-755] - extract copying libs from prepare.sh so that it can be reused
* [KYLIN-760] - Improve the hashing performance in Sampling cuboid size
* [KYLIN-772] - Continue cube job when hive query return empty resultset
* [KYLIN-773] - performance is slow list jobs
* [KYLIN-783] - update hdp version in test cases to 2.2.4
* [KYLIN-796] - Add REST API to trigger storage cleanup/GC
* [KYLIN-809] - Streaming cubing allow multiple Kafka clusters/topics
* [KYLIN-816] - Allow gap in cube segments, for streaming case
* [KYLIN-822] - list cube overview in one page
* [KYLIN-823] - replace FK on fact table on rowkey & aggregation group generate
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
* [KYLIN-879] - add a tool to collect orphan HBase
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
* [KYLIN-973] - add a tool to analyze streaming output logs
* [KYLIN-984] - Behavior change in streaming data consuming
* [KYLIN-987] - Rename 0.7-staging and 0.8 branch
* [KYLIN-1014] - Support Kerberos authentication while getting status from RM
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
* [KYLIN-1239] - attribute each HTable with team contact and owner name
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
* [KYLIN-1323] - Improve performance of converting data to HFile
* [KYLIN-1327] - Tool for batch updating host information of HTables
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
* [KYLIN-1239] - attribute each HTable with team contact and owner name
* [KYLIN-1245] - Switch between layer cubing and in-mem cubing according to stats
* [KYLIN-1265] - Make sure 1.4-rc query is no slower than 1.0
* [KYLIN-1266] - Tune release package size
* [KYLIN-1270] - improve TimedJsonStreamParser to support month_start,quarter_start,year_start
* [KYLIN-1283] - Replace GTScanRequest's SerDer form Kryo to manual
* [KYLIN-1297] - Diagnose query performance issues in 1.4 branch
* [KYLIN-1301] - fix segment pruning failure
* [KYLIN-1308] - query storage v2 enable parallel cube visiting
* [KYLIN-1318] - enable gc log for kylin server instance
* [KYLIN-1327] - Tool for batch updating host information of HTables
* [KYLIN-1343] - Upgrade calcite version to 1.6
* [KYLIN-1350] - hbase Result.binarySearch is found to be problematic in concurrent environments
* [KYLIN-1366] - Bind metadata version with release version
* [KYLIN-1389] - Formatting ODBC Drive C++ code
* [KYLIN-1405] - Aggregation group validation
* [KYLIN-1465] - Beautify kylin log to convenience both production trouble shooting and CI debugging
* [KYLIN-1475] - Inject ehcache manager for any test case that will touch ehcache manager



### Task

* [KYLIN-529] - Migrate ODBC source code to Apache Git
* [KYLIN-650] - Move all document from GitHub wiki to code repository (using md file)
* [KYLIN-762] - remove quartz dependency
* [KYLIN-763] - remove author name
* [KYLIN-820] - support streaming cube of exact timestamp range
* [KYLIN-907] - Improve Kylin community development experience
* [KYLIN-1112] - Reorganize InvertedIndex source codes into plug-in architecture
* [KYLIN-808] - streaming cubing support split by data timestamp
* [KYLIN-1427] - Enable partition date column to support date and hour as separate columns for increment cube build



### Bug

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
* [KYLIN-944] - update doc about how to consume kylin API in JavaScript
* [KYLIN-946] - [UI] refresh page show no results when Project selected as [--Select All--]
* [KYLIN-950] - Web UI "Jobs" tab view the job reduplicated
* [KYLIN-951] - Drop RowBlock concept from GridTable general API
* [KYLIN-952] - User can trigger a Refresh job on an non-existing cube segment via REST API
* [KYLIN-967] - Dump running queries on memory shortage
* [KYLIN-975] - change kylin.job.hive.database.for.intermediatetable cause job to fail
* [KYLIN-978] - GarbageCollectionStep dropped Hive Intermediate Table but didn't drop external hdfs path
* [KYLIN-982] - package.sh should grep out "Download\*" messages when determining version
* [KYLIN-983] - Query sql offset keyword bug
* [KYLIN-985] - Don't support aggregation AVG while executing SQL
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



### Test

* [KYLIN-677] - benchmark for Endpoint without dictionary
* [KYLIN-826] - create new test case for streaming building & queries