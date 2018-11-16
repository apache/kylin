---
layout: docs
title:  Kylin Release 2.0.0
categories: releases
permalink: /docs/releases/kylin_release_2_0_0.html
---

_Tag:_ [kylin-2.0.0](https://github.com/apache/kylin/tree/kylin-2.0.0)


This is a major release with **Spark Cubing**, **Snowflake Data Model** and runs **TPC-H Benchmark**. 



### New Feature

* [KYLIN-744] - Spark Cube Build Engine
* [KYLIN-2006] - Make job engine distributed and HA
* [KYLIN-2031] - New Fix_length_Hex encoding to support hash value and better Integer encoding to support negative value
* [KYLIN-2180] - Add project config and make config priority become "cube > project > server"
* [KYLIN-2240] - Add a toggle to ignore all cube signature inconsistency temporally
* [KYLIN-2317] - Hybrid Cube CLI Tools
* [KYLIN-2331] - By layer Spark cubing
* [KYLIN-2351] - Support Cloud DFS as kylin.env.hdfs-working-dir
* [KYLIN-2388] - Hot load kylin config from web
* [KYLIN-2394] - Upgrade Calcite to 1.11 and Avatica to 1.9
* [KYLIN-2396] - Percentile pre-aggregation implementation



### Improvements

* [KYLIN-227] - Support "Pause" on Kylin Job
* [KYLIN-490] - Support multiple column distinct count
* [KYLIN-995] - Enable kylin to support joining the same lookup table more than once
* [KYLIN-1832] - HyperLogLog codec performance improvement
* [KYLIN-1875] - Snowflake schema support
* [KYLIN-1971] - Cannot support columns with same name under different table
* [KYLIN-2029] - lookup table support count(distinct column)
* [KYLIN-2030] - lookup table support group by primary key when no derived dimension
* [KYLIN-2096] - Support "select version()" SQL statement
* [KYLIN-2131] - Load Kafka client configuration from properties files
* [KYLIN-2133] - Check web server port availability when startup
* [KYLIN-2135] - Enlarge FactDistinctColumns reducer number
* [KYLIN-2136] - Enhance cubing algorithm selection
* [KYLIN-2141] - Add include/exclude interface for ResourceTool
* [KYLIN-2144] - move useful operation tools to org.apache.kylin.tool
* [KYLIN-2163] - Refine kylin scripts, less verbose during start up
* [KYLIN-2165] - Use hive table statistics data to get the total count
* [KYLIN-2169] - Refactor AbstractExecutable to respect KylinConfig
* [KYLIN-2170] - Mapper/Reducer cleanup() exception handling
* [KYLIN-2175] - cubestatsreader support reading unfinished segments
* [KYLIN-2181] - remove integer as fixed_length in test_kylin_cube_with_slr_empty desc
* [KYLIN-2187] - Enhance TableExt metadata
* [KYLIN-2192] - More Robust Global Dictionary
* [KYLIN-2193] - parameterize org.apache.kylin.storage.translate.DerivedFilterTranslator#IN_THRESHOLD
* [KYLIN-2195] - Setup naming convention for kylin properties
* [KYLIN-2196] - Update Tomcat class loader to parallel loader
* [KYLIN-2198] - Add a framework to allow major changes in DimensionEncoding
* [KYLIN-2205] - Use column name as the default dimension name
* [KYLIN-2215] - Refactor DimensionEncoding.encode(byte[]) to encode(String)
* [KYLIN-2217] - Reducers build dictionaries locally
* [KYLIN-2220] - Enforce same name between Cube & CubeDesc
* [KYLIN-2222] - web ui uses rest api to decide which dim encoding is valid for different typed columns
* [KYLIN-2227] - rename kylin-log4j.properties to kylin-tools-log4j.properties and move it to global conf folder
* [KYLIN-2238] - Add query server scan threshold
* [KYLIN-2244] - "kylin.job.cuboid.size.memhungry.ratio" shouldn't be applied on measures like TopN
* [KYLIN-2246] - redesign the way to decide layer cubing reducer count
* [KYLIN-2248] - TopN merge further optimization after KYLIN-1917
* [KYLIN-2252] - Enhance project/model/cube name check
* [KYLIN-2255] - Drop v1 CubeStorageQuery, Storage Engine ID=0
* [KYLIN-2263] - Display reasonable exception message if could not find Kafka dependency for streaming build
* [KYLIN-2266] - Reduce memory usage for building global dict
* [KYLIN-2269] - Reduce MR memory usage for global dict
* [KYLIN-2280] - A easier way to change all the conflict ports when start multi kylin instance in the same server
* [KYLIN-2283] - Have a general purpose data generation tool
* [KYLIN-2287] - Speed up model and cube list load in Web
* [KYLIN-2290] - minor improvements on limit
* [KYLIN-2294] - Refactor CI, merge with_slr and without_slr cubes
* [KYLIN-2295] - Refactor CI, blend view cubes into the rest
* [KYLIN-2296] - Allow cube to override Kafka configuration
* [KYLIN-2304] - Only copy latest version dict for global dict
* [KYLIN-2306] - Tolerate Class missing when loading job list
* [KYLIN-2307] - Make HBase 1.x the default of master
* [KYLIN-2308] - Allow user to set more columnFamily in web
* [KYLIN-2310] - Refactor CI, add IT for date/time encoding & extended column
* [KYLIN-2312] - Display Server Config/Environment by order in system tab
* [KYLIN-2314] - Add Integration Test (IT) for snowflake
* [KYLIN-2323] - Refine Table load/unload error message
* [KYLIN-2328] - Reduce the size of metadata uploaded to distributed cache
* [KYLIN-2338] - refactor BitmapCounter.DataInputByteBuffer
* [KYLIN-2349] - Serialize BitmapCounter with peekLength
* [KYLIN-2353] - Serialize BitmapCounter with distinct count
* [KYLIN-2358] - CuboidReducer has too many "if (aggrMask[i])" checks
* [KYLIN-2359] - Update job build step name
* [KYLIN-2364] - Output table name to error info in LookupTable
* [KYLIN-2375] - Default cache size (10M) is too small
* [KYLIN-2377] - Add kylin client query timeout
* [KYLIN-2378] - Set job thread name with job UUID
* [KYLIN-2379] - Add UseCMSInitiatingOccupancyOnly to KYLIN_JVM_SETTINGS
* [KYLIN-2380] - Refactor DbUnit assertions
* [KYLIN-2387] - A new BitmapCounter with better performance
* [KYLIN-2389] - Improve resource utilization for DistributedScheduler
* [KYLIN-2393] - Add "hive.auto.convert.join" and "hive.stats.autogather" to kylin_hive_conf.xml
* [KYLIN-2400] - Simplify Dictionary interface
* [KYLIN-2404] - Add "hive.merge.mapfiles" and "hive.merge.mapredfiles" to kylin_hive_conf.xml
* [KYLIN-2409] - Performance tunning for in-mem cubing
* [KYLIN-2411] - Kill MR job on pause
* [KYLIN-2414] - Distinguish UHC columns from normal columns in KYLIN-2217
* [KYLIN-2415] - Change back default metadata name to "kylin_metadata"
* [KYLIN-2418] - Refactor pom.xml, drop unused parameter
* [KYLIN-2422] - NumberDictionary support for decimal with extra 0 after "."
* [KYLIN-2423] - Model should always include PK/FK as dimensions
* [KYLIN-2424] - Optimize the integration test's performance
* [KYLIN-2428] - Cleanup unnecessary shaded libraries for job/coprocessor/jdbc/server
* [KYLIN-2436] - add a configuration knob to disable spilling of aggregation cache
* [KYLIN-2437] - collect number of bytes scanned to query metrics
* [KYLIN-2438] - replace scan threshold with max scan bytes
* [KYLIN-2442] - Re-calculate expansion rate, count raw data size regardless of flat table compression
* [KYLIN-2443] - Report coprocessor error information back to client
* [KYLIN-2446] - Support project names filter in DeployCoprocessorCLI
* [KYLIN-2451] - Set HBASE_RPC_TIMEOUT according to kylin.storage.hbase.coprocessor-timeout-seconds
* [KYLIN-2489] - Upgrade zookeeper dependency to 3.4.8
* [KYLIN-2494] - Model has no dup column on dimensions and measures
* [KYLIN-2501] - Stream Aggregate GTRecords at Query Server
* [KYLIN-2503] - Spark cubing step should show YARN app link
* [KYLIN-2518] - Improve the sampling performance of FactDistinctColumns step
* [KYLIN-2525] - Smooth upgrade to 2.0.0 from older metadata
* [KYLIN-2527] - Speedup LookupStringTable, use HashMap instead of ConcurrentHashMap
* [KYLIN-2528] - refine job email notification to support starttls and customized port
* [KYLIN-2529] - Allow thread-local override of KylinConfig
* [KYLIN-2545] - Number2BytesConverter could tolerate malformed numbers
* [KYLIN-2560] - Fix license headers for 2.0.0 release



### Bug fix

* [KYLIN-1603] - Building job still finished even MR job error happened.
* [KYLIN-1770] - Upgrade Calcite dependency (v1.10)
* [KYLIN-1793] - Job couldn't stop when hive commands got error with beeline
* [KYLIN-1945] - Cuboid.translateToValidCuboid method throw exception while cube building or query execute
* [KYLIN-2077] - Inconsistent cube desc signature for CubeDesc
* [KYLIN-2153] - Allow user to skip the check in CubeMetaIngester
* [KYLIN-2155] - get-properties.sh doesn't support parameters starting with "-n"
* [KYLIN-2166] - Unclosed HBaseAdmin in StorageCleanupJob#cleanUnusedHBaseTables
* [KYLIN-2172] - Potential NPE in ExecutableManager#updateJobOutput
* [KYLIN-2174] - partition column format visibility issue
* [KYLIN-2176] - org.apache.kylin.rest.service.JobService#submitJob will leave orphan NEW segment in cube when exception is met
* [KYLIN-2191] - Integer encoding error for width from 5 to 7
* [KYLIN-2197] - Has only base cuboid for some cube desc
* [KYLIN-2202] - Fix the conflict between KYLIN-1851 and KYLIN-2135
* [KYLIN-2207] - Ineffective null check in ExtendCubeToHybridCLI#createFromCube()
* [KYLIN-2208] - Unclosed FileReader in HiveCmdBuilder#build()
* [KYLIN-2209] - Potential NPE in StreamingController#deserializeTableDesc()
* [KYLIN-2211] - IDictionaryValueEnumerator should return String instead of byte[]
* [KYLIN-2212] - 'NOT' operator in filter on derived column may get incorrect result
* [KYLIN-2213] - UnsupportedOperationException when execute 'not like' query on cube v1
* [KYLIN-2216] - Potential NPE in model#findTable() call
* [KYLIN-2224] - "select * from fact inner join lookup " does not return values for look up columns
* [KYLIN-2232] - cannot set partition date column pattern when edit a model
* [KYLIN-2236] - JDBC statement.setMaxRows(10) is not working
* [KYLIN-2237] - Ensure dimensions and measures of model don't have null column
* [KYLIN-2242] - Directly write hdfs file in reducer is dangerous
* [KYLIN-2243] - TopN memory estimation is inaccurate in some cases
* [KYLIN-2251] - JDBC Driver httpcore dependency conflict
* [KYLIN-2254] - A kind of sub-query does not work
* [KYLIN-2262] - Get "null" error when trigger a build with wrong cube name
* [KYLIN-2268] - Potential NPE in ModelDimensionDesc#init()
* [KYLIN-2271] - Purge cube may delete building segments
* [KYLIN-2275] - Remove dimensions cause wrong remove in advance settings
* [KYLIN-2277] - SELECT * query returns a "COUNT__" column, which is not expected
* [KYLIN-2282] - Step name "Build N-Dimension Cuboid Data : N-Dimension" is inaccurate
* [KYLIN-2284] - intersect_count function error
* [KYLIN-2288] - Kylin treat empty string as error measure which is inconsistent with hive
* [KYLIN-2292] - workaround for CALCITE-1540
* [KYLIN-2297] - Manually edit cube segment start/end time will throw error in UI
* [KYLIN-2298] - timer component get wrong seconds
* [KYLIN-2300] - Show MapReduce waiting time for each build step
* [KYLIN-2301] - ERROR when executing query with subquery in "NOT IN" clause.
* [KYLIN-2305] - Unable to use long searchBase/Pattern for LDAP
* [KYLIN-2313] - Cannot find a cube in a subquery case with count distinct
* [KYLIN-2316] - Build Base Cuboid Data ERROR
* [KYLIN-2320] - TrieDictionaryForest incorrect getSizeOfId() when empty dictionary
* [KYLIN-2326] - ERROR: ArrayIndexOutOfBoundsException: -1
* [KYLIN-2329] - Between 0.06 - 0.01 and 0.06 + 0.01, returns incorrect result
* [KYLIN-2330] - CubeDesc returns redundant DerivedInfo
* [KYLIN-2337] - Remove expensive toString in SortedIteratorMergerWithLimit
* [KYLIN-2340] - Some subquery returns incorrect result
* [KYLIN-2341] - sum(case .. when ..) is not supported
* [KYLIN-2342] - When NoClassDefFoundError occurred in building cube, no error in kylin.log
* [KYLIN-2343] - When syn hive table, got error but actually the table is synced
* [KYLIN-2347] - TPC-H query 13, too many HLLC objects exceed memory budget
* [KYLIN-2348] - TPC-H query 20, requires multiple models in one query
* [KYLIN-2356] - Incorrect result when filter on numeric columns
* [KYLIN-2357] - Make ERROR_RECORD_LOG_THRESHOLD configurable
* [KYLIN-2362] - Unify shell interpreter in scripts to avoid syntax diversity
* [KYLIN-2367] - raw query like select * where ... returns empty columns
* [KYLIN-2376] - Upgrade checkstyle plugin
* [KYLIN-2382] - The column order of "select \*" is not as defined in the table
* [KYLIN-2383] - count distinct should not include NULL
* [KYLIN-2390] - Wrong argument order for WinAggResetContextImpl()
* [KYLIN-2391] - Unclosed FileInputStream in KylinConfig#getConfigAsString()
* [KYLIN-2395] - Lots of warning messages about failing to scan jars in kylin.out
* [KYLIN-2406] - TPC-H query 20, prevent NPE and give error hint
* [KYLIN-2407] - TPC-H query 20, routing bug in lookup query and cube query
* [KYLIN-2410] - Global dictionary does not respect the Hadoop configuration in mapper & reducer
* [KYLIN-2416] - Max LDAP password length is 15 chars
* [KYLIN-2419] - Rollback KYLIN-2292 workaround
* [KYLIN-2426] - Tests will fail if env not satisfy hardcoded path in ITHDFSResourceStoreTest
* [KYLIN-2429] - Variable initialized should be declared volatile in SparkCubingByLayer#execute()
* [KYLIN-2430] - Unnecessary exception catching in BulkLoadJob
* [KYLIN-2432] - Couldn't select partition column in some old browser (such as Google Chrome 18.0.1025.162)
* [KYLIN-2433] - Handle the column that all records is null in MergeCuboidMapper
* [KYLIN-2434] - Spark cubing does not respect config kylin.source.hive.database-for-flat-table
* [KYLIN-2440] - Query failed if join condition columns not appear on cube
* [KYLIN-2448] - Cloning a Model with a '-' in the name
* [KYLIN-2449] - Rewrite should not run on OLAPAggregateRel if has no OLAPTable
* [KYLIN-2452] - Throw NoSuchElementException when AggregationGroup size is 0
* [KYLIN-2454] - Data generation tool will fail if column name is hive reserved keyword
* [KYLIN-2457] - Should copy the latest dictionaries on dimension tables in a batch merge job
* [KYLIN-2462] - PK and FK both as dimensions causes save cube failure
* [KYLIN-2464] - Use ConcurrentMap instead of ConcurrentHashMap to avoid runtime errors
* [KYLIN-2465] - Web page still has "Streaming cube build is not supported on UI" statements
* [KYLIN-2474] - Build snapshot should check lookup PK uniqueness
* [KYLIN-2481] - NoRealizationFoundException when there are similar cubes and models
* [KYLIN-2487] - IN condition will convert to subquery join when its elements number exceeds 20
* [KYLIN-2490] - Couldn't get cube size on Azure HDInsight
* [KYLIN-2491] - Cube with error job can be dropped
* [KYLIN-2502] - "Create flat table" and "redistribute table" steps don't show YARN application link
* [KYLIN-2504] - Clone cube didn't keep the "engine_type" property
* [KYLIN-2508] - Trans the time to UTC time when set the range of building cube
* [KYLIN-2510] - Unintended NPE in CubeMetaExtractor#requireProject()
* [KYLIN-2514] - Joins in data model fail to save when they disorder
* [KYLIN-2516] - a table field can not be used as both dimension and measure in kylin 2.0
* [KYLIN-2530] - Build cube failed with NoSuchObjectException, hive table not found 'default.kylin_intermediate_xxxx'
* [KYLIN-2536] - Replace the use of org.codehaus.jackson
* [KYLIN-2537] - HBase Read/Write separation bug introduced by KYLIN-2351
* [KYLIN-2539] - Useless filter dimension will impact cuboid selection.
* [KYLIN-2541] - Beeline SQL not printed in logs
* [KYLIN-2543] - Still build dictionary for TopN group by column even using non-dict encoding
* [KYLIN-2555] - minor issues about acl and granted authority

### Tasks

* [KYLIN-1799] - Add a document to setup kylin on spark engine?
* [KYLIN-2293] - Refactor KylinConfig to remove test related code
* [KYLIN-2327] - Enable check-style for test code
* [KYLIN-2344] - Package spark into Kylin binary package
* [KYLIN-2368] - Enable Findbugs plugin
* [KYLIN-2386] - Revert KYLIN-2349 and KYLIN-2353
* [KYLIN-2521] - upgrade to calcite 1.12.0