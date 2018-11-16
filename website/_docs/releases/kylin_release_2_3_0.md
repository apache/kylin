---
layout: docs
title:  Kylin Release 2.3.0
categories: releases
permalink: /docs/releases/kylin_release_2_3_0.html
---

_Tag:_ [kylin-2.3.0](https://github.com/apache/kylin/tree/kylin-2.3.0)

This is a major release after 2.2, with more than 250 bug fixes and enhancement.



### New Feature

* [KYLIN-3125] - Support SparkSql in Cube building step "Create Intermediate Flat Hive Table"
* [KYLIN-3052] - Support Redshift as data source
* [KYLIN-3044] - Support SQL Server as data source
* [KYLIN-2999] - One click migrate cube in web
* [KYLIN-2960] - Support user/group and role authentication for LDAP
* [KYLIN-2902] - Introduce project-level concurrent query number control
* [KYLIN-2776] - New metric framework based on dropwizard
* [KYLIN-2727] - Introduce cube planner able to select cost-effective cuboids to be built by cost-based algorithms
* [KYLIN-2726] - Introduce a dashboard for showing kylin service related metrics, like query count, query latency, job count, etc
* [KYLIN-1892] - Support volatile range for segments auto merge



### Improvement

* [KYLIN-3265] - Add "jobSearchMode" as a condition to "/kylin/api/jobs" API
* [KYLIN-3245] - Searching cube support fuzzy search
* [KYLIN-3243] - Optimize the code and keep the code consistent in the access.html
* [KYLIN-3239] - Refactor the ACL code about "checkPermission" and "hasPermission"
* [KYLIN-3215] - Remove 'drop' option when job status is stopped and error
* [KYLIN-3214] - Initialize ExternalAclProvider when starting kylin
* [KYLIN-3209] - Optimize job partial statistics path be consistent with existing one
* [KYLIN-3196] - Replace StringUtils.containsOnly with Regex
* [KYLIN-3194] - Tolerate broken job metadata caused by executable ClassNotFoundException
* [KYLIN-3193] - No model clone across projects
* [KYLIN-3182] - Update Kylin help menu links
* [KYLIN-3181] - The submit button status of refreshing cube is not suitable when the start time is equal or more than the end time.
* [KYLIN-3162] - Fix alignment problem of 'Save Query' pop-up box
* [KYLIN-3159] - Remove unnecessary cube access request
* [KYLIN-3158] - Metadata broadcast should only retry failed node
* [KYLIN-3157] - Enhance query timeout to entire query life cycle
* [KYLIN-3151] - Enable 'Query History' to show items filtered by different projects
* [KYLIN-3150] - Support different compression in PercentileCounter measure
* [KYLIN-3145] - Support Kafka JSON message whose property name includes "_"
* [KYLIN-3144] - Adopt Collections.emptyList() for empty list values
* [KYLIN-3129] - Fix the joda library conflicts during Kylin start on EMR 5.8+
* [KYLIN-3128] - Configs for allowing export query results for admin/nonadmin user
* [KYLIN-3127] - In the Insights tab, results section, make the list of Cubes hit by the query either scrollable or multiline
* [KYLIN-3124] - Support horizontal scroll bar in 'Insight'
* [KYLIN-3117] - Hide project config in cube level
* [KYLIN-3114] - Enable kylin.web.query-timeout for web query request
* [KYLIN-3113] - Editing Measure supports fuzzy search in web
* [KYLIN-3108] - Change IT embedded Kafka broker path to /kylin/streaming_config/UUID
* [KYLIN-3105] - Interface Scheduler's stop method should be removed
* [KYLIN-3100] - Building empty partitioned cube with rest api supports partition_start_date
* [KYLIN-3098] - Enable kylin.query.max-return-rows to limit the maximum row count returned to user
* [KYLIN-3092] - Synchronize read/write operations on Managers
* [KYLIN-3090] - Refactor to consolidate all caches and managers under KylinConfig
* [KYLIN-3088] - Spell Error of isCubeMatch
* [KYLIN-3086] - Ignore the intermediate tables when loading Hive source tables
* [KYLIN-3079] - Use Docker for document build environment
* [KYLIN-3078] - Optimize the estimated size of percentile measure
* [KYLIN-3076] - Make kylin remember the choices we have made in the "Monitor>Jobs" page
* [KYLIN-3074] - Change cube access to project access in ExternalAclProvider.java
* [KYLIN-3073] - Automatically refresh the 'Saved Queries' tab page when new query saved. 
* [KYLIN-3070] - Enable 'kylin.source.hive.flat-table-storage-format' for flat table storage format
* [KYLIN-3067] - Provide web interface for dimension capping feature
* [KYLIN-3065] - Add 'First' and 'Last' button in case 'Query History' is too much
* [KYLIN-3064] - Turn off Yarn timeline-service when submit mr job
* [KYLIN-3048] - Give warning when merge with holes, but allow user to force proceed at the same time
* [KYLIN-3043] - Don't need create materialized view for lookup tables without snapshot
* [KYLIN-3039] - Unclosed hbaseAdmin in ITAclTableMigrationToolTest
* [KYLIN-3036] - Allow complex column type when loading source table
* [KYLIN-3024] - Input Validator for "Auto Merge Thresholds" text box
* [KYLIN-3019] - The pop-up window of 'Calculate Cardinality' and 'Load Hive Table' should have the same hint
* [KYLIN-3009] - Rest API to get Cube join SQL
* [KYLIN-3008] - Introduce "submit-patch.py"
* [KYLIN-3006] - Upgrade Spark to 2.1.2
* [KYLIN-2997] - Allow change engineType even if there are segments in cube
* [KYLIN-2996] - Show DeployCoprocessorCLI Log failed tables info
* [KYLIN-2993] - Add special mr config for base cuboid step
* [KYLIN-2992] - Avoid OOM in  CubeHFileJob.Reducer
* [KYLIN-2990] - Add warning window of exist model names for other project selected
* [KYLIN-2987] - Add 'auto.purge=true' when creating intermediate hive table or redistribute a hive table
* [KYLIN-2985] - Cache temp json file created by each Calcite Connection
* [KYLIN-2984] - Only allow delete FINISHED or DISCARDED job
* [KYLIN-2982] - Avoid upgrade column in OLAPTable
* [KYLIN-2981] - Typo in Cube refresh setting page.
* [KYLIN-2980] - Remove getKey/Value setKey/Value from Kylin's Pair.
* [KYLIN-2975] - Unclosed Statement in test
* [KYLIN-2966] - push down jdbc column type id mapping
* [KYLIN-2965] - Keep the same cost calculation logic between RealizationChooser and CubeInstance
* [KYLIN-2947] - Changed the Pop-up box when no project selected
* [KYLIN-2941] - Configuration setting for SSO
* [KYLIN-2940] - List job restful throw NPE when time filter not set
* [KYLIN-2935] - Improve the way to deploy coprocessor
* [KYLIN-2928] - PUSH DOWN query cannot use order by function
* [KYLIN-2921] - Refactor DataModelDesc
* [KYLIN-2918] - Table ACL needs GUI
* [KYLIN-2913] - Enable job retry for configurable exceptions
* [KYLIN-2912] - Remove "hfile" folder after bulk load to HBase
* [KYLIN-2909] - Refine Email Template for notification by freemarker
* [KYLIN-2908] - Add one option for migration tool to indicate whether to migrate segment data
* [KYLIN-2905] - Refine the process of submitting a job
* [KYLIN-2884] - Add delete segment function for portal
* [KYLIN-2881] - Improve hbase coprocessor exception handling at kylin server side 
* [KYLIN-2875] - Cube e-mail notification Validation
* [KYLIN-2867] - split large fuzzy Key set
* [KYLIN-2866] - Enlarge the reducer number for hyperloglog statistics calculation at step FactDistinctColumnsJob
* [KYLIN-2847] - Avoid doing useless work by checking query deadline
* [KYLIN-2846] - Add a config of hbase namespace for cube storage
* [KYLIN-2809] - Support operator "+" as string concat operator
* [KYLIN-2801] - Make default precision and scale in DataType (for hive) configurable
* [KYLIN-2764] - Build the dict for UHC column with MR
* [KYLIN-2736] - Use multiple threads to calculate HyperLogLogPlusCounter in FactDistinctColumnsMapper
* [KYLIN-2672] - Only clean necessary cache for CubeMigrationCLI
* [KYLIN-2656] - Support Zookeeper ACL
* [KYLIN-2649] - Tableau could send "select \*" on a big table
* [KYLIN-2645] - Upgrade Kafka version to 0.11.0.1
* [KYLIN-2556] - Switch Findbugs to Spotbugs
* [KYLIN-2363] - Prune cuboids by capping number of dimensions
* [KYLIN-1925] - Do not allow cross project clone for cube
* [KYLIN-1872] - Make query visible and interruptible, improve server's stablility



### Bug fix

* [KYLIN-3268] - Tomcat Security Vulnerability Alert. The version of the tomcat for kylin should upgrade to 7.0.85.
* [KYLIN-3263] - AbstractExecutable's retry has problem
* [KYLIN-3247] - REST API 'GET /api/cubes/{cubeName}/segs/{segmentName}/sql' should return a cube segment sql
* [KYLIN-3242] - export result should use alias too
* [KYLIN-3241] - When refresh on 'Add Cube Page', a blank page will appear.
* [KYLIN-3228] - Should remove the related segment when deleting a job
* [KYLIN-3227] - Automatically remove the blank at the end of lines in properties files
* [KYLIN-3226] - When user logs in with only query permission, 'N/A' is displayed in the cube's action list.
* [KYLIN-3224] - data can't show when use kylin pushdown model 
* [KYLIN-3223] - Query for the list of hybrid cubes results in NPE
* [KYLIN-3222] - The function of editing 'Advanced Dictionaries' in cube is unavailable.
* [KYLIN-3219] - Fix NPE when updating metrics during Spark CubingJob
* [KYLIN-3216] - Remove the hard-code of spark-history path in 'check-env.sh'
* [KYLIN-3213] - Kylin help has duplicate items
* [KYLIN-3211] - Class IntegerDimEnc should give more exception information when the length is exceed the max or less than the min
* [KYLIN-3210] - The project shows '_null' in result page.
* [KYLIN-3205] - Allow one column is used for both dimension and precisely count distinct measure
* [KYLIN-3204] - Potentially unclosed resources in JdbcExplorer#evalQueryMetadata
* [KYLIN-3199] - The login dialog should be closed when ldap user with no permission login correctly
* [KYLIN-3190] - Fix wrong parameter in revoke access API
* [KYLIN-3184] - Fix '_null' project on the query page
* [KYLIN-3183] - Fix the bug of the 'Remove' button in 'Query History'
* [KYLIN-3178] - Delete table acl failed will cause the wabpage awalys shows "Please wait..."
* [KYLIN-3177] - Merged Streaming cube segment has no start/end time
* [KYLIN-3175] - Streaming segment lost TSRange after merge
* [KYLIN-3173] - DefaultScheduler shutdown didn't reset field initialized.
* [KYLIN-3172] - No such file or directory error with CreateLookupHiveViewMaterializationStep 
* [KYLIN-3167] - Datatype lost precision when using beeline
* [KYLIN-3165] - Fix the IllegalArgumentException during segments auto merge
* [KYLIN-3164] - HBase connection must be closed when clearing connection pool
* [KYLIN-3143] - Wrong use of Preconditions.checkNotNull() in ManagedUser#removeAuthoritie
* [KYLIN-3139] - Failure in map-reduce job due to undefined hdp.version variable when using HDP stack and remote HBase cluster
* [KYLIN-3136] - Endless status while subtask happens to be the illegal RUNNING
* [KYLIN-3135] - Fix regular expression bug in SQL comments
* [KYLIN-3131] - After refresh the page,the cubes can't sort by 'create_time'
* [KYLIN-3130] - If we add new cube then refresh the page,the page is blank
* [KYLIN-3116] - Fix cardinality calculate checkbox issue when loading tables
* [KYLIN-3112] - The job 'Pause' operation has logic bug in the kylin server.
* [KYLIN-3111] - Close of HBaseAdmin instance should be placed in finally block
* [KYLIN-3110] - The dashboard page has some display problems.
* [KYLIN-3106] - DefaultScheduler.shutdown should use ExecutorService.shutdownNow instead of ExecutorService.shutdown
* [KYLIN-3104] - When the user log out from "Monitor" page, an alert dialog will pop up warning "Failed to load query."
* [KYLIN-3102] - Solve the problems for incomplete display of Hive Table tree.
* [KYLIN-3101] - The "search" icon will separate from the "Filter" textbox when click the "showSteps" button of a job in the jobList
* [KYLIN-3097] - A few spell error in partials directory
* [KYLIN-3087] - Fix the DistributedLock release bug in GlobalDictionaryBuilder
* [KYLIN-3085] - CubeManager.updateCube() must not update the cached CubeInstance
* [KYLIN-3084] - File not found Exception when processing union-all in TEZ mode
* [KYLIN-3083] - potential overflow in CubeHBaseRPC#getCoprocessorTimeoutMillis
* [KYLIN-3082] - Close of GTBuilder should be placed in finally block in InMemCubeBuilder
* [KYLIN-3081] - Ineffective null check in CubeController#cuboidsExport
* [KYLIN-3077] - EDW.TEST_SELLER_TYPE_DIM_TABLE is not being created by the integration test, but it's presence in the Hive is expected
* [KYLIN-3069] - Add proper time zone support to the WebUI instead of GMT/PST kludge
* [KYLIN-3063] - load-hive-conf.sh should not get the commented configuration item
* [KYLIN-3061] - When we cancel the Topic modification for 'Kafka Setting' of streaming table, the 'Cancel' operation will make a mistake.
* [KYLIN-3060] - The logical processing of creating or updating streaming table has a bug in server, which will cause a NullPointerException.
* [KYLIN-3058] - We should limit the integer type ID and Port for "Kafka Setting" in "Streaming Cluster" page
* [KYLIN-3056] - Fix 'Cannot find segment null' bug when click 'SQL' in the cube view page
* [KYLIN-3055] - Fix NullPointerException for intersect_count
* [KYLIN-3054] - The drop-down menu in the grid column of query results missing a little bit.
* [KYLIN-3053] - When aggregation group verification failed, the error message about aggregation group number does not match with the actual on the Advanced Setting page
* [KYLIN-3049] - Filter the invalid zero value of "Auto Merge Thresholds" parameter when you create or update a cube.
* [KYLIN-3047] - Wrong column type when sync hive table via beeline
* [KYLIN-3042] - In query results page, the results data table should resize when click "fullScreen" button
* [KYLIN-3040] - Refresh a non-partitioned cube changes the segment name to "19700101000000_2922789940817071255"
* [KYLIN-3038] - cannot support sum of type-converted column SQL
* [KYLIN-3034] - In the models tree, the "Edit(JSON)" option is missing partly.
* [KYLIN-3032] - Cube size shows 0 but actually it isn't empty
* [KYLIN-3031] - KeywordDefaultDirtyHack should ignore case of default like other database does
* [KYLIN-3030] - In the cubes table, the options of last column action are missing partly.
* [KYLIN-3029] - The warning window of existing cube name does not work
* [KYLIN-3028] - Build cube error when set S3 as working-dir
* [KYLIN-3026] - Can not see full cube names on insight page
* [KYLIN-3020] - Improve org.apache.hadoop.util.ToolRunner to be thread-safe
* [KYLIN-3017] - Footer covers the selection box and some options can not be selected
* [KYLIN-3016] - StorageCleanup job doesn't clean up all the legacy fields in a in Read/Write seperation environment
* [KYLIN-3004] - Update validation when deleting segment
* [KYLIN-3001] - Fix the wrong Cache key issue 
* [KYLIN-2995] - Set SparkContext.hadoopConfiguration to HadoopUtil in Spark Cubing
* [KYLIN-2994] - Handle NPE when load dict in DictionaryManager
* [KYLIN-2991] - Query hit NumberFormatException if partitionDateFormat is not YYYY-MM-DD
* [KYLIN-2989] - Close of BufferedWriter should be placed in finally block in SCCreator
* [KYLIN-2974] - zero joint group can lead to query error
* [KYLIN-2971] - Fix the wrong "Realization Names" in logQuery when hit cache
* [KYLIN-2969] - Fix the wrong NumberBytesCodec cache in Number2BytesConverter 
* [KYLIN-2968] - misspelled word in table_load.html
* [KYLIN-2967] - Add the dependency check when deleting a  project
* [KYLIN-2962] - drop error job not delete segment
* [KYLIN-2959] - SAML logout issue
* [KYLIN-2956] - building trie dictionary blocked on value of length over 4095 
* [KYLIN-2953] - List readable project not correct if add limit and offset
* [KYLIN-2939] - Get config properties not correct in UI
* [KYLIN-2933] - Fix compilation against the Kafka 1.0.0 release
* [KYLIN-2930] - Selecting one column in union causes compile error
* [KYLIN-2929] - speed up Dump file performance
* [KYLIN-2922] - Query fails when a column is used as dimension and sum(column) at the same time
* [KYLIN-2917] - Dup alias on OLAPTableScan
* [KYLIN-2907] - Check if a number is a positive integer 
* [KYLIN-2901] - Update correct cardinality for empty table
* [KYLIN-2887] - Subquery columns not exported in OLAPContext allColumns
* [KYLIN-2876] - Ineffective check in ExternalAclProvider
* [KYLIN-2874] - Ineffective check in CubeDesc#getInitialCuboidScheduler
* [KYLIN-2849] - duplicate segmentÔºåcannot be deleted and data cannot be refreshed and merged
* [KYLIN-2837] - Ineffective call to toUpperCase() in MetadataManager
* [KYLIN-2836] - Lack of synchronization in CodahaleMetrics#close
* [KYLIN-2835] - Unclosed resources in JdbcExplorer
* [KYLIN-2794] - MultipleDictionaryValueEnumerator should output values in sorted order
* [KYLIN-2756] - Let "LIMIT" be optional in "Inspect" page
* [KYLIN-2470] - cube build failed when 0 bytes input for non-partition fact table
* [KYLIN-1664] - Harden security check for '/kylin/api/admin/config' API



### Task

* [KYLIN-3207] - Blog for Kylin Superset Integration
* [KYLIN-3200] - Enable SonarCloud for Code Analysis
* [KYLIN-3198] - More Chinese Howto Documents
* [KYLIN-3195] - Kylin v2.3.0 Release
* [KYLIN-3191] - Remove the deprecated configuration item kylin.security.acl.default-role
* [KYLIN-3189] - Documents for kylin python client
* [KYLIN-3080] - Kylin Qlik Sense Integration Documentation
* [KYLIN-3068] - Rename deprecated parameter for HDFS block size in HiveColumnCardinalityJob
* [KYLIN-3062] - Hide RAW measure
* [KYLIN-3010] - Remove v1 Spark engine code
* [KYLIN-2843] - Upgrade nvd3 version
* [KYLIN-2797] - Remove MR engine V1
* [KYLIN-2796] - Remove the legacy "statisticsenabled" codes in FactDistinctColumnsJob



### Sub-Task

* [KYLIN-3235] - add null check for SQL
* [KYLIN-3202] - Doc directory for 2.3
* [KYLIN-3155] - Create a document for how to use dashboard
* [KYLIN-3154] - Create a document for cube planner
* [KYLIN-3153] - Create a document for system cube creation
* [KYLIN-3018] - Change maxLevel for layered cubing
* [KYLIN-2946] - Introduce a tool for batch incremental building of system cubes
* [KYLIN-2934] - Provide user guide for KYLIN-2656(Support Zookeeper ACL)
* [KYLIN-2822] - Introduce sunburst chart to show cuboid tree
* [KYLIN-2746] - Separate filter row count & aggregated row count for metrics collection returned by coprocessor
* [KYLIN-2735] - Introduce an option to make job scheduler consider job priority
* [KYLIN-2734] - Introduce hot cuboids export & import
* [KYLIN-2733] - Introduce optimize job for adjusting cuboid set
* [KYLIN-2732] - Introduce base cuboid as a new input for cubing job
* [KYLIN-2731] - Introduce checkpoint executable
* [KYLIN-2725] - Introduce a tool for creating system cubes relating to query & job metrics
* [KYLIN-2723] - Introduce metrics collector for query & job metrics
* [KYLIN-2722] - Introduce a new measure, called active reservoir, for actively pushing metrics to reporters
