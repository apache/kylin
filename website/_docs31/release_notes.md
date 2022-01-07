---
layout: docs31
title:  Release Notes
categories: gettingstarted
permalink: /docs31/release_notes.html
---

To download latest release, please visit: [http://kylin.apache.org/download/](http://kylin.apache.org/download/), 
there are source code package, binary package and installation guide avaliable.

Any problem or issue, please report to Apache Kylin JIRA project: [https://issues.apache.org/jira/browse/KYLIN](https://issues.apache.org/jira/browse/KYLIN)

or send to Apache Kylin mailing list:

* User relative: [user@kylin.apache.org](mailto:user@kylin.apache.org)
* Development relative: [dev@kylin.apache.org](mailto:dev@kylin.apache.org)

## v4.0.1 - 2022-01-05

__New Feature__

* [KYLIN-5117] - Support percentile function after aggregate sub query

__Bug Fix__

* [KYLIN-5067] - CubeBuildJob build unnecessary snapshot
* [KYLIN-5071] - kylin.engine.build-base-cuboid-enabled=false and kylin.cube.cubeplanner.enabled=true are conflict on build cube step, and will throw NPE.
* [KYLIN-5083] - Docker for kylin4 need to check hadoop service before start kylin
* [KYLIN-5086] - When query pushdown, only measure result is empty
* [KYLIN-5112] - Use 'return' instead of 'exit' in script prepare_hadoop_dependency.sh
* [KYLIN-5131] - java.lang.UnsupportedOperationException: API getTableSnapshots is not supported in Kylin 4.0

__Improvement__

* [KYLIN-4864] - Support building and testing Kylin on ARM64 architecture platform
* [KYLIN-5069] - Remove find-hive-dependency.sh in Kylin 4
* [KYLIN-5076] - Missing tracking URL in spark standalone mode in kylin4
* [KYLIN-5082] - Exactly aggregation for percentile function
* [KYLIN-5084] - Kylin4.0.0 is incompatible with HDP-3.1.5's Hive-3.1.0
* [KYLIN-5090] - The error message was not printed in class JobWoker
* [KYLIN-5111] - Record the time spent for each stage of query in kylin4's log

## v3.1.3 - 2022-01-05

__New Feature__

* [KYLIN-4947] - Implement spark engine for cube optimization jobs
* [KYLIN-4948] - Provide an API to allow users to adjust cuboids manually
* [KYLIN-4982] - Add special spark-sql conf in create intermediate flat table

__Bug Fix__

* [KYLIN-3996] - postgresql can not to be used to construct the flat hive table in NO.1 step in building cube via datasourcedefaultSdk
* [KYLIN-4964] - Receiver consumer thread should be stoped while encounting unrecoverable error
* [KYLIN-4970] - Fix spark.executor.extraJavaOptions args
* [KYLIN-4974] - Kylin does not handle fucntion CURRENT_TIME().
* [KYLIN-4978] - NPE: submit refresh request using restful api when there is no segments
* [KYLIN-4983] - The stream cube will be paused when user append a batch segment first
* [KYLIN-4992] - Source row count statistics calculated in a wrong way in MergeDictionaryMapper
* [KYLIN-4995] - Query exception when the query statement contains a single left parenthesis
* [KYLIN-5003] - Fail to package Kylin due to legacy front end dependencies
* [KYLIN-5007] - queries with limit clause may fail when string dimension is encoded in integer type
* [KYLIN-5035] - Fix Merge Cuboid Statistics EOFException
* [KYLIN-5036] - The hive dependency of directory type is filtered out by mistake
* [KYLIN-5054] - Kylin_System project and cubes create time is wrong (1970).
* [KYLIN-5097] - modify hive dict table format from textfile to orc

__Improvement__

* [KYLIN-4554] - Validate "filter condition" on model saving
* [KYLIN-4864] - Support building and testing Kylin on ARM64 architecture platform
* [KYLIN-4944] - Upgrade CentOS version, Hadoop version and Spark version for Kylin Docker image
* [KYLIN-4972] - Don't allow segment merge when use dict.global.SegmentAppendTrieDictBuilder to build bitmap measure
* [KYLIN-5016] - Avoid potential NPE issue in RDBMS Pushdown case

__Task__

* [KYLIN-4971] - Add new measure bitmap_map for count distinct measure in UI
* [KYLIN-5053] - Update the website to link to TravisCI and Github Actions instead of Jenkins

## v3.1.2 - 2021-04-26

__New Feature__

* [KYLIN-4794] - Make it possible to force hit a cube set for sqls with cube join
* [KYLIN-4938] - Remove segment by UUID
* [KYLIN-4939] - Transform lookup table snapshot from segment level to cube level
* [KYLIN-4940] - Implement the step of "Extract Dictionary from Global Dictionary" for spark cubing engine

__Improvement__

* [KYLIN-4613] - add buildCubeCLi as hadoop main class and jobRestClient
* [KYLIN-4636] - Make /api/admin/public_config callable for profile saml
* [KYLIN-4658] - Union all issue with regarding to windows function & aggregation on
* [KYLIN-4667] - Automatically set kylin.query.cache-signature-enabled to be true when memcached is enabled
* [KYLIN-4702] - Missing cube-level lookup table snapshot when doing cube migration
* [KYLIN-4711] - Change default value to 3 for kylin.metadata.hbase-client-retries-number
* [KYLIN-4827] - SparkMergingDictionary parallelize not work
* [KYLIN-4833] - use distcp to control the speed of writting hfile data to hbase cluster
* [KYLIN-4836] - fix CubeMigrationCLI bug
* [KYLIN-4837] - optimize CubeMigrationCLI
* [KYLIN-4838] - fix KYLIN-4679 bug
* [KYLIN-4854] - the official website document about system cube have some errors
* [KYLIN-4862] - Build Cube use two job engine, cause there is no valid state transfer from:ERROR to:SUCCEED
* [KYLIN-4863] - dependency cache script files not fully used
* [KYLIN-4920] - stream lambda: hive table can be in database other than default
* [KYLIN-4929] - Skip metrics update for simple queries to avoid NPE warnings
* [KYLIN-4933] - Support set cache strength for dict cache
* [KYLIN-4955] - fix typo in KYLIN UI when not set dictionary for count_distinct measure

__Bug Fix__

* [KYLIN-4640] - StepInfo saved wrong key about flink or spark
* [KYLIN-4771] - Query streaming cube - Thread pool of MultiThreadsResultCollector be blocked.
* [KYLIN-4787] - The script sample.sh cannot automatically switch to the hive database set by the user to create sample hive tables
* [KYLIN-4810] - TrieDictionary is not correctly build
* [KYLIN-4819] - build cube failed when `kylin.metadata.hbase-client-retries-number` great than 1
* [KYLIN-4826] - The value of config kylin.source.hive.warehouse-dir can not be found
* [KYLIN-4841] - Spark RDD cache is invalid when building with spark engine
* [KYLIN-4847] - Cuboid to HFile step failed on multiple job server env because of trying to read the metric jar file from the inactive job server's location.
* [KYLIN-4855] - kylin metrics prefix bug in system-cube.sh
* [KYLIN-4879] - The function of sql to remove comments is not perfect. In some cases, the sql query conditions used will be modified
* [KYLIN-4882] - config["kylin.engine.spark-fact-distinct"] overwrite in the Cube-level is invalid
* [KYLIN-4896] - cube metadata miss
* [KYLIN-4900] - The result of derived time columns are error, when timezone is GMT-1 or GMT-N
* [KYLIN-4901] - Query result use diff timezone in real-time stream
* [KYLIN-4921] - stream config lost when create table with same table_name in diff project.
* [KYLIN-4930] - unexpected empty search result in group/user management page
* [KYLIN-4962] - Fix NPE in ShrunkDict step
* [KYLIN-4979] - Fix flink shaded jar version error in download-flink.sh

## v4.0.0-beta - 2021-02-07
_Tag:_ [kylin-4.0.0-beta](https://github.com/apache/kylin/tree/kylin-4.0.0-beta)
This is a major release after 4.0.0-alpha, with 25 new features/improvements and 14 bug fixes.

__New Feature__

* [KYLIN-4842] - Supports grouping sets function for Kylin 4
* [KYLIN-4843] - Support INTERSECT_COUNT/INTERSECT_VALUE function for Kylin 4

__Improvement__

* [KYLIN-4712] - Optimize CubeMetaIngester.java CLI
* [KYLIN-4754] - The Cleanup tool cannot clean the the parquet file of the deleted cube and project
* [KYLIN-4760] - Optimize TopN measure
* [KYLIN-4763] - Rename ISourceAware.ID_SPARK to ISourceAware.ID_CSV
* [KYLIN-4766] - After the job is discarded, the temporary file and segment file are not deleted
* [KYLIN-4782] - Verify if the query hit the true cuboid in IT
* [KYLIN-4790] - Automaticly copy required jars to SPARK_HOME/jars for HDI3.6
* [KYLIN-4792] - Verify several attribute values of segment in the build/merge test
* [KYLIN-4800] - Add canary tool for sparder-context
* [KYLIN-4808] - Auto copy hive-site.xml to hadoop_conf_dir in kylin4
* [KYLIN-4811] - Support cube level configuration for BuildingJob
* [KYLIN-4813] - Refine spark logger for Kylin 4 build engine
* [KYLIN-4814] - Support Kylin4 delopyment on EMR 5.X
* [KYLIN-4815] - Support Kylin4 delopyment on EMR 6.x
* [KYLIN-4817] - Refine Cube Migration Tool for Kylin4
* [KYLIN-4825] - Add spark job tracking url in step details page
* [KYLIN-4828] - Add more sql test cases into NBuildAndQueryTest
* [KYLIN-4829] - Support to use thread-level SparkSession to execute query
* [KYLIN-4844] - Add lookup table duplicate key check when building job
* [KYLIN-4850] - Cube's override kylin_properties were ignored in building jobs
* [KYLIN-4857] - Refactor system cube for kylin4
* [KYLIN-4875] - Remove executor configurations when execute resource detect step (local mode)
* [KYLIN-4877] - Use all dimension columns as sort columns when saving cuboid data

__Bug Fix__

* [KYLIN-4737] - The precision in the returned result is different from the one by Spark SQL
* [KYLIN-4738] - The order in the returned result is wrong when use window function to query in kylin
* [KYLIN-4751] - Throws NPE when run test case TestTopNUDAF
* [KYLIN-4761] - Update some missing values of new segment when merge segments
* [KYLIN-4764] - Throws NoClassDefFoundError when run query test cases
* [KYLIN-4791] - Throws exception 'UnsupportedOperationException: empty.reduceLeft' when there are cast expressions in the filters of FilePruner
* [KYLIN-4793] - In some Hadoop versions, kylin calcite reported error because there is no guava14
* [KYLIN-4820] - Can not auto set spark resources configurations when building cube
* [KYLIN-4822] - The metrics 'Total spark scan time' of query log is negative in some cases
* [KYLIN-4824] - The metric 'Total scan bytes' of 'Query Log' is always 0 when querying
* [KYLIN-4853] - QueryPreparedStatementCache invalid in Spark Query Engine
* [KYLIN-4858] - Support Kylin4 deployment on CDH 6.X
* [KYLIN-4872] - Fix NPE when there are more than one segment if cube planner is open
* [KYLIN-4874] - Fix CubeMigrationCLI bug for kylin4

__Sub-task__

* [KYLIN-4818] - Calculate cuboid statistics in Kylin 4

## v3.1.1 - 2020-10-18
_Tag:_ [kylin-3.1.1](https://github.com/apache/kylin/tree/kylin-3.1.1)
This is a bug-fix release after 3.1.0, with 37 improvements and 21 bug fixes.

__Sub-task__

* [KYLIN-4557] - Refactor JobService to improve code readability
* [KYLIN-4558] - get all chained executable jobs through job API
* [KYLIN-4559] - show cardinality and lookup snapshot job on job page
* [KYLIN-4560] - support to re-run/delete cardinality and lookup snapshot job
* [KYLIN-4561] - overall job number statistics in monitor page is incorrect after change the job status

__Bug Fix__

* [KYLIN-4515] - could not send mail on ssl port
* [KYLIN-4578] - Throws TableNotFoundException in step 'Convert Cuboid Data to HFile' when the value of property * 'kylin.storage.hbase.table-name-prefix' or 'kylin.storage.hbase.namespace' is lowercase.
* [KYLIN-4603] - listjob return NPE
* [KYLIN-4610] - update kylin.engine.livy.backtick.quote default value
* [KYLIN-4617] - Check whether project/jobid exists before download diagnosis package
* [KYLIN-4628] - Fail to use custom measure type when specifying cube to query
* [KYLIN-4634] - Fail to specify cube in model of low priority to query
* [KYLIN-4656] - Guava classpath conflict caused by kylin-jdbc 3.1.0 jar
* [KYLIN-4657] - dead-loop in org.apache.kylin.engine.mr.common.MapReduceExecutable.doWork
* [KYLIN-4672] - Using Real-time Lambda to refresh the data lead to result not incorrect
* [KYLIN-4677] - StorageCleanupJob throw NPE
* [KYLIN-4683] - Fail to consume kafka when partition number get larger
* [KYLIN-4684] - Streaming Table V2创建 - TSPattern下拉选项前端bug修复
* [KYLIN-4688] - Too many tmp files in HDFS tmp directory
* [KYLIN-4697] - User info update logic is not correct
* [KYLIN-4700] - Wrong engine type for realtime streaming
* [KYLIN-4731] - Kylin query failing with 'null while executing SQL'
* [KYLIN-4753] - Merging job stop working after Kylin upgrade
* [KYLIN-4755] - Error while compiling generated Java code when using Kylin UDF in "case when"
* [KYLIN-4756] - user/group page has duplicate information between adjacent page numbers
* [KYLIN-4757] - Impossible precision for decimal datatype in kylin if the source column is numeric in postgres

__Improvement__

* [KYLIN-4527] - Beautify the drop-down list of the cube on query page
* [KYLIN-4549] - Show column cardinality in rowkeys area of advanced settings
* [KYLIN-4550] - Provide advanced refresh interface inside the refresh panel
* [KYLIN-4551] - Provide interfaces to transfer cube/model/project ownership
* [KYLIN-4576] - Add hint about password length
* [KYLIN-4581] - Add spark and flink engine test case for release test
* [KYLIN-4585] - Add cube count column for project table
* [KYLIN-4606] - throw olap exception when olap query and pushdown both error
* [KYLIN-4608] - add deletecubefast api for delete 300 cubes fast
* [KYLIN-4609] - setenv.sh add zgc config for big memory
* [KYLIN-4611] - modify PATTERN_SPARK_APP_URL to Tracking URL，ignore case
* [KYLIN-4612] - Support job status write to kafka
* [KYLIN-4616] - The value of config kylin.source.hive.databasedir can be self detected
* [KYLIN-4618] - Upgrade kylin docker image for kylin 3.1.0
* [KYLIN-4619] - Make shrunken dict able to coexist with mr-hive global dict
* [KYLIN-4626] - add set kylin home sh
* [KYLIN-4635] - Set Kylin default log level to info
* [KYLIN-4653] - Make the capacity for the LinkedBlockingQueue of BlockingReservoir configurable
* [KYLIN-4665] - set flink job name shown in resource manager
* [KYLIN-4678] - continue execute job when StorageCleanupJob sub step has error
* [KYLIN-4679] - StorageCleanupJob clean hive table support hive table prefix
* [KYLIN-4685] - return user friendly msg when stackoverflowerror
* [KYLIN-4686] - clean metadata support to delete all jobs
* [KYLIN-4687] - add unify clean sh to excute some clean shells
* [KYLIN-4709] - Upgrade spring to 4.3.26
* [KYLIN-4712] - Optimize CubeMetaIngester.java CLI
* [KYLIN-4714] - Failed to revoke role access of the project
* [KYLIN-4716] - Optimize the project page
* [KYLIN-4752] - Refine server mode checking
* [KYLIN-4770] - Move Kylin service on k8s from background to foreground

__Task__

* [KYLIN-4526] - Enhance get the hive table rows
* [KYLIN-4648] - Upgrade Spark to latest 2.3 or 2.4 version

## v4.0.0-alpha - 2020-09-13
_Tag:_ [kylin-4.0.0-alpha](https://github.com/apache/kylin/tree/kylin-4.0.0-alpha)
This is a major release after 3.1.0, with 35 new features/improvements and 22 bug fixes.

__New Feature__

* [KYLIN-4188] - Parquet as Cube storage V2
* [KYLIN-4213] - The new build engine with Spark-SQL
* [KYLIN-4452] - Kylin on Parquet with Docker
* [KYLIN-4462] - Support Count Distinct,TopN and Percentile by kylin on Parquet
* [KYLIN-4659] - Prepare a technical preview version for Parquet Storage

__Improvement__

* [KYLIN-4449] - A running build job will still running when cancel from front end
* [KYLIN-4450] - Add the feature that adjusting spark driver memory adaptively
* [KYLIN-4456] - Temporary files generated by UT or Integration Tests need to be deleted
* [KYLIN-4458] - FilePruner prune shards
* [KYLIN-4459] - Continuous print warning log-DFSInputStream has been closed already
* [KYLIN-4467] - Support TopN by kylin on Parquet
* [KYLIN-4468] - Support Percentile by kylin on Parquet
* [KYLIN-4474] - Support window function for Kylin on Parquet
* [KYLIN-4475] - Support intersect count for Kylin on Parquet
* [KYLIN-4541] - Kylin.log output error information during build job
* [KYLIN-4542] - After downloading spark with bin/download-spark.sh , still need set SPARK_HOME manually .
* [KYLIN-4621] - Avoid annoying log message when build cube and query
* [KYLIN-4625] - Debug the code of Kylin on Parquet without hadoop environment
* [KYLIN-4631] - Set the default build engine type to spark for Kylin on Parquet
* [KYLIN-4644] - New tool to clean up  intermediate files for Kylin 4.0
* [KYLIN-4680] - Avoid annoying log messages of unit test and integration test
* [KYLIN-4695] - Automatically start sparder (for query) application when start kylin instance.
* [KYLIN-4699] - Delete job_tmp path after build/merge successfully
* [KYLIN-4713] - Support use diff spark schedule pool for diff query
* [KYLIN-4722] - Add more statistics to the query results
* [KYLIN-4723] - Set the configurations about shard by to cube level
* [KYLIN-4744] - Add tracking URL for build spark job on yarn
* [KYLIN-4746] - Improve build performance by reducing the count of calling 'count()' function
* [KYLIN-4747] - Use the first dimension column as sort column within a partition

__Bug Fix__

* [KYLIN-4444] - Error when refresh segment
* [KYLIN-4451] - ClassCastException when querying on cluster with binary package
* [KYLIN-4453] - Query on refreshed cube failed with FileNotFoundException
* [KYLIN-4454] - Query snapshot table failed
* [KYLIN-4455] - Query will fail when set calcite.debug=true
* [KYLIN-4457] - Query cube result doesn't math with spark sql
* [KYLIN-4461] - When querying with measure whose return type is decimal, it will throw type cast exception
* [KYLIN-4465] - Will get direct parent and ancestor cuboids with method findDirectParentCandidates
* [KYLIN-4466] - Cannot unload table which is loaded from CSV source
* [KYLIN-4469] - Cannot clone model
* [KYLIN-4471] - Cannot query sql about left join
* [KYLIN-4482] - Too many logging segment info with CubeBuildJob step
* [KYLIN-4483] - Avoid to build global dictionaries with empty ColumnDesc collection
* [KYLIN-4632] - No such element exception:spark.driver.cores
* [KYLIN-4681] - Use KylinSession instead of SparkSession for some test cases
* [KYLIN-4694] - Fix 'NoClassDefFoundError: Lcom/esotericsoftware/kryo/io/Output' when query with sparder on yarn
* [KYLIN-4698] - Delete segment storage path after merging segment, deleting segment and droping cube
* [KYLIN-4721] - The default source source type should be CSV not Hive with the local debug mode
* [KYLIN-4732] - The cube size is wrong after disabling the cube
* [KYLIN-4733] - the cube size is inconsistent with the size of all segments
* [KYLIN-4734] - the duration is still increasing after discarding the job
* [KYLIN-4742] - NullPointerException when auto merge segments if exist discard jobs* 

## v3.1.0 - 2020-07-02
_Tag:_ [kylin-3.1.0](https://github.com/apache/kylin/tree/kylin-3.1.0)
This is a major release after 3.0.0, with 10 new features and 68 enhancements and 75 bug fixes.

__New Feature__

* [KYLIN-3361] - Add a two layer udaf stddev_sum
* [KYLIN-3758] - Flink Cube Build Engine
* [KYLIN-3832] - Kylin pushdown to support postgresql
* [KYLIN-4104] - Support multi jdbc pushdown runners to execute query/update
* [KYLIN-4240] - Use SSO without LDAP
* [KYLIN-4335] - Reuse global dictionary from other cube,global domain dict
* [KYLIN-4445] - Provide a kylin on kubernetes solution
* [KYLIN-4480] - Implement runtime non-equi join
* [KYLIN-4485] - Create a self service interface for cube migration
* [KYLIN-4491] - Query push down to Presto

__Improvement__

* [KYLIN-2230] - can not catch kylin.sh path in linux resouce PATH setting
* [KYLIN-3237] - Fix NPE
* [KYLIN-3487] - Create a new measure for precise count distinct
* [KYLIN-3698] - check-env.sh should print more details about checking items
* [KYLIN-3844] - Add instruction about config 'kylin.metadata.hbasemapping-adapter'
* [KYLIN-3956] - Segments of not only streaming cube but also batch cube need to show their status
* [KYLIN-4168] - Fix sonar reported static code issues phase 2
* [KYLIN-4185] - CubeStatsReader estimate wrong cube size
* [KYLIN-4192] - Build UHC dictionary with spark
* [KYLIN-4197] - DiagnosisInfoCLI block forever at "beeline --version"
* [KYLIN-4211] - PartitionDesc support custom year、month、day partitions name
* [KYLIN-4212] - Add a user profile page
* [KYLIN-4225] - unclosed hive session cause too many temp file
* [KYLIN-4226] - Skip current unavailable tables when updating hbase coprocessor
* [KYLIN-4237] - Return error when execute 'explain plan for SQL' to get the execution plan of SQL
* [KYLIN-4249] - DistributedScheduler can selectively assign task nodes according to cube extra configuration
* [KYLIN-4251] - Add livy to docker
* [KYLIN-4255] - Display detailed error message when using livy build error
* [KYLIN-4271] - Use configurable certificate to support LDAPs authentication of Kylin
* [KYLIN-4280] - SegmentPruner add the checks for "OR" filtering
* [KYLIN-4281] - Precisely set the data type of tuple expression
* [KYLIN-4282] - support case when in count (distinct)
* [KYLIN-4287] - SegmentPruner cannot prune segment with "IN" or "OR" CompareTupleFilter
* [KYLIN-4290] - Add file lock to kylin startup script to avoid starting multiple instances on one node
* [KYLIN-4292] - Use HFileOutputFormat3 in all places to replace HFileOutputFormat2
* [KYLIN-4293] - Backport HBASE-22887 to Kylin HFileOutputFormat3
* [KYLIN-4294] - Add http api for metrics 
* [KYLIN-4305] - Streaming Receiver cannot limit income query request or cancel long-running query
* [KYLIN-4308] - Make kylin.sh tips clearer and more explicit
* [KYLIN-4311] - Fix bugs in Sonar to be compliant
* [KYLIN-4312] - Specified cube when querying by API
* [KYLIN-4314] - Support union in intersect_count() function
* [KYLIN-4315] - Use metadata numRows in beeline client for quick row counting
* [KYLIN-4317] - Update doc for KYLIN-4104
* [KYLIN-4319] - in "Extract Dictionary from Global Dictionary" step, only need load global dictonaries instead of all dictonaries.
* [KYLIN-4321] - Create fact distinct columns using spark by default when build engine is spark
* [KYLIN-4327] - TOPN Comparator may  violate its general contract
* [KYLIN-4328] - Kylin should skip succeed jobs in scheduler
* [KYLIN-4333] - Build Server OOM
* [KYLIN-4342] - Build Global Dict by MR/Hive New Version
* [KYLIN-4356] - Failed "Hive Column Cardinality calculation for table" jobs cannot be delete 
* [KYLIN-4358] - statement cache eviction invalidation base on time
* [KYLIN-4364] - Limit varchar length to DefaultVarcharPrecison in RDBMS Source
* [KYLIN-4371] - Integrate System Cube with Real-time OLAP
* [KYLIN-4390] - Update tomcat to 7.0.100
* [KYLIN-4394] - Upgrade dependency version for several CVEs
* [KYLIN-4399] - Redirect http url is not override as https when using vip
* [KYLIN-4407] - Protect query engine to provide more stable service
* [KYLIN-4411] - Job Engine Improvement
* [KYLIN-4412] - Show cluster name in job notification email title
* [KYLIN-4419] - Make it possible to change metadata in some cases without rebuilding data
* [KYLIN-4437] - Should replace deprecated  "mapred.job.name"
* [KYLIN-4424] - use static web server to help frontend development
* [KYLIN-4441] - Add restart command to kylin.sh
* [KYLIN-4477] - Usage of "TLS" is insecure
* [KYLIN-4478] - Usage of "AES/ECB/PKCS5Padding" is insecure
* [KYLIN-4495] - Support custom date formats for partition date column
* [KYLIN-4504] - Cache usage improvement
* [KYLIN-4508] - Add more unit tests to improve test coverage
* [KYLIN-4510] - Automatically refresh the page after reload table
* [KYLIN-4511] - Support project admin user to calculate column cardinality by web
* [KYLIN-4512] - Update NOTICE file
* [KYLIN-4520] - Improve StorageCleanJobHbaseUtil to cleanup HBase tables in parallel
* [KYLIN-4537] - Give a friendly tips to the user when getting task list fails
* [KYLIN-4543] - Remove the usage for jackson enableDefaultTyping()
* [KYLIN-4571] - Always set GMT timezone for RecordEventTimeDetail
* [KYLIN-4573] - Add option to indicate whether to close file for every append for Hive Producer
* [KYLIN-4580] - Add 90, 360 days auto-merge thresholds for system cubes

__Bug Fix__

* [KYLIN-2214] - NOT IN result incorrect result
* [KYLIN-2971] - Fix the wrong "Realization Names" in logQuery when hit cache
* [KYLIN-3409] - Write metric error when run a query.
* [KYLIN-3733] - kylin can't response correct data when using "in" filter
* [KYLIN-4080] - Project schema update event causes error reload NEW DataModelDesc
* [KYLIN-4119] - The admin of project can't operate the action of Hybrids
* [KYLIN-4120] - Failed to query "select * from {lookup}" if a lookup table joined in two different models
* [KYLIN-4124] - Fix bug in map partition function en cuboid children is empty or null
* [KYLIN-4145] - Compile failed due to incompatible version between scala and scala-maven-plugin
* [KYLIN-4151] - FileSplit ClassCastException in KafkaMRInput
* [KYLIN-4161] - exception in update metrics when the response is null 
* [KYLIN-4166] - kylin parse sql error
* [KYLIN-4206] - Build kylin on EMR 5.23. The kylin version is 2.6.4. When building the cube, the hive table cannot be found
* [KYLIN-4235] - Failed to load table metadata from JDBC data source
* [KYLIN-4238] - kylin_streaming_model broke when changing kylin.source.hive.database-for-flat-table to non-default value
* [KYLIN-4243] - read function in NoCompressedColumnReader is wrong.
* [KYLIN-4245] - SqlConverter returns wrong syntax SQL when SqlParser fails for JDBC source
* [KYLIN-4250] - FechRunnner should skip the job to process other jobs instead of throwing exception when the job section metadata is not found
* [KYLIN-4252] - Fix the error "Cannot read property 'index' of null" in visualization page
* [KYLIN-4259] - potential NPE bug reported by Findbugs
* [KYLIN-4260] - When using server side PreparedStatement cache, the query result are not match on TopN scenario
* [KYLIN-4263] - Inapproprate exception handling causes job stuck on running status
* [KYLIN-4272] - problems of docker/build_image.sh
* [KYLIN-4275] - Result of count(case when) is not correct
* [KYLIN-4291] - Parallel segment building may causes WriteConflictException
* [KYLIN-4295] - Instances displayed on Query Node are inconsistent with Job Node
* [KYLIN-4297] - Build cube throw NPE error when partition column is not set in JDBC Data Source
* [KYLIN-4298] - Issue with shrunken dictionary on S3
* [KYLIN-4299] - Issue with building real-time segment cache into HBase when using S3 as working dir
* [KYLIN-4300] - Create a Real-time streaming cube but not define a partition column should throw a exception 
* [KYLIN-4302] - Fix the bug that InputStream is not closed properly
* [KYLIN-4303] - Fix the bug that HBaseAdmin is not closed properly
* [KYLIN-4304] - Project list cannot be correctly sorted by "Create Time"
* [KYLIN-4307] - ExponentialBackoffRetry implemented a incorrect retry policy
* [KYLIN-4309] - One user's mailbox is not suffixed and other messages cannot be sent
* [KYLIN-4320] - number of replicas of Cuboid files cannot be configured for Spark engine
* [KYLIN-4324] - User query returns Unknown error
* [KYLIN-4340] - Fix bug of get value of isSparkFactDistinctEnable for cube not correct
* [KYLIN-4352] - A empty segment will cause incomplete query result in Realtime OLAP
* [KYLIN-4354] - Prune segment not using given filter when using jdbc preparestatement
* [KYLIN-4355] - Add validation for cube re-assignmnet(Realtime OLAP)
* [KYLIN-4357] - NPE in drop/cancel unexist job
* [KYLIN-4359] - Param Value should be required when creating a cube and adding a new measure
* [KYLIN-4363] - Query failed with "I failed to find one of the right cookies" error
* [KYLIN-4365] - RDBMS pushdown failed with with clause
* [KYLIN-4377] - Project's admin cannot operate Hybrid model
* [KYLIN-4383] - Kylin Integrated Issue with Amazon EMR and AWS Glue in HiveMetaStoreClientFactory.java
* [KYLIN-4384] - Spark build failed in EMR when KYLIN-4224 introduced
* [KYLIN-4385] - KYLIN system cube failing to update table when run on EMR with S3 as storage and EMRFS
* [KYLIN-4386] - Use LinkedHashMap for a deterministic order in test
* [KYLIN-4387] - Flink cubing merge step failed
* [KYLIN-4393] - There are several CVEs in the project dependencies
* [KYLIN-4396] - File Descriptor Leakage in MR Build Engine
* [KYLIN-4397] - Use newLinkedHashMap in AssignmentUtil.java
* [KYLIN-4405] - Internal exception when trying to build cube whose modal has null PartitionDesc 
* [KYLIN-4425] - Refactor Diagnosis Tool
* [KYLIN-4426] - Refine CliCommandExecutor 
* [KYLIN-4432] - Duplicated queries with sytax error take unexpect long time when lazy query enabled
* [KYLIN-4434] - The segment build job was not submitted successfully, but the storage of the cube has saved this segment.
* [KYLIN-4438] - Null password may cause RuntimeException when starting up
* [KYLIN-4464] - Query ... row_number over(order by c1) ... order by c2 ... get wrong order result
* [KYLIN-4469] - Cannot clone model
* [KYLIN-4470] - The user cannot log in kylin normally after being assigned to a group
* [KYLIN-4472] - After running docker image, it sometimes failed to run kylin successfully.
* [KYLIN-4481] - Project-level ACL lookups not working for non-admin SAML-federated users
* [KYLIN-4490] - Fix minor bug which add measure on frontend
* [KYLIN-4492] - Bad query leads to full gc
* [KYLIN-4496] - Metric data missing
* [KYLIN-4521] - The default-time-filter config is not available
* [KYLIN-4523] - Loading HttpContext class twice leads to LinkageError error.
* [KYLIN-4524] - system-cube.sh script can't work.
* [KYLIN-4539] - Spark build failed due to com.codahale.metrics.json.MetricsModule loaded  from kylin job jar
* [KYLIN-4577] - Throw UnrecognizedPropertyException when server self discovery enables on cdh5.7 env
* [KYLIN-4597] - Throws NPE when download diagnosis info for a job
* [KYLIN-4598] - Missing dependency when run kylin.sh org.apache.kylin.*


## v3.0.2 - 2020-05-19
_Tag:_ [kylin-3.0.2](https://github.com/apache/kylin/tree/kylin-3.0.2)
This is a bugfix release after 3.0.1, with 6 enhancements and 19 bug fixes.

__Improvement__

* [KYLIN-3628] - Query with lookup table always use latest snapshot
* [KYLIN-4132] - Kylin needn't use "org.apache.directory.api.util.Strings" to import api-util.jar
* [KYLIN-4388] - Refine the Dockerfile
* [KYLIN-4390] - Update tomcat to 7.0.100
* [KYLIN-4400] - Use beeline as hive client in system-cube.sh
* [KYLIN-4437] - Should replace deprecated "mapred.job.name"

__Bug Fix__

* [KYLIN-4119] - The admin of project can't operate the action of Hybrids
* [KYLIN-4206] - Build kylin on EMR 5.23. The kylin version is 2.6.4. When building the cube, the hive table cannot be found
* [KYLIN-4340] - Fix bug of get value of isSparkFactDistinctEnable for cube not correct
* [KYLIN-4353] - Realtime Segment is not closed in expected duration
* [KYLIN-4354] - Prune segment not using given filter when using jdbc preparestatement
* [KYLIN-4370] - Spark job failing with JDBC source on 8th step with error : org.apache.kylin.engine.spark.SparkCubingByLayer. Root cause: Table or view not found: `default`.`kylin_intermediate table'
* [KYLIN-4372] - Docker entrypoint delete file too later cause ZK started by HBase crash
* [KYLIN-4379] - Calculate column cardinality cannot use kylin config overwrite cause job failed
* [KYLIN-4383] - Kylin Integrated Issue with Amazon EMR and AWS Glue in HiveMetaStoreClientFactory.java
* [KYLIN-4385] - KYLIN system cube failing to update table when run on EMR with S3 as storage and EMRFS
* [KYLIN-4396] - File Descriptor Leakage in MR Build Engine
* [KYLIN-4397] - Use newLinkedHashMap in AssignmentUtil.java
* [KYLIN-4405] - Internal exception when trying to build cube whose modal has null PartitionDesc
* [KYLIN-4425] - Refactor Diagnosis Tool
* [KYLIN-4426] - Refine CliCommandExecutor
* [KYLIN-4433] - When uhc step is turned on, Build Dimension Dictionary job cannot get correct configuration
* [KYLIN-4438] - Null password may cause RuntimeException when starting up
* [KYLIN-4470] - The user cannot log in kylin normally after being assigned to a group
* [KYLIN-4481] - Project-level ACL lookups not working for non-admin SAML-federated users

## v3.0.1 - 2020-02-20
_Tag:_ [kylin-3.0.1](https://github.com/apache/kylin/tree/kylin-3.0.1)
This is a bugfix release after 3.0.0, with 10 enhancements and 14 bug fixes.

__Improvement__

* [KYLIN-3956] - Segments of not only streaming cube but also batch cube need to show their status
* [KYLIN-4197] - DiagnosisInfoCLI block forever at "beeline --version"
* [KYLIN-4225] - unclosed hive session cause too many temp file
* [KYLIN-4237] - Return error when execute 'explain plan for SQL' to get the execution plan of SQL
* [KYLIN-4280] - SegmentPruner add the checks for "OR" filtering
* [KYLIN-4287] - SegmentPruner cannot prune segment with "IN" or "OR" CompareTupleFilter
* [KYLIN-4292] - Use HFileOutputFormat3 in all places to replace HFileOutputFormat2
* [KYLIN-4327] - TOPN Comparator may violate its general contract
* [KYLIN-4333] - Build Server OOM
* [KYLIN-4374] - Fix security issues reported by code analysis platform LGTM

__Bug Fix__

* [KYLIN-4080] - Project schema update event causes error reload NEW DataModelDesc
* [KYLIN-4161] - exception in update metrics when the response is null
* [KYLIN-4166] - kylin parse sql error
* [KYLIN-4235] - Failed to load table metadata from JDBC data source
* [KYLIN-4238] - kylin_streaming_model broke when changing kylin.source.hive.database-for-flat-table to non-default value
* [KYLIN-4243] - read function in NoCompressedColumnReader is wrong.
* [KYLIN-4250] - FechRunnner should skip the job to process other jobs instead of throwing exception when the job section metadata is not found
* [KYLIN-4252] - Fix the error "Cannot read property 'index' of null" in visualization page
* [KYLIN-4260] - When using server side PreparedStatement cache, the query result are not match on TopN scenario
* [KYLIN-4295] - Instances displayed on Query Node are inconsistent with Job Node
* [KYLIN-4297] - Build cube throw NPE error when partition column is not set in JDBC Data Source
* [KYLIN-4300] - Create a Real-time streaming cube but not define a partition column should throw a exception
* [KYLIN-4304] - Project list cannot be correctly sorted by "Create Time"
* [KYLIN-4359] - Param Value should be required when creating a cube and adding a new measure

## v3.0.0 - 2019-12-20
_Tag:_ [kylin-3.0.0](https://github.com/apache/kylin/tree/kylin-3.0.0)
This is the GA release of Kylin's next generation after 2.x, with the new real-time OLAP feature.

__New Feature__

* [KYLIN-4098] - Add cube auto merge api
* [KYLIN-3883] - Kylin supports column count aggregation

__Improvement__

* [KYLIN-565] - Unsupported SQL Functions
* [KYLIN-1772] - Highlight segment at HBase tab page of cube admin view when the segment is not healthy.
* [KYLIN-1850] - Show Kylin Version on GUI
* [KYLIN-2431] - StorageCleanupJob will remove intermediate tables created by other kylin instances
* [KYLIN-3756] - Support check-port-availability script for mac os x
* [KYLIN-3865] - Centralize the zookeeper related info
* [KYLIN-3906] - ExecutableManager is spelled as ExecutableManger
* [KYLIN-3907] - Sort the cube list by create time in descending order.
* [KYLIN-3917] - Add max segment merge span to cleanup intermediate data of cube building
* [KYLIN-4010] - Auto adjust offset according to query server's timezone for time derived column
* [KYLIN-4096] - Make cube metadata validator rules configuable
* [KYLIN-4097] - Throw exception when too many dict slice eviction in AppendTrieDictionary
* [KYLIN-4163] - CreateFlatHiveTableStep has not yarn app url when hive job running
* [KYLIN-4167] - Refactor streaming coordinator
* [KYLIN-4175] - Support secondary hbase storage config for hbase cluster migration
* [KYLIN-4178] - Job scheduler support safe mode
* [KYLIN-4180] - Prevent abnormal CPU usage by limiting flat filters length
* [KYLIN-4187] - Building dimension dictionary using spark
* [KYLIN-4193] - More user-friendly page for loading streaming tables
* [KYLIN-4198] - “bin/system-cube.sh cron” will overwrite user's crontab
* [KYLIN-4201] - Allow users to delete unused receivers from streaming page
* [KYLIN-4208] - RT OLAP kylin.stream.node configure optimization support all receiver can have the same config
* [KYLIN-4257] - Build historical data by layer in real time Lambda cube
* [KYLIN-4258] - Real-time OLAP may return incorrect result for some case
* [KYLIN-4273] - Make cube planner works for real-time streaming job
* [KYLIN-4283] - FileNotFound error in "Garbage Collection" step should not break cube building.

__Bug Fix__

* [KYLIN-1716] - leave executing query page action stop bug
* [KYLIN-3730] - TableMetadataManager.reloadSourceTableQuietly is wrong
* [KYLIN-3741] - when the sql result is empty and limit is 0 , should not have "load more" bar
* [KYLIN-3842] - kylinProperties.js Unable to get the public configuration of the first line in the front end
* [KYLIN-3881] - Calcite isolating expression with its condition may throw 'Division Undefined' exception
* [KYLIN-3887] - Query with decimal sum measure of double complied failed after KYLIN-3703
* [KYLIN-3933] - Currently replica set related operation need refresh current front-end page
* [KYLIN-4135] - Real time streaming segment build task discard but can't be rebuilt
* [KYLIN-4147] - User has project's admin permission but doesn't have permission to see the Storage/Planner/streaming tab in Model page
* [KYLIN-4162] - After drop the build task on the monitor page, subsequent segments cannot be constructed.
* [KYLIN-4165] - RT OLAP building job on "Save Cube Dictionaries" step concurrency error
* [KYLIN-4169] - Too many logs while DataModelManager init, cause the first RESTful API hang for a long time
* [KYLIN-4172] - Can't rename field when map streaming schema to table
* [KYLIN-4176] - Filter the intermediate tables when loading table metadata from tree
* [KYLIN-4183] - Clicking 'Submit' button is unresponsive, when the segment is not selected.
* [KYLIN-4190] - hiveproducer write() function throw exception because hive mertics table location path prefix is different with defaut fs when hdfs uses router-based federation
* [KYLIN-4194] - Throw KylinConfigCannotInitException at STEP "Extract Fact Table Distinct Columns" with spark
* [KYLIN-4203] - Disable a real time cube and then enable it ,this cube may can't submit build job anymore
* [KYLIN-4229] - String index out of range -1
* [KYLIN-4242] - Usage instructions in 'PasswordPlaceholderConfigurer' doesn't work
* [KYLIN-4244] - ClassNotFoundException while use org.apache.kylin.engine.mr.common.CubeStatsReader in bash
* [KYLIN-4246] - Wrong results from real-time streaming when an optional field is used as a dimension
* [KYLIN-4248] - When adding a user, the prompt message is incorrect when the user name is empty.
* [KYLIN-4254] - The result exporting from Insight with CSV format is empty, when sql contains Chinese
* [KYLIN-4262] - pid in GC filename inconsistent with real pid
* [KYLIN-4265] - SQL tab of cube failed when filter is not empty

## v3.0.0-beta - 2019-10-25
_Tag:_ [kylin-3.0.0-beta](https://github.com/apache/kylin/tree/kylin-3.0.0-beta)
This is the beta release of Kylin's next generation after 2.x, with the new real-time OLAP feature.

__New Feature__

* [KYLIN-4114] - Provided a self-contained docker image for Kylin
* [KYLIN-4122] - Add kylin user and group manage modules

__Improvement__

* [KYLIN-3519] - Upgrade Jacoco version to 0.8.2
* [KYLIN-3628] - Query with lookup table always use latest snapshot
* [KYLIN-3901] - Use multi threads to speed up the storage cleanup job
* [KYLIN-4010] - Auto adjust offset according to query server's timezone for time derived column
* [KYLIN-4055] - cube quey and ad-hoc query return different meta info
* [KYLIN-4067] - Speed up response of kylin cube page
* [KYLIN-4091] - support fast mode and simple mode for running CI
* [KYLIN-4092] - Support setting seperate jvm params for kylin backgroud tools
* [KYLIN-4093] - Slow query pages should be open to all users of the project
* [KYLIN-4095] - Add RESOURCE_PATH_PREFIX option in ResourceTool
* [KYLIN-4099] - Using no blocking RDD unpersist in spark cubing job
* [KYLIN-4100] - Add overall job number statistics in monitor page
* [KYLIN-4101] - set hive and spark job name when building cube
* [KYLIN-4108] - Show slow query hit cube in slow query page
* [KYLIN-4112] - Add hdfs keberos token delegation in Spark to support HBase and MR use different HDFSclusters
* [KYLIN-4121] - Cleanup hive view intermediate tables after job be finished
* [KYLIN-4127] - Remove never called classes
* [KYLIN-4128] - Remove never called methods
* [KYLIN-4129] - Remove useless code
* [KYLIN-4130] - Coordinator->StreamingBuildJobStatusChecker thread always hold a old CubeManager
* [KYLIN-4133] - support override configuration in kafka job
* [KYLIN-4137] - Accelerate metadata reloading
* [KYLIN-4139] -  Compatible old user security xml config when user upgrate new kylin version
* [KYLIN-4140] - Add the time filter for current day jobs and make default values for web configurable
* [KYLIN-4141] - Build Global Dictionary in no time
* [KYLIN-4149] - Allow user to edit streaming v2 table's  kafka cluster address and topic name
* [KYLIN-4150] - Improve docker for kylin instructions
* [KYLIN-4160] - Auto redirect to host:port/kylin when user only enter host:port in broswer
* [KYLIN-4167] - Refactor streaming coordinator
* [KYLIN-4180] - Prevent abnormal CPU usage by limiting flat filters length

__Bug Fix__

 * [KYLIN-1856] - Kylin shows old error in job step output after resume - specifically in #4 Step Name:Build Dimension Dictionary
 * [KYLIN-2820] - Query can't read window function's result from subquery
 * [KYLIN-3121] - NPE while executing a query with two left outer joins and floating point expressionson nullable fields
 * [KYLIN-3845] - Kylin build error If the Kafka data source lacks selected dimensions or metrics in thekylin stream build.
 * [KYLIN-4034] - The table should not display in Insight page when the user has no access to the table
 * [KYLIN-4039] - ZookeeperDistributedLock may not release lock when unlock operation was interrupted
 * [KYLIN-4049] - Refresh segment job will always delete old segment storage
 * [KYLIN-4057] - autoMerge job can not stop
 * [KYLIN-4066] - No planner for not ROLE_ADMIN user on WebSite
 * [KYLIN-4072] - CDH 6.x find-hbase-dependency.sh return with "base-common lib not found"
 * [KYLIN-4085] - Segment parallel building may cause segment not found
 * [KYLIN-4089] - Integration test failed with JDBCMetastore
 * [KYLIN-4103] - Make the user string in granting operation of project is case insensitive
 * [KYLIN-4106] - Illegal partition for SelfDefineSortableKey when “Extract Fact Table Distinct Columns”
 * [KYLIN-4107] - StorageCleanupJob fails to delete Hive tables with "Argument list too long" error
 * [KYLIN-4111] - drop table failed with no valid privileges after KYLIN-3857
 * [KYLIN-4115] - Always load KafkaConsumerProperties
 * [KYLIN-4117] - Intersect_count() return wrong result when column type is time
 * [KYLIN-4120] - Failed to query "select * from {lookup}" if a lookup table joined in two differentmodels
 * [KYLIN-4126] - cube name validate code cause the wrong judge of streaming type
 * [KYLIN-4135] - Real time streaming segment build task discard but can't  be rebuilt
 * [KYLIN-4143] - truncate spark executable job output
 * [KYLIN-4148] - Execute 'bin/kylin-port-replace-util.sh' to change port will cause the configurationof  'kylin.metadata.url' lost
 * [KYLIN-4153] - Failed to read big resource  /dict/xxxx at "Build Dimension Dictionary" Step
 * [KYLIN-4154] - Metadata inconsistency between multi Kylin server caused by Broadcaster closing
 * [KYLIN-4155] - Cube status can not change immediately when executed disable or enable button in web
 * [KYLIN-4157] - When using PrepareStatement query, functions within WHERE will causeInternalErrorException
 * [KYLIN-4158] - Query failed for GroupBy an expression of column with limit in SQL
 * [KYLIN-4159] - The first step of build cube job will fail and throw "Column 'xx' in where clause isambiguous" in jdbc datasource.
 * [KYLIN-4162] - After drop the build task on the monitor page, subsequent segments cannot beconstructed.
 * [KYLIN-4173] - cube list search can not work

__Test__

* [KYLIN-3878] - NPE to run sonar analysis

## v3.0.0-alpha2 - 2019-07-31
_Tag:_ [kylin-3.0.0-alpha2](https://github.com/apache/kylin/tree/kylin-3.0.0-alpha2)
This is the alpha2 release of Kylin's next generation after 2.x, with the new real-time OLAP feature.

__New Feature__

* [KYLIN-3843] - List kylin instances with their server mode on web

__Improvement__

* [KYLIN-3628] - Query with lookup table always use latest snapshot
* [KYLIN-3812] - optimize the child CompareTupleFilter in a CompareTupleFilter
* [KYLIN-3813] - don't do push down when both of the children of CompareTupleFilter are CompareTupleFilter with column included
* [KYLIN-3841] - Build Global Dict by MR/Hive
* [KYLIN-3912] - Support cube level mapreduce queue config for BeelineHiveClient
* [KYLIN-3918] - Add project name in cube and job pages
* [KYLIN-3925] - Add reduce step for FilterRecommendCuboidDataJob & UpdateOldCuboidShardJob to avoid generating small hdfs files
* [KYLIN-3932] - KafkaConfigOverride to take effect
* [KYLIN-3958] - MrHive-Dict support build by livy
* [KYLIN-3960] - Only update user when login in LDAP environment
* [KYLIN-3997] - Add a health check job of Kylin
* [KYLIN-4001] - Allow user-specified time format using real-time
* [KYLIN-4012] - optimize cache in TrieDictionary/TrieDictionaryForest
* [KYLIN-4013] - Only show the cubes under one model
* [KYLIN-4026] - Avoid too many file append operations in HiveProducer of hive metrics reporter
* [KYLIN-4028] - Speed up startup progress using cached dependency
* [KYLIN-4031] - RestClient will throw exception with message contains clear-text password
* [KYLIN-4033] - Can not access Kerberized Cluster with DebugTomcat
* [KYLIN-4035] - Calculate column cardinality by using spark engine
* [KYLIN-4041] - CONCAT NULL not working properly
* [KYLIN-4062] - Too many "if else" clause in PushDownRunnerJdbcImpl#toSqlType
* [KYLIN-4081] - Use absolute path instead of relative path for local segment cache
* [KYLIN-4084] - Reset kylin.stream.node in kylin-port-replace-util.sh
* [KYLIN-4086] - Support connect Kylin with Tableau by JDBC

__Bug Fix__

* [KYLIN-3935] - ZKUtil acquire the wrong Zookeeper Path on windows
* [KYLIN-3942] - Rea-time OLAP don't support multi-level json event
* [KYLIN-3946] - No cube for AVG measure after include count column
* [KYLIN-3959] - Realtime OLAP query result should not be cached
* [KYLIN-3981] - Auto Merge Job failed to execute on windows
* [KYLIN-4005] - Saving Cube of a aggregation Groups(40 Dimensions, Max Dimension Combination:5) may cause kylin server OOM
* [KYLIN-4017] - Build engine get zk(zookeeper) lock failed when building job, it causes the whole build engine doesn't work.
* [KYLIN-4027] - Kylin-jdbc module has tcp resource leak
* [KYLIN-4037] - Can't Cleanup Data in Hbase's HDFS Storage When Deploy Apache Kylin with Standalone HBase Cluster
* [KYLIN-4039] - ZookeeperDistributedLock may not release lock when unlock operation was interrupted
* [KYLIN-4044] - CuratorScheduler may throw NPE when init service Cache
* [KYLIN-4046] - Refine JDBC Source(source.default=8)
* [KYLIN-4064] - parameter 'engineType' is not working when running integration test
* [KYLIN-4072] - CDH 6.x find-hbase-dependency.sh return with "base-common lib not found"
* [KYLIN-4074] - Exception in thread "Memcached IO over {MemcachedConnection to ..." java.lang.NullPointerException

## v3.0.0-alpha - 2019-04-12
_Tag:_ [kylin-3.0.0-alpha](https://github.com/apache/kylin/tree/kylin-3.0.0-alpha)
This is the alpha release of Kylin's next generation after 2.x, with the new real-time OLAP feature.

__New Feature__

* [KYLIN-3654] - Kylin Real-time Streaming
* [KYLIN-3795] - Submit Spark jobs via Apache Livy
* [KYLIN-3820] - Add a curator-based scheduler

__Improvement__

* [KYLIN-3716] - FastThreadLocal replaces ThreadLocal
* [KYLIN-3744] - Add javadoc and unittest for Kylin New Streaming Solution
* [KYLIN-3759] - Streaming ClassNotFoundExeception when lambda is enable in MR job
* [KYLIN-3786] - Add integration test for real-time streaming
* [KYLIN-3791] - Map return by Maps.transformValues is a immutable view
* [KYLIN-3797] - Too many or filters may break Kylin server when flatting filter
* [KYLIN-3814] - Add pause interval for job retry
* [KYLIN-3821] - Expose real-time streaming data consuming lag info
* [KYLIN-3834] - Add monitor for curator-based scheduler
* [KYLIN-3839] - Storage clean up after refreshing or deleting a segment
* [KYLIN-3864] - Provide a function to judge whether the os type is Mac os x or not
* [KYLIN-3867] - Enable JDBC to use key store & trust store for https connection
* [KYLIN-3901] - Use multi threads to speed up the storage cleanup job
* [KYLIN-3905] - Enable shrunken dictionary default
* [KYLIN-3908] - KylinClient's HttpRequest.releaseConnection is not needed in retrieveMetaData & executeKylinQuery
* [KYLIN-3929] - Check satisfaction before execute cubeplanner algorithm
* [KYLIN-3690] - New streaming backend implementation
* [KYLIN-3691] - New streaming ui implementation
* [KYLIN-3692] - New streaming ui implementation
* [KYLIN-3745] - Real-time segment state changed from active to immutable is not sequently
* [KYLIN-3747] - Use FQDN to register a streaming receiver instead of ip
* [KYLIN-3768] - Save streaming metadata a standard kylin path in zookeeper

__Bug Fix__

* [KYLIN-3787] - NPE throws when dimension value has null when query real-time data
* [KYLIN-3789] - Stream receiver admin page issue fix
* [KYLIN-3800] - Real-time streaming count distinct result wrong
* [KYLIN-3817] - Duration in Cube building is a negative number
* [KYLIN-3818] - After Cube disabled, auto-merge cube job still running
* [KYLIN-3830] - Wrong result when 'SELECT SUM(dim1)' without set a relative metric of dim1.
* [KYLIN-3866] - Whether to set mapreduce.application.classpath is determined by the user
* [KYLIN-3880] - DataType is incompatible in Kylin HBase coprocessor
* [KYLIN-3888] - TableNotDisabledException when running "Convert Lookup Table to HFile"
* [KYLIN-3898] - Cube level properties are ineffective in the some build steps
* [KYLIN-3902] - NoRealizationFoundException due to creating a wrong JoinDesc
* [KYLIN-3909] - Spark cubing job failed for MappeableRunContainer is not registered
* [KYLIN-3911] - Check if HBase table is enabled before diabling table in DeployCoprocessorCLI
* [KYLIN-3916] - Fix cube build action issue after streaming migrate
* [KYLIN-3922] - Fail to update coprocessor when run DeployCoprocessorCLI
* [KYLIN-3923] - UT GeneralColumnDataTest fail

## v2.6.6 - 2020-05-19
_Tag:_ [kylin-2.6.6](https://github.com/apache/kylin/tree/kylin-2.6.6)
This is a bugfix release after 2.6.5, with 6 enhancements and 14 bug fixes.

__Improvement__

* [KYLIN-3628] - Query with lookup table always use latest snapshot
* [KYLIN-4132] - Kylin needn't use "org.apache.directory.api.util.Strings" to import api-util.jar
* [KYLIN-4160] - Auto redirect to host:port/kylin when user only enter host:port in broswer
* [KYLIN-4314] - Support union in intersect_count() function
* [KYLIN-4390] - Update tomcat to 7.0.100
* [KYLIN-4400] - Use beeline as hive client in system-cube.sh

__Bug Fix__

* [KYLIN-4072] - CDH 6.x find-hbase-dependency.sh return with "base-common lib not found"
* [KYLIN-4119] - The admin of project can't operate the action of Hybrids
* [KYLIN-4206] - Build kylin on EMR 5.23. The kylin version is 2.6.4. When building the cube, the hive table cannot be found
* [KYLIN-4245] - SqlConverter returns wrong syntax SQL when SqlParser fails for JDBC source
* [KYLIN-4354] - Prune segment not using given filter when using jdbc preparestatement
* [KYLIN-4370] - Spark job failing with JDBC source on 8th step with error : org.apache.kylin.engine.spark.SparkCubingByLayer. Root cause: Table or view not found: `default`.`kylin_intermediate table'
* [KYLIN-4379] - Calculate column cardinality cannot use kylin config overwrite cause job failed
* [KYLIN-4383] - Kylin Integrated Issue with Amazon EMR and AWS Glue in HiveMetaStoreClientFactory.java
* [KYLIN-4385] - KYLIN system cube failing to update table when run on EMR with S3 as storage and EMRFS
* [KYLIN-4405] - Internal exception when trying to build cube whose modal has null PartitionDesc
* [KYLIN-4426] - Refine CliCommandExecutor
* [KYLIN-4433] - When uhc step is turned on, Build Dimension Dictionary job cannot get correct configuration
* [KYLIN-4438] - Null password may cause RuntimeException when starting up
* [KYLIN-4481] - Project-level ACL lookups not working for non-admin SAML-federated users

## v2.6.5 - 2020-02-20
_Tag:_ [kylin-2.6.5](https://github.com/apache/kylin/tree/kylin-2.6.5)
This is a bugfix release after 2.6.4, with 12 enhancements and 20 bug fixes.

__Improvement__

* [KYLIN-2230] - Can not catch kylin.sh path in linux resouce PATH setting
* [KYLIN-2431] - StorageCleanupJob will remove intermediate tables created by other kylin instances
* [KYLIN-4180] - Prevent abnormal CPU usage by limiting flat filters length
* [KYLIN-4198] - “bin/system-cube.sh cron” will overwrite user's crontab
* [KYLIN-4225] - Unclosed hive session cause too many temp file
* [KYLIN-4226] - Skip current unavailable tables when updating hbase coprocessor
* [KYLIN-4280] - SegmentPruner add the checks for "OR" filtering
* [KYLIN-4283] - FileNotFound error in "Garbage Collection" step should not break cube building.
* [KYLIN-4290] - Add file lock to kylin startup script to avoid starting multiple instances on one node
* [KYLIN-4292] - Use HFileOutputFormat3 in all places to replace HFileOutputFormat2
* [KYLIN-4293] - Backport HBASE-22887 to Kylin HFileOutputFormat3
* [KYLIN-4374] - Fix security issues reported by code analysis platform LGTM

__Bug Fix__

* [KYLIN-1716] - Leave executing query page action stop bug
* [KYLIN-3409] - Write metric error when run a query.
* [KYLIN-3741] - when the sql result is empty and limit is 0 , should not have "load more" bar
* [KYLIN-4080] - Project schema update event causes error reload NEW DataModelDesc
* [KYLIN-4161] - exception in update metrics when the response is null
* [KYLIN-4166] - kylin parse sql error
* [KYLIN-4169] - Too many logs while DataModelManager init, cause the first RESTful API hang for a long time
* [KYLIN-4183] - Clicking 'Submit' button is unresponsive, when the segment is not selected.
* [KYLIN-4195] - The cube size is "NaN KB" after purging one cube.
* [KYLIN-4238] - kylin_streaming_model broke when changing kylin.source.hive.database-for-flat-table to non-default value
* [KYLIN-4244] - ClassNotFoundException while use org.apache.kylin.engine.mr.common.CubeStatsReader in bash
* [KYLIN-4250] - FechRunnner should skip the job to process other jobs instead of throwing exception when the job section metadata is not found
* [KYLIN-4252] - Fix the error "Cannot read property 'index' of null" in visualization page
* [KYLIN-4254] - The result exporting from Insight with CSV format is empty, when sql contains Chinese
* [KYLIN-4262] - pid in GC filename inconsistent with real pid
* [KYLIN-4263] - Inappropriate exception handling causes job stuck on running status
* [KYLIN-4291] - Parallel segment building may causes WriteConflictException
* [KYLIN-4304] - Project list cannot be correctly sorted by "Create Time"
* [KYLIN-4309] - One user's mailbox is not suffixed and other messages cannot be sent
* [KYLIN-4359] - Param Value should be required when creating a cube and adding a new measure

## v2.6.4 - 2019-10-12
_Tag:_ [kylin-2.6.4](https://github.com/apache/kylin/tree/kylin-2.6.4)
This is a bugfix release after 2.6.3, with 10 enhancements and 17 bug fixes.

__Improvement__

* [KYLIN-3628] - Query with lookup table always use latest snapshot
* [KYLIN-3797] - Too many or filters may break Kylin server when flatting filter
* [KYLIN-4013] - Only show the cubes under one model
* [KYLIN-4047] - Use push-down query when division dynamic column cube query is not supported
* [KYLIN-4055] - cube quey and ad-hoc query return different meta info
* [KYLIN-4093] - Slow query pages should be open to all users of the project
* [KYLIN-4099] - Using no blocking RDD unpersist in spark cubing job
* [KYLIN-4121] - Cleanup hive view intermediate tables after job be finished
* [KYLIN-4140] - Add the time filter for current day jobs and make default values for web configurable

__Bug Fix__

* [KYLIN-1856] - Kylin shows old error in job step output after resume - specifically in #4 Step Name: Build Dimension Dictionary
* [KYLIN-4034] - The table should not display in Insight page when the user has no access to the table
* [KYLIN-4037] - Can't Cleanup Data in Hbase's HDFS Storage When Deploy Apache Kylin with Standalone HBase Cluster
* [KYLIN-4046] - Refine JDBC Source(source.default=8)
* [KYLIN-4057] - autoMerge job can not stop
* [KYLIN-4066] - No planner for not ROLE_ADMIN user on WebSite
* [KYLIN-4074] - Exception in thread "Memcached IO over {MemcachedConnection to ..." java.lang.NullPointerException
* [KYLIN-4103] - Make the user string in granting operation of project is case insensitive
* [KYLIN-4106] - Illegal partition for SelfDefineSortableKey when “Extract Fact Table Distinct Columns”
* [KYLIN-4111] - drop table failed with no valid privileges after KYLIN-3857
* [KYLIN-4115] - Always load KafkaConsumerProperties
* [KYLIN-4131] - Broadcaster memory leak
* [KYLIN-4152] - Should Disable Before Deleting HBase Table using HBaseAdmin
* [KYLIN-4153] - Failed to read big resource  /dict/xxxx at "Build Dimension Dictionary" Step
* [KYLIN-4157] - When using PrepareStatement query, functions within WHERE will cause InternalErrorException
* [KYLIN-4158] - Query failed for GroupBy an expression of column with limit in SQL
* [KYLIN-4159] - The first step of build cube job will fail and throw "Column 'xx' in where clause is ambiguous" in jdbc datasource.

## v2.6.3 - 2019-07-06
_Tag:_ [kylin-2.6.3](https://github.com/apache/kylin/tree/kylin-2.6.3)
This is a bugfix release after 2.6.2, with 7 enhancements and 9 bug fixes.

__Improvement__

* [KYLIN-4024] - Support pushdown to Presto
* [KYLIN-3977] - Avoid mistaken deleting dicts by storage cleanup while building jobs are running
* [KYLIN-4023] - Convert to local time for column with timestamp or date type of queries by Jdbc
* [KYLIN-3893] - Cube build failed for wrong row key column description
* [KYLIN-4020] - Add  check for the input of fix_length rowkey encoding
* [KYLIN-3998] - Make "bpus-min-benefit-ratio" configurable in cube planner phase 1
* [KYLIN-4025] - Add detail exception in kylin http response

__Bug Fix__

* [KYLIN-4015] - Fix build cube error at the "Build UHC Dictionary" step
* [KYLIN-4022] - Error with message "Unrecognized column type: DECIMAL(xx,xx)" happens when do query pushdown
* [KYLIN-3994] - Storage cleanup job may delete data of newly built segment because of cube cache in CubeManager
* [KYLIN-2620] - Result of sum measure may by replaced by the TopN measure with the same column during query
* [KYLIN-4041] - Concat function with null value does not work correctly
* [KYLIN-3978] - InternalErrorException happens for queries with precise count distinct measures
* [KYLIN-3845] - Streaming cube build error if the Kafka data source lacks selected dimensions or metrics
* [KYLIN-3980] - Cube planner cuboid id error there are too many dimensions
* [KYLIN-4049] - Storage should be cleaned after cube refreshing with config kylin.storage.clean-after-delete-operation set to true

## v2.6.2 - 2019-05-19
_Tag:_ [kylin-2.6.2](https://github.com/apache/kylin/tree/kylin-2.6.2)
This is a bugfix release after 2.6.1, with 9 enhancements and 27 bug fixes.

__Improvement__

* [KYLIN-3905] - Enable shrunken dictionary default
* [KYLIN-3571] - Do not package Spark in Kylin's binary package
* [KYLIN-3866] - Add config to determine whether use local classpath for mapreduce's classpath
* [KYLIN-3839] - Storage clean up after refreshing or deleting a segment
* [KYLIN-3885] - Build dimension dictionary with spark costs too much time
* [KYLIN-3873] - Fix inappropriate use of memory in SparkFactDistinct.java
* [KYLIN-3892] - Enable set job's building priority
* [KYLIN-3857] - Add parameter to change sql quoting character for sqoop
* [KYLIN-3987] - Give more reducer for ultra high cardinality columns in fact distinct job

__Bug Fix__

* [KYLIN-3835] - Source table reloading does not check table's reference by models
* [KYLIN-2620] - Result of sum(measure) may not be accurate if there exists a topn measure for the same column
* [KYLIN-3838] - Retry mechanism is invalid when build cube with spark
* [KYLIN-3808] - Some time fields in response of rest api for listing jobs are always 0
* [KYLIN-3818] - After Cube disabled, auto-merge cube job still running
* [KYLIN-3830] - Wrong result for query 'SELECT SUM(dim1)' without setting a relative measure of dim1
* [KYLIN-3817] - Duration in Cube building is a negative number
* [KYLIN-3874] - Building step "Convert Cuboid Data to HFile" failed when HBase and MR use different HDFS clusters
* [KYLIN-3880] - DataType is incompatible in Kylin HBase coprocessor
* [KYLIN-3888] - TableNotDisabledException when run step "Convert Lookup Table to HFile"
* [KYLIN-3884] - Loading hfile to HBase failed for temporary dir in output path
* [KYLIN-3474] - Tableau 10.5 gets malformed token after connected with Kylin
* [KYLIN-3895] - Failed to register new MBean when "kylin.server.query-metrics-enabled" set true
* [KYLIN-3909] - Spark cubing job failed for MappeableRunContainer is not registered
* [KYLIN-3898] - Cube level properties are ineffective in the some build steps
* [KYLIN-3911] - Check if HBase table is enabled before disabling tables when update coprocessor
* [KYLIN-3922] - Fail to update coprocessor when run DeployCoprocessorCLI
* [KYLIN-3938] - Can't delete job with type "OPTIMIZE CHECKPOINT"
* [KYLIN-3950] - Cube planner optimize job only use inmem algorithm
* [KYLIN-3788] - Incorrect time zone conversion in parsing Kafka Streaming Data
* [KYLIN-3957] - Query system cube get exception "cannot cast java.math.BigDecimal to java.lang.Double"
* [KYLIN-3943] - Fix hardcode in system-cube.sh
* [KYLIN-3936] - MR/Spark task will still run after the job is stopped
* [KYLIN-3968] - Customized precision of sum measure doesn't work on web
* [KYLIN-3965] - SQLException with message "No suitable driver found for jdbc:kylin://" when try to connect Kylin with JDBC
* [KYLIN-3926] - Set source record count for cubes with topn and count distinct measures when updating statistics
* [KYLIN-3934] - Sqoop import param '--null-string' result in null value become blank string in hive table

## v2.6.1 - 2019-03-08
_Tag:_ [kylin-2.6.1](https://github.com/apache/kylin/tree/kylin-2.6.1)
This is a bugfix release after 2.6.0, with 7 enhancements and 19 bug fixes.

__Improvement__

* [KYLIN-3780] - Add built instance in Job info and email notification
* [KYLIN-3794] - mergeToInClause in TupleFilterVisitor cannot work properly in some edge cases
* [KYLIN-3804] - Advanced Snapshot Table save not friendly
* [KYLIN-3816] - Current CI doesn't cover the case of streaming table join lookup table
* [KYLIN-3819] - kylin.sh run in the foreground
* [KYLIN-3822] - "bin/kylin.sh org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI [file_path] [nOfThread] all" can not run successfully
* [KYLIN-3826] - MergeCuboidJob only uploads necessary segment's dictionary

__Bug Fix__

* [KYLIN-3494] - Build cube with spark reports ArrayIndexOutOfBoundsException
* [KYLIN-3537] - Use Spark to build Cube on Yarn faild at Setp8 on HDP3.0
* [KYLIN-3703] - Get negative value when sum on an integer column
* [KYLIN-3714] - com.esotericsoftware.kryo.KryoException: java.lang.IllegalArgumentException: Class is not registered: scala.reflect.ManifestFactory$$anon$2
* [KYLIN-3727] - Can not build empty segment, HadoopShellException
* [KYLIN-3770] - ZipFileUtils is not compatible CubeMetaIngester
* [KYLIN-3772] - CubeMetaIngester works abnormally
* [KYLIN-3773] - Using Mysql instead of Hive as Data Source fails with LinkageError
* [KYLIN-3776] - Float type in MySQL not properly converted to HIVE Double Type
* [KYLIN-3778] - Invalid data source for each project at project level
* [KYLIN-3782] - ZookeeperDistributedLock can't acquir lock on windows because wrong Path
* [KYLIN-3790] - Wrong error code when command executor is interrupted
* [KYLIN-3793] - org.apache.kylin.source.kafka.util.KafkaSampleProducer exit after generating 1 message
* [KYLIN-3798] - SQL Server cannot list databases
* [KYLIN-3799] - Wrong hive-exec jar may be found by find-hive-dependency.sh on CDH
* [KYLIN-3801] - find-hive-dependency.sh fail to grep env:CLASSPATH from beeline output
* [KYLIN-3815] - Unexpected behavior when joinning streaming table and hive table
* [KYLIN-3828] - ArrayIndexOutOfBoundsException thrown when build a streaming cube with empty data in its first dimension
* [KYLIN-3833] - Potential OOM in Spark Extract Fact Table Distinct Columns step
* [KYLIN-3836] - Kylin StringUtil.join() may cause NPE if iterator is empty


## v2.6.0 - 2019-01-12
_Tag:_ [kylin-2.6.0](https://github.com/apache/kylin/tree/kylin-2.6.0)
This is a major release after 2.5, with more than 90 bug fixes and enhancements.

__New Feature__

* [KYLIN-3552] - Data Source SDK to ingest data from different JDBC sources

__Improvement__

* [KYLIN-1111] - Ignore unsupported hive column types when sync hive table
* [KYLIN-2861] - For dictionary building of lookup table columns, reduce the table scan chance
* [KYLIN-2895] - Refine query cache
* [KYLIN-2932] - Simplify the thread model for in-memory cubing
* [KYLIN-2972] - CacheKey from SQLRequest should ignore the case of project name
* [KYLIN-3005] - getAllDictColumnsOnFact in CubeManager may include fact table's foreign key
* [KYLIN-3021] - Check MapReduce job failed reason and include the diagnostics into email notification
* [KYLIN-3272] - Upgrade Spark dependency to 2.3.2
* [KYLIN-3326] - Better way to update migration target cluster's metadata after cube migration
* [KYLIN-3430] - Global Dictionary Cleanup
* [KYLIN-3445] - Upgrade checkstyle version to 8.6
* [KYLIN-3496] - Make calcite extras props available in JDBC Driver
* [KYLIN-3497] - Make JDBC Module more testable
* [KYLIN-3499] - Double check timestamp in HBase when got "RetriesExhaustedException"
* [KYLIN-3540] - Improve Mandatory Cuboid Recommendation Algorithm
* [KYLIN-3544] - Refine guava cache for the recommended cuboids of cube planner
* [KYLIN-3570] - Scripts to automatically build system cube
* [KYLIN-3592] - Synchronized should be placed after static in declaration
* [KYLIN-3597] - Fix sonar reported static code issues phase 1
* [KYLIN-3600] - Utility classes should not have public constructors
* [KYLIN-3602] - Enable more checkstyle rules
* [KYLIN-3611] - Upgrade Tomcat to 7.0.91, 8.5.34 or later
* [KYLIN-3617] - Reduce number of visiting metastore for job scheduler
* [KYLIN-3628] - Query with lookup table always use latest snapshot
* [KYLIN-3630] - Remove unused fields in the implementations of MeasureType
* [KYLIN-3631] - Utilize Arrays#parallelSort for better performance
* [KYLIN-3640] - Cube with desc broken should be able to delete
* [KYLIN-3655] - Reinitialize CubeInstance when clear segments
* [KYLIN-3656] - Improve HLLCounter performance
* [KYLIN-3670] - Misspelled constant DEFAUL_JOB_CONF_SUFFIX
* [KYLIN-3671] - Improve ResourceTool and JDBCResourceStore's performance
* [KYLIN-3700] - Quote sql identities when creating flat table
* [KYLIN-3707] - Add configuration for setting isolation-level for sqoop
* [KYLIN-3720] - Add column family check when save/update cube desc
* [KYLIN-3729] - CLUSTER BY CAST(field AS STRING) will accelerate base cuboid build with UHC global dict
* [KYLIN-3737] - Refactor cache part for RDBMS
* [KYLIN-3749] - Add configuration to override xml for RDBMS

__Bug Fix__

* [KYLIN-1819] - Exception swallowed when start DefaultScheduler fail
* [KYLIN-2841] - LIMIT pushdown should be applied to subquery
* [KYLIN-2973] - Potential issue of not atomically update cube instance map
* [KYLIN-3291] - 在构建好的cube上提交逻辑相同的sql查询结果不同
* [KYLIN-3406] - When the /execute_output/ metadata file sinking to HDFS is deleted, the monitoring page does not display any tasks.
* [KYLIN-3416] - Kylin bitmap null pointer exception when "group by" is an expression
* [KYLIN-3439] - configuration "kylin.web.timezone" is Inconsistent between kylin-defaults.properties and KylinConfigBase.java
* [KYLIN-3515] - Cubing jobs may interfere with each other if use same hive view
* [KYLIN-3574] - Unclosed lookup table in DictionaryGeneratorCLI#processSegment
* [KYLIN-3575] - Unclosed Connection in DriverTest
* [KYLIN-3576] - IllegalArgumentException: No enum constant org.apache.kylin.rest.service.JobService.JobSearchMode.
* [KYLIN-3578] - Do not synchronize on the intrinsic locks of high-level concurrency objects
* [KYLIN-3579] - entrySet iterator should be used in BPUSCalculator
* [KYLIN-3580] - CuboidStatsUtil#complementRowCountForMandatoryCuboids uses entrySet to add elements
* [KYLIN-3581] - compareTo method should be paired with equals method
* [KYLIN-3586] - Boxing/unboxing to parse a primitive is suboptimal
* [KYLIN-3588] - Potentially duplicate put to RemoveBlackoutRealizationsRule#filters map
* [KYLIN-3589] - Different type is used to check presence in Collection in AclPermissionUtil
* [KYLIN-3590] - Missing argument to exception message String in ModelController
* [KYLIN-3594] - Select with Catalog fails
* [KYLIN-3603] - HBase connection isn't closed in UpdateHTableHostCLI
* [KYLIN-3607] - can't build cube with spark in v2.5.0
* [KYLIN-3619] - Some job won't clean up temp directory after finished
* [KYLIN-3620] - "--" should not be a comment marker use between single quotes in SQL
* [KYLIN-3643] - Derived column from windowSpec not working in where
* [KYLIN-3653] - After kylin configured with hive data source with beeline, build failed if two jobs for creating flat table submitted at same time
* [KYLIN-3662] - exception message "Cannot find project '%s'." should be formated
* [KYLIN-3663] - Failed to delete project when project has more than one table
* [KYLIN-3665] - Partition time column may never be added
* [KYLIN-3680] - Spark cubing failed with JDBC resource
* [KYLIN-3684] - [Script] find-hive-dependency.sh HIVE_LIB is not set or not resolved correctly
* [KYLIN-3695] - Error while creating hive table through Kylin build cube with mysql imported tables
* [KYLIN-3697] - check port availability when starts kylin instance
* [KYLIN-3699] - SparkCubingByLayer. Root cause: null
* [KYLIN-3710] - JDBC data source not support Spark cubing
* [KYLIN-3715] - ProjectL2Cache don't be invalidated when adding override config in cube level
* [KYLIN-3718] - Segments in volatile range is more than designated value
* [KYLIN-3721] - Failed to get source table when write the wrong password at the first time
* [KYLIN-3722] - Error Limit Push Down in Join Related Query
* [KYLIN-3724] - Kylin IT test sql is unreasonable
* [KYLIN-3731] - java.lang.IllegalArgumentException: Unsupported data type array<string> at
* [KYLIN-3734] - UT Failed:Invalid path string "/kylin/../examples/test_metadata/job_engine/global_job_engine_lock"
* [KYLIN-3740] - system_cube构建出现bug
* [KYLIN-3748] - No realization found exception thrown when a ready cube is building
* [KYLIN-3752] - NullPointerException in the first cubing step：org.apache.kylin.source.hive.CreateFlatHiveTableStep.getCubeSpecificConfig(CreateFlatHiveTableStep.java:92)

__Task__

* [KYLIN-3232] - Need document for ops tools
* [KYLIN-3290] - Avoid calling Class#newInstance
* [KYLIN-3559] - Use Splitter for splitting String
* [KYLIN-3560] - Should not depend on personal repository
* [KYLIN-3642] - Exclude conflict jar

__Sub-task__

* [KYLIN-2894] - Change the query cache expiration strategy by signature checking
* [KYLIN-2896] - Refine query exception cache
* [KYLIN-2897] - Improve the query execution for a set of duplicate queries in a short period
* [KYLIN-2898] - Introduce memcached as a distributed cache for queries
* [KYLIN-2899] - Introduce segment level query cache

__Test__

* [KYLIN-3365] - Add unit test for the coprocessor code, CubeVisitService

## v2.5.2 - 2018-12-04
_Tag:_ [kylin-2.5.2](https://github.com/apache/kylin/tree/kylin-2.5.2)
This is a bugfix release after 2.5.1, with 12 bug fixes and enhancement. 

__Improvement__

* [KYLIN-3669] - Add log to GTStreamAggregateScanner
* [KYLIN-3676] - Update to custom calcite and remove the "atopcalcite" module
* [KYLIN-3704] - Upgrade the calcite version to 1.16

__Bug fix__

* [KYLIN-3187] - JDK APIs using the default locale, time zone or character set should be avoided
* [KYLIN-3636] - Wrong "storage_type" in CubeDesc causing cube building error
* [KYLIN-3666] - Mege cube step 2: Update dictionary throws IllegalStateException
* [KYLIN-3672] - Performance is poor when multiple queries occur in short period
* [KYLIN-3678] - CacheStateChecker may remove a cache file that under building
* [KYLIN-3683] - package org.apache.commons.lang3 not exists
* [KYLIN-3689] - When the startTime is equal to the endTime in build request, the segment will build all data.
* [KYLIN-3693] - TopN, Count distinct incorrect in Spark engine
* [KYLIN-3705] - Segment Pruner mis-functions when the source data has Chinese characters

## v2.5.1 - 2018-11-06
_Tag:_ [kylin-2.5.1](https://github.com/apache/kylin/tree/kylin-2.5.1)
This is a bugfix release after 2.5.0, with 30 bug fixes and enhancement. Check [How to upgrade](/docs31/howto/howto_upgrade.html).

__Improvement__

* [KYLIN-3520] - Deal with NULL values of measures for inmem cubing
* [KYLIN-3526] - No check for hybrid name with special character
* [KYLIN-3528] - No details page for hybrid
* [KYLIN-3599] - Bulk Add Measures
* [KYLIN-3611] - Upgrade Tomcat to 7.0.91, 8.5.34 or later
* [KYLIN-3632] - Add configuration that can switch on/off preparedStatement cache in Kylin server
* [KYLIN-3646] - Add instruction about rowkey sequence in "Advanced setting" page

__Bug fix__

* [KYLIN-2200] - CompileException on UNION ALL query when result only contains one column
* [KYLIN-3439] - configuration "kylin.web.timezone" is Inconsistent between kylin-defaults.properties and KylinConfigBase.java
* [KYLIN-3527] - Hybrid couldn't save when there is only 1 cube
* [KYLIN-3531] - Login failed with case-insensitive username
* [KYLIN-3543] - Unclosed Job instance in CreateHTableJob#exportHBaseConfiguration
* [KYLIN-3546] - Kylin doesn't persist FK to intermediate table if FK is not a dimension
* [KYLIN-3556] - Interned string should not be used as lock object
* [KYLIN-3562] - TS conflict when kylin update metadata in HBase
* [KYLIN-3565] - User login error message is inaccurate
* [KYLIN-3567] - Change scala dependency to provided
* [KYLIN-3582] - String comparison should not use == in RecordEvent
* [KYLIN-3595] - Beeline does not retrieve CLASSPATH and hangs
* [KYLIN-3604] - Can't build cube with spark in HBase standalone mode
* [KYLIN-3613] - Kylin with Standalone HBase Cluster (enabled kerberos) could not find the main cluster namespace at "Create HTable" step
* [KYLIN-3629] - NullPointException throws when use preparedStatement cache in some case
* [KYLIN-3633] - Dead lock may happen in building global dictionary
* [KYLIN-3634] - When filter column has null value may cause incorrect query result
* [KYLIN-3635] - Percentile calculation on Spark engine is wrong
* [KYLIN-3644] - NumberFormatExcetion on null values when building cube with Spark
* [KYLIN-3645] - Kylin does not clean table metadata when drop project(Kafka Source)
* [KYLIN-3647] - Fix inconsistent states of job and its sub-task
* [KYLIN-3649] - segment region count and size are not correct when using mysql as Kylin metadata storage
* [KYLIN-3651] - JDBCResourceStore doesn't list all resources

## v2.5.0 - 2018-09-16
_Tag:_ [kylin-2.5.0](https://github.com/apache/kylin/tree/kylin-2.5.0)
This is a major release after 2.4, with 96 bug fixes and enhancement. Check [How to upgrade](/docs31/howto/howto_upgrade.html).

__New Feature__

* [KYLIN-2565] - Support Hadoop 3.0
* [KYLIN-3488] - Support MySQL as Kylin metadata storage
* [KYLIN-3366] - Configure automatic enabling of cubes after a build process

__Improvement__

* [KYLIN-2998] - Kill spark app when cube job was discarded
* [KYLIN-3033] - Support HBase 2.0
* [KYLIN-3071] - Add config to reuse dict to reduce dict size
* [KYLIN-3094] - Upgrade zookeeper to 3.4.12
* [KYLIN-3146] - Response code and exception should be standardised for cube checking
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
* [KYLIN-3479] - Model can save when kafka partition date column not select
* [KYLIN-3480] - Change the conformance of calcite from default to lenient
* [KYLIN-3481] - Kylin Jdbc: Shaded dependencies should not be transitive
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

__Bug fix__

* [KYLIN-2522] - Compilation fails with Java 8 when upgrading to hbase 1.2.5
* [KYLIN-2662] - NegativeArraySizeException in "Extract Fact Table Distinct Columns"
* [KYLIN-2933] - Fix compilation against the Kafka 1.0.0 release
* [KYLIN-3025] - kylin odbc error : {fn CONVERT} for bigint type in tableau 10.4
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
* [KYLIN-3500] - kylin 2.4 use jdbc datasource :Unknown column 'A.A.CRT_DATE' in 'where clause'
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

## v2.4.1 - 2018-09-09
_Tag:_ [kylin-2.4.1](https://github.com/apache/kylin/tree/kylin-2.4.1)
This is a bug fix release after 2.4.0, with 22 bug fixes and enhancement. Check [How to upgrade](/docs31/howto/howto_upgrade.html).

__Improvement__

* [KYLIN-3421] - Improve job scheduler fetch performance
* [KYLIN-3424] - Missing invoke addCubingGarbageCollectionSteps in the cleanup step for HBaseMROutput2Transition
* [KYLIN-3422] - Support multi-path of domain for kylin connection
* [KYLIN-3463] - Improve optimize job by avoiding creating empty output files on HDFS
* [KYLIN-3503] - Missing java.util.logging.config.file when starting kylin instance
* [KYLIN-3507] - Query NPE when project is not found

__Bug fix__

* [KYLIN-2662] - NegativeArraySizeException in "Extract Fact Table Distinct Columns
* [KYLIN-3025] - kylin odbc error : {fn CONVERT} for bigint type in tableau 10.4
* [KYLIN-3255] - Cannot save cube
* [KYLIN-3347] - QueryService Exception when using calcite function ex : {fn CURRENT_TIMESTAMP(0)}
* [KYLIN-3391] - BadQueryDetector only detect first query
* [KYLIN-3403] - Querying sample cube with filter "KYLIN_CAL_DT.WEEK_BEG_DT >= CAST('2001-09-09' AS DATE)" returns unexpected empty result set
* [KYLIN-3428] - java.lang.OutOfMemoryError: Requested array size exceeds VM limit
* [KYLIN-3438] - mapreduce.job.queuename does not work at 'Convert Cuboid Data to HFile' Step
* [KYLIN-3451] - Cloned cube doesn't have Mandatory Cuboids copied
* [KYLIN-3456] - Cube level's snapshot config does not work
* [KYLIN-3460] - {fn CURRENT_DATE()} parse error
* [KYLIN-3461] - "metastore.sh refresh-cube-signature" not updating cube signature as expected
* [KYLIN-3476] - Fix TupleExpression verification when parsing sql
* [KYLIN-3492] - Wrong constant value in KylinConfigBase.getDefaultVarcharPrecision
* [KYLIN-3500] - kylin 2.4 use jdbc datasource :Unknown column 'A.A.CRT_DATE' in 'where clause'
* [KYLIN-3505] - DataType.getType wrong usage of cache


## v2.4.0 - 2018-06-23
_Tag:_ [kylin-2.4.0](https://github.com/apache/kylin/tree/kylin-2.4.0)
This is a major release after 2.3.x, with 8 new features and more than 30 bug fixes bug fixes and enhancement. Check [How to upgrade](/docs31/howto/howto_upgrade.html).

__New Feature__

* [KYLIN-2484] - Spark engine to support source from Kafka
* [KYLIN-3221] - Allow externalizing lookup table snapshot
* [KYLIN-3283] - Support values RelNode
* [KYLIN-3315] - Allow each project to set its own source at project level
* [KYLIN-3343] - Support JDBC source on UI
* [KYLIN-3358] - Support sum(case when...), sum(2*price+1), count(column) and more
* [KYLIN-3378] - Support Kafka table join with Hive tables

__Improvement__

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
* [KYLIN-3331] - Kylin start script hangs during retrieving hive dependencys
* [KYLIN-3345] - Use Apache Parent POM 19
* [KYLIN-3354] - KeywordDefaultDirtyHack cannot handle double-quoted defaultCatalog identifier
* [KYLIN-3369] - Reduce the data size sink from Kafka topic to HDFS
* [KYLIN-3380] - Allow to configure sqoop for jdbc source with a kylin_sqoop_conf.xml like hive
* [KYLIN-3386] - TopN measure validate code refactor to make it more clear

__Bug fix__

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

__Task__

* [KYLIN-3327] - Upgrade surefire version to 2.21.0
* [KYLIN-3372] - Upgrade jackson-databind version due to security concerns
* [KYLIN-3415] - Remove "external" module

__Sub-task__

* [KYLIN-3359] - Support sum(expression) if possible
* [KYLIN-3362] - Support dynamic dimension push down
* [KYLIN-3364] - Make the behavior of BigDecimalSumAggregator consistent with hive
* [KYLIN-3373] - Some improvements for lookup table - UI part change
* [KYLIN-3374] - Some improvements for lookup table - metadata change
* [KYLIN-3375] - Some improvements for lookup table - build change
* [KYLIN-3376] - Some improvements for lookup table - query change
* [KYLIN-3377] - Some improvements for lookup table - snapshot management

## v2.3.2 - 2018-07-08
_Tag:_ [kylin-2.3.2](https://github.com/apache/kylin/tree/kylin-2.3.2)
This is a bug fix release after 2.3.1, with 12 bug fixes and enhancement. Check [How to upgrade](/docs23/howto/howto_upgrade.html).

__Improvement__

* [KYLIN-3345] - Use Apache Parent POM 19
* [KYLIN-3372] - Upgrade jackson-databind version due to security concerns
* [KYLIN-3415] - Remove "external" module

__Bug fix__

* [KYLIN-3115] - Incompatible RowKeySplitter initialize between build and merge job
* [KYLIN-3336] - java.lang.NoSuchMethodException: org.apache.kylin.tool.HBaseUsageExtractor.execute([Ljava.lang.String;)
* [KYLIN-3348] - "missing LastBuildJobID" error when building new cube segment
* [KYLIN-3352] - Segment pruning bug, e.g. date_col > "max_date+1"
* [KYLIN-3363] - Wrong partition condition appended in JDBC Source
* [KYLIN-3388] - Data may become not correct if mappers fail during the redistribute step, "distribute by rand()"
* [KYLIN-3400] - WipeCache and createCubeDesc causes deadlock
* [KYLIN-3401] - The current using zip compress tool has an arbitrary file write vulnerability
* [KYLIN-3404] - Last optimized time detail was not showing after cube optimization

## v2.3.1 - 2018-03-28
_Tag:_ [kylin-2.3.1](https://github.com/apache/kylin/tree/kylin-2.3.1)
This is a bug fix release after 2.3.0, with 12 bug fixes and enhancement. Check [How to upgrade](/docs31/howto/howto_upgrade.html).

__Improvement__

* [KYLIN-3233] - CacheController can not handle if cacheKey has "/"
* [KYLIN-3278] - Kylin should not distribute hive table by random at Step1
* [KYLIN-3300] - Upgrade jackson-databind to 2.6.7.1 with security issue fixed
* [KYLIN-3301] - Upgrade opensaml to 2.6.6 with security issue fixed

__Bug fix__

* [KYLIN-3270] - Fix the blocking issue in Cube optimizing job
* [KYLIN-3276] - Fix the query cache bug with dynamic parameter
* [KYLIN-3288] - "Sqoop To Flat Hive Table" step should specify "mapreduce.queue.name"
* [KYLIN-3306] - Fix the rarely happened unit test exception of generic algorithm
* [KYLIN-3287] - When a shard by column is in dict encoding, dict building error.
* [KYLIN-3280] - The delete button should not be enabled without any segment in cube segment delete confirm dialog
* [KYLIN-3119] - A few bugs in the function 'massageSql' of 'QueryUtil.java'
* [KYLIN-3236] - The function 'reGenerateAdvancedDict()' has an error logical judgment, which will cause an exception when you edit the cube.


## v2.3.0 - 2018-03-04
_Tag:_ [kylin-2.3.0](https://github.com/apache/kylin/tree/kylin-2.3.0)
This is a major release after 2.2, with more than 250 bug fixes and enhancement. Check [How to upgrade](/docs31/howto/howto_upgrade.html).

__New Feature__

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

__Improvement__

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
* [KYLIN-2649] - Tableau could send "select *" on a big table
* [KYLIN-2645] - Upgrade Kafka version to 0.11.0.1
* [KYLIN-2556] - Switch Findbugs to Spotbugs
* [KYLIN-2363] - Prune cuboids by capping number of dimensions
* [KYLIN-1925] - Do not allow cross project clone for cube
* [KYLIN-1872] - Make query visible and interruptible, improve server's stablility

__Bug fix__

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
* [KYLIN-3211] - Class IntegerDimEnc shuould give more exception information when the length is exceed the max or less than the min
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
* [KYLIN-3116] - Fix cardinality caculate checkbox issue when loading tables
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
* [KYLIN-3049] - Filter the invalid zero value of "Auto Merge Thresholds" parameter when you create or upate a cube.
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
* [KYLIN-3020] - Improve org.apache.hadoop.util.ToolRunner to be threadsafe
* [KYLIN-3017] - Footer covers the selection box and some options can not be selected
* [KYLIN-3016] - StorageCleanup job doesn't clean up all the legacy fiels in a in Read/Write seperation environment
* [KYLIN-3004] - Update validation when deleting segment
* [KYLIN-3001] - Fix the wrong Cache key issue 
* [KYLIN-2995] - Set SparkContext.hadoopConfiguration to HadoopUtil in Spark Cubing
* [KYLIN-2994] - Handle NPE when load dict in DictionaryManager
* [KYLIN-2991] - Query hit NumberFormatException if partitionDateFormat is not yyyy-MM-dd
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

__Task__

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

__Sub-Task__

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

## v2.2.0 - 2017-11-03

_Tag:_ [kylin-2.2.0](https://github.com/apache/kylin/tree/kylin-2.2.0)
This is a major release after 2.1, with more than 70 bug fixes and enhancements. Check [How to upgrade](/docs21/howto/howto_upgrade.html).

__New Feature__

* [KYLIN-2703] - Manage ACL through Apache Ranger
* [KYLIN-2752] - Make HTable name prefix configurable
* [KYLIN-2761] - Table Level ACL
* [KYLIN-2775] - Streaming Cube Sample

__Improvement__

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

__Bug__

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


__Task__

* [KYLIN-2782] - Replace DailyRollingFileAppender with RollingFileAppender to allow log retention
* [KYLIN-2925] - Provide document for Ranger security integration
* [KYLIN-2549] - Modify tools that related to Acl
* [KYLIN-2728] - Introduce a new cuboid scheduler based on cuboid tree rather than static rules
* [KYLIN-2729] - Introduce greedy algorithm for cube planner
* [KYLIN-2730] - Introduce genetic algorithm for cube planner
* [KYLIN-2802] - Enable cube planner phase one
* [KYLIN-2826] - Add basic support classes for cube planner algorithms
* [KYLIN-2961] - Provide user guide for Ranger Kylin Plugin

## v2.1.0 - 2017-08-17

_Tag:_ [kylin-2.1.0](https://github.com/apache/kylin/tree/kylin-2.1.0)
This is a major release after 2.0, with more than 100 bug fixes and enhancements. Check [How to upgrade](/docs21/howto/howto_upgrade.html).

__New Feature__

* [KYLIN-1351] - Support RDBMS as data source
* [KYLIN-2515] - Route unsupported query back to source
* [KYLIN-2646] - Project level query authorization
* [KYLIN-2665] - Add model JSON edit in web 

__Improvement__

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

__Bug fix__

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
* [KYLIN-2588] - Query failed when two top-n measure with order by count(*) exists in one cube
* [KYLIN-2589] - Enhance thread-safe in Authentication
* [KYLIN-2592] - Fix distinct count measure build failed issue with spark cubing 
* [KYLIN-2593] - Fix NPE issue when querying with Ton-N by count(*) 
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
* [KYLIN-2652] - Make KylinConfig threadsafe in CubeVisitService
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
* [KYLIN-2548] - Keep ACL information backward compatibile

## v2.0.0 - 2017-04-30

_Tag:_ [kylin-2.0.0](https://github.com/apache/kylin/tree/kylin-2.0.0)
This is a major release with **Spark Cubing**, **Snowflake Data Model** and runs **TPC-H Benchmark**. Check out [the download](/download/) and the [how to upgrade guide](/docs20/howto/howto_upgrade.html).

__New Feature__

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

__Improvements__

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
* [KYLIN-2193] - parameterise org.apache.kylin.storage.translate.DerivedFilterTranslator#IN_THRESHOLD
* [KYLIN-2195] - Setup naming convention for kylin properties
* [KYLIN-2196] - Update Tomcat clas loader to parallel loader
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
* [KYLIN-2263] - Display reasonable exception message if could not find kafka dependency for streaming build
* [KYLIN-2266] - Reduce memory usage for building global dict
* [KYLIN-2269] - Reduce MR memory usage for global dict
* [KYLIN-2280] - A easier way to change all the conflict ports when start multi kylin instance in the same server
* [KYLIN-2283] - Have a general purpose data generation tool
* [KYLIN-2287] - Speed up model and cube list load in Web
* [KYLIN-2290] - minor improvements on limit
* [KYLIN-2294] - Refactor CI, merge with_slr and without_slr cubes
* [KYLIN-2295] - Refactor CI, blend view cubes into the rest
* [KYLIN-2296] - Allow cube to override kafka configuration
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
* [KYLIN-2378] - Set job thread name with job uuid
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

__Bug fix__

* [KYLIN-1603] - Building job still finished even MR job error happened.
* [KYLIN-1770] - Upgrade Calcite dependency (v1.10)
* [KYLIN-1793] - Job couldn't stop when hive commands got error with beeline
* [KYLIN-1945] - Cuboid.translateToValidCuboid method throw exception while cube building or query execute
* [KYLIN-2077] - Inconsistent cube desc signature for CubeDesc
* [KYLIN-2153] - Allow user to skip the check in CubeMetaIngester
* [KYLIN-2155] - get-properties.sh doesn't support parameters starting with "-n"
* [KYLIN-2166] - Unclosed HBaseAdmin in StorageCleanupJob#cleanUnusedHBaseTables
* [KYLIN-2172] - Potential NPE in ExecutableManager#updateJobOutput
* [KYLIN-2174] - partitoin column format visibility issue
* [KYLIN-2176] - org.apache.kylin.rest.service.JobService#submitJob will leave orphan NEW segment in cube when exception is met
* [KYLIN-2191] - Integer encoding error for width from 5 to 7
* [KYLIN-2197] - Has only base cuboid for some cube desc
* [KYLIN-2202] - Fix the conflict between KYLIN-1851 and KYLIN-2135
* [KYLIN-2207] - Ineffective null check in ExtendCubeToHybridCLI#createFromCube()
* [KYLIN-2208] - Unclosed FileReader in HiveCmdBuilder#build()
* [KYLIN-2209] - Potential NPE in StreamingController#deserializeTableDesc()
* [KYLIN-2211] - IDictionaryValueEnumerator should return String instead of byte[]
* [KYLIN-2212] - 'NOT' operator in filter on derived column may get incorrect result
* [KYLIN-2213] - UnsupportedOperationException when excute 'not like' query on cube v1
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
* [KYLIN-2382] - The column order of "select *" is not as defined in the table
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
* [KYLIN-2555] - minor issues about acl and granted autority

__Tasks__

* [KYLIN-1799] - Add a document to setup kylin on spark engine?
* [KYLIN-2293] - Refactor KylinConfig to remove test related code
* [KYLIN-2327] - Enable check-style for test code
* [KYLIN-2344] - Package spark into Kylin binary package
* [KYLIN-2368] - Enable Findbugs plugin
* [KYLIN-2386] - Revert KYLIN-2349 and KYLIN-2353
* [KYLIN-2521] - upgrade to calcite 1.12.0


## v1.6.0 - 2016-11-26

_Tag:_ [kylin-1.6.0](https://github.com/apache/kylin/tree/kylin-1.6.0)
This is a major release with better support for using Apache Kafka as data source. Check [how to upgrade](/docs16/howto/howto_upgrade.html) to do the upgrading.

__New Feature__

* [KYLIN-1726] - Scalable streaming cubing
* [KYLIN-1919] - Support Embedded Structure when Parsing Streaming Message
* [KYLIN-2055] - Add an encoder for Boolean type
* [KYLIN-2067] - Add API to check and fill segment holes
* [KYLIN-2079] - add explicit configuration knob for coprocessor timeout
* [KYLIN-2088] - Support intersect count for calculation of retention or conversion rates
* [KYLIN-2125] - Support using beeline to load hive table metadata

__Bug fix__

* [KYLIN-1565] - Read the kv max size from HBase config
* [KYLIN-1820] - Column autocomplete should remove the user input in model designer
* [KYLIN-1828] - java.lang.StringIndexOutOfBoundsException in org.apache.kylin.storage.hbase.util.StorageCleanupJob
* [KYLIN-1967] - Dictionary rounding can cause IllegalArgumentException in GTScanRangePlanner
* [KYLIN-1978] - kylin.sh compatible issue on Ubuntu
* [KYLIN-1990] - The SweetAlert at the front page may out of the page if the content is too long.
* [KYLIN-2007] - CUBOID_CACHE is not cleared when rebuilding ALL cache
* [KYLIN-2012] - more robust approach to hive schema changes
* [KYLIN-2024] - kylin TopN only support the first measure 
* [KYLIN-2027] - Error "connection timed out" occurs when zookeeper's port is set in hbase.zookeeper.quorum of hbase-site.xml
* [KYLIN-2028] - find-*-dependency script fail on Mac OS
* [KYLIN-2035] - Auto Merge Submit Continuously
* [KYLIN-2041] - Wrong parameter definition in Get Hive Tables REST API
* [KYLIN-2043] - Rollback httpclient to 4.2.5 to align with Hadoop 2.6/2.7
* [KYLIN-2044] - Unclosed DataInputByteBuffer in BitmapCounter#peekLength
* [KYLIN-2045] - Wrong argument order in JobInstanceExtractor#executeExtract()
* [KYLIN-2047] - Ineffective null check in MetadataManager
* [KYLIN-2050] - Potentially ineffective call to close() in QueryCli
* [KYLIN-2051] - Potentially ineffective call to IOUtils.closeQuietly()
* [KYLIN-2052] - Edit "Top N" measure, the "group by" column wasn't displayed
* [KYLIN-2059] - Concurrent build issue in CubeManager.calculateToBeSegments()
* [KYLIN-2069] - NPE in LookupStringTable
* [KYLIN-2078] - Can't see generated SQL at Web UI
* [KYLIN-2084] - Unload sample table failed
* [KYLIN-2085] - PrepareStatement return incorrect result in some cases
* [KYLIN-2086] - Still report error when there is more than 12 dimensions in one agg group
* [KYLIN-2093] - Clear cache in CubeMetaIngester
* [KYLIN-2097] - Get 'Column does not exist in row key desc" on cube has TopN measure
* [KYLIN-2099] - Import table error of sample table KYLIN_CAL_DT
* [KYLIN-2106] - UI bug - Advanced Settings - Rowkeys - new Integer dictionary encoding - could possibly impact also cube metadata
* [KYLIN-2109] - Deploy coprocessor only this server own the table
* [KYLIN-2110] - Ineffective comparison in BooleanDimEnc#equals()
* [KYLIN-2114] - WEB-Global-Dictionary bug fix and improve
* [KYLIN-2115] - some extended column query returns wrong answer
* [KYLIN-2116] - when hive field delimitor exists in table field values, fields order is wrong
* [KYLIN-2119] - Wrong chart value and sort when process scientific notation 
* [KYLIN-2120] - kylin1.5.4.1 with cdh5.7 cube sql Oops Faild to take action
* [KYLIN-2121] - Failed to pull data to PowerBI or Excel on some query
* [KYLIN-2127] - UI bug fix for Extend Column
* [KYLIN-2130] - QueryMetrics concurrent bug fix
* [KYLIN-2132] - Unable to pull data from Kylin Cube ( learn_kylin cube ) to Excel or Power BI for Visualization and some dimensions are not showing up.
* [KYLIN-2134] - Kylin will treat empty string as NULL by mistake
* [KYLIN-2137] - Failed to run mr job when user put a kafka jar in hive's lib folder
* [KYLIN-2138] - Unclosed ResultSet in BeelineHiveClient
* [KYLIN-2146] - "Streaming Cluster" page should remove "Margin" inputbox
* [KYLIN-2152] - TopN group by column does not distinguish between NULL and ""
* [KYLIN-2154] - source table rows will be skipped if TOPN's group column contains NULL values
* [KYLIN-2158] - Delete joint dimension not right
* [KYLIN-2159] - Redistribution Hive Table Step always requires row_count filename as 000000_0 
* [KYLIN-2167] - FactDistinctColumnsReducer may get wrong max/min partition col value
* [KYLIN-2173] - push down limit leads to wrong answer when filter is loosened
* [KYLIN-2178] - CubeDescTest is unstable
* [KYLIN-2201] - Cube desc and aggregation group rule combination max check fail
* [KYLIN-2226] - Build Dimension Dictionary Error

__Improvement__

* [KYLIN-1042] - Horizontal scalable solution for streaming cubing
* [KYLIN-1827] - Send mail notification when runtime exception throws during build/merge cube
* [KYLIN-1839] - improvement set classpath before submitting mr job
* [KYLIN-1917] - TopN counter merge performance improvement
* [KYLIN-1962] - Split kylin.properties into two files
* [KYLIN-1999] - Use some compression at UT/IT
* [KYLIN-2019] - Add license checker into checkstyle rule
* [KYLIN-2033] - Refactor broadcast of metadata change
* [KYLIN-2042] - QueryController puts entry in Cache w/o checking QueryCacheEnabled
* [KYLIN-2054] - TimedJsonStreamParser should support other time format
* [KYLIN-2068] - Import hive comment when sync tables
* [KYLIN-2070] - UI changes for allowing concurrent build/refresh/merge
* [KYLIN-2073] - Need timestamp info for diagnose  
* [KYLIN-2075] - TopN measure: need select "constant" + "1" as the SUM|ORDER parameter
* [KYLIN-2076] - Improve sample cube and data
* [KYLIN-2080] - UI: allow multiple building jobs for the same cube
* [KYLIN-2082] - Support to change streaming configuration
* [KYLIN-2089] - Make update HBase coprocessor concurrent
* [KYLIN-2090] - Allow updating cube level config even the cube is ready
* [KYLIN-2091] - Add API to init the start-point (of each parition) for streaming cube
* [KYLIN-2095] - Hive mr job use overrided MR job configuration by cube properties
* [KYLIN-2098] - TopN support query UHC column without sorting by sum value
* [KYLIN-2100] - Allow cube to override HIVE job configuration by properties
* [KYLIN-2108] - Support usage of schema name "default" in SQL
* [KYLIN-2111] - only allow columns from Model dimensions when add group by column to TOP_N
* [KYLIN-2112] - Allow a column be a dimension as well as "group by" column in TopN measure
* [KYLIN-2113] - Need sort by columns in SQLDigest
* [KYLIN-2118] - allow user view CubeInstance json even cube is ready
* [KYLIN-2122] - Move the partition offset calculation before submitting job
* [KYLIN-2126] - use column name as default dimension name when auto generate dimension for lookup table
* [KYLIN-2140] - rename packaged js with different name when build
* [KYLIN-2143] - allow more options from Extended Columns,COUNT_DISTINCT,RAW_TABLE
* [KYLIN-2162] - Improve the cube validation error message
* [KYLIN-2221] - rethink on KYLIN-1684
* [KYLIN-2083] - more RAM estimation test for MeasureAggregator and GTAggregateScanner
* [KYLIN-2105] - add QueryId
* [KYLIN-1321] - Add derived checkbox for lookup table columns on Auto Generate Dimensions panel
* [KYLIN-1995] - Upgrade MapReduce properties which are deprecated

__Task__

* [KYLIN-2072] - Cleanup old streaming code
* [KYLIN-2081] - UI change to support embeded streaming message
* [KYLIN-2171] - Release 1.6.0


## v1.5.4.1 - 2016-09-28
_Tag:_ [kylin-1.5.4.1](https://github.com/apache/kylin/tree/kylin-1.5.4.1)
This version fixes two major bugs introduced in 1.5.4; The metadata and HBase coprocessor is compatible with 1.5.4.

__Bug fix__

* [KYLIN-2010] - Date dictionary return wrong SQL result
* [KYLIN-2026] - NPE occurs when build a cube without partition column
* [KYLIN-2032] - Cube build failed when partition column isn't in dimension list

## v1.5.4 - 2016-09-15
_Tag:_ [kylin-1.5.4](https://github.com/apache/kylin/tree/kylin-1.5.4)
This version includes bug fixs/enhancements as well as new features; It is backward compatiple with v1.5.3; While after upgrade, you still need update coprocessor, refer to [How to update coprocessor](/docs15/howto/howto_update_coprocessor.html).

__New Feature__

* [KYLIN-1732] - Support Window Function
* [KYLIN-1767] - UI for TopN: specify encoding and multiple "group by"
* [KYLIN-1849] - Search cube by name in Web UI
* [KYLIN-1908] - Collect Metrics to JMX
* [KYLIN-1921] - Support Grouping Funtions
* [KYLIN-1964] - Add a companion tool of CubeMetaExtractor for cube importing

__Bug__

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

__Improvement__

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

## v1.5.3 - 2016-07-28
_Tag:_ [kylin-1.5.3](https://github.com/apache/kylin/tree/kylin-1.5.3)
This version includes many bug fixs/enhancements as well as new features; It is backward compatiple with v1.5.2; But after upgrade, you need to update coprocessor, refer to [How to update coprocessor](/docs15/howto/howto_update_coprocessor.html).

__New Feature__

* [KYLIN-1478] - TopN measure should support non-dictionary encoding for ultra high cardinality
* [KYLIN-1693] - Support multiple group-by columns for TOP_N meausre
* [KYLIN-1752] - Add an option to fail cube build job when source table is empty
* [KYLIN-1756] - Allow user to run MR jobs against different Hadoop queues

__Bug fix__

* [KYLIN-1499] - Couldn't save query, error in backend
* [KYLIN-1568] - Calculate row value buffer size instead of hard coded ROWVALUE_BUFFER_SIZE
* [KYLIN-1645] - Exception inside coprocessor should report back to the query thread
* [KYLIN-1646] - Column appeared twice if it was declared as both dimension and measure
* [KYLIN-1676] - High CPU in TrieDictionary due to incorrect use of HashMap
* [KYLIN-1679] - bin/get-properties.sh cannot get property which contains space or equals sign
* [KYLIN-1684] - query on table "kylin_sales" return empty resultset after cube "kylin_sales_cube" which generated by sample.sh is ready
* [KYLIN-1694] - make multiply coefficient configurable when estimating cuboid size
* [KYLIN-1695] - Skip cardinality calculation job when loading hive table
* [KYLIN-1703] - The not-thread-safe ToolRunner.run() will cause concurrency issue in job engine
* [KYLIN-1704] - When load empty snapshot, NULL Pointer Exception occurs
* [KYLIN-1723] - GTAggregateScanner$Dump.flush() must not write the WHOLE metrics buffer
* [KYLIN-1738] - MRJob Id is not saved to kylin jobs if MR job is killed
* [KYLIN-1742] - kylin.sh should always set KYLIN_HOME to an absolute path
* [KYLIN-1755] - TopN Measure IndexOutOfBoundsException
* [KYLIN-1760] - Save query hits org.apache.hadoop.hbase.TableNotFoundException: kylin_metadata_user
* [KYLIN-1762] - Query threw NPE with 3 or more join conditions
* [KYLIN-1769] - There is no response when click "Property" button at Cube Designer
* [KYLIN-1777] - Streaming cube build shouldn't check working segment
* [KYLIN-1780] - Potential issue in SnapshotTable.equals()
* [KYLIN-1781] - kylin.properties encoding error while contain chinese prop key or value
* [KYLIN-1783] - Can't add override property at cube design 'Configuration Overwrites' step.
* [KYLIN-1785] - NoSuchElementException when Mandatory Dimensions contains all Dimensions
* [KYLIN-1787] - Properly deal with limit clause in CubeHBaseEndpointRPC (SELECT * problem)
* [KYLIN-1788] - Allow arbitrary number of mandatory dimensions in one aggregation group
* [KYLIN-1789] - Couldn't use View as Lookup when join type is "inner"
* [KYLIN-1795] - bin/sample.sh doesn't work when configured hive client is beeline
* [KYLIN-1800] - IllegalArgumentExceptio: Too many digits for NumberDictionary: -0.009999999999877218. Expect 19 digits before decimal point at max.
* [KYLIN-1803] - ExtendedColumn Measure Encoding with Non-ascii Characters
* [KYLIN-1811] - Error step may be skipped sometimes when resume a cube job
* [KYLIN-1816] - More than one base KylinConfig exist in spring JVM
* [KYLIN-1817] - No result from JDBC with Date filter in prepareStatement
* [KYLIN-1838] - Fix sample cube definition
* [KYLIN-1848] - Can't sort cubes by any field in Web UI
* [KYLIN-1862] - "table not found" in "Build Dimension Dictionary" step
* [KYLIN-1879] - RestAPI /api/jobs always returns 0 for exec_start_time and exec_end_time fields
* [KYLIN-1882] - it report can't find the intermediate table in '#4 Step Name: Build Dimension Dictionary' when use hive view as lookup table
* [KYLIN-1896] - JDBC support mybatis
* [KYLIN-1905] - Wrong Default Date in Cube Build Web UI
* [KYLIN-1909] - Wrong access control to rest get cubes
* [KYLIN-1911] - NPE when extended column has NULL value
* [KYLIN-1912] - Create Intermediate Flat Hive Table failed when using beeline
* [KYLIN-1913] - query log printed abnormally if the query contains "\r" (not "\r\n")
* [KYLIN-1918] - java.lang.UnsupportedOperationException when unload hive table

__Improvement__

* [KYLIN-1319] - Find a better way to check hadoop job status
* [KYLIN-1379] - More stable and functional precise count distinct implements after KYLIN-1186
* [KYLIN-1656] - Improve performance of MRv2 engine by making each mapper handles a configured number of records
* [KYLIN-1657] - Add new configuration kylin.job.mapreduce.min.reducer.number
* [KYLIN-1669] - Deprecate the "Capacity" field from DataModel
* [KYLIN-1677] - Distribute source data by certain columns when creating flat table
* [KYLIN-1705] - Global (and more scalable) dictionary
* [KYLIN-1706] - Allow cube to override MR job configuration by properties
* [KYLIN-1714] - Make job/source/storage engines configurable from kylin.properties
* [KYLIN-1717] - Make job engine scheduler configurable
* [KYLIN-1718] - Grow ByteBuffer Dynamically in Cube Building and Query
* [KYLIN-1719] - Add config in scan request to control compress the query result or not
* [KYLIN-1724] - Support Amazon EMR
* [KYLIN-1725] - Use KylinConfig inside coprocessor
* [KYLIN-1728] - Introduce dictionary metadata
* [KYLIN-1731] - allow non-admin user to edit 'Advenced Setting' step in CubeDesigner
* [KYLIN-1747] - Calculate all 0 (except mandatory) cuboids
* [KYLIN-1749] - Allow mandatory only cuboid
* [KYLIN-1751] - Make kylin log configurable
* [KYLIN-1766] - CubeTupleConverter.translateResult() is slow due to date conversion
* [KYLIN-1775] - Add Cube Migrate Support for Global Dictionary
* [KYLIN-1782] - API redesign for CubeDesc
* [KYLIN-1786] - Frontend work for KYLIN-1313 (extended columns as measure)
* [KYLIN-1792] - behaviours for non-aggregated queries
* [KYLIN-1805] - It's easily got stuck when deleting HTables during running the StorageCleanupJob
* [KYLIN-1815] - Cleanup package size
* [KYLIN-1818] - change kafka dependency to provided
* [KYLIN-1821] - Reformat all of the java files and enable checkstyle to enforce code formatting
* [KYLIN-1823] - refactor kylin-server packaging
* [KYLIN-1846] - minimize dependencies of JDBC driver
* [KYLIN-1884] - Reload metadata automatically after migrating cube
* [KYLIN-1894] - GlobalDictionary may corrupt when server suddenly crash
* [KYLIN-1744] - Separate concepts of source offset and date range on cube segments
* [KYLIN-1654] - Upgrade httpclient dependency
* [KYLIN-1774] - Update Kylin's tomcat version to 7.0.69
* [KYLIN-1861] - Hive may fail to create flat table with "GC overhead error"

## v1.5.2.1 - 2016-06-07
_Tag:_ [kylin-1.5.2.1](https://github.com/apache/kylin/tree/kylin-1.5.2.1)

This is a hot-fix version on v1.5.2, no new feature introduced, please upgrade to this version;

__Bug fix__

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

__Bug fix__

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

__Bug fix__

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
* ​
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

__Bug fix__

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

__Bug fix__

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

__Bug fix__

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

__Bug fix__

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

