---
layout: docs30
title:  "Kylin Configuration"
categories: install
permalink: /docs30/install/configuration.html
---


- [Configuration Files and Overriding](#kylin-config)
    - [Kylin Configuration Files](#kylin-config)
	- [Configuration Overriding](#config-override)
		- [Project-level Configuration Overriding](#project-config-override)
		- [Cube-level Configuration Overriding](#cube-config-override)
		- [MapReduce Configuration Overriding](#mr-config-override)
		- [Hive Configuration Overriding](#hive-config-override)
        - [Spark Configuration Overriding](#spark-config-override)
- [Deployment configuration](#kylin-deploy)
    - [Deploy Kylin](#deploy-config)
	- [Allocate More Memory for Kylin](#kylin-jvm-settings)
	- [Job Engine HA](#job-engine-ha)
	- [Job Engine Safemode](#job-engine-safemode)
	- [Read/Write Separation](#rw-deploy)
	- [RESTful Webservice](#rest-config)
- [Metastore Configuration](#kylin_metastore)
    - [Metadata-related](#metadata)
    - [MySQL Metastore Configuration (Beta)](#mysql-metastore)
- [Modeling Configuration](#kylin-build)
    - [Hive Client and SparkSQL](#hive-client-and-sparksql)
    - [JDBC Datasource Configuration](#jdbc-datasource)
    - [Data Type Precision](#precision-config)
    - [Cube Design](#cube-config)
    - [Cube Size Estimation](#cube-estimate)
	- [Cube Algorithm](#cube-algorithm)
	- [Auto Merge Segments](#auto-merge)
	- [Lookup Table Snapshot](#snapshot)
	- [Build Cube](#cube-build)
	- [Dictionary-related](#dict-config)
	- [Deal with Ultra-High-Cardinality Columns](#uhc-config)
	- [Spark as Build Engine](#spark-cubing)
	- [Submit Spark jobs via Livy](#livy-submit-spark-job)
	- [Spark Dynamic Allocation](#dynamic-allocation)
	- [Job-related](#job-config)
	- [Enable Email Notification](#email-notification)
	- [Enable Cube Planner](#cube-planner)
    - [HBase Storage](#hbase-config)
    - [Enable Compression](#compress-config)
    - [Real-time OLAP](#realtime-olap)
- [Storage Clean up Configuration](#storage-clean-up-configuration)
    - [Storage-clean-up-related](#storage-clean-up-config)
- [Query Configuration](#kylin-query)
    - [Query-related](#query-config)
    - [Fuzzy Query](#fuzzy)
	- [Query Cache](#cache-config)
	- [Query Limits](#query-limit)
	- [Bad Query](#bad-query)
	- [Query Pushdown](#query-pushdown)
	- [Query rewriting](#convert-sql)
	- [Collect Query Metrics to JMX](#jmx-metrics)
	- [Collect Query Metrics to dropwizard](#dropwizard-metrics)
- [Security Configuration](#kylin-security)
	- [Integrated LDAP for SSO](#ldap-sso)
	- [Integrate with Apache Ranger](#ranger)
	- [Enable ZooKeeper ACL](#zookeeper-acl)
- [Distributed query cache with Memcached](#distributed-cache)



### Configuration Files and Overriding {#kylin-config}

This section introduces Kylin's configuration files and how to perform Configuration Overriding.



### Kylin Configuration Files	 {#kylin-config-file}

Kylin will automatically read the Hadoop configuration (`core-site.xml`), Hive configuration (`hive-site.xml`) and HBase configuration (`hbase-site.xml`) from the environment, in addition, Kylin's configuration files are in the `$KYLIN_HOME/conf/` directory.
Kylin's configuration file is as follows:

- `kylin_hive_conf.xml`: This file contains the configuration for the Hive job.
- `kylin_job_conf.xml` & `kylin_job_conf_inmem.xml`: This file contains configuration for the MapReduce job. When performing the *In-mem Cubing* job, user need to request more memory for the mapper in `kylin_job_conf_inmem.xml`
- `kylin-kafka-consumer.xml`: This file contains the configuration for the Kafka job.
- `kylin-server-log4j.properties`: This file contains the log configuration for the Kylin server.
- `kylin-tools-log4j.properties`: This file contains the log configuration for the Kylin command line.
- `setenv.sh` : This file is a shell script for setting environment variables. Users can adjust the size of the Kylin JVM stack with `KYLIN_JVM_SETTINGS` and set other environment variables such as `KAFKA_HOME`.
- `kylin.properties`: This file contains Kylin global configuration.



### Configuration Overriding {#config-override}

Some configuration files in `$KYLIN_HOME/conf/` can be overridden in the Web UI. Configuration Overriding has two scope: *Project level* and *Cube level*. The priority order can be stated as: Cube level configurations > Project level configurations > configuration files.



### Project-level Configuration Overriding {#project-config-override}

Click *Manage Project* in the web UI interface, select a project, click *Edit* -> *Project Config* -> *+ Property* to add configuration properties which could override property values in configuration files, as the figure below shown,
![](/images/install/override_config_project.png)



### Cube-level Configuration Overriding		{#cube-config-override}

In the *Configuration overrides* step of *Cube Designer*, user could rewrite property values to override those in project level and configuration files, as the figure below shown,
![](/images/install/override_config_cube.png)

The following configurations can be override in the Cube-level,

- `kylin.cube.size-estimate*`
- `kylin.cube.algorithm*`
- `kylin.cube.aggrgroup*`
- `kylin.metadata.dimension-encoding-max-length`
- `kylin.cube.max-building-segments`
- `kylin.cube.is-automerge-enabled`
- `kylin.job.allow-empty-segment`
- `kylin.job.sampling-percentage`
- `kylin.source.hive.redistribute-flat-table`
- `kylin.engine.spark*`
- `kylin.query.skip-empty-segments`



### MapReduce Configuration Overriding {#mr-config-override}

Kylin supports overriding configuration properties in `kylin_job_conf.xml` and `kylin_job_conf_inmem.xml` at the project and cube level, in the form of key-value pairs, in the following format:
`kylin.engine.mr.config-override.<key> = <value>`
* If user wants getting more memory from YARN for jobs, user can set:  `kylin.engine.mr.config-override.mapreduce.map.java.opts=-Xmx7g` and `kylin.engine.mr.config-override.mapreduce.map.memory.mb=8192`
* If user wants the cube's build job to use a different YARN resource queue, user can set: 
`kylin.engine.mr.config-override.mapreduce.job.queuename={queueName}` 



### Hive Configuration Overriding {#hive-config-override}

Kylin supports overriding configuration properties in `kylin_hive_conf.xml` at the project and cube level, in the form of key-value pairs, in the following format:
`kylin.source.hive.config-override.<key> = <value>`
If user wants Hive to use a different YARN resource queue, user can set: 
`kylin.source.hive.config-override.mapreduce.job.queuename={queueName}` 



### Spark Configuration Overriding {#spark-config-override}

Kylin supports overriding configuration properties in `kylin.properties` at the project and cube level, in the form of key-value pairs, in the following format:
`kylin.engine.spark-conf.<key> = <value>`
If user wants Spark to use a different YARN resource queue, user can set: 
`kylin.engine.spark-conf.spark.yarn.queue={queueName}`



### Deployment configuration {#kylin-deploy}

This section introduces Kylin Deployment related configuration.



### Deploy Kylin {#deploy-config}

- `kylin.env.hdfs-working-dir`: specifies the HDFS path used by Kylin service. The default value is `/kylin`. Make sure that the user who starts the Kylin instance has permission to read and write to this directory.
- `kylin.env`: specifies the purpose of the Kylin deployment. Optional values include `DEV`, `QA` and `PROD`. The default value is *DEV*. Some developer functions will be enabled in *DEV* mode.
- `kylin.env.zookeeper-base-path`: specifies the ZooKeeper path used by the Kylin service. The default value is `/kylin`
- `kylin.env.zookeeper-connect-string`: specifies the ZooKeeper connection string. If it is empty, use HBase's ZooKeeper
- `kylin.env.hadoop-conf-dir`: specifies the Hadoop configuration file directory. If not specified, get `HADOOP_CONF_DIR` in the environment.
- `kylin.server.mode`: Optional values include `all`, `job` and `query`, among them *all* is the default one. *job* mode means the instance schedules Cube job only; *query* mode means the instance serves SQL queries only; *all* mode means the instance handles both of them.
- `kylin.server.cluster-name`: specifies the cluster name



### Allocate More Memory for Kylin {#kylin-jvm-settings}

There are two sample settings for `KYLIN_JVM_SETTINGS` are given in `$KYLIN_HOME/conf/setenv.sh`.
The default setting use relatively less memory. You can comment it and then uncomment the next line to allocate more memory for Kyligence Enterprise. The default configuration is:

```shell
Export KYLIN_JVM_SETTINGS="-Xms1024M -Xmx4096M -Xss1024K -XX`MaxPermSize=512M -verbose`gc -XX`+PrintGCDetails -XX`+PrintGCDateStamps -Xloggc`$KYLIN_HOME/logs/kylin.gc.$$ -XX`+UseGCLogFileRotation - XX`NumberOfGCLogFiles=10 -XX`GCLogFileSize=64M"
# export KYLIN_JVM_SETTINGS="-Xms16g -Xmx16g -XX`MaxPermSize=512m -XX`NewSize=3g -XX`MaxNewSize=3g -XX`SurvivorRatio=4 -XX`+CMSClassUnloadingEnabled -XX`+CMSParallelRemarkEnabled -XX`+UseConcMarkSweepGC -XX `+CMSIncrementalMode -XX`CMSInitiatingOccupancyFraction=70 -XX`+UseCMSInitiatingOccupancyOnly -XX`+DisableExplicitGC -XX`+HeapDumpOnOutOfMemoryError -verbose`gc -XX`+PrintGCDetails -XX`+PrintGCDateStamps -Xloggc`$KYLIN_HOME/logs/kylin.gc. $$ -XX`+UseGCLogFileRotation -XX`NumberOfGCLogFiles=10 -XX`GCLogFileSize=64M"
```



### Job Engine HA  {#job-engine-ha}

- `kylin.job.scheduler.default=2`: to enable the distributed job scheduler.
- `kylin.job.lock=org.apache.kylin.storage.hbase.util.ZookeeperJobLock`: to enable distributed job lock

> Note: For more information, please refer to the **Enable Job Engine HA** section in [Deploy in Cluster Mode](/docs/install/kylin_cluster.html) 


### Job Engine Safemode {#job-engine-safemode}

Safemode can be only used in default schedule.

- `kylin.job.scheduler.safemode=TRUE`: to enable job scheduler safemode. In safemode, Newly submitted job will not be executed
- `kylin.job.scheduler.safemode.runable-projects=project1,project2`: provide list of projects as exceptional case in safemode.


### Read/Write Separation   {#rw-deploy}

- `kylin.storage.hbase.cluster-fs`: specifies the HDFS file system of the HBase cluster
- `kylin.storage.hbase.cluster-hdfs-config-file`: specifies HDFS configuration file pointing to the HBase cluster

> Note: For more information, please refer to [Deploy Apache Kylin with Standalone HBase Cluster](http://kylin.apache.org/blog/2016/06/10/standalone-hbase-cluster/)



### RESTful Webservice {#rest-config}

- `kylin.web.timezone`: specifies the time zone used by Kylin's REST service. The default value is GMT+8.
- `kylin.web.cross-domain-enabled`: whether cross-domain access is supported. The default value is TRUE
- `kylin.web.export-allow-admin`: whether to support administrator user export information. The default value is TRUE
- `kylin.web.export-allow-other`: whether to support other users to export information. The default value is TRUE
- `kylin.web.dashboard-enabled`: whether to enable Dashboard. The default value is FALSE



### Metastore Configuration {#kylin_metastore}

This section introduces Kylin Metastore related configuration.



### Metadata-related {#metadata}

- `kylin.metadata.url`: specifies the Metadata path. The default value is *kylin_metadata@hbase*
- `kylin.metadata.dimension-encoding-max-length`: specifies the maximum length when the dimension is used as Rowkeys with fix_length encoding. The default value is 256.
- `kylin.metadata.sync-retries`: specifies the number of Metadata sync retries. The default value is 3.
- `kylin.metadata.sync-error-handler`: The default value is *DefaultSyncErrorHandler*
- `kylin.metadata.check-copy-on-write`: whether clear metadata cache, default value is *FALSE*
- `kylin.metadata.hbase-client-scanner-timeout-period`: specifies the total timeout between the RPC call initiated by the HBase client. The default value is 10000 (ms).
- `kylin.metadata.hbase-rpc-timeout`: specifies the timeout for HBase to perform RPC operations. The default value is 5000 (ms).
- `kylin.metadata.hbase-client-retries-number`: specifies the number of HBase retries. The default value is 1 (times).
- `kylin.metadata.resource-store-provider.jdbc`: specifies the class used by JDBC. The default value is *org.apache.kylin.common.persistence.JDBCResourceStore*



### MySQL Metastore Configuration (Beta) {#mysql-metastore}

> *Note*: This feature is still being tested and it is recommended to use it with caution.

- `kylin.metadata.url`: specifies the metadata path
- `kylin.metadata.jdbc.dialect`: specifies JDBC dialect
- `kylin.metadata.jdbc.json-always-small-cell`: The default value is TRUE
- `kylin.metadata.jdbc.small-cell-meta-size-warning-threshold`: The default value is 100 (MB)
- `kylin.metadata.jdbc.small-cell-meta-size-error-threshold`: The default value is 1 (GB)
- `kylin.metadata.jdbc.max-cell-size`: The default value is 1 (MB)
- `kylin.metadata.resource-store-provider.jdbc`: specifies the class used by JDBC. The default value is org.apache.kylin.common.persistence.JDBCResourceStore

> Note: For more information, please refer to [MySQL-based Metastore Configuration](/docs/tutorial/mysql_metastore.html)



### Modeling Configuration {#kylin-build}

This section introduces Kylin data modeling and build related configuration.



### Hive Client and SparkSQL {#hive-client-and-sparksql}

- `kylin.source.hive.client`: specifies the Hive command line type. Optional values include *cli* or *beeline*. The default value is *cli*.
- `kylin.source.hive.beeline-shell`: specifies the absolute path of the Beeline shell. The default is beeline
- `kylin.source.hive.beeline-params`: when using Beeline as the Client tool for Hive, user need to configure this parameter to provide more information to Beeline
- `kylin.source.hive.enable-sparksql-for-table-ops`: the default value is *FALSE*, which needs to be set to *TRUE* when using SparkSQL
- `kylin.source.hive.sparksql-beeline-shell`: when using SparkSQL Beeline as the client tool for Hive, user need to configure this parameter as /path/to/spark-client/bin/beeline
- `kylin.source.hive.sparksql-beeline-params`: when using SparkSQL Beeline as the Client tool for Hive,user need to configure this parameter to provide more information to SparkSQL




### JDBC Datasource Configuration {#jdbc-datasource}

- `kylin.source.default`: specifies the type of data source used by JDBC
- `kylin.source.jdbc.connection-url`: specifies JDBC connection string
- `kylin.source.jdbc.driver`: specifies JDBC driver class name
- `kylin.source.jdbc.dialect`: specifies JDBC dialect. The default value is default
- `kylin.source.jdbc.user`: specifies JDBC connection username
- `kylin.source.jdbc.pass`: specifies JDBC connection password
- `kylin.source.jdbc.sqoop-home`: specifies Sqoop installation path
- `kylin.source.jdbc.sqoop-mapper-num`: specifies how many slices should be split. Sqoop will run a mapper for each slice. The default value is 4.
- `kylin.source.jdbc.field-delimiter`: specifies the field separator. The default value is \

> Note: For more information, please refer to [Building a JDBC Data Source](/docs/tutorial/setup_jdbc_datasource.html).




### Data Type Precision {#precision-config}

- `kylin.source.hive.default-varchar-precision`: specifies the maximum length of the *varchar* field. The default value is 256.
- `kylin.source.hive.default-char-precision`: specifies the maximum length of the *char* field. The default value is 255.
- `kylin.source.hive.default-decimal-precision`: specifies the precision of the *decimal* field. The default value is 19
- `kylin.source.hive.default-decimal-scale`: specifies the scale of the *decimal* field. The default value is 4.



### Cube Design {#cube-config}

- `kylin.cube.ignore-signature-inconsistency`:The signature in Cube desc ensures that the cube is not changed to a corrupt state. The default value is *FALSE*
- `kylin.cube.aggrgroup.max-combination`: specifies the max combination number of aggregation groups. The default value is 32768.
- `kylin.cube.aggrgroup.is-mandatory-only-valid`: whether to allow Cube contains only Base Cuboid. The default value is *FALSE*, set to *TRUE* when using Spark Cubing
- `kylin.cube.rowkey.max-size`: specifies the maximum number of columns that can be set to Rowkeys. The default value is 63, and it can not be more than 63.
- `kylin.cube.allow-appear-in-multiple-projects`: whether to allow a cube to appear in multiple projects
- `kylin.cube.gtscanrequest-serialization-level`: the default value is 1
- `kylin.web.hide-measures`: hides some measures that may not be needed, the default value is RAW.   



### Cube Size Estimation {#cube-estimate}

Both Kylin and HBase use compression when writing to disk, so Kylin will multiply its original size by the ratio to estimate the size of the cube.

- `kylin.cube.size-estimate-ratio`: normal cube, default value is 0.25
- `kylin.cube.size-estimate-memhungry-ratio`: Deprecated, default is 0.05
- `kylin.cube.size-estimate-countdistinct-ratio`: Cube Size Estimation with count distinct h= metric, default value is 0.5
- `kylin.cube.size-estimate-topn-ratio`: Cube Size Estimation with TopN metric, default value is 0.5



### Cube Algorithm {#cube-algorithm}

- `kylin.cube.algorithm`: specifies the algorithm of the Build Cube. Optional values include `auto`, `layer` and `inmem`. The default value is `auto`, that is, Kylin will dynamically select an algorithm by collecting data ( Layer or inmem), if user knows Kylin, user data and cluster condition well, user can directly set the algorithm.
- `kylin.cube.algorithm.layer-or-inmem-threshold`: the default value is 7
- `kylin.cube.algorithm.inmem-split-limit`: the default value is 500
- `kylin.cube.algorithm.inmem-concurrent-threads`: the default value is 1
- `kylin.job.sampling-percentage`: specifies the data sampling percentage. The default value is 100.



### Auto Merge Segments {#auto-merge}

- `kylin.cube.is-automerge-enabled`: whether to enable auto-merge. The default value is *TRUE*. When this parameter is set to *FALSE*, the auto-merge function will be turned off, even if it is enabled in Cube Design.



### Lookup Table Snapshot {#snapshot}

- `kylin.snapshot.max-mb`: specifies the max size of the snapshot. The default value is 300(M)
- `kylin.snapshot.max-cache-entry`: The maximum number of snapshots that can be stored in the cache. The default value is 500.
- `kylin.snapshot.ext.shard-mb`: specifies the size of HBase shard. The default value is 500(M).
- `kylin.snapshot.ext.local.cache.path`: specifies local cache path, default value is lookup_cache
- `kylin.snapshot.ext.local.cache.max-size-gb`: specifies local snapshot cache size, default is 200(M)



### Build Cube {#cube-build}

- `kylin.storage.default`: specifies the default build engine. The default value is 2, which means HBase.
- `kylin.source.hive.keep-flat-table`: whether to keep the Hive intermediate table after the build job is complete. The default value is *FALSE*
- `kylin.source.hive.database-for-flat-table`: specifies the name of the Hive database that stores the Hive intermediate table. The default is *default*. Make sure that the user who started the Kylin instance has permission to operate the database.
- `kylin.source.hive.flat-table-storage-format`: specifies the storage format of the Hive intermediate table. The default value is *SEQUENCEFILE*
- `kylin.source.hive.flat-table-field-delimiter`: specifies the delimiter of the Hive intermediate table. The default value is *\u001F*
- - `kylin.source.hive.intermediate-table-prefix`: specifies the table name prefix of the Hive intermediate table. The default value is *kylin\_intermediate\_*
- `kylin.source.hive.redistribute-flat-table`: whether to redistribute the Hive flat table. The default value is *TRUE*
- `kylin.source.hive.redistribute-column-count`: number of redistributed columns. The default value is *3*
- `kylin.source.hive.table-dir-create-first`: the default value is *FALSE*
- `kylin.storage.partition.aggr-spill-enabled`: the default value is *TRUE*
- `kylin.engine.mr.lib-dir`: specifies the path to the jar package used by the MapReduce job
- `kylin.engine.mr.reduce-input-mb`: used to estimate the number of Reducers. The default value is 500(MB).
- `kylin.engine.mr.reduce-count-ratio`: used to estimate the number of Reducers. The default value is 1.0
- `kylin.engine.mr.min-reducer-number`: specifies the minimum number of Reducers in the MapReduce job. The default is 1
- `kylin.engine.mr.max-reducer-number`: specifies the maximum number of Reducers in the MapReduce job. The default is 500.
- `kylin.engine.mr.mapper-input-rows`: specifies the number of rows that each Mapper can handle. The default value is 1000000. If user change this value, it will start more Mapper.
- `kylin.engine.mr.max-cuboid-stats-calculator-number`: specifies the number of threads used to calculate Cube statistics. The default value is 1
- `kylin.engine.mr.build-dict-in-reducer`: whether to build the dictionary in the Reduce phase of the build job *Extract Fact Table Distinct Columns*. The default value is `TRUE`
- `kylin.engine.mr.yarn-check-interval-seconds`: How often the build engine is checked for the status of the Hadoop job. The default value is 10(s)
- `kylin.engine.mr.use-local-classpath`: whether to use local mapreduce application classpath. The default value is TRUE



### Dictionary-related {#dict-config}

- `kylin.dictionary.use-forest-trie`: The default value is TRUE
- `kylin.dictionary.forest-trie-max-mb`: The default value is 500
- `kylin.dictionary.max-cache-entry`: The default value is 3000
- `kylin.dictionary.growing-enabled`: The default value is FALSE
- `kylin.dictionary.append-entry-size`: The default value is 10000000
- `kylin.dictionary.append-max-versions`: The default value is 3
- `kylin.dictionary.append-version-ttl`: The default value is 259200000
- `kylin.dictionary.resuable`: whether to reuse the dictionary. The default value is FALSE
- `kylin.dictionary.shrunken-from-global-enabled`: whether to reduce the size of global dictionary. The default value is *TRUE*



### Deal with Ultra-High-Cardinality Columns {#uhc-config}

- `kylin.engine.mr.build-uhc-dict-in-additional-step`: the default value is *FALSE*, set to *TRUE*
- `kylin.engine.mr.uhc-reducer-count`: the default value is 1, which can be set to 5 to allocate 5 Reducers for each super-high base column.



### Spark as Build Engine {#spark-cubing}

- `kylin.engine.spark-conf.spark.master`: specifies the Spark operation mode. The default value is *yarn*
- `kylin.engine.spark-conf.spark.submit.deployMode`: specifies the deployment mode of Spark on YARN. The default value is *cluster*
- `kylin.engine.spark-conf.spark.yarn.queue`: specifies the Spark resource queue. The default value is *default*
- `kylin.engine.spark-conf.spark.driver.memory`: specifies the Spark Driver memory The default value is 2G.
- `kylin.engine.spark-conf.spark.executor.memory`: specifies the Spark Executor memory. The default value is 4G.
- `kylin.engine.spark-conf.spark.yarn.executor.memoryOverhead`: specifies the size of the Spark Executor heap memory. The default value is 1024(MB).
- `kylin.engine.spark-conf.spark.executor.cores`: specifies the number of cores available for a single Spark Executor. The default value is 1
- `kylin.engine.spark-conf.spark.network.timeout`: specifies the Spark network timeout period, 600
- `kylin.engine.spark-conf.spark.executor.instances`: specifies the number of Spark Executors owned by an Application. The default value is 1
- `kylin.engine.spark-conf.spark.eventLog.enabled`: whether to record the Spark event. The default value is *TRUE*
- `kylin.engine.spark-conf.spark.hadoop.dfs.replication`: replication number of HDFS, default is 2
- `kylin.engine.spark-conf.spark.hadoop.mapreduce.output.fileoutputformat.compress`: whether to compress the output. The default value is *TRUE*
- `kylin.engine.spark-conf.spark.hadoop.mapreduce.output.fileoutputformat.compress.codec`: specifies Output compression, default is *org.apache.hadoop.io.compress.DefaultCodec*
- `kylin.engine.spark.rdd-partition-cut-mb`: Kylin uses the size of this parameter to split the partition. The default value is 10 (MB)
- `kylin.engine.spark.min-partition`: specifies the minimum number of partitions. The default value is 1
- `kylin.engine.spark.max-partition`: specifies maximum number of partitions, default is 5000
- `kylin.engine.spark.storage-level`: specifies RDD partition data cache level, default value is *MEMORY_AND_DISK_SER*
- `kylin.engine.spark-conf-mergedict.spark.executor.memory`: whether to request more memory for merging dictionary.The default value is 6G.
- `kylin.engine.spark-conf-mergedict.spark.memory.fraction`: specifies the percentage of memory reserved for the system. The default value is 0.2

> Note: For more information, please refer to [Building Cubes with Spark](/docs/tutorial/cube_spark.html).



### Submit Spark jobs via Livy {#livy-submit-spark-job}

- `kylin.engine.livy-conf.livy-enabled`: whether to enable Livy as submit Spark job service. The default value is *FALSE*
- `kylin.engine.livy-conf.livy-url`: specifies the URL of Livy. Such as *http://127.0.0.1:8998*
- `kylin.engine.livy-conf.livy-key.*`: specifies the name-key configuration of Livy. Such as *kylin.engine.livy-conf.livy-key.name=kylin-livy-1*
- `kylin.engine.livy-conf.livy-arr.*`: specifies the array type configuration of Livy. Separated by commas. Such as *kylin.engine.livy-conf.livy-arr.jars=hdfs://your_self_path/hbase-common-1.4.8.jar,hdfs://your_self_path/hbase-server-1.4.8.jar,hdfs://your_self_path/hbase-client-1.4.8.jar*
- `kylin.engine.livy-conf.livy-map.*`: specifies the Spark configuration properties. Such as *kylin.engine.livy-conf.livy-map.spark.executor.instances=10*

> Note: For more information, please refer to [Apache Livy Rest API](http://livy.incubator.apache.org/docs/latest/rest-api.html).


### Spark Dynamic Allocation {#dynamic-allocation}

- `kylin.engine.spark-conf.spark.shuffle.service.enabled`: whether to enable shuffle service
- `kylin.engine.spark-conf.spark.dynamicAllocation.enabled`: whether to enable Spark Dynamic Allocation
- `kylin.engine.spark-conf.spark.dynamicAllocation.initialExecutors`: specifies the initial number of Executors
- `kylin.engine.spark-conf.spark.dynamicAllocation.minExecutors`: specifies the minimum number of Executors retained
- `kylin.engine.spark-conf.spark.dynamicAllocation.maxExecutors`: specifies the maximum number of Executors applied for
- `kylin.engine.spark-conf.spark.dynamicAllocation.executorIdleTimeout`: specifies the threshold of Executor being removed after being idle. The default value is 60(s)

> Note: For more information, please refer to the official documentation: [Dynamic Resource Allocation](http://spark.apache.org/docs/1.6.2/job-scheduling.html#dynamic-resource-allocation).



### Job-related {#job-config}

- `kylin.job.log-dir`: the default value is */tmp/kylin/logs*
- `kylin.job.allow-empty-segment`: whether tolerant data source is empty. The default value is *TRUE*
- `kylin.job.max-concurrent-jobs`: specifies maximum build concurrency, default is 10
- `kylin.job.retry`: specifies retry times after the job is failed. The default value is 0
- `kylin.job.retry-interval`: specifies retry interval in milliseconds. The default value is 30000
- `kylin.job.scheduler.priority-considered`: whether to consider the job priority. The default value is FALSE
- `kylin.job.scheduler.priority-bar-fetch-from-queue`: specifies the time interval for getting jobs from the priority queue. The default value is 20(s)
- `kylin.job.scheduler.poll-interval-second`: The time interval for getting the job from the queue. The default value is 30(s)
- `kylin.job.error-record-threshold`: specifies the threshold for the job to throw an error message. The default value is 0
- `kylin.job.cube-auto-ready-enabled`: whether to enable Cube automatically after the build is complete. The default value is *TRUE*
- `kylin.cube.max-building-segments`: specifies the maximum number of building job for the one Cube. The default value is 10



### Enable Email Notification {#email-notification}

- `kylin.job.notification-enabled`: whether to notify the email when the job succeeds or fails. The default value is *FALSE*
- `kylin.job.notification-mail-enable-starttls`:# whether to enable starttls. The default value is *FALSE*
- `kylin.job.notification-mail-host`: specifies the SMTP server address of the mail
- `kylin.job.notification-mail-port`: specifies the SMTP server port of the mail. The default value is 25
- `kylin.job.notification-mail-username`: specifies the login user name of the mail
- `kylin.job.notification-mail-password`: specifies the username and password of the email
- `kylin.job.notification-mail-sender`: specifies the email address of the email
- `kylin.job.notification-admin-emails`: specifies the administrator's mailbox for email notifications



### Enable Cube Planner {#cube-planner}

- `kylin.cube.cubeplanner.enabled`: the default value is *TRUE*
- `kylin.server.query-metrics2-enabled`: the default value is *TRUE*
- `kylin.metrics.reporter-query-enabled`: the default value is *TRUE*
- `kylin.metrics.reporter-job-enabled`: the default value is *TRUE*
- `kylin.metrics.monitor-enabled`: the default value is *TRUE*
- `kylin.cube.cubeplanner.enabled`: whether to enable Cube Planner, The default value is *TRUE*
- `kylin.cube.cubeplanner.enabled-for-existing-cube`: whether to enable Cube Planner for the existing Cube. The default value is *TRUE*
- `kylin.cube.cubeplanner.algorithm-threshold-greedy`: the default value is 8
- `kylin.cube.cubeplanner.expansion-threshold`: the default value is 15.0
- `kylin.cube.cubeplanner.recommend-cache-max-size`: the default value is 200
- `kylin.cube.cubeplanner.query-uncertainty-ratio`: the default value is 0.1
- `kylin.cube.cubeplanner.bpus-min-benefit-ratio`: the default value is 0.01
- `kylin.cube.cubeplanner.algorithm-threshold-genetic`: the default value is 23

> Note: For more information, please refer to [Using Cube Planner](/docs/tutorial/use_cube_planner.html).



### HBase Storage {#hbase-config}

- `kylin.storage.hbase.table-name-prefix`: specifies the prefix of HTable. The default value is *KYLIN\_*
- `kylin.storage.hbase.namespace`: specifies the default namespace of HBase Storage. The default value is *default*
- `kylin.storage.hbase.coprocessor-local-jar`: specifies jar package related to HBase coprocessor
- `kylin.storage.hbase.coprocessor-mem-gb`: specifies the HBase coprocessor memory. The default value is 3.0(GB).
- `kylin.storage.hbase.run-local-coprocessor`: whether to run the local HBase coprocessor. The default value is *FALSE*
- `kylin.storage.hbase.coprocessor-timeout-seconds`: specifies the timeout period. The default value is 0
- `kylin.storage.hbase.region-cut-gb`: specifies the size of a single Region, default is 5.0
- `kylin.storage.hbase.min-region-count`: specifies the minimum number of regions. The default value is 1
- `kylin.storage.hbase.max-region-count`: specifies the maximum number of Regions. The default value is 500
- `kylin.storage.hbase.hfile-size-gb`: specifies the HFile size. The default value is 2.0(GB)
- `kylin.storage.hbase.max-scan-result-bytes`: specifies the maximum value of the scan return. The default value is 5242880 (byte), which is 5 (MB).
- `kylin.storage.hbase.compression-codec`: whether it is compressed. The default value is *none*, that is, compression is not enabled
- `kylin.storage.hbase.rowkey-encoding`: specifies the encoding method of Rowkey. The default value is *FAST_DIFF*
- `kylin.storage.hbase.block-size-bytes`: the default value is 1048576
- `kylin.storage.hbase.small-family-block-size-bytes`: specifies the block size. The default value is 65536 (byte), which is 64 (KB).
- `kylin.storage.hbase.owner-tag`: specifies the owner of the Kylin platform. The default value is whoami@kylin.apache.org
- `kylin.storage.hbase.endpoint-compress-result`: whether to return the compression result. The default value is TRUE
- `kylin.storage.hbase.max-hconnection-threads`: specifies the maximum number of connection threads. The default value is 2048.
- `kylin.storage.hbase.core-hconnection-threads`: specifies the number of core connection threads. The default value is 2048.
- `kylin.storage.hbase.hconnection-threads-alive-seconds`: specifies the thread lifetime. The default value is 60.
- `kylin.storage.hbase.replication-scope`: specifies the cluster replication range. The default value is 0
- `kylin.storage.hbase.scan-cache-rows`: specifies the number of scan cache lines. The default value is 1024.



### Enable Compression {#compress-config}

Kylin does not enable Enable Compression by default. Unsupported compression algorithms can hinder Kylin's build jobs, but a suitable compression algorithm can reduce storage overhead and network overhead and improve overall system operation efficiency.
Kylin can use three types of compression, HBase table compression, Hive output compression, and MapReduce job output compression.

> *Note*: The compression settings will not take effect until the Kylin instance is restarted.

* HBase table compression

This compression is configured by `kylin.storage.hbase.compression-codec` in `kyiln.properties`. Optional values include `none`, `snappy`, `lzo`, `gzip` and `lz4`. The default value is none, which means no data is compressed.

> *Note*: Before modifying the compression algorithm, make sure userr HBase cluster supports the selected compression algorithm.


* Hive output compression

This compression is configured by `kylin_hive_conf.xml`. The default configuration is empty, which means that the default configuration of Hive is used directly. If user want to override the configuration, add (or replace) the following properties in `kylin_hive_conf.xml`. Take SNAPPY compression as an example:

```xml
<property>
<name>mapreduce.map.output.compress.codec</name>
<value>org.apache.hadoop.io.compress.SnappyCodec</value>
<description></description>
</property>
<property>
<name>mapreduce.output.fileoutputformat.compress.codec</name>
<value>org.apache.hadoop.io.compress.SnappyCodec</value>
<description></description>
</property>
```

* MapReduce job output compression

This compression is configured via `kylin_job_conf.xml` and `kylin_job_conf_inmem.xml`. The default is empty, which uses the default configuration of MapReduce. If user want to override the configuration, add (or replace) the following properties in `kylin_job_conf.xml` and `kylin_job_conf_inmem.xml`. Take SNAPPY compression as an example:

```xml
<property>
<name>mapreduce.map.output.compress.codec</name>
<value>org.apache.hadoop.io.compress.SnappyCodec</value>
<description></description>
</property>
<property>
<name>mapreduce.output.fileoutputformat.compress.codec</name>
<value>org.apache.hadoop.io.compress.SnappyCodec</value>
<description></description>
</property>
```



### Real-time OLAP    {#realtime-olap}
- `kylin.stream.job.dfs.block.size`: specifies the HDFS block size of the streaming Base Cuboid job using. The default value is *16M*.
- `kylin.stream.index.path`: specifies the path to store local segment cache. The default value is *stream_index*.
- `kylin.stream.cube-num-of-consumer-tasks`: specifies the number of replica sets that share the whole topic partition. It affects how many partitions will be assigned to different replica sets. The default value is *3*.
- `kylin.stream.cube.window`: specifies the length of duration of each segment, value in seconds. The default value is *3600*.
- `kylin.stream.cube.duration`: specifies the wait time that a segment's status changes from active to IMMUTABLE, value in seconds. The default value is *7200*.
- `kylin.stream.cube.duration.max`: specifies the maximum duration that segment can keep active, value in seconds. The default value is *43200*.
- `kylin.stream.checkpoint.file.max.num`: specifies the maximum number of checkpoint file for each cube. The default value is *5*.
- `kylin.stream.index.checkpoint.intervals`: specifies the time interval between setting two checkpoints. The default value is *300*.
- `kylin.stream.index.maxrows`: specifies the maximum number of the entered event be cached in heap/memory. The default value is *50000*.
- `kylin.stream.immutable.segments.max.num`: specifies the maximum number of the IMMUTABLE segment in each Cube of the current streaming receiver, if exceed, consumption of current topic will be paused. The default value is *100*.
- `kylin.stream.consume.offsets.latest`: whether to consume from the latest offset. The default value is *true*.
- `kylin.stream.node`: specifies the node of coordinator/receiver. Such as host:port. The default value is *null*.
- `kylin.stream.metadata.store.type`: specifies the position of metadata store. The default value is *zk*.
- `kylin.stream.segment.retention.policy`: specifies the strategy to process local segment cache when segment become IMMUTABLE. Optional values include `purge` and `fullBuild`. `purge` means when the segment become IMMUTABLE, it will be dropped. `fullBuild` means when the segment become IMMUTABLE, it will be uploaded to HDFS. The default value is *fullBuild*.
- `kylin.stream.assigner`: specifies the implementation class which used to assign the topic partition to different replica sets. The class should be the implementation class of `org.apache.kylin.stream.coordinator.assign.Assigner`. The default value is *DefaultAssigner*.
- `kylin.stream.coordinator.client.timeout.millsecond`: specifies the connection timeout of the coordinator client. The default value is *5000*.
- `kylin.stream.receiver.client.timeout.millsecond`: specifies the connection timeout of the receiver client. The default value is *5000*.
- `kylin.stream.receiver.http.max.threads`: specifies the maximum connection threads of the receiver. The default value is *200*.
- `kylin.stream.receiver.http.min.threads`: specifies the minimum connection threads of the receiver. The default value is *10*.
- `kylin.stream.receiver.query-core-threads`: specifies the number of query threads be used for the current streaming receiver. The default value is *50*.
- `kylin.stream.receiver.query-max-threads`: specifies the maximum number of query threads be used for the current streaming receiver. The default value is *200*.
- `kylin.stream.receiver.use-threads-per-query`: specifies the threads number that each query use. The default value is *8*.
- `kylin.stream.build.additional.cuboids`: whether to build additional Cuboids. The additional Cuboids mean the aggregation of Mandatory Dimensions that chosen in Cube Advanced Setting page. The default value is *false*. Only build Base Cuboid by default.
- `kylin.stream.segment-max-fragments`: specifies the maximum number of fragments that each segment keep. The default value is *50*.
- `kylin.stream.segment-min-fragments`: specifies the minimum number of fragments that each segment keep. The default value is *15*.
- `kylin.stream.max-fragment-size-mb`: specifies the maximum size of each fragment. The default value is *300*.
- `kylin.stream.fragments-auto-merge-enable`: whether to enable fragments auto merge. The default value is *true*.

> Note: For more information, please refer to the [Real-time OLAP](http://kylin.apache.org/docs30/tutorial/real_time_olap.html).

### Storage Clean up Configuration    {#storage-clean-up-configuration}

This section introduces Kylin storage clean up related configuration.



### Storage-clean-up-related {#storage-clean-up-config}

- `kylin.storage.clean-after-delete-operation`: whether to clean segment data in HBase and HDFS. The default value is FALSE.



### Query Configuration    {#kylin-query}

This section introduces Kylin query related configuration.



### Query-related {#query-config}

- `kylin.query.skip-empty-segments`: whether to skip empty segments when querying. The default value is *TRUE*
- `kylin.query.large-query-threshold`: specifies the maximum number of rows returned. The default value is 1000000.
- `kylin.query.security-enabled`: whether to check the ACL when querying. The default value is *TRUE*
- `kylin.query.security.table-acl-enabled`: whether to check the ACL of the corresponding table when querying. The default value is *TRUE*
- `kylin.query.calcite.extras-props.conformance`: whether to strictly parsed. The default value is *LENIENT*
- `kylin.query.calcite.extras-props.caseSensitive`: whether is case sensitive. The default value is *TRUE*
- `kylin.query.calcite.extras-props.unquotedCasing`: optional values include `UNCHANGED`, `TO_UPPER` and `TO_LOWER`. The default value is *TO_UPPER*, that is, all uppercase
- `kylin.query.calcite.extras-props.quoting`: whether to add quotes, Optional values include `DOUBLE_QUOTE`, `BACK_TICK` and `BRACKET`. The default value is *DOUBLE_QUOTE*
- `kylin.query.statement-cache-max-num`: specifies the maximum number of cached PreparedStatements. The default value is 50000
- `kylin.query.statement-cache-max-num-per-key`: specifies the maximum number of PreparedStatements per key cache. The default value is 50.
- `kylin.query.enable-dict-enumerator`: whether to enable the dictionary enumerator. The default value is *FALSE*
- `kylin.query.enable-dynamic-column`: whether to enable dynamic columns. The default value is *FALSE*, set to *TRUE* to query the number of rows in a column that do not contain NULL



### Fuzzy Query {#fuzzy}

- `kylin.storage.hbase.max-fuzzykey-scan`: specifies the threshold for the scanned fuzzy key. If the value is exceeded, the fuzzy key will not be scanned. The default value is 200.
- `kylin.storage.hbase.max-fuzzykey-scan-split`: split the large fuzzy key set to reduce the number of fuzzy keys per scan. The default value is 1
- `kylin.storage.hbase.max-visit-scanrange`: the default value is 1000000



### Query Cache {#cache-config}

- `kylin.query.cache-enabled`: whether to enable caching. The default value is TRUE
- `kylin.query.cache-threshold-duration`: the query duration exceeding the threshold is saved in the cache. The default value is 2000 (ms).
- `kylin.query.cache-threshold-scan-count`: the row count scanned in the query exceeding the threshold is saved in the cache. The default value is 10240 (rows).
- `kylin.query.cache-threshold-scan-bytes`: the bytes scanned in the query exceeding the threshold is saved in the cache. The default value is 1048576 (byte).



### Query Limits {#query-limit}

- `kylin.query.timeout-seconds`: specifies the query timeout in seconds. The default value is 0, that is, no timeout limit on query. If the value is less than 60, it will set to 60 seconds.
- `kylin.query.timeout-seconds-coefficient`: specifies the coefficient of the query timeout seconds. The default value is 0.5.
- `kylin.query.max-scan-bytes`: specifies the maximum bytes scanned by the query. The default value is 0, that is, there is no limit.
- `kylin.storage.partition.max-scan-bytes`: specifies the maximum number of bytes for the query scan. The default value is 3221225472 (bytes), which is 3GB.
- `kylin.query.max-return-rows`: specifies the maximum number of rows returned by the query. The default value is 5000000.



### Bad Query {#bad-query}

The value of `kylin.query.timeout-seconds` is greater than 60 or equals 0, the max value of `kylin.query.timeout-seconds-coefficient` is the upper limit of double. The result of multiplying two properties is the interval time of detecting bad query, if it equals 0, it will be set to 60 seconds, the max value of it is the upper limit of int.

- `kylin.query.badquery-stacktrace-depth`: specifies the depth of stack trace. The default value is 10.
- `kylin.query.badquery-history-number`: specifies the showing number of bad query history. The default value is 50.
- `kylin.query.badquery-alerting-seconds`: The default value is 90, if the time of running is greater than the value of this property, it will print the log of query firstly, including (duration, project, thread, user, query id). Whether to save the recent query, it depends on another property. Secondly, record the stack log, the depth of log depend on another property, so as to the analysis later
- `kylin.query.badquery-persistent-enabled`: The default value is true, it will save the recent bad query, and cannot override in Cube-level


### Query Pushdown		{#query-pushdown}

- `kylin.query.pushdown.runner-class-name=org.apache.kylin.query.adhoc.PushDownRunnerJdbcImpl`: whether to enable query pushdown
- `kylin.query.pushdown.jdbc.url`: specifies JDBC URL
- `kylin.query.pushdown.jdbc.driver`: specifies JDBC driver class name. The default value is *org.apache.hive.jdbc.HiveDriver*
- `kylin.query.pushdown.jdbc.username`: specifies the Username of the JDBC database. The default value is *hive*
- `kylin.query.pushdown.jdbc.password`: specifies JDBC password for the database. The default value is
- `kylin.query.pushdown.jdbc.pool-max-total`: specifies the maximum number of connections to the JDBC connection pool. The default value is 8.
- `kylin.query.pushdown.jdbc.pool-max-idle`: specifies the maximum number of idle connections for the JDBC connection pool. The default value is 8.
- `kylin.query.pushdown.jdbc.pool-min-idle`: the default value is 0
- `kylin.query.pushdown.update-enabled`: specifies whether to enable update in Query Pushdown. The default value is *FALSE*
- `kylin.query.pushdown.cache-enabled`: whether to enable the cache of the pushdown query to improve the query efficiency of the same query. The default value is *FALSE*

> Note: For more information, please refer to [Query Pushdown](/docs/tutorial/query_pushdown.html)



### Query rewriting {#convert-sql}

- `kylin.query.force-limit`: this parameter achieves the purpose of shortening the query duration by forcing a LIMIT clause for the select * statement. The default value is *-1*, and the parameter value is set to a positive integer, such as 1000, the value will be applied to the LIMIT clause, and the query will eventually be converted to select * from fact_table limit 1000
- `kylin.storage.limit-push-down-enabled`: the default value is *TRUE*, set to *FALSE* to close the limit-pushdown of storage layer
- `kylin.query.flat-filter-max-children`: specifies the maximum number of filters when flatting filter. The default value is 500000



### Collect Query Metrics to JMX {#jmx-metrics}

- `kylin.server.query-metrics-enabled`: the default value is *FALSE*, set to *TRUE* to collect query metrics to JMX

> Note: For more information, please refer to [JMX](https://www.oracle.com/technetwork/java/javase/tech/javamanagement-140525.html)



### Collect Query Metrics to dropwizard {#dropwizard-metrics}

- `kylin.server.query-metrics2-enabled`: the default value is *FALSE*, set to *TRUE* to collect query metrics into dropwizard

> Note: For more information, please refer to [dropwizard](https://metrics.dropwizard.io/4.0.0/)



### Security Configuration {#kylin-security}

This section introduces Kylin security-related configuration.



### Integrated LDAP for SSO {#ldap-sso}

- `kylin.security.profile`: specifies the way of security authentication, optional values include `ldap`, `testing`, `saml`, it should be set to `ldap` when integrating LDAP for SSO
- `kylin.security.ldap.connection-server`: specifies LDAP server, such as *ldap://ldap_server:389*
- `kylin.security.ldap.connection-username`: specifies LDAP username
- `kylin.security.ldap.connection-password`: specifies LDAP password
- `kylin.security.ldap.user-search-base`: specifies the scope of users synced to Kylin
- `kylin.security.ldap.user-search-pattern`: specifies the username for the login verification match
- `kylin.security.ldap.user-group-search-base`: specifies the scope of the user group synchronized to Kylin
- `kylin.security.ldap.user-group-search-filter`: specifies the type of user synced to Kylin
- `kylin.security.ldap.service-search-base`: need to be specifies when a service account is required to access Kylin
- `kylin.security.ldap.service-search-pattern`: need to be specifies when a service account is required to access Kylin
- `kylin.security.ldap.service-group-search-base`: need to be specifies when a service account is required to access Kylin
- `kylin.security.acl.admin-role`: map an LDAP group to an admin role (group name case sensitive)
- `kylin.server.auth-user-cache.expire-seconds`: specifies LDAP user information cache time, default is 300(s)
- `kylin.server.auth-user-cache.max-entries`: specifies maximum number of LDAP users, default is 100



### Integrate with Apache Ranger {#ranger}

- `kylin.server.external-acl-provider=org.apache.ranger.authorization.kylin.authorizer.RangerKylinAuthorizer`

> Note: For more information, please refer to [How to integrate the Kylin plugin in the installation documentation for Ranger](https://cwiki.apache.org/confluence/display/RANGER/Kylin+Plugin)



### Enable ZooKeeper ACL {#zookeeper-acl}

- `kylin.env.zookeeper-acl-enabled`: Enable ZooKeeper ACL to prevent unauthorized users from accessing the Znode or reducing the risk of bad operations resulting from this. The default value is *FALSE*
- `kylin.env.zookeeper.zk-auth`: use username: password as the ACL identifier. The default value is *digest:ADMIN:KYLIN*
- `kylin.env.zookeeper.zk-acl`: Use a single ID as the ACL identifier. The default value is *world:anyone:rwcda*, *anyone* for anyone

### Distributed query cache with Memcached {#distributed-cache}

From v2.6.0, Kylin can use Memcached as the distributed cache, and also there are some improvements on the cache policy ([KYLIN-2895](https://issues.apache.org/jira/browse/KYLIN-2895)). To enable these new features, you need to do the following steps:

1. Install Memcached (latest v1.5.12) on 1 or multiple nodes; You can install it on all Kylin nodes if resource is enough;

2. Modify the applicationContext.xml under $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/classes directory, comment the following code:
{% highlight Groff markup %}
<bean id="ehcache"
      class="org.springframework.cache.ehcache.EhCacheManagerFactoryBean"
      p:configLocation="classpath:ehcache-test.xml" p:shared="true"/>

<bean id="cacheManager" class="org.springframework.cache.ehcache.EhCacheCacheManager"
      p:cacheManager-ref="ehcache"/>
{% endhighlight %}
Uncomment the following code:
{% highlight Groff markup %}
<bean id="ehcache" class="org.springframework.cache.ehcache.EhCacheManagerFactoryBean"
      p:configLocation="classpath:ehcache-test.xml" p:shared="true"/>

<bean id="remoteCacheManager" class="org.apache.kylin.cache.cachemanager.MemcachedCacheManager" />
<bean id="localCacheManager" class="org.apache.kylin.cache.cachemanager.InstrumentedEhCacheCacheManager"
      p:cacheManager-ref="ehcache"/>
<bean id="cacheManager" class="org.apache.kylin.cache.cachemanager.RemoteLocalFailOverCacheManager" />

<bean id="memcachedCacheConfig" class="org.apache.kylin.cache.memcached.MemcachedCacheConfig">
    <property name="timeout" value="500" />
    <property name="hosts" value="${kylin.cache.memcached.hosts}" />
</bean>
{% endhighlight %}
The value of `${kylin.cache.memcached.hosts}` in applicationContext.xml is from the value of `kylin.cache.memcached.hosts` in conf/kylin.properties. 

3.Add the following parameters to `conf/kylin.properties`:
{% highlight Groff markup %}
kylin.query.cache-enabled=true
kylin.query.lazy-query-enabled=true
kylin.query.cache-signature-enabled=true
kylin.query.segment-cache-enabled=true
kylin.cache.memcached.hosts=memcached1:11211,memcached2:11211,memcached3:11211
{% endhighlight %}

- `kylin.query.cache-enabled` controls the on-off of query cache, its default value is `true`.
- `kylin.query.lazy-query-enabled` : whether to lazily answer the queries that be sent repeatedly in a short time (hold it until the previous query be returned, and then reuse the result); The default value is `false`. 
- `kylin.query.cache-signature-enabled` : whether to use the signature of a query to determine the cache's validity. The signature is calculated by the cube/hybrid list of the project, their last build time and other information (at the moment when cache is persisted); It's default value is `false`, highly recommend to set it to `true`.
- `kylin.query.segment-cache-enabled` : whether to cache the segment level returned data (from HBase storage) into Memcached. This feature is mainly for the cube that built very frequently (e.g, streaming cube, whose last build time always changed a couple minutes, the whole SQL statement level cache is very likely be cleaned; in this case, the by-segment cache can reduce the I/O). This only works when Memcached configured, the default value is `false`.
- `kylin.cache.memcached.hosts`: a list of memcached node and port, connected with comma.