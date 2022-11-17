---
title: Asynchronous query
language: en
sidebar_label: Asynchronous query
pagination_label: Asynchronous query
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
   - asynchronous query
   - async query
draft: false
last_update:
   date: 08/17/2022
---


Asynchronous query supports users to execute SQL queries asynchronously and provides a more efficient way to export data. For example, if the result set of a SQL query
Too large (million results) or SQL execution time is too long, through asynchronous query, the query result set can be exported efficiently to realize self-service data retrieval
Various application scenarios.

Currently, asynchronous query only supports calling REST API. For how to use asynchronous query API, please read-[Asynchronous Query API](../../restapi/async_query_api.md).

### Configure the retention time for asynchronous query results

Asynchronous query supports the following configuration in `kylin.properties`:

- `kylin.query.async.result-retain-days=7d`: The retention time of asynchronous query results on HDFS. The default is 7 days, that is, asynchronous query results and related files older than 7 days will be cleaned up.

### Configure a separate cluster queue for asynchronous query

In general, the same cluster queue can be used for asynchronous query and normal query. In some advanced scenarios, if you want to prevent asynchronous queries from affecting ordinary queries, you can deploy a separate queue for asynchronous queries. The specific configuration method is as follows:

1. Enable asynchronous query to deploy a separate cluster queue setting. Set `kylin.query.unique-async-query-yarn-queue-enabled` to `true`. Support project-level configuration and system-level configuration. The priority of project-level configuration is higher than system-level configuration. If neither is configured, asynchronous query and normal query use the same cluster queue.

2. Specify the queue used for asynchronous queries. Three levels are supported for designation, the priority from high to low is as follows:

   - Query level, specified by API request parameter `spark_queue`
   - Project level, specified by setting `kylin.query.async-query.spark-conf.spark.yarn.queue`
   - System level, specified by setting `kylin.query.async-query.spark-conf.spark.yarn.queue` in the configuration file `/conf/kylin.properties`

   > **Tip**: If none of the three are configured, the default queue is `default`

3. Set configuration: `kylin.query.async-query.submit-hadoop-conf-dir=$KYLIN_HOME/async_query_hadoop_conf`

4. Put the hadoop configuration of the asynchronous query cluster into the `$KYLIN_HOME/async_query_hadoop_conf` directory, and put the hive-site.xml for building the cluster into this directory.
    If Kerberos authentication is enabled, you need to copy the `krb5.conf` file to the `$KYLIN_HOME/async_query_hadoop_conf` directory.

5. If Kerberos authentication is enabled between asynchronous query cluster, query cluster, and build cluster, the following additional configuration is required:

    ```
     kylin.storage.columnar.spark-conf.spark.yarn.access.hadoopFileSystems=hdfs://readcluster,hdfs://asyncquerycluster,hdfs://writecluster
     kylin.query.async-query.spark-conf.spark.yarn.access.hadoopFileSystems=hdfs://readcluster,hdfs://asyncquerycluster,hdfs://writecluster
     kylin.engine.spark-conf.spark.yarn.access.hadoopFileSystems=hdfs://readcluster,hdfs://asyncquerycluster,hdfs://writecluster
    ```

6. In general, the above configuration can meet the requirements. In some more advanced scenarios, you can configure spark related configurations in `kylin.properties` to achieve more fine-grained control with guidance of Kylin expert.

   The configuration starts with `kylin.query.async-query.spark-conf`, as shown below:

```
kylin.query.async-query.spark-conf.spark.yarn.queue=default
kylin.query.async-query.spark-conf.spark.executor.extraJavaOptions=-Dhdp.version=current -Dlog4j.configuration=spark-executor-log4j.properties -Dlog4j.debug -Dkylin.hdfs.working.dir=$ {kylin.env.hdfs-working-dir} -Dkap.metadata.identifier=${kylin.metadata.url.identifier} -Dkap.spark.category=sparder -Dkap.spark.project=${job.project}- Dkap.spark.mountDir=${kylin.tool.mount-spark-log-dir} -XX:MaxDirectMemorySize=896M
kylin.query.async-query.spark-conf.spark.yarn.am.extraJavaOptions=-Dhdp.version=current
kylin.query.async-query.spark-conf.spark.driver.extraJavaOptions=-Dhdp.version=current
kylin.query.async-query.spark-conf.spark.port.maxRetries=128
kylin.query.async-query.spark-conf.spark.driver.memory=4096m
kylin.query.async-query.spark-conf.spark.sql.driver.maxCollectSize=3600m
kylin.query.async-query.spark-conf.spark.executor.memory=12288m
kylin.query.async-query.spark-conf.spark.executor.memoryOverhead=3072m
kylin.query.async-query.spark-conf.spark.yarn.am.memory=1024m
kylin.query.async-query.spark-conf.spark.executor.cores=5
kylin.query.async-query.spark-conf.spark.executor.instances=4
kylin.query.async-query.spark-conf.spark.task.maxFailures=1
kylin.query.async-query.spark-conf.spark.ui.port=4041
kylin.query.async-query.spark-conf.spark.locality.wait=0s
kylin.query.async-query.spark-conf.spark.sql.dialect=hiveql
kylin.query.async-query.spark-conf.spark.sql.constraintPropagation.enabled=false
kylin.query.async-query.spark-conf.spark.ui.retainedStages=300
kylin.query.async-query.spark-conf.spark.hadoop.yarn.timeline-service.enabled=false
kylin.query.async-query.spark-conf.spark.hadoop.hive.exec.scratchdir=${kylin.env.hdfs-working-dir}/hive-scratch
kylin.query.async-query.spark-conf.hive.execution.engine=MR
kylin.query.async-query.spark-conf.spark.sql.crossJoin.enabled=true
kylin.query.async-query.spark-conf.spark.broadcast.autoClean.enabled=true
kylin.query.async-query.spark-conf.spark.sql.objectHashAggregate.sortBased.fallbackThreshold=1
kylin.query.async-query.spark-conf.spark.sql.hive.caseSensitiveInferenceMode=NEVER_INFER
kylin.query.async-query.spark-conf.spark.sql.sources.bucketing.enabled=false
kylin.query.async-query.spark-conf.spark.yarn.stagingDir=${kylin.env.hdfs-working-dir}
kylin.query.async-query.spark-conf.spark.eventLog.enabled=true
kylin.query.async-query.spark-conf.spark.history.fs.logDirectory=${kylin.env.hdfs-working-dir}/sparder-history
kylin.query.async-query.spark-conf.spark.eventLog.dir=${kylin.env.hdfs-working-dir}/sparder-history
kylin.query.async-query.spark-conf.spark.eventLog.rolling.enabled=true
kylin.query.async-query.spark-conf.spark.eventLog.rolling.maxFileSize=100m
kylin.query.async-query.spark-conf.spark.sql.cartesianPartitionNumThreshold=-1
kylin.query.async-query.spark-conf.parquet.filter.columnindex.enabled=false
kylin.query.async-query.spark-conf.spark.master=yarn
kylin.query.async-query.spark-conf.spark.submit.deployMode=client
```


### Limitations

- Asynchronous query does not support query cache.
- When the select column of SQL contains `,;{}()=`, these characters will be converted to `_` in the result file.

