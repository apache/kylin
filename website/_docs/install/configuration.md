---
layout: docs
title:  "Kylin Configuration"
categories: install
permalink: /docs/install/configuration.html
---

Kylin detects Hadoop/Hive/HBase configurations from the environments automatically, for example the "core-site.xml", the "hbase-site.xml" and others. Besides, Kylin has its own configurations, managed in the "conf" folder.

{% highlight Groff markup %}
-bash-4.1# ls -l $KYLIN_HOME/conf

kylin_hive_conf.xml
kylin_job_conf_inmem.xml
kylin_job_conf.xml
kylin-kafka-consumer.xml
kylin.properties
kylin-server-log4j.properties
kylin-tools-log4j.properties
setenv.sh 
{% endhighlight %}

## kylin_hive_conf.xml

The Hive configurations that Kylin applied when fetching data from Hive.

## kylin_job_conf.xml and kylin_job_conf_inmem.xml

Hadoop MR configurations when Kylin run MapReduce jobs. The "kylin_job_conf_inmem.xml" one requests more memory for mapper, used for Kylin's "In-mem cubing" job.

## kylin-kafka-consumer.xml

Kafka configurations when Kylin fetching data from Kafka brokers.


## kylin-server-log4j.properties

Kylin server's log configurations.

## kylin-tools-log4j.properties

Kylin command line's log configurations.

## setenv.sh 

Shell script to set environment variables. It will be invoked in "kylin.sh" and other scripts in "bin" folder. Typically, you can adjust the Kylin JVM heap size here, and set "KAFKA_HOME" and other environment variables.

## kylin.properties

The main configuration file of Kylin. 


| Key                                                   | Default value        | Description                                                  | Overwritten at Cube |
| ----------------------------------------------------- | -------------------- | ------------------------------------------------------------ | ------------------------- |
| kylin.env                                             | Dev                  | Whether this env is a Dev, QA, or Prod environment           | No                        |
| kylin.env.hdfs-working-dir                            | /kylin               | Working directory on HDFS                                    | No                        |
| kylin.env.zookeeper-base-path                         | /kylin               | Path on ZK                                                   | No                        |
| kylin.env.zookeeper-connect-string                    |                      | ZK connection string; If blank, use HBase's ZK               | No                        |
| kylin.env.zookeeper-acl-enabled                       | false                |                                                              | No                        |
| kylin.env.zookeeper.zk-auth                           | digest:ADMIN:KYLIN   |                                                              | No                        |
| kylin.env.zookeeper.zk-acl                            | world:anyone:rwcda   |                                                              | No                        |
| kylin.metadata.dimension-encoding-max-length          | 256                  | Max length for one dimension's encoding                      | Yes                       |
| kylin.metadata.url                                    | kylin_metadata@hbase | Kylin metadata storage                                       | No                        |
| kylin.metadata.sync-retries                           | 3                    |                                                              | No                        |
| kylin.metadata.sync-error-handler                     |                      |                                                              | No                        |
| kylin.metadata.check-copy-on-write                    | false                |                                                              | No                        |
| kylin.metadata.hbase-client-scanner-timeout-period    | 10000                |                                                              | No                        |
| kylin.metadata.hbase-rpc-timeout                      | 5000                 |                                                              | No                        |
| kylin.metadata.hbase-client-retries-number            | 1                    |                                                              | No                        |
| kylin.metadata.jdbc.dialect                           | mysql                | clarify the type of dialect                                  | Yes                       |
| kylin.metadata.resource-store-provider.jdbc           | org.apache.kylin.common.persistence.JDBCResourceStore| specify the class that jdbc used|                        |
| kylin.metadata.jdbc.json-always-small-cell            | true                 |                                                              | Yes                       |
| kylin.metadata.jdbc.small-cell-meta-size-warning-threshold| 100mb            |                                                              | Yes                       |
| kylin.metadata.jdbc.small-cell-meta-size-error-threshold| 1gb                |                                                              | Yes                       |
| kylin.metadata.jdbc.max-cell-size                     | 1mb                  |                                                              | Yes                       |
| kylin.dictionary.use-forest-trie                      | true                 |                                                              | No                        |
| kylin.dictionary.forest-trie-max-mb                   | 500                  |                                                              | No                        |
| kylin.dictionary.max-cache-entry                      | 3000                 |                                                              | No                        |
| kylin.dictionary.growing-enabled                      | false                |                                                              | No                        |
| kylin.dictionary.append-entry-size                    | 10000000             |                                                              | No                        |
| kylin.dictionary.append-max-versions                  | 3                    |                                                              | No                        |
| kylin.dictionary.append-version-ttl                   | 259200000            |                                                              | No                        |
| kylin.dictionary.resuable                             | false                | Whether reuse dict                                           | Yes                       |
| kylin.dictionary.shrunken-from-global-enabled         | false                | Whether shrink global dict                                   | Yes                       |
| kylin.snapshot.max-cache-entry                        | 500                  |                                                              | No                        |
| kylin.snapshot.max-mb                                 | 300                  |                                                              | No                        |
| kylin.snapshot.ext.shard-mb                           | 500                  |                                                              | No                        |
| kylin.snapshot.ext.local.cache.path                   | lookup_cache         |                                                              | No                        |
| kylin.snapshot.ext.local.cache.max-size-gb            | 200                  |                                                              | No                        |
| kylin.cube.size-estimate-ratio                        | 0.25                 |                                                              | Yes                       |
| kylin.cube.size-estimate-memhungry-ratio              | 0.05                 | Deprecated                                                   | Yes                       |
| kylin.cube.size-estimate-countdistinct-ratio          | 0.5                  |                                                              | Yes                       |
| kylin.cube.size-estimate-topn-ratio                   | 0.5                  |                                                              | Yes                       |
| kylin.cube.algorithm                                  | auto                 | Cubing algorithm for MR engine, other options: layer, inmem  | Yes                       |
| kylin.cube.algorithm.layer-or-inmem-threshold         | 7                    |                                                              | Yes                       |
| kylin.cube.algorithm.inmem-split-limit                | 500                  |                                                              | Yes                       |
| kylin.cube.algorithm.inmem-concurrent-threads         | 1                    |                                                              | Yes                       |
| kylin.cube.ignore-signature-inconsistency             | false                |                                                              |                           |
| kylin.cube.aggrgroup.max-combination                  | 32768                | Max cuboid numbers in a Cube                                 | Yes                       |
| kylin.cube.aggrgroup.is-mandatory-only-valid          | false                | Whether allow a Cube only has the base cuboid.               | Yes                       |
| kylin.cube.cubeplanner.enabled                        | true                 | Whether enable cubeplanner                                   | Yes                       |
| kylin.cube.cubeplanner.enabled-for-existing-cube      | true                 | Whether enable cubeplanner for existing cube                 | Yes                       |
| kylin.cube.cubeplanner.algorithm-threshold-greedy     | 8                    |                                                              | Yes                       |
| kylin.cube.cubeplanner.expansion-threshold            | 15.0                 |                                                              | Yes                       |
| kylin.cube.cubeplanner.recommend-cache-max-size       | 200                  |                                                              | No                        |
| kylin.cube.cubeplanner.mandatory-rollup-threshold     | 1000                 |                                                              | Yes                       |
| kylin.cube.cubeplanner.algorithm-threshold-genetic    | 23                   |                                                              | Yes                       |
| kylin.cube.rowkey.max-size                            | 63                   | Max columns in Rowkey                                        | No                        |
| kylin.cube.max-building-segments                      | 10                   | Max building segments in one Cube                            | Yes                       |
| kylin.cube.allow-appear-in-multiple-projects          | false                | Whether allow a Cueb appeared in multiple projects           | No                        |
| kylin.cube.gtscanrequest-serialization-level          | 1                    |                                                              |                           |
| kylin.cube.is-automerge-enabled                       | true                 | Whether enable auto merge.                                   | Yes                       |
| kylin.job.log-dir                                     | /tmp/kylin/logs      |                                                              |                           |
| kylin.job.allow-empty-segment                         | true                 | Whether tolerant data source is emtpy.                       | Yes                       |
| kylin.job.max-concurrent-jobs                         | 10                   | Max concurrent running jobs                                  | No                        |
| kylin.job.sampling-percentage                         | 100                  | Data sampling percentage, to calculate Cube statistics; Default be all. | Yes                       |
| kylin.job.notification-enabled                        | false                | Whether send email notification on job error/succeed.        | No                        |
| kylin.job.notification-mail-enable-starttls           | false                |                                                              | No                        |
| kylin.job.notification-mail-port                      | 25                   |                                                              | No                        |
| kylin.job.notification-mail-host                      |                      |                                                              | No                        |
| kylin.job.notification-mail-username                  |                      |                                                              | No                        |
| kylin.job.notification-mail-password                  |                      |                                                              | No                        |
| kylin.job.notification-mail-sender                    |                      |                                                              | No                        |
| kylin.job.notification-admin-emails                   |                      |                                                              | No                        |
| kylin.job.retry                                       | 0                    |                                                              | No                        |
|                                                       |                      |                                                              |                           |
| kylin.job.scheduler.priority-considered               | false                |                                                              | No                        |
| kylin.job.scheduler.priority-bar-fetch-from-queue     | 20                   |                                                              | No                        |
| kylin.job.scheduler.poll-interval-second              | 30                   |                                                              | No                        |
| kylin.job.error-record-threshold                      | 0                    |                                                              | No                        |
| kylin.job.cube-auto-ready-enabled                     | true                 | Whether enable the cube automatically when finish build      | Yes                       |
| kylin.source.hive.keep-flat-table                     | false                | Whether keep the intermediate Hive table after job finished. | No                        |
| kylin.source.hive.database-for-flat-table             | default              | Hive database to create the intermediate table.              | No                        |
| kylin.source.hive.flat-table-storage-format           | SEQUENCEFILE         |                                                              | No                        |
| kylin.source.hive.flat-table-field-delimiter          | \u001F               |                                                              | No                        |
| kylin.source.hive.redistribute-flat-table             | true                 | Whether or not to redistribute the flat table.               | Yes                       |
| kylin.source.hive.redistribute-column-count           | 3                    | The number of redistribute column                            | Yes                       |
| kylin.source.hive.client                              | cli                  |                                                              | No                        |
| kylin.source.hive.beeline-shell                       | beeline              |                                                              | No                        |
| kylin.source.hive.beeline-params                      |                      |                                                              | No                        |
| kylin.source.hive.enable-sparksql-for-table-ops       | false                |                                                              | No                        |
| kylin.source.hive.sparksql-beeline-shell              |                      |                                                              | No                        |
| kylin.source.hive.sparksql-beeline-params             |                      |                                                              | No                        |
| kylin.source.hive.table-dir-create-first              | false                |                                                              | No                        |
| kylin.source.hive.flat-table-cluster-by-dict-column   |                      |                                                              |                           |
| kylin.source.hive.default-varchar-precision           | 256                  |                                                              | No                        |
| kylin.source.hive.default-char-precision              | 255                  |                                                              | No                        |
| kylin.source.hive.default-decimal-precision           | 19                   |                                                              | No                        |
| kylin.source.hive.default-decimal-scale               | 4                    |                                                              | No                        |
| kylin.source.jdbc.connection-url                      |                      |                                                              |                           |
| kylin.source.jdbc.driver                              |                      |                                                              |                           |
| kylin.source.jdbc.dialect                             | default              |                                                              |                           |
| kylin.source.jdbc.user                                |                      |                                                              |                           |
| kylin.source.jdbc.pass                                |                      |                                                              |                           |
| kylin.source.jdbc.sqoop-home                          |                      |                                                              |                           |
| kylin.source.jdbc.sqoop-mapper-num                    | 4                    |                                                              |                           |
| kylin.source.jdbc.field-delimiter                     | \|                   |                                                              |                           |
| kylin.storage.default                                 | 2                    |                                                              | No                        |
| kylin.storage.hbase.table-name-prefix                 | KYLIN_               |                                                              | No                        |
| kylin.storage.hbase.namespace                         | default              |                                                              | No                        |
| kylin.storage.hbase.cluster-fs                        |                      |                                                              |                           |
| kylin.storage.hbase.cluster-hdfs-config-file          |                      |                                                              |                           |
| kylin.storage.hbase.coprocessor-local-jar             |                      |                                                              |                           |
| kylin.storage.hbase.min-region-count                  | 1                    |                                                              |                           |
| kylin.storage.hbase.max-region-count                  | 500                  |                                                              |                           |
| kylin.storage.hbase.hfile-size-gb                     | 2.0                  |                                                              |                           |
| kylin.storage.hbase.run-local-coprocessor             | false                |                                                              |                           |
| kylin.storage.hbase.coprocessor-mem-gb                | 3.0                  |                                                              |                           |
| kylin.storage.partition.aggr-spill-enabled            | true                 |                                                              |                           |
| kylin.storage.partition.max-scan-bytes                | 3221225472           |                                                              |                           |
| kylin.storage.hbase.coprocessor-timeout-seconds       | 0                    |                                                              |                           |
| kylin.storage.hbase.max-fuzzykey-scan                 | 200                  |                                                              |                           |
| kylin.storage.hbase.max-fuzzykey-scan-split           | 1                    |                                                              |                           |
| kylin.storage.hbase.max-visit-scanrange               | 1000000              |                                                              |                           |
| kylin.storage.hbase.scan-cache-rows                   | 1024                 |                                                              |                           |
| kylin.storage.hbase.region-cut-gb                     | 5.0                  |                                                              |                           |
| kylin.storage.hbase.max-scan-result-bytes             | 5242880              |                                                              |                        |
| kylin.storage.hbase.compression-codec                 | none                 |                                                              |                           |
| kylin.storage.hbase.rowkey-encoding                   | FAST_DIFF            |                                                              |                           |
| kylin.storage.hbase.block-size-bytes                  | 1048576              |                                                              |                           |
| kylin.storage.hbase.small-family-block-size-bytes     | 65536                |                                                              |                           |
| kylin.storage.hbase.owner-tag                         |                      |                                                              |                           |
| kylin.storage.hbase.endpoint-compress-result          | true                 |                                                              |                           |
| kylin.storage.hbase.max-hconnection-threads           | 2048                 |                                                              |                           |
| kylin.storage.hbase.core-hconnection-threads          | 2048                 |                                                              |                           |
| kylin.storage.hbase.hconnection-threads-alive-seconds | 60                   |                                                              |                           |
| kylin.storage.hbase.replication-scope                 | 0                    | whether config hbase cluster replication                     | Yes                       |
| kylin.engine.mr.lib-dir                               |                      |                                                              |                           |
| kylin.engine.mr.reduce-input-mb                       | 500                  |                                                              |                           |
| kylin.engine.mr.reduce-count-ratio                    | 1.0                  |                                                              |                           |
| kylin.engine.mr.min-reducer-number                    | 1                    |                                                              |                           |
| kylin.engine.mr.max-reducer-number                    | 500                  |                                                              |                           |
| kylin.engine.mr.mapper-input-rows                     | 1000000              |                                                              |                           |
| kylin.engine.mr.max-cuboid-stats-calculator-number    | 1                    |                                                              |                           |
| kylin.engine.mr.uhc-reducer-count                     | 1                    |                                                              |                           |
| kylin.engine.mr.build-uhc-dict-in-additional-step     | false                |                                                              |                           |
| kylin.engine.mr.build-dict-in-reducer                 | true                 |                                                              |                           |
| kylin.engine.mr.yarn-check-interval-seconds           | 10                   |                                                              |                           |
| kylin.env.hadoop-conf-dir                             |                      | Hadoop conf directory; If not specified, parse from environment. | No                        |
| kylin.engine.spark.rdd-partition-cut-mb               | 10.0                 | Spark Cubing RDD partition split size.                       | Yes                       |
| kylin.engine.spark.min-partition                      | 1                    | Spark Cubing RDD min partition number                        | Yes                       |
| kylin.engine.spark.max-partition                      | 5000                 | RDD max partition number                                     | Yes                       |
| kylin.engine.spark.storage-level                      | MEMORY_AND_DISK_SER  | RDD persistent level.                                        | Yes                       |
| kylin.engine.spark-conf.spark.hadoop.dfs.replication  | 2                    |                                                              |                           |
| kylin.engine.spark-conf.spark.hadoop.mapreduce.output.fileoutputformat.compress| true|                                                      |                           |
| kylin.engine.spark-conf.spark.hadoop.mapreduce.output.fileoutputformat.compress.codec  | org.apache.hadoop.io.compress.DefaultCodec|        |                           |
| kylin.engine.spark-conf-mergedict.spark.executor.memory| 6G                  |                                                              | Yes                       |
| kylin.engine.spark-conf-mergedict.spark.memory.fraction| 0.2                 |                                                              | Yes                       |
| kylin.query.skip-empty-segments                       | true                 | Whether directly skip empty segment (metadata shows size be 0) when run SQL query. | Yes |
| kylin.query.force-limit                               | -1                   |                                                              |                           |
| kylin.query.max-scan-bytes                            | 0                    |                                                              |                           |
| kylin.query.max-return-rows                           | 5000000              |                                                              |                           |
| kylin.query.large-query-threshold                     | 1000000              |                                                              |                           |
| kylin.query.cache-threshold-duration                  | 2000                 |                                                              |                           |
| kylin.query.cache-threshold-scan-count                | 10240                |                                                              |                           |
| kylin.query.cache-threshold-scan-bytes                | 1048576              |                                                              |                           |
| kylin.query.security-enabled                          | true                 |                                                              |                           |
| kylin.query.cache-enabled                             | true                 |                                                              |                           |
| kylin.query.timeout-seconds                           | 0                    |                                                              |                           |
| kylin.query.timeout-seconds-coefficient               | 0.5                  | the coefficient to controll query timeout seconds            | Yes                       |
| kylin.query.pushdown.runner-class-name                |                      |                                                              |                           |
| kylin.query.pushdown.update-enabled                   | false                |                                                              |                           |
| kylin.query.pushdown.cache-enabled                    | false                |                                                              |                           |
| kylin.query.pushdown.jdbc.url                         |                      |                                                              |                           |
| kylin.query.pushdown.jdbc.driver                      |                      |                                                              |                           |
| kylin.query.pushdown.jdbc.username                    |                      |                                                              |                           |
| kylin.query.pushdown.jdbc.password                    |                      |                                                              |                           |
| kylin.query.pushdown.jdbc.pool-max-total              | 8                    |                                                              |                           |
| kylin.query.pushdown.jdbc.pool-max-idle               | 8                    |                                                              |                           |
| kylin.query.pushdown.jdbc.pool-min-idle               | 0                    |                                                              |                           |
| kylin.query.security.table-acl-enabled                | true                 |                                                              | No                        |
| kylin.query.calcite.extras-props.conformance          | LENIENT              |                                                              | Yes                       |
| kylin.query.calcite.extras-props.caseSensitive        | true                 | Whether enable case sensitive                                | Yes                       |
| kylin.query.calcite.extras-props.unquotedCasing       | TO_UPPER             | Options: UNCHANGED, TO_UPPER, TO_LOWER                       | Yes                       |
| kylin.query.calcite.extras-props.quoting              | DOUBLE_QUOTE         | Options: DOUBLE_QUOTE, BACK_TICK, BRACKET                    | Yes                       |
| kylin.query.statement-cache-max-num                   | 50000                | Max number for cache query statement                         | Yes                       |
| kylin.query.statement-cache-max-num-per-key           | 50                   |                                                              | Yes                       |
| kylin.query.enable-dict-enumerator                    | false                | Whether enable dict enumerator                               | Yes                       |
| kylin.server.mode                                     | all                  | Kylin node mode: all\|job\|query.                            | No                        |
| kylin.server.cluster-servers                          | localhost:7070       |                                                              | No                        |
| kylin.server.cluster-name                             |                      |                                                              | No                        |
| kylin.server.query-metrics-enabled                    | false                |                                                              | No                        |
| kylin.server.query-metrics2-enabled                   | false                |                                                              | No                        |
| kylin.server.auth-user-cache.expire-seconds           | 300                  |                                                              | No                        |
| kylin.server.auth-user-cache.max-entries              | 100                  |                                                              | No                        |
| kylin.server.external-acl-provider                    |                      |                                                              | No                        |
| kylin.security.ldap.user-search-base                  |                      |                                                              | No                        |
| kylin.security.ldap.user-group-search-base            |                      |                                                              | No                        |
| kylin.security.acl.admin-role                         |                      |                                                              | No                        |
| kylin.web.timezone                                    | PST                  |                                                              | No                        |
| kylin.web.cross-domain-enabled                        | true                 |                                                              | No                        |
| kylin.web.export-allow-admin                          | true                 |                                                              | No                        |
| kylin.web.export-allow-other                          | true                 |                                                              | No                        |
| kylin.web.dashboard-enabled                           | false                |                                                              | No                        |

