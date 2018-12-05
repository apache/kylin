---
layout: docs
title:  Upgrade From Old Versions
categories: howto
permalink: /docs/howto/howto_upgrade.html
since: v1.5.1
---

Running as a Hadoop client, Apache Kylin's metadata and Cube data are persisted in Hadoop (HBase and HDFS), so the upgrade is relatively easy and user does not need worry about data loss. The upgrade can be performed in the following steps:



### General upgrade

1.Stop and confirm that there are no running Kylin processes:

```sh
$KYLIN_HOME/bin/kylin.sh stop
ps -ef | grep kylin
```

2.(Optional) Back up Kylin metadata to ensure data security:

```sh
$KYLIN_HOME/bin/metastore.sh backup
```

3.Unzip the new version of the Kylin installation package and update the `KYLIN_HOME` environment variable:

```sh
tar -zxvf apache-kylin-{version}-bin-{env}.tar.gz
export KYLIN_HOME={path_to_your_installation_folder}
```

4.Update the configuration file and merge the old configuration files (such as `$KYLIN_HOME/conf/*`, `$KYLIN_HOME/tomcat/conf/*`) into the new configuration file.

5.Start the Kylin process:

```sh
$KYLIN_HOME/bin/kylin.sh start
```

6.To upgrade the coprocessor, please refer to [Upgrade Coprocessor](/cn/docs/howto/howto_upgrade_coprocessor.html).

7.Check if the Web UI is successfully started and etc..

8.After confirming that the upgrade is complete, the previously backed up metadata can be safely deleted.



### Specific versions upgrade

- **Upgrade v2.4 to v2.5.0**

1.Kylin v2.5 need Java 8. Please upgrade Java if you're running with Java 7.

2.Kylin metadata is compatible between v2.4 and v2.5. No migration is needed.

3.Spark engine will move more steps from MR to Spark, you may see performance difference for the same cube after the upgrade.

4.Modify the configuration `kylin.source.jdbc.sqoop-home` as the location of Sqoop installation, not its "bin" sub-folder if users use RDBMS as the data source.

5.The Cube planner is enabled by default. New cubes will be optimized by it on first build. System cube and dashboard still need manual enablement.



- **Upgrade v2.1.0 to v2.2.0**

Kylin v2.2.0 cube metadata is compatible with v2.1.0, but you need aware the following changes:

1.Cube ACL is removed, use Project Level ACL instead. You need to manually configure Project Permissions to migrate your existing Cube Permissions. Please refer to [Project Level ACL](/docs/tutorial/project_level_acl.html).
2.Update HBase coprocessor. The HBase tables for existing cubes need be updated to the latest coprocessor.


- **Upgrade from v2.0.0 to v2.1.0**

Kylin v2.1.0 cube metadata is compatible with v2.0.0, but you need aware the following changes. 

1.In previous version, Kylin uses additional two HBase tables `kylin_metadata_user` and `kylin_metadata_acl` to persistent the user and ACL info. From v2.1, Kylin consolidates all the info into one table: `kylin_metadata`. This will make the backup/restore and maintenance more easier. When you start Kylin v2.1.0, it will detect whether need migration; if true, it will print the command to do migration:

```
ERROR: Legacy ACL metadata detected. Please migrate ACL metadata first. Run command 'bin/kylin.sh org.apache.kylin.tool.AclTableMigrationCLI MIGRATE'.
```

After the migration finished, you can delete the legacy "kylin_metadata_user" and "kylin_metadata_acl" tables from HBase.

2.From v2.1, Kylin hides the default settings in `kylin.properties`, users only need uncomment or add the customized properties in it.

3.Spark is upgraded from v1.6.3 to v2.1.1, if user customized Spark configurations in `kylin.properties`, please upgrade them as well by referring to [Spark documentation](https://spark.apache.org/docs/2.1.0/).

4.If Kylin runs with two clusters (compute/query separated), need copy the big metadata files (which are persisted in HDFS instead of HBase) from the Hadoop cluster to HBase cluster.

```
hadoop distcp hdfs://compute-cluster:8020/kylin/kylin_metadata/resources hdfs://query-cluster:8020/kylin/kylin_metadata/resources
```



- **Upgrade from v1.6.0 to v2.0.0**

Kylin v2.0.0 can read v1.6.0 metadata directly. Please follow the common upgrade steps above.

Configuration names in `kylin.properties` have changed since v2.0.0. While the old property names still work, it is recommended to use the new property names as they follow [the naming convention](/development/coding_naming_convention.html) and are easier to understand. There is [a mapping from the old properties to the new properties](https://github.com/apache/kylin/blob/2.0.x/core-common/src/main/resources/kylin-backward-compatibility.properties).



- **Upgrade from 1.5.2 to v1.5.3**

Kylin v1.5.3 metadata is compatible with v1.5.2, your cubes don't need rebuilt, as usual, some actions need to be performed:

1.Update HBase coprocessor

2.Update `$KYLIN_HOME/conf/kylin_hive_conf.xml`

From v1.5.3, Kylin doesn't need Hive to merge small files anymore; For users who copy the conf/ from previous version, please remove the "merge" related properties in kylin_hive_conf.xml, including "hive.merge.mapfiles", "hive.merge.mapredfiles", and "hive.merge.size.per.task"; this will save the time on extracting data from Hive.



- **Upgrade from 1.5.1 to v1.5.2**

Kylin v1.5.2 metadata is compatible with v1.5.1, your cubes don't need upgrade, while some actions need to be performed:

1.Update HBase coprocessor

2.Update `kylin.properties`

In v1.5.2 several properties are deprecated, and several new one are added:

Deprecated:

* `kylin.hbase.region.cut.small`
* `kylin.hbase.region.cut.medium`
* `kylin.hbase.region.cut.large`

New:

* `kylin.hbase.region.cut`
* `kylin.hbase.hfile.size.gb`

These new parameters determines how to split HBase region; To use different size you can override these parameters in Cube level. 

3.Add `$KYLIN_HOME/conf/kylin_job_conf_inmem.xml`
A new job conf file named `kylin_job_conf_inmem.xml` is added in `$KYLIN_HOME/conf/` folder; As Kylin v1.5 introduced the "fast cubing" algorithm, which aims to leverage more memory to do the in-mem aggregation; Kylin will use this new conf file for submitting the in-mem cube build job, which requesting different memory with a normal job; Please update it properly according to your cluster capacity.

Besides, if you have used separate config files for different capacity cubes, for example `kylin_job_conf_small.xml`, `kylin_job_conf_medium.xml` and `kylin_job_conf_large.xml`, please note that they are deprecated now; Only `kylin_job_conf.xml` and `kylin_job_conf_inmem.xml` will be used for submitting cube job; If you have cube level job configurations (like using different Yarn job queue), you can customize at cube level, check [KYLIN-1706](https://issues.apache.org/jira/browse/KYLIN-1706)

