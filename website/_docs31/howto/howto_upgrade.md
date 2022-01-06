---
layout: docs31
title:  Upgrade From Old Versions
categories: howto
permalink: /docs31/howto/howto_upgrade.html
since: v1.5.1
---

Running as a Hadoop client, Apache Kylin's metadata and Cube data are persistended in Hadoop (HBase and HDFS), so the upgrade is relatively easy and user does not need worry about data loss. The upgrade can be performed in the following steps:

* Download the new Apache Kylin binary package for your Hadoop version from Kylin download page.
* Unpack the new version Kylin package to a new folder, e.g, /usr/local/kylin/apache-kylin-2.1.0/ (directly overwrite old instance is not recommended).
* Merge the old configuration files (`$KYLIN_HOME/conf/*`) into the new ones. It is not recommended to overwrite the new configuration files, although that works in most cases. If you have modified tomcat configuration ($KYLIN_HOME/tomcat/conf/), do the same for it.
* Stop the current Kylin instance with `bin/kylin.sh stop`
* Set the `KYLIN_HOME` env variable to the new installation folder. If you have set `KYLIN_HOME` in `~/.bash_profile` or other scripts, remember to update them as well.
* Start the new Kylin instance with `$KYLIN_HOME/bin/kylin start`. After be started, login Kylin web to check whether your cubes can be loaded correctly.
* [Upgrade coprocessor](howto_update_coprocessor.html) to ensure the HBase region servers use the latest Kylin coprocessor.
* Verify your SQL queries can be performed successfully.

Below are versions specific guides:

## Upgrade from 3.1.2 to 3.1.3
1) When using the real-time function, users need to set `kylin.server.mode=stream_coordinator` for the coordinator node, which can no longer be set to `kylin.server.mode=all`.
2) Kylin users can customize the IV value of the encryption algorithm by config `kylin.security.encrypt.cipher.ivSpec` in kylin 3.1.3.
If you uses the default value, there is no need to modify the encryption password in kylin.properties.
If you changes the value of `kylin.security.encrypt.cipher.ivSpec`, the encrypted password needs to be re-encrypted.

The encryption algorithm may be used in `kylin.metadata.url(mysql password)`, `kylin.security.ldap.connection-password`, etc.

## Upgrade from 3.0 to 3.1.0

1)`Set Config` on web of kylin v3.1.0 is turned off by default.
2) [KYLIN-4478](https://issues.apache.org/jira/browse/KYLIN-4478) modified the encryption algorithm. If you enable LDAP in kylin, you need to re-encoded the password of the LDAP server configured in kylin.properties:

```
cd $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib
java -classpath kylin-server-base-\<versioin\>.jar:kylin-core-common-\<versioin\>.jar:spring-beans-4.3.10.RELEASE.jar:spring-core-4.3.10.RELEASE.jar:commons-codec-1.7.jar org.apache.kylin.rest.security.PasswordPlaceholderConfigurer AES <your_password>
```

Then update the re-encoded password to `kylin.security.ldap.connection-password=<your_password_encrypted>`.

## Upgrade from 2.4 to 2.5.0

* Kylin 2.5 need Java 8; Please upgrade Java if you're running with Java 7.
* Kylin metadata is compatible between 2.4 and 2.5. No migration is needed.
* Spark engine will move more steps from MR to Spark, you may see performance difference for the same cube after the upgrade.
* Property `kylin.source.jdbc.sqoop-home` need be the location of sqoop installation, not its "bin" subfolder, please modify it if you're using RDBMS as the data source. 
* The Cube planner is enabled by default now; New cubes will be optimized by it on first build. System cube and dashboard still need manual enablement.

## Upgrade from v2.1.0 to v2.2.0

Kylin v2.2.0 cube metadata is compitable with v2.1.0, but you need aware the following changes:

* Cube ACL is removed, use Project Level ACL instead. You need to manually configure Project Permissions to migrate your existing Cube Permissions. Please refer to [Project Level ACL](/docs21/tutorial/project_level_acl.html).
* Update HBase coprocessor. The HBase tables for existing cubes need be updated to the latest coprocessor. Follow [this guide](/docs21/howto/howto_update_coprocessor.html) to update.


## Upgrade from v2.0.0 to v2.1.0

Kylin v2.1.0 cube metadata is compitable with v2.0.0, but you need aware the following changes. 

1) In previous version, Kylin uses additional two HBase tables "kylin_metadata_user" and "kylin_metadata_acl" to persistent the user and ACL info. From 2.1, Kylin consolidates all the info into one table: "kylin_metadata". This will make the backup/restore and maintenance more easier. When you start Kylin 2.1.0, it will detect whether need migration; if true, it will print the command to do migration:

```
ERROR: Legacy ACL metadata detected. Please migrate ACL metadata first. Run command 'bin/kylin.sh org.apache.kylin.tool.AclTableMigrationCLI MIGRATE'.
```

After the migration finished, you can delete the legacy "kylin_metadata_user" and "kylin_metadata_acl" tables from HBase.

2) From v2.1, Kylin hides the default settings in "conf/kylin.properties"; You only need uncomment or add the customized properties in it.

3) Spark is upgraded from v1.6.3 to v2.1.1, if you customized Spark configurations in kylin.properties, please upgrade them as well by referring to [Spark documentation](https://spark.apache.org/docs/2.1.0/).

4) If you are running Kylin with two clusters (compute/query separated), need copy the big metadata files (which are persisted in HDFS instead of HBase) from the Hadoop cluster to HBase cluster.

```
hadoop distcp hdfs://compute-cluster:8020/kylin/kylin_metadata/resources hdfs://query-cluster:8020/kylin/kylin_metadata/resources
```


## Upgrade from v1.6.0 to v2.0.0

Kylin v2.0.0 can read v1.6.0 metadata directly. Please follow the common upgrade steps above.

Configuration names in `kylin.properties` have changed since v2.0.0. While the old property names still work, it is recommended to use the new property names as they follow [the naming convention](/development/coding_naming_convention.html) and are easier to understand. There is [a mapping from the old properties to the new properties](https://github.com/apache/kylin/blob/2.0.x/core-common/src/main/resources/kylin-backward-compatibility.properties).

## Upgrade from v1.5.4 to v1.6.0

Kylin v1.5.4 and v1.6.0 are compatible in metadata. Please follow the common upgrade steps above.

## Upgrade from v1.5.3 to v1.5.4
Kylin v1.5.3 and v1.5.4 are compatible in metadata. Please follow the common upgrade steps above.

## Upgrade from 1.5.2 to v1.5.3
Kylin v1.5.3 metadata is compatible with v1.5.2, your cubes don't need rebuilt, as usual, some actions need to be performed:

#### 1. Update HBase coprocessor
The HBase tables for existing cubes need be updated to the latest coprocessor; Follow [this guide](howto_update_coprocessor.html) to update;

#### 2. Update conf/kylin_hive_conf.xml
From 1.5.3, Kylin doesn't need Hive to merge small files anymore; For users who copy the conf/ from previous version, please remove the "merge" related properties in kylin_hive_conf.xml, including "hive.merge.mapfiles", "hive.merge.mapredfiles", and "hive.merge.size.per.task"; this will save the time on extracting data from Hive.


## Upgrade from 1.5.1 to v1.5.2
Kylin v1.5.2 metadata is compatible with v1.5.1, your cubes don't need upgrade, while some actions need to be performed:

#### 1. Update HBase coprocessor
The HBase tables for existing cubes need be updated to the latest coprocessor; Follow [this guide](howto_update_coprocessor.html) to update;

#### 2. Update conf/kylin.properties
In v1.5.2 several properties are deprecated, and several new one are added:

Deprecated:

* kylin.hbase.region.cut.small=5
* kylin.hbase.region.cut.medium=10
* kylin.hbase.region.cut.large=50

New:

* kylin.hbase.region.cut=5
* kylin.hbase.hfile.size.gb=2

These new parameters determines how to split HBase region; To use different size you can overwite these params in Cube level. 

When copy from old kylin.properties file, suggest to remove the deprecated ones and add the new ones.

#### 3. Add conf/kylin\_job\_conf\_inmem.xml
A new job conf file named "kylin\_job\_conf\_inmem.xml" is added in "conf" folder; As Kylin 1.5 introduced the "fast cubing" algorithm, which aims to leverage more memory to do the in-mem aggregation; Kylin will use this new conf file for submitting the in-mem cube build job, which requesting different memory with a normal job; Please update it properly according to your cluster capacity.

Besides, if you have used separate config files for different capacity cubes, for example "kylin\_job\_conf\_small.xml", "kylin\_job\_conf\_medium.xml" and "kylin\_job\_conf\_large.xml", please note that they are deprecated now; Only "kylin\_job\_conf.xml" and "kylin\_job\_conf\_inmem.xml" will be used for submitting cube job; If you have cube level job configurations (like using different Yarn job queue), you can customize at cube level, check [KYLIN-1706](https://issues.apache.org/jira/browse/KYLIN-1706)

