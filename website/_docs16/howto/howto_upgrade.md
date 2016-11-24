---
layout: docs16
title:  Upgrade From Old Versions
categories: howto
permalink: /docs16/howto/howto_upgrade.html
since: v1.5.1
---

Running as a Hadoop client, Apache Kylin's metadata and Cube data are persistended in Hadoop (HBase and HDFS), so the upgrade is relatively easy and user doesn't need worry about data loss. The upgrade can be performed in the following steps:

* Download the new Apache Kylin binary package for your Hadoop version from Kylin's download page;
* Uncompress the new version Kylin package to a new folder, e.g, /usr/local/kylin/apache-kylin-1.6.0/ (directly overwrite old instance is not recommended);
* Copy the configuration files (`$KYLIN_HOME/conf/*`) from old instance (e.g /usr/local/kylin/apache-kylin-1.5.4/) to the new instance's `conf` folder if you have customized configurations; It is recommended to do a compare and merge since there might be new parameters introduced. If you have modified tomcat configuration ($KYLIN_HOME/tomcat/conf/), also remember to do the same.
* Stop the current Kylin instance with `./bin/kylin.sh stop`;
* Set the `KYLIN_HOME` env variable to the new instance's installation folder. If you have set `KYLIN_HOME` in `~/.bash_profile` or other scripts, remember to update them as well.
* Start the new Kylin instance with `$KYLIN_HOME/bin/kylin start`; After be started, login Kylin web to check whether your cubes can be loaded correctly.
* [Upgrade coprocessor](howto_update_coprocessor.html) to ensure the HBase region servers use the latest Kylin coprocessor.
* Verify your SQL queries can be performed successfully.

Below are versions specific guides:

## Upgrade from v1.5.4 to v1.6.0
Kylin v1.5.4 and v1.6.0 are compitible in metadata; Please follow the common upgrade steps above.

## Upgrade from v1.5.3 to v1.5.4
Kylin v1.5.3 and v1.5.4 are compitible in metadata; Please follow the common upgrade steps above.

## Upgrade from 1.5.2 to v1.5.3
Kylin v1.5.3 metadata is compitible with v1.5.2, your cubes don't need rebuilt, as usual, some actions need to be performed:

#### 1. Update HBase coprocessor
The HBase tables for existing cubes need be updated to the latest coprocessor; Follow [this guide](howto_update_coprocessor.html) to update;

#### 2. Update conf/kylin_hive_conf.xml
From 1.5.3, Kylin doesn't need Hive to merge small files anymore; For users who copy the conf/ from previous version, please remove the "merge" related properties in kylin_hive_conf.xml, including "hive.merge.mapfiles", "hive.merge.mapredfiles", and "hive.merge.size.per.task"; this will save the time on extracting data from Hive.


## Upgrade from 1.5.1 to v1.5.2
Kylin v1.5.2 metadata is compitible with v1.5.1, your cubes don't need upgrade, while some actions need to be performed:

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

