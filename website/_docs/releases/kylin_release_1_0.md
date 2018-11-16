---
layout: docs
title:  Kylin (incubating) Release 1.0
categories: releases
permalink: /docs/releases/kylin_release_1_0.html
---


_Tag:_ [kylin-1.0-incubating](https://github.com/apache/kylin/tree/kylin-1.0-incubating)



### New Feature

* [KYLIN-591] - Leverage Zeppelin to interactive with Kylin



### Improvement

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



### Task

* [KYLIN-884] - Restructure docs and website
* [KYLIN-907] - Improve Kylin community development experience
* [KYLIN-954] - Release v1.0 (formerly v0.7.3)
* [KYLIN-863] - create empty segment when there is no data in one single streaming batch
* [KYLIN-908] - Help community developer to setup develop/debug environment
* [KYLIN-931] - Port KYLIN-921 to 0.8 branch



### Bug fix

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
* [KYLIN-940] - NPE when close the null resource
* [KYLIN-945] - Kylin JDBC - Get Connection from DataSource results in NullPointerException
* [KYLIN-946] - [UI] refresh page show no results when Project selected as [--Select All--]
* [KYLIN-949] - Query cache doesn't work properly for prepareStatement queries