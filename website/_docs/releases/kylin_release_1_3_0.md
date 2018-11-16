---
layout: docs
title:  Kylin Release 1.3.0
categories: releases
permalink: /docs/releases/kylin_release_1_3_0.html
---

_Tag:_ [kylin-1.3.0](https://github.com/apache/kylin/tree/kylin-1.3.0)



### New Feature

* [KYLIN-579] - Unload table from Kylin
* [KYLIN-976] - Support Custom Aggregation Types
* [KYLIN-1054] - Support Hive client Beeline
* [KYLIN-1128] - Clone Cube Metadata
* [KYLIN-1186] - Support precise Count Distinct using bitmap (under limited conditions)



### Improvement

* [KYLIN-955] - HiveColumnCardinalityJob should use configurations in conf/kylin_job_conf.xml
* [KYLIN-1014] - Support Kerberos authentication while getting status from RM
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
* [KYLIN-1323] - Improve performance of converting data to HFile
* [KYLIN-1333] - Kylin Entity Permission Control 
* [KYLIN-1343] - Upgrade calcite version to 1.6
* [KYLIN-1365] - Kylin ACL enhancement
* [KYLIN-1368] - JDBC Driver is not generic to restAPI json result



### Bug fix

* [KYLIN-918] - Calcite throws "java.lang.Float cannot be cast to java.lang.Double" error while executing SQL
* [KYLIN-1075] - select [MeasureCol] from [FactTbl] is not supported
* [KYLIN-1078] - Cannot have comments in the end of New Query textbox
* [KYLIN-1104] - Long dimension value cause ArrayIndexOutOfBoundsException
* [KYLIN-1110] - can not see project options after clear browser cookie and cache
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
* [KYLIN-1419] - NullPointerException occurs when query from sub-queries with order by
* [KYLIN-1423] - HBase size precision issue
* [KYLIN-1443] - For setting Auto Merge Time Ranges, before sending them to backend, the related time ranges should be sorted increasingly
* [KYLIN-1445] - Kylin should throw error if HIVE_CONF dir cannot be found
* [KYLIN-1456] - Shouldn't use "1970-01-01" as the default end date
* [KYLIN-1466] - Some environment variables are not used in bin/kylin.sh <RUNNABLE_CLASS_NAME>
* [KYLIN-1469] - Hive dependency jars are hard coded in test



### Test

* [KYLIN-1335] - Disable PrintResult in KylinQueryTest