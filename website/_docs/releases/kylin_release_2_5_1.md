---
layout: docs
title:  Kylin Release 2.5.1
categories: releases
permalink: /docs/releases/kylin_release_2_5_1.html
---

*Tag*: [kylin-2.5.1](https://github.com/apache/kylin/tree/kylin-2.5.1)

This is a bug fix release after v2.5.0, with 30 bug fixes and enhancement.



### Improvement

* [KYLIN-3520] - Deal with NULL values of measures for in-mem cubing
* [KYLIN-3526] - No check for hybrid name with special character
* [KYLIN-3528] - No details page for hybrid
* [KYLIN-3599] - Bulk Add Measures
* [KYLIN-3611] - Upgrade Tomcat to 7.0.91, 8.5.34 or later
* [KYLIN-3632] - Add configuration that can switch on/off preparedStatement cache in Kylin server
* [KYLIN-3646] - Add instruction about rowkey sequence in "Advanced setting" page



### Bug fix

* [KYLIN-2200] - CompileException on UNION ALL query when result only contains one column
* [KYLIN-3439] - configuration "kylin.web.timezone" is Inconsistent between kylin-defaults.properties and KylinConfigBase.java
* [KYLIN-3527] - Hybrid couldn't save when there is only 1 cube
* [KYLIN-3531] - Login failed with case-insensitive username
* [KYLIN-3543] - Unclosed Job instance in CreateHTableJob#exportHBaseConfiguration
* [KYLIN-3546] - Kylin doesn't persist FK to intermediate table if FK is not a dimension
* [KYLIN-3556] - Interned string should not be used as lock object
* [KYLIN-3562] - TS conflict when kylin update metadata in HBase
* [KYLIN-3565] - User login error message is inaccurate
* [KYLIN-3567] - Change Scala dependency to provided
* [KYLIN-3582] - String comparison should not use == in RecordEvent
* [KYLIN-3595] - Beeline does not retrieve CLASSPATH and hangs
* [KYLIN-3604] - Can't build cube with spark in HBase standalone mode
* [KYLIN-3613] - Kylin with Standalone HBase Cluster (enabled Kerberos) could not find the main cluster namespace at "Create HTable" step
* [KYLIN-3629] - NullPointException throws when use preparedStatement cache in some case
* [KYLIN-3633] - Dead lock may happen in building global dictionary
* [KYLIN-3634] - When filter column has null value may cause incorrect query result
* [KYLIN-3635] - Percentile calculation on Spark engine is wrong
* [KYLIN-3644] - NumberFormatExcetion on null values when building cube with Spark
* [KYLIN-3645] - Kylin does not clean table metadata when drop project(Kafka Source)
* [KYLIN-3647] - Fix inconsistent states of job and its sub-task
* [KYLIN-3649] - segment region count and size are not correct when using MySQL as Kylin metadata storage
* [KYLIN-3651] - JDBCResourceStore doesn't list all resources
