---
layout: docs
title:  Kylin Release 1.2
categories: releases
permalink: /docs/releases/kylin_release_1_2.html
---

_Tag:_ [kylin-1.2](https://github.com/apache/kylin/tree/kylin-1.2)



### New Feature

* [KYLIN-596] - Support Excel and Power BI



### Improvement

* [KYLIN-389] - Can't edit cube name for existing cubes
* [KYLIN-702] - When Kylin create the flat hive table, it generates large number of small files in HDFS 
* [KYLIN-1021] - upload dependent jars of kylin to HDFS and set tmp jars
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
* [KYLIN-1161] - Rest API /api/cubes?cubeName= is doing fuzzy match instead of exact match
* [KYLIN-1162] - Enhance HadoopStatusGetter to be compatible with YARN-2605
* [KYLIN-1166] - CubeMigrationCLI should disable and purge the cube in source store after be migrated
* [KYLIN-1168] - Couldn't save cube after doing some modification, get "Update data model is not allowed! Please create a new cube if needed" error
* [KYLIN-1190] - Make memory budget per query configurable



### Task

* [KYLIN-1170] - Update website and status files to TLP



### Bug fix

* [KYLIN-693] - Couldn't change a cube's name after it be created
* [KYLIN-930] - can't see realizations under each project at project list page
* [KYLIN-966] - When user creates a cube, if enter a name which already exists, Kylin will thrown exception on last step
* [KYLIN-1033] - Error when joining two sub-queries
* [KYLIN-1039] - Filter like (A or false) yields wrong result
* [KYLIN-1067] - Support get MapReduce Job status for ResourceManager HA Env
* [KYLIN-1070] - changing case in table name in model desc
* [KYLIN-1093] - Consolidate getCurrentHBaseConfiguration() and newHBaseConfiguration() in HadoopUtil
* [KYLIN-1098] - two "kylin.hbase.region.count.min" in conf/kylin.properties
* [KYLIN-1106] - Can not send email caused by Build Base Cuboid Data step failed
* [KYLIN-1108] - Return Type Empty When Measure-> Count In Cube Design
* [KYLIN-1120] - MapReduce job read local meta issue
* [KYLIN-1121] - ResourceTool download/upload does not work in binary package
* [KYLIN-1140] - Kylin's sample cube "kylin_sales_cube" couldn't be saved.
* [KYLIN-1148] - Edit project's name and cancel edit, project's name still modified
* [KYLIN-1152] - ResourceStore should read content and timestamp in one go
* [KYLIN-1155] - unit test with mini cluster doesn't work on 1.x
* [KYLIN-1203] - Cannot save cube after correcting the configuration mistake
* [KYLIN-1205] - hbase RpcClient java.io.IOException: Unexpected closed connection
* [KYLIN-1216] - Can't parse DateFormat like 'YYYYMMDD' correctly in query