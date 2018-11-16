---
layout: docs
title:  Kylin Release 1.5.1
categories: releases
permalink: /docs/releases/kylin_release_1_5_1.html
---

_Tag:_ [kylin-1.5.1](https://github.com/apache/kylin/tree/kylin-1.5.1)


This version is backward compatible with v1.5.0. But after upgrade to v1.5.1 from v1.5.0, you need to update coprocessor, refer to [How to update coprocessor](/docs/howto/howto_update_coprocessor.html).



### Highlights

* [KYLIN-1122] - Kylin support detail data query from fact table
* [KYLIN-1492] - Custom dimension encoding
* [KYLIN-1495] - Metadata upgrade from 1.0~1.3 to 1.5, including metadata correction, relevant tools, etc.
* [KYLIN-1534] - Cube specific config, override global kylin.properties
* [KYLIN-1546] - Tool to dump information for diagnosis



### New Feature

* [KYLIN-1122] - Kylin support detail data query from fact table
* [KYLIN-1378] - Add UI for TopN measure
* [KYLIN-1492] - Custom dimension encoding
* [KYLIN-1495] - Metadata upgrade from 1.0~1.3 to 1.5, including metadata correction, relevant tools, etc.
* [KYLIN-1501] - Run some classes at the beginning of kylin server startup
* [KYLIN-1503] - Print version information with kylin.sh
* [KYLIN-1531] - Add smoke test scripts
* [KYLIN-1534] - Cube specific config, override global kylin.properties
* [KYLIN-1540] - REST API for deleting segment
* [KYLIN-1541] - IntegerDimEnc, custom dimension encoding for integers
* [KYLIN-1546] - Tool to dump information for diagnosis
* [KYLIN-1550] - Persist some recent bad query



### Improvement

* [KYLIN-1490] - Use InstallShield 2015 to generate ODBC Driver setup files
* [KYLIN-1498] - cube desc signature not calculated correctly
* [KYLIN-1500] - streaming_fillgap cause out of memory
* [KYLIN-1502] - When cube is not empty, only signature consistent cube desc updates are allowed
* [KYLIN-1504] - Use NavigableSet to store rowkey and use prefix filter to check resource path prefix instead String comparison on tomcat side
* [KYLIN-1505] - Combine guava filters with Predicates.and
* [KYLIN-1543] - GTFilterScanner performance tuning
* [KYLIN-1557] - Enhance the check on aggregation group dimension number



### Bug fix

* [KYLIN-1373] - need to encode export query url to get right result in query page
* [KYLIN-1434] - Kylin Job Monitor API: /kylin/api/jobs is too slow in large kylin deployment
* [KYLIN-1472] - Export csv get error when there is a plus sign in the sql
* [KYLIN-1486] - java.lang.IllegalArgumentException: Too many digits for NumberDictionary
* [KYLIN-1491] - Should return base cuboid as valid cuboid if no aggregation group matches
* [KYLIN-1493] - make ExecutableManager.getInstance thread safe
* [KYLIN-1497] - Make three <class>.getInstance thread safe
* [KYLIN-1507] - Couldn't find hive dependency jar on some platform like CDH
* [KYLIN-1513] - Time partitioning doesn't work across multiple days
* [KYLIN-1514] - MD5 validation of Tomcat does not work when package tar
* [KYLIN-1521] - Couldn't refresh a cube segment whose start time is before 1970-01-01
* [KYLIN-1522] - HLLC is incorrect when result is feed from cache
* [KYLIN-1524] - Get "java.lang.Double cannot be cast to java.lang.Long" error when Top-N metrics data type is BigInt
* [KYLIN-1527] - Columns with all NULL values can't be queried
* [KYLIN-1537] - Failed to create flat hive table, when name is too long
* [KYLIN-1538] - DoubleDeltaSerializer cause obvious error after deserialize and serialize
* [KYLIN-1553] - Cannot find rowkey column "COL_NAME" in cube CubeDesc
* [KYLIN-1564] - Unclosed table in BuildCubeWithEngine#checkHFilesInHBase()
* [KYLIN-1569] - Select any column when adding a custom aggregation in GUI