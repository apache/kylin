---
layout: docs
title:  Kylin Release 1.6.0
categories: releases
permalink: /docs/releases/kylin_release_1_6_0.html
---

_Tag:_ [kylin-1.6.0](https://github.com/apache/kylin/tree/kylin-1.6.0)

This is a major release with better support for using Apache Kafka as data source. 



### New Feature

* [KYLIN-1726] - Scalable streaming cubing
* [KYLIN-1919] - Support Embedded Structure when Parsing Streaming Message
* [KYLIN-2055] - Add an encoder for Boolean type
* [KYLIN-2067] - Add API to check and fill segment holes
* [KYLIN-2079] - add explicit configuration knob for coprocessor timeout
* [KYLIN-2088] - Support intersect count for calculation of retention or conversion rates
* [KYLIN-2125] - Support using beeline to load hive table metadata



### Improvement

* [KYLIN-1042] - Horizontal scalable solution for streaming cubing
* [KYLIN-1827] - Send mail notification when runtime exception throws during build/merge cube
* [KYLIN-1839] - improvement set classpath before submitting MR job
* [KYLIN-1917] - TopN counter merge performance improvement
* [KYLIN-1962] - Split kylin.properties into two files
* [KYLIN-1999] - Use some compression at UT/IT
* [KYLIN-2019] - Add license checker into checkstyle rule
* [KYLIN-2033] - Refactor broadcast of metadata change
* [KYLIN-2042] - QueryController puts entry in Cache w/o checking QueryCacheEnabled
* [KYLIN-2054] - TimedJsonStreamParser should support other time format
* [KYLIN-2068] - Import hive comment when sync tables
* [KYLIN-2070] - UI changes for allowing concurrent build/refresh/merge
* [KYLIN-2073] - Need timestamp info for diagnose  
* [KYLIN-2075] - TopN measure: need select "constant" + "1" as the SUM|ORDER parameter
* [KYLIN-2076] - Improve sample cube and data
* [KYLIN-2080] - UI: allow multiple building jobs for the same cube
* [KYLIN-2082] - Support to change streaming configuration
* [KYLIN-2089] - Make update HBase coprocessor concurrent
* [KYLIN-2090] - Allow updating cube level config even the cube is ready
* [KYLIN-2091] - Add API to init the start-point (of each partition) for streaming cube
* [KYLIN-2095] - Hive MR job use overridden MR job configuration by cube properties
* [KYLIN-2098] - TopN support query UHC column without sorting by sum value
* [KYLIN-2100] - Allow cube to override HIVE job configuration by properties
* [KYLIN-2108] - Support usage of schema name "default" in SQL
* [KYLIN-2111] - only allow columns from Model dimensions when add group by column to TOP_N
* [KYLIN-2112] - Allow a column be a dimension as well as "group by" column in TopN measure
* [KYLIN-2113] - Need sort by columns in SQLDigest
* [KYLIN-2118] - allow user view CubeInstance json even cube is ready
* [KYLIN-2122] - Move the partition offset calculation before submitting job
* [KYLIN-2126] - use column name as default dimension name when auto generate dimension for lookup table
* [KYLIN-2140] - rename packaged js with different name when build
* [KYLIN-2143] - allow more options from Extended Columns,COUNT_DISTINCT,RAW_TABLE
* [KYLIN-2162] - Improve the cube validation error message
* [KYLIN-2221] - rethink on KYLIN-1684
* [KYLIN-2083] - more RAM estimation test for MeasureAggregator and GTAggregateScanner
* [KYLIN-2105] - add QueryId
* [KYLIN-1321] - Add derived check-box for lookup table columns on Auto Generate Dimensions panel
* [KYLIN-1995] - Upgrade MapReduce properties which are deprecated



### Task

* [KYLIN-2072] - Cleanup old streaming code
* [KYLIN-2081] - UI change to support embedded streaming message
* [KYLIN-2171] - Release 1.6.0



### Bug fix

* [KYLIN-1565] - Read the kv max size from HBase config
* [KYLIN-1820] - Column auto-complete should remove the user input in model designer
* [KYLIN-1828] - java.lang.StringIndexOutOfBoundsException in org.apache.kylin.storage.hbase.util.StorageCleanupJob
* [KYLIN-1967] - Dictionary rounding can cause IllegalArgumentException in GTScanRangePlanner
* [KYLIN-1978] - kylin.sh compatible issue on Ubuntu
* [KYLIN-1990] - The SweetAlert at the front page may out of the page if the content is too long.
* [KYLIN-2007] - CUBOID_CACHE is not cleared when rebuilding ALL cache
* [KYLIN-2012] - more robust approach to hive schema changes
* [KYLIN-2024] - kylin TopN only support the first measure 
* [KYLIN-2027] - Error "connection timed out" occurs when ZooKeeper's port is set in hbase.zookeeper.quorum of hbase-site.xml
* [KYLIN-2028] - find-\*-dependency script fail on Mac OS
* [KYLIN-2035] - Auto Merge Submit Continuously
* [KYLIN-2041] - Wrong parameter definition in Get Hive Tables REST API
* [KYLIN-2043] - Rollback http-client to 4.2.5 to align with Hadoop 2.6/2.7
* [KYLIN-2044] - Unclosed DataInputByteBuffer in BitmapCounter#peekLength
* [KYLIN-2045] - Wrong argument order in JobInstanceExtractor#executeExtract()
* [KYLIN-2047] - Ineffective null check in MetadataManager
* [KYLIN-2050] - Potentially ineffective call to close() in QueryCli
* [KYLIN-2051] - Potentially ineffective call to IOUtils.closeQuietly()
* [KYLIN-2052] - Edit "Top N" measure, the "group by" column wasn't displayed
* [KYLIN-2059] - Concurrent build issue in CubeManager.calculateToBeSegments()
* [KYLIN-2069] - NPE in LookupStringTable
* [KYLIN-2078] - Can't see generated SQL at Web UI
* [KYLIN-2084] - Unload sample table failed
* [KYLIN-2085] - PrepareStatement return incorrect result in some cases
* [KYLIN-2086] - Still report error when there is more than 12 dimensions in one agg group
* [KYLIN-2093] - Clear cache in CubeMetaIngester
* [KYLIN-2097] - Get 'Column does not exist in row key desc" on cube has TopN measure
* [KYLIN-2099] - Import table error of sample table KYLIN_CAL_DT
* [KYLIN-2106] - UI bug - Advanced Settings - Rowkeys - new Integer dictionary encoding - could possibly impact also cube metadata
* [KYLIN-2109] - Deploy coprocessor only this server own the table
* [KYLIN-2110] - Ineffective comparison in BooleanDimEnc#equals()
* [KYLIN-2114] - WEB-Global-Dictionary bug fix and improve
* [KYLIN-2115] - some extended column query returns wrong answer
* [KYLIN-2116] - when hive field delimiter exists in table field values, fields order is wrong
* [KYLIN-2119] - Wrong chart value and sort when process scientific notation 
* [KYLIN-2120] - kylin 1.5.4.1 with CDH 5.7 cube sql Oops Failed to take action
* [KYLIN-2121] - Failed to pull data to PowerBI or Excel on some query
* [KYLIN-2127] - UI bug fix for Extend Column
* [KYLIN-2130] - QueryMetrics concurrent bug fix
* [KYLIN-2132] - Unable to pull data from Kylin Cube ( learn_kylin cube ) to Excel or Power BI for Visualization and some dimensions are not showing up.
* [KYLIN-2134] - Kylin will treat empty string as NULL by mistake
* [KYLIN-2137] - Failed to run mr job when user put a Kafka jar in hive's lib folder
* [KYLIN-2138] - Unclosed ResultSet in BeelineHiveClient
* [KYLIN-2146] - "Streaming Cluster" page should remove "Margin" input-box
* [KYLIN-2152] - TopN group by column does not distinguish between NULL and ""
* [KYLIN-2154] - source table rows will be skipped if TOPN's group column contains NULL values
* [KYLIN-2158] - Delete joint dimension not right
* [KYLIN-2159] - Redistribution Hive Table Step always requires row_count filename as 000000_0 
* [KYLIN-2167] - FactDistinctColumnsReducer may get wrong max/min partition col value
* [KYLIN-2173] - push down limit leads to wrong answer when filter is loosened
* [KYLIN-2178] - CubeDescTest is unstable
* [KYLIN-2201] - Cube desc and aggregation group rule combination max check fail
* [KYLIN-2226] - Build Dimension Dictionary Error

