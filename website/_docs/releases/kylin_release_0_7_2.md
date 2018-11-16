---
layout: docs
title:  Kylin (incubating) Release 0.7.2
categories: releases
permalink: /docs/releases/kylin_release_0_7_2.html
---

_Tag:_ [kylin-0.7.2-incubating](https://github.com/apache/kylin/tree/kylin-0.7.2-incubating)

This is a critical bug fix release after v0.7.1, please go with this version directly for new case and upgrade to this version for existing deployment.



### New Feature

* [KYLIN-211] - Bitmap Inverted Index
* [KYLIN-285] - Enhance alert program for whole system
* [KYLIN-467] - Validation Rule: Check duplicate rows in lookup table
* [KYLIN-471] - Support "Copy" on grid result



### Improvement

* [KYLIN-159] - Metadata migrate tool 
* [KYLIN-199] - Validation Rule: Unique value of Lookup table's key columns
* [KYLIN-207] - Support SQL pagination
* [KYLIN-209] - Merge tail small MR jobs into one
* [KYLIN-210] - Split heavy MR job to more small jobs
* [KYLIN-221] - Convert cleanup and GC to job 
* [KYLIN-284] - add log for all Rest API Request
* [KYLIN-488] - Increase HDFS block size 1GB
* [KYLIN-600] - measure return type update
* [KYLIN-611] - Allow Implicit Joins
* [KYLIN-623] - update Kylin UI Style to latest AdminLTE
* [KYLIN-727] - Cube build in BuildCubeWithEngine does not cover incremental build/cube merge
* [KYLIN-752] - Improved IN clause performance
* [KYLIN-773] - performance is slow list jobs
* [KYLIN-839] - Optimize Snapshot table memory usage 



### Task

* [KYLIN-7] - Enable maven checkstyle plugin
* [KYLIN-885] - Release v0.7.2
* [KYLIN-812] - Upgrade to Calcite 0.9.2



### Bug

* [KYLIN-514] - Error message is not helpful to user when doing something in Jason Editor window
* [KYLIN-598] - Kylin detecting hive table delim failure
* [KYLIN-660] - Make configurable of dictionary cardinality cap
* [KYLIN-765] - When a cube job is failed, still be possible to submit a new job
* [KYLIN-814] - Duplicate columns error for sub-queries on fact table
* [KYLIN-819] - Fix necessary ColumnMetaData order for Calcite (Optic)
* [KYLIN-824] - Cube Build fails if lookup table doesn't have any files under HDFS location
* [KYLIN-829] - Cube "Actions" shows "NA"; but after expand the "access" tab, the button shows up
* [KYLIN-830] - Cube merge failed after migrating from v0.6 to v0.7
* [KYLIN-831] - Kylin report "Column 'ABC' not found in table 'TABLE' while executing SQL", when that column is FK but not define as a dimension
* [KYLIN-840] - HBase table compress not enabled even LZO is installed
* [KYLIN-848] - Couldn't resume or discard a cube job
* [KYLIN-849] - Couldn't query metrics on lookup table PK
* [KYLIN-865] - Cube has been built but couldn't query; In log it said "Realization 'CUBE.CUBE_NAME' defined under project PROJECT_NAME is not found
* [KYLIN-873] - cancel button does not work when [resume][discard] job
* [KYLIN-888] - "Jobs" page only shows 15 job at max, the "Load more" button was disappeared
