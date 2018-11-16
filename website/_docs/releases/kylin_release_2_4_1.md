---
layout: docs
title:  Kylin Release 2.4.1
categories: releases
permalink: /docs/releases/kylin_release_2_4_1.html
---

_Tag:_ [kylin-2.4.1](https://github.com/apache/kylin/tree/kylin-2.4.1)


This is a bug fix release after 2.4.0, with 22 bug fixes and enhancement.


### Improvement

* [KYLIN-3421] - Improve job scheduler fetch performance
* [KYLIN-3424] - Missing invoke addCubingGarbageCollectionSteps in the cleanup step for HBaseMROutput2Transition
* [KYLIN-3422] - Support multi-path of domain for kylin connection
* [KYLIN-3463] - Improve optimize job by avoiding creating empty output files on HDFS
* [KYLIN-3503] - Missing java.util.logging.config.file when starting kylin instance
* [KYLIN-3507] - Query NPE when project is not found



### Bug fix

* [KYLIN-2662] - NegativeArraySizeException in "Extract Fact Table Distinct Columns
* [KYLIN-3025] - kylin ODBC error : {fn CONVERT} for bigint type in tableau 10.4
* [KYLIN-3255] - Cannot save cube
* [KYLIN-3347] - QueryService Exception when using calcite function ex : {fn CURRENT_TIMESTAMP(0)}
* [KYLIN-3391] - BadQueryDetector only detect first query
* [KYLIN-3403] - Querying sample cube with filter "KYLIN_CAL_DT.WEEK_BEG_DT >= CAST('2001-09-09' AS DATE)" returns unexpected empty result set
* [KYLIN-3428] - java.lang.OutOfMemoryError: Requested array size exceeds VM limit
* [KYLIN-3438] - mapreduce.job.queuename does not work at 'Convert Cuboid Data to HFile' Step
* [KYLIN-3451] - Cloned cube doesn't have Mandatory Cuboids copied
* [KYLIN-3456] - Cube level's snapshot config does not work
* [KYLIN-3460] - {fn CURRENT_DATE()} parse error
* [KYLIN-3461] - "metastore.sh refresh-cube-signature" not updating cube signature as expected
* [KYLIN-3476] - Fix TupleExpression verification when parsing sql
* [KYLIN-3492] - Wrong constant value in KylinConfigBase.getDefaultVarcharPrecision
* [KYLIN-3500] - kylin 2.4 use jdbc datasource :Unknown column 'A.A.CRT_DATE' in 'where clause'
* [KYLIN-3505] - DataType.getType wrong usage of cache
