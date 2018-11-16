---
layout: docs
title:  Kylin Release 2.3.2
categories: releases
permalink: /docs/releases/kylin_release_2_3_2.html
---

_Tag:_ [kylin-2.3.2](https://github.com/apache/kylin/tree/kylin-2.3.2)


This is a bug fix release after 2.3.1, with 12 bug fixes and enhancement.


### Improvement

* [KYLIN-3345] - Use Apache Parent POM 19
* [KYLIN-3372] - Upgrade jackson-databind version due to security concerns
* [KYLIN-3415] - Remove "external" module



### Bug fix

* [KYLIN-3115] - Incompatible RowKeySplitter initialize between build and merge job
* [KYLIN-3336] - java.lang.NoSuchMethodException: org.apache.kylin.tool.HBaseUsageExtractor.execute([Ljava.lang.String;)
* [KYLIN-3348] - "missing LastBuildJobID" error when building new cube segment
* [KYLIN-3352] - Segment pruning bug, e.g. date_col > "max_date+1"
* [KYLIN-3363] - Wrong partition condition appended in JDBC Source
* [KYLIN-3388] - Data may become not correct if mappers fail during the redistribute step, "distribute by rand()"
* [KYLIN-3400] - WipeCache and createCubeDesc causes deadlock
* [KYLIN-3401] - The current using zip compress tool has an arbitrary file write vulnerability
* [KYLIN-3404] - Last optimized time detail was not showing after cube optimization
