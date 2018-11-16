---
layout: docs
title:  Kylin Release 2.3.1
categories: releases
permalink: /docs/releases/kylin_release_2_3_1.html
---

_Tag:_ [kylin-2.3.1](https://github.com/apache/kylin/tree/kylin-2.3.1)

This is a bug fix release after 2.3.0, with 12 bug fixes and enhancement.



### Improvement

* [KYLIN-3233] - CacheController can not handle if cacheKey has "/"
* [KYLIN-3278] - Kylin should not distribute hive table by random at Step1
* [KYLIN-3300] - Upgrade jackson-databind to 2.6.7.1 with security issue fixed
* [KYLIN-3301] - Upgrade opensaml to 2.6.6 with security issue fixed



### Bug fix

* [KYLIN-3270] - Fix the blocking issue in Cube optimizing job
* [KYLIN-3276] - Fix the query cache bug with dynamic parameter
* [KYLIN-3288] - "Sqoop To Flat Hive Table" step should specify "mapreduce.queue.name"
* [KYLIN-3306] - Fix the rarely happened unit test exception of generic algorithm
* [KYLIN-3287] - When a shard by column is in dict encoding, dict building error.
* [KYLIN-3280] - The delete button should not be enabled without any segment in cube segment delete confirm dialog
* [KYLIN-3119] - A few bugs in the function 'massageSql' of 'QueryUtil.java'
* [KYLIN-3236] - The function 'reGenerateAdvancedDict()' has an error logical judgment, which will cause an exception when you edit the cube.
