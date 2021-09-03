---
layout: docs
title:  Enable Query Pushdown
categories: tutorial
permalink: /docs/tutorial/query_pushdown.html
since: v2.1
---

### Introduction

If a query can not be answered by any cube, Kylin supports pushing down to SparkSql to query Hive data source.


### Enable Query Pushdown

```
kylin.query.pushdown.runner-class-name=org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl
```

### Do Query Pushdown

After Query Pushdown is configured, user is allowed to do flexible queries to the imported tables without available cubes.

   ![](/images/tutorial/2.1/push_down/push_down_1.png)

If query is answered by backup engine, `Is Query Push-Down` is set to `true` in the log.

   ![](/images/tutorial/2.1/push_down/push_down_2.png)
      
