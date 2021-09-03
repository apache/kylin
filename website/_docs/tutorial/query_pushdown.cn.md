---
layout: docs-cn
title:  查询下压
categories: tutorial
permalink: /cn/docs/tutorial/query_pushdown.html
since: v4.0.0
---

### Kylin 支持查询下压

对于没有cube能查得结果的sql，Kylin4.0 支持将这类查询下压至 SparkSql 去查询 Hive 源数据。

### 开启查询下压

```
kylin.query.pushdown.runner-class-name=org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl
```

### 进行查询下压

开启查询下压后，即可按同步的表进行灵活查询，而无需根据查询构建对应Cube。

   ![](/images/tutorial/2.1/push_down/push_down_1.png)

用户在提交查询时，若查询下压发挥作用，则在log里有相应的记录。

   ![](/images/tutorial/2.1/push_down/push_down_2.png)
   