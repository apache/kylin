---
layout: docs31-cn
title:  "样例 Cube 快速入门"
categories: tutorial
permalink: /cn/docs31/tutorial/kylin_sample.html
---

Kylin 提供了一个创建样例 Cube 脚本；脚本会创建五个样例 Hive 表:

1. 运行 `${KYLIN_HOME}/bin/sample.sh`；重启 Kylin 服务器刷新缓存;
2. 用默认的用户名和密码 ADMIN/KYLIN 登陆 Kylin 网站，选择 project 下拉框（左上角）中的 `learn_kylin` 工程;
3. 选择名为 `kylin_sales_cube` 的样例 Cube，点击 "Actions" -> "Build"，选择一个在 2014-01-01 之后的日期（覆盖所有的 10000 样例记录);
4. 点击 "Monitor" 标签，查看 build 进度直至 100%;
5. 点击 "Insight" 标签，执行 SQLs，例如:

```
select part_dt, sum(price) as total_sold, count(distinct seller_id) as sellers from kylin_sales group by part_dt order by part_dt
```

 6.您可以验证查询结果且与 Hive 的响应时间进行比较;

   
## Streaming 样例 Cube 快速入门

Kylin 也提供了 streaming 样例 Cube 脚本。该脚本将会创建 Kafka topic 且不断的向生成的 topic 发送随机 messages。

1. 首先设置 KAFKA_HOME，然后启动 Kylin。
2. 运行 `${KYLIN_HOME}/bin/sample.sh`，它会在 `learn_kylin` 工程中生成 `DEFAULT.KYLIN_STREAMING_TABLE` 表，`kylin_streaming_model` 模型和 `kylin_streaming_cube` Cube。
3. 运行 `${KYLIN_HOME}/bin/sample-streaming.sh`，它会在 localhost:9092 broker 中创建名为 `kylin_streaming_topic` 的 Kafka Topic。它也会每秒随机发送 100 条 messages 到 `kylin_streaming_topic`。
4. 遵循标准 Cube build 过程，并触发名为 `kylin_streaming_cube` 的 Cube 的构建。  
5. 点击 "Monitor" 标签，查看 build 进度。直至至少有一个 job 达到 100%。
6. 点击 "Insight" 标签，执行 SQLs，例如:

```
select count(*), HOUR_START from kylin_streaming_table group by HOUR_START
```

 7.验证查询结果。
 
## 下一步干什么

您可以通过接下来的教程用同一张表创建另一个 Cube。
