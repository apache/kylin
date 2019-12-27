---
layout: docs31
title:  Quick Start with Sample Cube
categories: tutorial
permalink: /docs31/tutorial/kylin_sample.html
---

Kylin provides a script for you to create a sample Cube; the script will also create five sample Hive tables:

1. Run `${KYLIN_HOME}/bin/sample.sh`; Restart Kylin server to flush the caches;
2. Logon Kylin web with default user and password ADMIN/KYLIN, select project `learn_kylin` in the project dropdown list (left upper corner);
3. Select the sample Cube `kylin_sales_cube`, click "Actions" -> "Build", pick up a date later than 2014-01-01 (to cover all 10000 sample records);
4. Check the build progress in the "Monitor" tab, until 100%;
5. Execute SQLs in the "Insight" tab, for example:

```
select part_dt, sum(price) as total_sold, count(distinct seller_id) as sellers from kylin_sales group by part_dt order by part_dt
```

 6.You can verify the query result and compare the response time with Hive;

   
## Quick Start with Streaming Sample Cube

Kylin provides a script for streaming sample Cube also. This script will create a Kafka topic and send random messages constantly to the generated topic.

1. Export KAFKA_HOME first, and start Kylin.
2. Run `${KYLIN_HOME}/bin/sample.sh`, it will generate Table `DEFAULT.KYLIN_STREAMING_TABLE`, Model `kylin_streaming_model`, Cube `kylin_streaming_cube` in `learn_kylin project`.
3. Run `${KYLIN_HOME}/bin/sample-streaming.sh`, it will create Kafka `Topic kylin_streaming_topic` into the localhost:9092 broker. It also sends the random 100 messages into `Kylin_streaming_topic` per second.
4. Follow the standard Cube build process, and trigger the Cube `kylin_streaming_cube` build.  
5. Check the build process in the "Monitor" tab, until at least one job is 100%.
6. Execute SQLs in the "Insight" tab, for example:

```
select count(*), HOUR_START from kylin_streaming_table group by HOUR_START
```

 7.Verify the query result.
 
## What's next

You can create another Cube with the sample tables, by following the tutorials.
