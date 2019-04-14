---
layout: docs24
title:  Quick Start with Sample Cube
categories: tutorial
permalink: /docs24/tutorial/kylin_sample.html
---

Kylin provides a script for you to create a sample Cube; the script will also create five sample hive tables:

1. Run ${KYLIN_HOME}/bin/sample.sh ; Restart kylin server to flush the caches;
2. Logon Kylin web with default user ADMIN/KYLIN, select project "learn_kylin" in the project dropdown list (left upper corner);
3. Select the sample cube "kylin_sales_cube", click "Actions" -> "Build", pick up a date later than 2014-01-01 (to cover all 10000 sample records);
4. Check the build progress in "Monitor" tab, until 100%;
5. Execute SQLs in the "Insight" tab, for example:
	select part_dt, sum(price) as total_selled, count(distinct seller_id) as sellers from kylin_sales group by part_dt order by part_dt
6. You can verify the query result and compare the response time with hive;

   
## Quick Start with Streaming Sample Cube

Kylin provides a script for streaming sample cube also. This script will create Kafka topic and send the random messages constantly to the generated topic.

1. Export KAFKA_HOME first, and start Kylin.
2. Run ${KYLIN_HOME}/bin/sample.sh, it will generate Table DEFAULT.KYLIN_STREAMING_TABLE, Model kylin_streaming_model, Cube kylin_streaming_cube in learn_kylin project.
3. Run ${KYLIN_HOME}/bin/sample-streaming.sh, it will create Kafka Topic kylin_streaming_topic into the localhost:9092 broker. It also send the random 100 messages into kylin_streaming_topic per second.
4. Follow the the standard cube build process, and trigger the Cube kylin_streaming_cube build.  
5. Check the build process in "Monitor" tab, until at least one job is 100%.
6. Execute SQLs in the "Insight" tab, for example:
         select count(*), HOUR_START from kylin_streaming_table group by HOUR_START
7. Verify the query result.
 
## What's next

You can create another cube with the sample tables, by following the tutorials.
