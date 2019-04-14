---
layout: docs24
title:  Use distributed job scheduler
categories: howto
permalink: /docs24/howto/howto_use_distributed_scheduler.html
---

Since Kylin 2.0, Kylin support distributed job scheduler.
Which is more extensible, available and reliable than default job scheduler.
To enable the distributed job scheduler, you need to set or update three configs in the kylin.properties:

```
1. kylin.job.scheduler.default=2
2. kylin.job.lock=org.apache.kylin.storage.hbase.util.ZookeeperDistributedJobLock
3. add all job servers and query servers to the kylin.server.cluster-servers
```
