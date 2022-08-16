---
title: Query Cache Settings
language: en
sidebar_label: Query Cache Settings
pagination_label: Query Cache Settings
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - query cache settings
draft: false
last_update:
    date: 08/16/2022
---

By default, Kylin enables query cache in each process to improve query performance.

> **Note**: In order to ensure data consistency, query cache is not available in pushdown.


###Use Default Cache

Kylin enables query cache by default at each node/process level. The configuration details are described  below. You can change them in `$KYLIN_HOME/conf/kylin.properties` under Kylin installation directory.

> **Caution:** Must restart for any configurations to take effect. 

| Properties                | Description                                                  | Default | Options |
| ------------------------- | ------------------------------------------------------------ | ------- | ------- |
| kylin.query.cache-enabled | Whether to enable query cache. When this property is enabled, the following properties take effect. | true    | false   |


### Query Cache Criteria
Kylin doesn't cache the query result of all SQL queries by default (because the memory resource might be limited). It only caches slow queries and the result size is appropriate. The criterion are configured by the following parameters. 
The query that satisfies any one of the No.1, No.2, No.3 configuration and also satisfies No.4 configuration will be cached.

|No |  Properties                         | Description                                                  | Default        | Default unit |
| ----| ---------------------------------- | ------------------------------------------------------------ | -------------- | ------- |
| 1|kylin.query.cache-threshold-duration          | Queries whose duration is above this value | 2000           | millisecond |
| 2|kylin.query.cache-threshold-scan-count          | Queries whose scan row count is above this value | 10240           | row |
| 3|kylin.query.cache-threshold-scan-bytes          | Queries whose scan bytes is above this value | 1048576           | byte |
| 4|kylin.query.large-query-threshold          | Queries whose result set size is below this value  | 1000000           | cell |

### Ehcache Cache Configuration

By default, Kylin uses Ehcache as the query cache. You can configure Ehcache to control the query cache size and policy. You can replace the default query cache configuration by modifying the following configuration item. For more Ehcache configuration items, please refer to the official website [ehcache documentation](https://www.ehcache.org/generated/2.9.0/html/ehc-all/#page/Ehcache_Documentation_Set%2Fehcache_all.1.017.html%23).

| Properties | Description | Default |
| ----- | ---- | ----- |
| kylin.cache.config | The path to ehcache.xml. To replace the default query cache configuration file, you can create a new file `xml`, for exemple `ekcache2.xml`, in the directory  `${KYLIN_HOME}/conf/`, and modify the value of this configuration item: `file://${KYLIN_HOME}/conf/ehcache2.xml` | classpath:ehcache.xml |


### Redis Cache Configuration

The default query cache cannot be shared among different nodes or processes because it is process level. Because of this,  when subsequent and same queries are routed to different Kylin nodes, the cache of the first query result cannot be used in cluster deployment mode. Therefore, you can configure Redis cluster as distributed cache, which can be shared across all Kylin nodes. The detail configurations are described as below:
(Redis 5.0 or 5.0.5 is recommended.)

| Properties                         | Description                                                  | Default        | Options |
| ---------------------------------- | ------------------------------------------------------------ | -------------- | ------- |
| kylin.cache.redis.enabled          | Whether to enable query cache by using Redis cluster.         | false          | true    |
| kylin.cache.redis.cluster-enabled  | Whether to enable Redis cluster mode.                         | false          | true    |
| kylin.cache.redis.hosts             | Redis host. If you need to connect to a Redis cluster, please use comma to split the hosts, such as, kylin.cache.redis.hosts=localhost:6379,localhost:6380 | localhost:6379 |         |
| kylin.cache.redis.expire-time-unit | Time unit for cache period. EX means seconds and PX means milliseconds. | EX             | PX      |
| kylin.cache.redis.expire-time      | Valid cache period.                                           | 86400          |         |
| kylin.cache.redis.reconnection.enabled | Whether to enable redis reconnection when cache degrades to ehcache | true | false |
| kylin.cache.redis.reconnection.interval | Automatic reconnection interval, in minutes | 60 | |
| kylin.cache.redis.password | Redis password | | |

#### Limitation
Due to metadata inconsistency between Query nodes and All/Job nodes, the redis cache swith `kylin.cache.redis.enabled=true` should be configured along with `kylin.server.store-type=jdbc`.

> **Caution:** Redis passwords can be encrypted, please refer to: [Use MySQL as Metastore](../deployment/rdbms_metastore/mysql/mysql_metastore.md)
