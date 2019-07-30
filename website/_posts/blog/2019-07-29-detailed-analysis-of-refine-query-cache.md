---
layout: post-blog
title:  Detailed Analysis of refine query cache
date:   2019-07-30 10:30:00
author: Xiaoxiang Yu
categories: blog
---

----

## Part-I Basic Introduction

### Backgroud
In the past, query cache are not efficiently used in Kylin due to two aspects: **coarse-grained cache expiration strategy** and **lack of external cache**. Because of the aggressive cache expiration strategy, useful caches are often cleaned up unnecessarily. Because query caches are stored in local servers, they cannot be shared between servers. And because of the size limitation of local cache, not all useful query results can be cached.

To deal with these shortcomings, we change the query cache expiration strategy by signature checking and introduce the memcached as Kylin's distributed cache so that Kylin servers are able to share cache between servers. And it's easy to add memcached servers to scale out distributed cache.

These features is proposed and developed by eBay Kylin team. Thanks so much for their contribution.

### Related JIRA

- [KYLIN-2895 Refine Query Cache](https://issues.apache.org/jira/browse/KYLIN-2895)
    - [KYLIN-2899 Introduce segment level query cache](https://issues.apache.org/jira/browse/KYLIN-2899)
    - [KYLIN-2898 Introduce memcached as a distributed cache for queries](https://issues.apache.org/jira/browse/KYLIN-2898)
    - [KYLIN-2894 Change the query cache expiration strategy by signature checking](https://issues.apache.org/jira/browse/KYLIN-2894)
    - [KYLIN-2897 Improve the query execution for a set of duplicate queries in a short period](https://issues.apache.org/jira/browse/KYLIN-2897)
    - [KYLIN-2896 Refine query exception cache](https://issues.apache.org/jira/browse/KYLIN-2896)

----

## Part-II Deep Dive

- Introduce memcached as a Distributed Query Cache
- Segment Level Cache
- Query Cache Expiration Strategy by Signature Checking
- Other Enhancement

### Introduce memcached as a Distributed Query Cache

**Memcached** is a Free and open source, high-performance, distributed memory object caching system. It is an in-memory key-value store for small chunks of arbitrary data (strings, objects) from results of database calls, API calls, or page rendering. It is simple yet powerful. Its simple design promotes quick deployment, ease of development, and solves many problems facing large data caches. Its API is available for most popular languages.

By KYLIN-2898, Kylin use **Memcached** as distributed cache service, and use **EhCache** as local cache service. When `RemoteLocalFailOverCacheManager` is configured in `applicationContext.xml`, for each cache put/get action, Kylin will first check if remote cache service is available, only if remote cache service is unavailable, local cache service will be used.

Firstly, multi query server can share query cache. For each kylin server, less jvm memory will be occupied which help to reduce GC pressure. Secondly, since memcached is centralized so duplicated cache entry will avoid in serval Kylin process. Thirdly, memcached has larger size and easy to scale out, this will help to reduce the chance which useful cache entry have to be dropped due to limited memory capacity.

To handle node failure and to scale out memcached cluster, author has introduced a consistent hash strategy to smoothly solve such problem. Ketama is an implementation of a consistent hashing algorithm, meaning you can add or remove servers from the memcached pool without causing a complete remap of all keys. Detail could be checked at [Ketama consistent hash strategy](https://www.last.fm/user/RJ/journal/2007/04/10/rz_libketama_-_a_consistent_hashing_algo_for_memcache_clients).

![consistent hashing](/images/blog/refine-query-cache/consistent-hashing.png)

### Segment level Cache

Currently Kylin use sql as the cache key, when sql comes, if result exists in the cache, it will directly returned the cached result and don't need to query hbase. When there is new segment build or existing segment refresh, all related cache result need to be evicted. For some frequently build cube such as streaming cube(NRT Streaming or Real-time OLAP), the cache miss will increase dramatically, that may decrease the query performance.

Since for Kylin cube, most historical segments are immutable, the same query against historical segments should be always same, don't need to be evicted for new segment building. So we decide to implement the segment level cache, it is a complement of the existing front-end cache, the idea is similar as the level1/level2 cache in operating system.

![l1-l2-cache](/images/blog/refine-query-cache/l1-l2-cache.png)

### Query Cache Expiration Strategy by Signature Checking

Currently, to invalid query cache, `CacheService` will either invoke `cleanDataCache` or `cleanAllDataCache`. Both methods will clear all of the query cache , which is very inefficient and unnecessary. In production environment, there's around hundreds of cubing jobs per day, which means the query cache will be cleared very several minutes. Then we introduced a signature to upgrade cache invalidation strategy.

The basic idea is as follows:
When put SQLResponse into cache, we add signature for each SQLResponse. To calculate signature for SQLResponse, we choose the cube last build time and its segments to as input of `SignatureCalculator`.
When fetch `SQLResponse` for cache, first check whether the signature is consistent. If not, this cached value is overdue and will be invalidate.

As for the calculation of signature is show as follows:
1. `toString` of `ComponentSignature` will concatenate member varible into a large String; if a `ComponentSignature` has other `ComponentSignature` as member, toString will be calculated recursively
2. return value of `toString` will be input of `SignatureCalculator`,
`SignatureCalculator` encode string using MD5 as identifer of signature of query cache

![cache-signature](/images/blog/refine-query-cache/cache-signature.png)

### Other Enhancement

#### Improve the query execution for a set of duplicate queries in a short period

If same query enter Kylin at the same time by different client, for each query they can not find query cache so they must be calculated respectively. And even wrose, if these query are complex, they usually cost a long duration so Kylin have less chance to utilize cache query; and them cost large computation resources that will make query server has poor performance has harm to HBase cluster.

To reduce the impact of duplicated and complex query, it may be a good idea to block query which came later, wait to first one return result as far as possible. This lazy strategy is especially useful if you have duplicated complex query came in same time. To enbale it, you should set `kylin.query.lazy-query-enabled` to `true`. Optionlly, you may set `kylin.query.lazy-query-waiting-timeout-milliseconds` to what you think later duplicated query wait duration to meet your situation.


#### Remove exception cache
Formerly, query cache has been divided into two part, one part for storing success query result, another for failed query result, and they are invalidated respectively. It looks like not a good classification criteria because it is not fine-grained enough. After query cache signature was introduced, we have no reason to take them apart, so exception cache was removed.

----

## Part-III How to Use

To get prepared, you need to install memcached, you may refer to https://github.com/memcached/memcached/wiki/Install. Then you should modify `kylin.properties` and `applicationContext.xml`.

- kylin.properties
{% highlight Groff markup %}
kylin.cache.memcached.hosts=10.1.2.42:11211
kylin.query.cache-signature-enabled=true
kylin.query.lazy-query-enabled=true
kylin.metrics.memcached.enabled=true
kylin.query.segment-cache-enabled=true
{% endhighlight %}

- applicationContext.xml
{% highlight Groff markup %}

<cache:annotation-driven/>

<bean id="ehcache" class="org.springframework.cache.ehcache.EhCacheManagerFactoryBean"
      p:configLocation="classpath:ehcache-test.xml" p:shared="true"/>

<bean id="remoteCacheManager" class="org.apache.kylin.cache.cachemanager.MemcachedCacheManager"/>
<bean id="localCacheManager" class="org.apache.kylin.cache.cachemanager.InstrumentedEhCacheCacheManager"
      p:cacheManager-ref="ehcache"/>
<bean id="cacheManager" class="org.apache.kylin.cache.cachemanager.RemoteLocalFailOverCacheManager"/>

<bean id="memcachedCacheConfig" class="org.apache.kylin.cache.memcached.MemcachedCacheConfig">
    <property name="timeout" value="500"/>
    <property name="hosts" value="${kylin.cache.memcached.hosts}"/>
</bean>
{% endhighlight %}

### Configuration for query cache

#### General part

|Conf Key | Conf value| Explanation|
|:------------ | :-------------| :---|
|kylin.query.cache-enabled|boolean, default true|whether to enable query cache|
|kylin.query.cache-threshold-duration| long, in milliseconds, default is 2000|query duration threshold|
|kylin.query.cache-threshold-scan-count| long, default is 10240|query scan row count threshold|
|kylin.query.cache-threshold-scan-bytes| long, default is 1024 * 1024 (1MB)|query scan byte threshold|

#### Memcached part

|Conf Key | Conf value| Explanation|
|:------------ | :------------- | :--- |
|kylin.cache.memcached.hosts | host1:port1,host2:port2|host list of memcached host|
|kylin.query.segment-cache-enabled|default false|wether to enable|
|kylin.query.segment-cache-timeout|default 2000| timeout of memcached|
|kylin.query.segment-cache-max-size|200 (MB) |max size put into memcached|

#### Cache signature part

|Conf Key | Conf value| Explanation|
| :------------ | :------------- | :--- |
|kylin.query.cache-signature-enabled| default false| whether to use signature for query cache|
|kylin.query.signature-class|default is org.apache.kylin.rest.signature.FactTableRealizationSetCalculator| use which class to calculate signature of query cache|

#### Other optimize part

| Conf Key | Conf value| Explanation|
| :------------ | :------------- | :--- |
|kylin.query.lazy-query-enabled|default false| whether to block duplicated sql query|
|kylin.query.lazy-query-waiting-timeout-milliseconds|long , in milliseconds, default is 60000| max druation for blocking duplicated sql query|

#### Metrics part

|           Conf Key                |       Conf value     |       Explanation      |
| :---------------------------      | :--------------------| :-------------------- |
|kylin.metrics.memcached.enabled    |true    |Enable memcached metrics in memcached.|
|kylin.metrics.memcached.metricstype|off/performance/debug|refer to net.spy.memcached.metrics.MetricType|
