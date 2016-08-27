---
layout: post-blog
title:  Query Metrics in Apache Kylin
date:   2016-08-27 17:30:00
author: Kaisen Kang
categories: blog
---

Apache Kylin support query metrics since 1.5.4. This blog will introduce why Kylin need query metrics, the concrete contents and meaning of query metrics, the daily function of query metrics and how to collect query metrics.

## Background
When Kylin become an enterprise application, you must ensure Kylin query service is high availability and high performance, besides, you need to provide commitment of the SLA of query service to users, Which need Kylin to support query metrics.

## Introduction
The query metrics have Server, Project, Cube three levels.

For example, `QueryCount` will have three kinds of metrics:
```
Hadoop:name=Server_Total,service=Kylin.QueryCount
Hadoop:name=learn_kylin,service=Kylin.QueryCount
Hadoop:name=learn_kylin,service=Kylin,sub=kylin_sales_cube.QueryCount

Server_Total is represent for a query server node,
learn_kylin is a project name,
kylin_sales_cube is a cube name.
```
### The Key Query Metrics

 - `QueryCount`: the total of query count.
 - `QueryFailCount`: the total of failed query count.
 - `QuerySuccessCount`: the total of successful query count.
 - `CacheHitCount`: the total of query cache hit count.
 - `QueryLatency60s99thPercentile`: the 99th percentile of query latency in the 60s.(there are 99th, 95th, 90th, 75th, 50th five percentiles and 60s, 360s, 3600s three time intervals in Kylin query metrics. the time intervals could set by `kylin.query.metrics.percentiles.intervals`, which default value is `60, 360, 3600`)
 - `QueryLatencyAvgTime`，`QueryLatencyIMaxTime`，`QueryLatencyIMinTime`: the average, max, min of query latency.
 - `ScanRowCount`: the rows count of scan HBase, it's like `QueryLatency`.
 - `ResultRowCount`: the result count of query, it's like `QueryLatency`.


## Daily Function
Besides providing SLA of query service to users, in the daily operation and maintenance, you could make Kylin query daily and Kylin query dashboard by query metrics. Which will help you know the rules, performance of Kylin query and analyze the Kylin query accident case.

## How To Use
Firstly, you should set config `kylin.query.metrics.enabled` as true to collect query metrics to JMX.

Secondly, you could use arbitrary JMX collection tool to collect the query metrics to your monitor system. Notice that, The query metrics have Server, Project, Cube three levels,  which was implemented by dynamic `ObjectName`, so you should get `ObjectName` by regular expression.
