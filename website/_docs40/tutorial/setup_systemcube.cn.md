---
layout: docs40-cn
title:  建立系统 Cube
categories: tutorial
permalink: /cn/docs40/tutorial/setup_systemcube.html
---


## 什么是系统 Cube

为了更好的支持自我监控，在系统 project 下创建一组系统 Cubes，叫做 "KYLIN_SYSTEM"。现在，这里有五个 Cubes。三个用于查询指标，"METRICS_QUERY_EXECUTION"，"METRICS_QUERY_SPARK_JOB"，"METRICS_QUERY_SPARK_STAGE"。另外两个是 job 指标，"METRICS_JOB"，"METRICS_JOB_EXCEPTION"。

## 如何建立系统 Cube
请查看文档：[How to use System Cube in Kylin4.0](https://cwiki.apache.org/confluence/display/KYLIN/How+to+use+System+Cube+in+Kylin+4)

## 系统 Cube 的细节

### 普通 Dimension
对于这些 Cube，admins 能够用四个时间粒度查询。从高级别到低级别，如下：

<table>
  <tr>
    <td>KYEAR_BEGIN_DATE</td>
    <td>year</td>
  </tr>
  <tr>
    <td>KMONTH_BEGIN_DATE</td>
    <td>month</td>
  </tr>
  <tr>
    <td>KWEEK_BEGIN_DATE</td>
    <td>week</td>
  </tr>
  <tr>
    <td>KDAY_DATE</td>
    <td>date</td>
  </tr>
</table>

### METRICS_QUERY_EXECUTION
这个 Cube 用于在 Spark Execution 级别收集查询 metrics。在 kylin4.0 中，每一条查询对应一个 Spark Execution。


### METRICS_QUERY_SPARK_JOB
这个 Cube 用于收集 Spark Job 级别的查询 metrics。

### METRICS_QUERY_SPARK_STAGE
这个 Cube 用于收集 Spark Stage 级别的查询 metrics。


### METRICS_JOB
在 Kylin 中，主要有三种类型的 job：
- "BUILD"，为了从 **HIVE** 中 building Cube segments。
- "MERGE"，为了在存储引擎中 merging Cube segments。
- "OPTIMIZE"，为了在存储引擎中基于 **base cuboid** 动态调整预计算 cuboid tree。

这个 Cube 是用来收集 job 指标。细节如下：

<table>
  <tr>
    <th colspan="2">Dimension</th>
  </tr>
  <tr>
    <td>PROJECT</td>
    <td></td>
  </tr>
  <tr>
    <td>CUBE_NAME</td>
    <td></td>
  </tr>
  <tr>
    <td>JOB_TYPE</td>
    <td></td>
  </tr>
  <tr>
    <td>CUBING_TYPE</td>
    <td>in kylin，there are two cubing algorithms，Layered & Fast(InMemory)</td>
  </tr>
</table>

<table>
  <tr>
    <th colspan="2">Measure</th>
  </tr>
  <tr>
    <td>COUNT</td>
    <td></td>
  </tr>
  <tr>
    <td>MIN，MAX，SUM of DURATION</td>
    <td>the duration from a job start to finish</td>
  </tr>
  <tr>
    <td>MIN，MAX，SUM of TABLE_SIZE</td>
    <td>the size of data source in bytes</td>
  </tr>
  <tr>
    <td>MIN，MAX，SUM of CUBE_SIZE</td>
    <td>the size of created Cube segment in bytes</td>
  </tr>
  <tr>
    <td>MIN，MAX，SUM of PER_BYTES_TIME_COST</td>
    <td>= DURATION / TABLE_SIZE</td>
  </tr>
  <tr>
    <td>MIN，MAX，SUM of WAIT_RESOURCE_TIME</td>
    <td>a job may includes serveral MR(map reduce) jobs. Those MR jobs may wait because of lack of Hadoop resources.</td>
  </tr>
</table>

### METRICS_JOB_EXCEPTION
这个 Cube 是用来收集 job exception 指标。细节如下：

<table>
  <tr>
    <th colspan="2">Dimension</th>
  </tr>
  <tr>
    <td>PROJECT</td>
    <td></td>
  </tr>
  <tr>
    <td>CUBE_NAME</td>
    <td></td>
  </tr>
  <tr>
    <td>JOB_TYPE</td>
    <td></td>
  </tr>
  <tr>
    <td>CUBING_TYPE</td>
    <td></td>
  </tr>
  <tr>
    <td>EXCEPTION</td>
    <td>when running a job，exceptions may happen. It's for classifying different exception types</td>
  </tr>
</table>

<table>
  <tr>
    <th>Measure</th>
  </tr>
  <tr>
    <td>COUNT</td>
  </tr>
</table>
