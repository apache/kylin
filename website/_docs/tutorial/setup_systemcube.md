---
layout: docs
title:  Set Up System Cube
categories: tutorial
permalink: /docs/tutorial/setup_systemcube.html
---

## What is System Cube

For better supporting self-monitoring, a set of system Cubes are created under the system project, called "KYLIN_SYSTEM". Currently, there are five Cubes. Three are for query metrics, "METRICS_QUERY_EXECUTION", "METRICS_QUERY_SPARK_JOB", "METRICS_QUERY_SPARK_STAGE". And the other two are for job metrics, "METRICS_JOB", "METRICS_JOB_EXCEPTION".

## How to Set Up System Cube

Please check document: [How to use System Cube in Kylin4.0](https://cwiki.apache.org/confluence/display/KYLIN/How+to+use+System+Cube+in+Kylin+4)

## Details of System Cube

### Common Dimension
For all of these Cube, admins can query at four time granularities. From higher level to lower, it's as follows:

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

### METRICS_QUERY
This Cube is for collecting query metrics at the Spark Execution level.


### METRICS_QUERY_SPARK_JOB
This Cube is for collecting Spark Job level query metrics. 

### METRICS_QUERY_SPARK_STAGE
This Cube is for collecting Spark Stage level query metrics. 

### METRICS_JOB
In Kylin, there are mainly three types of job:
- "BUILD", for building Cube segments from **HIVE**.
- "MERGE", for merging Cube segments in storage engine.
- "OPTIMIZE", for dynamically adjusting the precalculated cuboid tree base on the **base cuboid** in storage engine.

This Cube is for collecting job metrics. The details are as follows:

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
    <td>in kylin, there are two cubing algorithms, Layered & Fast(InMemory)</td>
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
    <td>MIN, MAX, SUM of DURATION</td>
    <td>the duration from a job start to finish</td>
  </tr>
  <tr>
    <td>MIN, MAX, SUM of TABLE_SIZE</td>
    <td>the size of data source in bytes</td>
  </tr>
  <tr>
    <td>MIN, MAX, SUM of CUBE_SIZE</td>
    <td>the size of created Cube segment in bytes</td>
  </tr>
  <tr>
    <td>MIN, MAX, SUM of PER_BYTES_TIME_COST</td>
    <td>= DURATION / TABLE_SIZE</td>
  </tr>
  <tr>
    <td>MIN, MAX, SUM of WAIT_RESOURCE_TIME</td>
    <td>a job may includes several MR(map reduce) jobs. Those MR jobs may wait because of lack of Hadoop resources.</td>
  </tr>
</table>

### METRICS_JOB_EXCEPTION
This Cube is for collecting job exception metrics. The details are as follows:

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
    <td>when running a job, exceptions may happen. It's for classifying different exception types</td>
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
