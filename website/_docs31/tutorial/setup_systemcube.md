---
layout: docs31
title:  Set Up System Cube
categories: tutorial
permalink: /docs31/tutorial/setup_systemcube.html
---

> Available since Apache Kylin v2.3.0

## What is System Cube

For better supporting self-monitoring, a set of system Cubes are created under the system project, called "KYLIN_SYSTEM". Currently, there are five Cubes. Three are for query metrics, "METRICS_QUERY", "METRICS_QUERY_CUBE", "METRICS_QUERY_RPC". And the other two are for job metrics, "METRICS_JOB", "METRICS_JOB_EXCEPTION".

## How to Set Up System Cube

### Prepare
Create a configuration file SCSinkTools.json in KYLIN_HOME directory.

For example:

```
[
  [
    "org.apache.kylin.tool.metrics.systemcube.util.HiveSinkTool",
    {
      "storage_type": 2,
      "cube_desc_override_properties": [
        "java.util.HashMap",
        {
          "kylin.cube.algorithm": "INMEM",
          "kylin.cube.max-building-segments": "1"
        }
      ]
    }
  ]
]
```

### 1. Generate Metadata
Run the following command in KYLIN_HOME folder to generate related metadata:

```
./bin/kylin.sh org.apache.kylin.tool.metrics.systemcube.SCCreator \
-inputConfig SCSinkTools.json \
-output <output_folder>
```

By this command, the related metadata will be generated and its location is under the directory `<output_folder>`. The details are as follows, system_cube is our `<output_folder>`:

![metadata](/images/SystemCube/metadata.png)

### 2. Set Up Datasource
Running the following command to create source hive tables:

```
hive -f <output_folder>/create_hive_tables_for_system_cubes.sql
```

By this command, the related hive table will be created.

![hive_table](/images/SystemCube/hive_table.png)

### 3. Upload Metadata for System Cubes
Then we need to upload metadata to hbase by the following command:

```
./bin/metastore.sh restore <output_folder>
```

### 4. Reload Metadata
Finally, we need to reload metadata in Kylin web UI.


Then, a set of system Cubes will be created under the system project, called "KYLIN_SYSTEM".


### 5. System Cube build
When the system Cube is created, we need to build the Cube regularly.

1. Create a shell script that builds the system Cube by calling org.apache.kylin.tool.job.CubeBuildingCLI
  
	For example:

{% highlight Groff markup %}
#!/bin/bash

dir=$(dirname ${0})
export KYLIN_HOME=${dir}/../

CUBE=$1
INTERVAL=$2
DELAY=$3
CURRENT_TIME_IN_SECOND=`date +%s`
CURRENT_TIME=$((CURRENT_TIME_IN_SECOND * 1000))
END_TIME=$((CURRENT_TIME-DELAY))
END=$((END_TIME - END_TIME%INTERVAL))

ID="$END"
echo "building for ${CUBE}_${ID}" >> ${KYLIN_HOME}/logs/build_trace.log
sh ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.job.CubeBuildingCLI --cube ${CUBE} --endTime ${END} > ${KYLIN_HOME}/logs/system_cube_${CUBE}_${END}.log 2>&1 &

{% endhighlight %}

2. Then run this shell script regularly

	For example, add a cron job as follows:

{% highlight Groff markup %}
0 */2 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_QUERY_QA 3600000 1200000

20 */2 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_QUERY_CUBE_QA 3600000 1200000

40 */4 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_QUERY_RPC_QA 3600000 1200000

30 */4 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_JOB_QA 3600000 1200000

50 */12 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_JOB_EXCEPTION_QA 3600000 12000

{% endhighlight %}

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
This Cube is for collecting query metrics at the highest level. The details are as follows:

<table>
  <tr>
    <th colspan="2">Dimension</th>
  </tr>
  <tr>
    <td>HOST</td>
    <td>the host of server for query engine</td>
  </tr>
  <tr>
    <td>PROJECT</td>
    <td></td>
  </tr>
  <tr>
    <td>REALIZATION</td>
    <td>in Kylin, there are two OLAP realizations: Cube, or Hybrid of Cubes</td>
  </tr>
  <tr>
    <td>REALIZATION_TYPE</td>
    <td></td>
  </tr>
  <tr>
    <td>QUERY_TYPE</td>
    <td>users can query on different data sources, CACHE, OLAP, LOOKUP_TABLE, HIVE</td>
  </tr>
  <tr>
    <td>EXCEPTION</td>
    <td>when doing query, exceptions may happen. It's for classifying different exception types</td>
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
    <td>MIN, MAX, SUM of QUERY_TIME_COST</td>
    <td>the time cost for the whole query</td>
  </tr>
  <tr>
    <td>MAX, SUM of CALCITE_SIZE_RETURN</td>
    <td>the row count of the result Calcite returns</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_SIZE_RETURN</td>
    <td>the row count of the input to Calcite</td>
  </tr>
  <tr>
    <td>MAX, SUM of CALCITE_SIZE_AGGREGATE_FILTER</td>
    <td>the row count of Calcite aggregates and filters</td>
  </tr>
  <tr>
    <td>COUNT DISTINCT of QUERY_HASH_CODE</td>
    <td>the number of different queries</td>
  </tr>
</table>

### METRICS_QUERY_RPC
This Cube is for collecting query metrics at the lowest level. For a query, the related aggregation and filter can be pushed down to each rpc target server. The robustness of rpc target servers is the foundation for better serving queries. The details are as follows:

<table>
  <tr>
    <th colspan="2">Dimension</th>
  </tr>
  <tr>
    <td>HOST</td>
    <td>the host of server for query engine</td>
  </tr>
  <tr>
    <td>PROJECT</td>
    <td></td>
  </tr>
  <tr>
    <td>REALIZATION</td>
    <td></td>
  </tr>
  <tr>
    <td>RPC_SERVER</td>
    <td>the rpc related target server</td>
  </tr>
  <tr>
    <td>EXCEPTION</td>
    <td>the exception of a rpc call. If no exception, "NULL" is used</td>
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
    <td>MAX, SUM of CALL_TIME</td>
    <td>the time cost of a rpc all</td>
  </tr>
  <tr>
    <td>MAX, SUM of COUNT_SKIP</td>
    <td>based on fuzzy filters or else, a few rows will be skipped. This indicates the skipped row count</td>
  </tr>
  <tr>
    <td>MAX, SUM of SIZE_SCAN</td>
    <td>the row count actually scanned</td>
  </tr>
  <tr>
    <td>MAX, SUM of SIZE_RETURN</td>
    <td>the row count actually returned</td>
  </tr>
  <tr>
    <td>MAX, SUM of SIZE_AGGREGATE</td>
    <td>the row count actually aggregated</td>
  </tr>
  <tr>
    <td>MAX, SUM of SIZE_AGGREGATE_FILTER</td>
    <td>the row count actually aggregated and filtered, = SIZE_SCAN - SIZE_RETURN</td>
  </tr>
</table>

### METRICS_QUERY_CUBE
This Cube is for collecting query metrics at the Cube level. The most important are cuboids related, which will serve for Cube planner. The details are as follows:

<table>
  <tr>
    <th colspan="2">Dimension</th>
  </tr>
  <tr>
    <td>CUBE_NAME</td>
    <td></td>
  </tr>
  <tr>
    <td>CUBOID_SOURCE</td>
    <td>source cuboid parsed based on query and Cube design</td>
  </tr>
  <tr>
    <td>CUBOID_TARGET</td>
    <td>target cuboid already pre-calculated and served for source cuboid</td>
  </tr>
  <tr>
    <td>IF_MATCH</td>
    <td>whether source cuboid and target cuboid are equal</td>
  </tr>
  <tr>
    <td>IF_SUCCESS</td>
    <td>whether a query on this Cube is successful or not</td>
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
    <td>MAX, SUM of STORAGE_CALL_COUNT</td>
    <td>the number of rpc calls for a query hit on this Cube</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_CALL_TIME_SUM</td>
    <td>sum of time cost for the rpc calls of a query</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_CALL_TIME_MAX</td>
    <td>max of time cost among the rpc calls of a query</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_COUNT_SKIP</td>
    <td>the sum of row count skipped for the related rpc calls</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_SIZE_SCAN</td>
    <td>the sum of row count scanned for the related rpc calls</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_SIZE_RETURN</td>
    <td>the sum of row count returned for the related rpc calls</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_SIZE_AGGREGATE</td>
    <td>the sum of row count aggregated for the related rpc calls</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_SIZE_AGGREGATE_FILTER</td>
    <td>the sum of row count aggregated and filtered for the related rpc calls, = STORAGE_SIZE_SCAN - STORAGE_SIZE_RETURN</td>
  </tr>
</table>

### METRICS_JOB
In Kylin, there are mainly three types of job:
- "BUILD", for building Cube segments from **HIVE**.
- "MERGE", for merging Cube segments in **HBASE**.
- "OPTIMIZE", for dynamically adjusting the precalculated cuboid tree base on the **base cuboid** in **HBASE**.

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
