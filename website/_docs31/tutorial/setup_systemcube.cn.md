---
layout: docs31-cn
title:  建立系统 Cube
categories: tutorial
permalink: /cn/docs31/tutorial/setup_systemcube.html
---

> 自 Apache Kylin v2.3.0 起有效

本节主要内容：

- [什么是系统 Cube](#什么是系统 Cube)
- [如何建立系统 Cube](#如何建立系统 Cube)
- [自动创建系统 Cube](#自动创建系统 Cube)
- [系统 Cube 的细节](#系统 Cube 的细节)

## <span id="什么是系统 Cube">什么是系统 Cube</span>

为了更好的支持自我监控，在系统 project 下创建一组系统 Cubes，叫做 "KYLIN_SYSTEM"。现在，这里有五个 Cubes。三个用于查询指标，"METRICS_QUERY"，"METRICS_QUERY_CUBE"，"METRICS_QUERY_RPC"。另外两个是 job 指标，"METRICS_JOB"，"METRICS_JOB_EXCEPTION"。

## <span id="如何建立系统 Cube">如何建立系统 Cube</span>

本节我们介绍手动启用系统 Cube 的方法，如果您希望通过 shell 脚本自动创建系统 Cube，请参考[自动创建系统 Cube](#自动创建系统 Cube)。

### 1. 准备

在 KYLIN_HOME 目录下创建一个配置文件 SCSinkTools.json。例如：

```
[
    {
       "sink": "hive",
       "storage_type": 2,
       "cube_desc_override_properties": {
         "kylin.cube.algorithm": "INMEM",
         "kylin.cube.max-building-segments": "1"
       }
    }
]
```

### 2. 生成 Metadata
在 KYLIN_HOME 文件夹下运行以下命令生成相关的 metadata：

```
./bin/kylin.sh org.apache.kylin.tool.metrics.systemcube.SCCreator \
-inputConfig SCSinkTools.json \
-output <output_forder>
```

通过这个命令，相关的 metadata 将会生成且其位置位于 `<output_forder>` 下。细节如下，system_cube 就是我们的 `<output_forder>`：

![metadata](/images/SystemCube/metadata.png)

### 3. 建立数据源
运行下列命令生成 Hive 源表：

```
hive -f <output_forder>/create_hive_tables_for_system_cubes.sql
```

通过这个命令，相关的 hive 表将会被创建。每一个系统 Cube 中的事实表对应了一张 Hive 源表，Hive 源表中记录了查询或任务相关的数据，这些数据将为系统 Cube 服务。

![hive_table](/images/SystemCube/hive_table.png)

### 4. 为系统 Cubes 上传 Metadata 
然后我们需要通过下列命令上传 metadata 到 hbase：

```
./bin/metastore.sh restore <output_forder>
```

### 5. 重载 Metadata
最终，我们需要在 Kylin web UI 重载 metadata。然后，一组系统 Cubes 将会被创建在系统 project 下，称为 "KYLIN_SYSTEM"。


### 6. 构建系统 Cube
当系统 Cube 被创建，我们需要定期构建 Cube。方法如下：

**步骤一**：创建一个 shell 脚本，通过调用 org.apache.kylin.tool.job.CubeBuildingCLI 来构建系统 Cube。例如：

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

**步骤二**：定期运行这个 shell 脚本。例如，像接下来这样添加一个 cron job：

{% highlight Groff markup %}
0 */2 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_QUERY_QA 3600000 1200000

20 */2 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_QUERY_CUBE_QA 3600000 1200000

40 */4 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_QUERY_RPC_QA 3600000 1200000

30 */4 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_JOB_QA 3600000 1200000

50 */12 * * * sh ${KYLIN_HOME}/bin/system_cube_build.sh KYLIN_HIVE_METRICS_JOB_EXCEPTION_QA 3600000 12000

{% endhighlight %}

## <span id="自动创建系统 Cube">自动创建系统 Cube</span>

从kylin 2.6.0 开始提供 system-cube.sh 脚本，用户可以通过执行此脚本来自动创建系统 Cube。

- 创建系统 Cube：`sh bin/system-cube.sh setup`

- 构建系统 Cube：`sh bin/system-cube.sh build`

- 为系统 Cube 添加定时任务：`sh bin/system-cube.sh cron`

**注意**：System-cube.sh 会调用 ${KYLIN_HOME}/bin/build-incremental-cube.sh 脚本来提交构建任务，build-incremental-cube.sh 中默认使用 ADMIN:KYLIN 的用户名和密码作为 authtication 来调用 kylin 的 rebuild API，如果您修改了 ADMIN 用户的密码或者想使用 ADMIN 以外的用户来提交构建任务，请找到 build-incremental-cube.sh 中的 ADMIN:KYLIN，将其替换为正确的用户名和密码。

## <span id="系统 Cube 的细节">系统 Cube 的细节</span>

Hive 中有 5 张表记录了 Kylin 系统的相关指标数据，每一个系统 Cube 的事实表对应了一张 Hive 表，共有 5 个系统 Cube。

### 普通 Dimension

对于这些系统 Cube，admins 能够用四个时间粒度查询，这些维度在 5 个系统 Cube 中均生效。从高级别到低级别，如下：

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
这个 Cube 用于在最高级别收集查询 metrics。细节如下：

<table>
  <tr>
    <th colspan="2">Dimension</th>
  </tr>
  <tr>
    <td>HOST</td>
    <td>the host of server for query engine</td>
  </tr>
  <tr>
    <td>KUSER</td>
    <td>the user who executes the query</td>
  </tr>
  <tr>
    <td>PROJECT</td>
    <td>the project where the query executes</td>
  </tr>
  <tr>
    <td>REALIZATION</td>
    <td>the cube which the query hits. In Kylin, there are two OLAP realizations: Cube, or Hybrid of Cubes</td>
  </tr>
  <tr>
    <td>REALIZATION_TYPE</td>
    <td></td>
  </tr>
  <tr>
    <td>QUERY_TYPE</td>
    <td>users can query on different data sources: CACHE, OLAP, LOOKUP_TABLE, HIVE</td>
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
    <td>MIN, MAX, SUM, PERCENTILE_APPROX of QUERY_TIME_COST</td>
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
这个 Cube 用于在最低级别收集查询 metrics。对于一个查询，相关的 aggregation 和 filter 能够下推到每一个 rpc 目标服务器。Rpc 目标服务器的健壮性是更好查询性能的基础。细节如下：

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
    <td>the project where the query executes</td>
  </tr>
  <tr>
    <td>REALIZATION</td>
    <td>the cube which the query hits</td>
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
    <td>MAX, SUM, PERCENTILE_APPROX of CALL_TIME</td>
    <td>the time cost of a rpc all</td>
  </tr>
  <tr>
    <td>MAX, SUM of COUNT_SKIP</td>
    <td>based on fuzzy filters or else, a few rows will be skiped. This indicates the skipped row count</td>
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
这个 Cube 用于在 Cube 级别收集查询 metrics。最重要的是 cuboids 相关的，其为 Cube planner 提供服务。细节如下：

<table>
  <tr>
    <th colspan="2">Dimension</th>
  </tr>
  <tr>
    <td>CUBE_NAME</td>
    <td></td>
  </tr>
  <tr>
    <td>SEGMENT_NAME</td>
    <td></td>
  </tr>
  <tr>
    <td>CUBOID_SOURCE</td>
    <td>source cuboid parsed based on query and Cube design</td>
  </tr>
  <tr>
    <td>CUBOID_TARGET</td>
    <td>target cuboid already precalculated and served for source cuboid</td>
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
    <td>WEIGHT_PER_HIT</td>
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
    <td>MAX, SUM of STORAGE_COUNT_SCAN</td>
    <td>the sum of row count scanned for the related rpc calls</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_COUNT_RETURN</td>
    <td>the sum of row count returned for the related rpc calls</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_COUNT_AGGREGATE</td>
    <td>the sum of row count aggregated for the related rpc calls</td>
  </tr>
  <tr>
    <td>MAX, SUM of STORAGE_COUNT_AGGREGATE_FILTER</td>
    <td>the sum of row count aggregated and filtered for the related rpc calls, = STORAGE_SIZE_SCAN - STORAGE_SIZE_RETURN</td>
  </tr>
</table>



### METRICS_JOB
在 Kylin 中，主要有三种类型的 job：
- "BUILD"，为了从 **HIVE** 中 building Cube segments。
- "MERGE"，为了在 **HBASE** 中 merging Cube segments。
- "OPTIMIZE"，为了在 **HBASE** 中基于 **base cuboid** 动态调整预计算 cuboid tree。

这个 Cube 是用来收集 job 指标。细节如下：

<table>
  <tr>
    <th colspan="2">Dimension</th>
  </tr>
  <tr>
    <td>HOST</td>
    <td>the host of server for job engine</td>
  </tr>
  <tr>
    <td>KUSER</td>
    <td>the user who run the job</td>
  </tr>
  <tr>
    <td>PROJECT</td>
    <td>the project where the job runs</td>
  </tr>
  <tr>
    <td>CUBE_NAME</td>
    <td>the cube with which the job is related</td>
  </tr>
  <tr>
    <td>JOB_TYPE</td>
    <td>build, merge or optimize</td>
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
    <td>MIN, MAX, SUM, PERCENTILE_APPROX of DURATION</td>
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
    <td>a job may includes serveral MR(map reduce) jobs. Those MR jobs may wait because of lack of Hadoop resources.</td>
  </tr>
  <tr>
    <td>MAX, SUM of step_duration_distinct_columns</td>
    <td></td>
  </tr>
  <tr>
    <td>MAX, SUM of step_duration_dictionary</td>
    <td></td>
  </tr>
  <tr>
    <td>MAX, SUM of step_duration_inmem_cubing</td>
    <td></td>
  </tr>
  <tr>
    <td>MAX, SUM of step_duration_hfile_convert</td>
    <td></td>
  </tr>
</table>



### METRICS_JOB_EXCEPTION
这个 Cube 是用来收集 job exception 指标。细节如下：

<table>
  <tr>
    <th colspan="2">Dimension</th>
  </tr>
  <tr>
    <td>HOST</td>
    <td>the host of server for job engine</td>
  </tr>
  <tr>
    <td>KUSER</td>
    <td>the user who run a job</td>
  </tr>
  <tr>
    <td>PROJECT</td>
    <td>the project where the job runs</td>
  </tr>
  <tr>
    <td>CUBE_NAME</td>
    <td>the cube with which the job is related</td>
  </tr>
  <tr>
    <td>JOB_TYPE</td>
    <td>build, merge or optimize</td>
  </tr>
  <tr>
    <td>CUBING_TYPE</td>
    <td>in kylin, there are two cubing algorithms, Layered & Fast(InMemory)</td>
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
