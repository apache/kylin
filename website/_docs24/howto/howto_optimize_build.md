---
layout: docs24
title:  Optimize Cube Build
categories: howto
permalink: /docs24/howto/howto_optimize_build.html
---

Kylin decomposes a Cube build task into several steps and then executes them in sequence. These steps include Hive operations, MapReduce jobs, and other types job. When you have many Cubes to build daily, then you definitely want to speed up this process. Here are some practices that you probably want to know, and they are organized in the same order as the steps sequence.



## Create Intermediate Flat Hive Table

This step extracts data from source Hive tables (with all tables joined) and inserts them into an intermediate flat table. If Cube is partitioned, Kylin will add a time condition so that only the data in the range would be fetched. You can check the related Hive command in the log of this step, e.g: 

```
hive -e "USE default;
DROP TABLE IF EXISTS kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34;

CREATE EXTERNAL TABLE IF NOT EXISTS kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34
(AIRLINE_FLIGHTDATE date,AIRLINE_YEAR int,AIRLINE_QUARTER int,...,AIRLINE_ARRDELAYMINUTES int)
STORED AS SEQUENCEFILE
LOCATION 'hdfs:///kylin/kylin200instance/kylin-0a8d71e8-df77-495f-b501-03c06f785b6c/kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34';

SET dfs.replication=2;
SET hive.exec.compress.output=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=100000000;
SET mapreduce.job.split.metainfo.maxsize=-1;

INSERT OVERWRITE TABLE kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34 SELECT
AIRLINE.FLIGHTDATE
,AIRLINE.YEAR
,AIRLINE.QUARTER
,...
,AIRLINE.ARRDELAYMINUTES
FROM AIRLINE.AIRLINE as AIRLINE
WHERE (AIRLINE.FLIGHTDATE >= '1987-10-01' AND AIRLINE.FLIGHTDATE < '2017-01-01');
"

```

Kylin applies the configuration in conf/kylin\_hive\_conf.xml while Hive commands are running, for instance, use less replication and enable Hive's mapper side join. If it is needed, you can add other configurations which are good for your cluster.

If Cube's partition column ("FLIGHTDATE" in this case) is the same as Hive table's partition column, then filtering on it will let Hive smartly skip those non-matched partitions. So it is highly recommended to use Hive table's paritition column (if it is a date column) as the Cube's partition column. This is almost required for those very large tables, or Hive has to scan all files each time in this step, costing terribly long time.

If your Hive enables file merge, you can disable them in "conf/kylin\_hive\_conf.xml" as Kylin has its own way to merge files (in the next step): 

    <property>
        <name>hive.merge.mapfiles</name>
        <value>false</value>
        <description>Disable Hive's auto merge</description>
    </property>


## Redistribute intermediate table

After the previous step, Hive generates the data files in HDFS folder: while some files are large, some are small or even empty. The imbalanced file distribution would lead subsequent MR jobs to imbalance as well: some mappers finish quickly yet some others are very slow. To balance them, Kylin adds this step to "redistribute" the data and here is a sample output:

```
total input rows = 159869711
expected input rows per mapper = 1000000
num reducers for RedistributeFlatHiveTableStep = 160

```


Redistribute table, cmd: 

```
hive -e "USE default;
SET dfs.replication=2;
SET hive.exec.compress.output=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=100000000;
SET mapreduce.job.split.metainfo.maxsize=-1;
set mapreduce.job.reduces=160;
set hive.merge.mapredfiles=false;

INSERT OVERWRITE TABLE kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34 SELECT * FROM kylin_intermediate_airline_cube_v3610f668a3cdb437e8373c034430f6c34 DISTRIBUTE BY RAND();
"

```



Firstly, Kylin gets the row count of this intermediate table; then based on the number of row count, it would get amount of files needed to get data redistributed. By default, Kylin allocates one file per 1 million rows. In this sample, there are 160 million rows and exist 160 reducers, and each reducer would write 1 file. In following MR step over this table, Hadoop will start the same number Mappers as the files to process (usually 1 million's data size is small than a HDFS block size). If your daily data scale isn't so large or Hadoop cluster has enough resources, you may want to get more concurrency. Setting "kylin.job.mapreduce.mapper.input.rows" in "conf/kylin.properties" to a smaller value will get that, e.g:

`kylin.job.mapreduce.mapper.input.rows=500000`


Secondly, Kylin runs a *"INSERT OVERWIRTE TABLE .... DISTRIBUTE BY "* HiveQL to distribute the rows among a specified number of reducers.

In most cases, Kylin asks Hive to randomly distributes the rows among reducers, then get files very closed in size. The distribute clause is "DISTRIBUTE BY RAND()".

If your Cube has specified a "shard by" dimension (in Cube's "Advanced setting" page), which is a high cardinality column (like "USER\_ID"), Kylin will ask Hive to redistribute data by that column's value. Then for the rows that have the same value as this column has, they will go to the same file. This is much better than "by random",  because the data will be not only redistributed but also pre-categorized without additional cost, thus benefiting the subsequent Cube build process. Under a typical scenario, this optimization can cut off 40% building time. In this case the distribute clause will be "DISTRIBUTE BY USER_ID":

**Please note:** 1) The "shard by" column should be a high cardinality dimension column, and it appears in many cuboids (not just appears in seldom cuboids). Utilize it to distribute properly can get equidistribution in every time range; otherwise it will cause data incline, which will reduce the building speed. Typical good cases are: "USER\_ID", "SELLER\_ID", "PRODUCT", "CELL\_NUMBER", so forth, whose cardinality is higher than one thousand (should be much more than the reducer numbers). 2) Using "shard by" has other advantage in Cube storage, but it is out of this doc's scope.



## Extract Fact Table Distinct Columns

In this step Kylin runs a MR job to fetch distinct values for the dimensions, which are using dictionary encoding. 

Actually this step does more: it collects the Cube statistics by using HyperLogLog counters to estimate the row count of each Cuboid. If you find that mappers work incredible slowly, it usually indicates that the Cube design is too complex, please check [optimize cube design](howto_optimize_cubes.html) to make the Cube thinner. If the reducers get OutOfMemory error, it indicates that the Cuboid combination does explode or the default YARN memory allocation cannot meet demands. If this step couldn't finish in a reasonable time by all means, you can give up and revisit the design as the real building will take longer.

You can reduce the sampling percentage (kylin.job.cubing.inmem.sampling.percen in kylin.properties) to get this step accelerated, but this may not help much and impact on the accuracy of Cube statistics, thus we don't recommend.  



## Build Dimension Dictionary

With the distinct values fetched in previous step, Kylin will build dictionaries in memory (in next version this will be moved to MR). Usually this step is fast, but if the value set is large, Kylin may report error like "Too high cardinality is not suitable for dictionary". For UHC column, please use other encoding method for the UHC column, such as "fixed_length", "integer" and so on.



## Save Cuboid Statistics and Create HTable

These two steps are lightweight and fast.



## Build Base Cuboid 

This step is building the base cuboid from the intermediate table, which is the first round MR of the "by-layer" cubing algorithm. The mapper number is equals to the reducer number of step 2; The reducer number is estimated with the cube statistics: by default use 1 reducer every 500MB output; If you observed the reducer number is small, you can set "kylin.job.mapreduce.default.reduce.input.mb" in kylin.properties to a smaller value to get more resources, e.g: `kylin.job.mapreduce.default.reduce.input.mb=200`


## Build N-Dimension Cuboid 

These steps are the "by-layer" cubing process, each step uses the output of previous step as the input, and then cut off one dimension to aggregate to get one child cuboid. For example, from cuboid ABCD, cut off A get BCD, cut off B get ACD etc. 

Some cuboid can be aggregated from more than 1 parent cubiods, in this case, Kylin will select the minimal parent cuboid. For example, AB can be generated from ABC (id: 1110) and ABD (id: 1101), so ABD will be used as its id is smaller than ABC. Based on this, if D's cardinality is small, the aggregation will be cost-efficient. So, when you design the Cube rowkey sequence, please remember to put low cardinality dimensions to the tail position. This not only benefit the Cube build, but also benefit the Cube query as the post-aggregation follows the same rule.

Usually from the N-D to (N/2)-D the building is slow, because it is the cuboid explosion process: N-D has 1 Cuboid, (N-1)-D has N cuboids, (N-2)-D has N*(N-1)/2 cuboids, etc. After (N/2)-D step, the building gets faster gradually.



## Build Cube

This step uses a new algorithm to build the Cube: "by-split" Cubing (also called as "in-mem" cubing). It will use one round MR to calculate all cuboids, but it requests more memory than normal. The "conf/kylin\_job\_conf\_inmem.xml" is made for this step. By default it requests 3GB memory for each mapper. If your cluster has enough memory, you can allocate more in "conf/kylin\_job\_conf\_inmem.xml" so it will use as much possible memory to hold the data and gain a better performance, e.g:

    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>6144</value>
        <description></description>
    </property>
    
    <property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx5632m</value>
        <description></description>
    </property>


Please note, Kylin will automatically select the best algorithm based on the data distribution (get in Cube statistics). The not-selected algorithm's steps will be skipped. You don't need to select the algorithm explicitly.



## Convert Cuboid Data to HFile

This step starts a MR job to convert the Cuboid files (sequence file format) into HBase's HFile format. Kylin calculates the HBase region number with the Cube statistics, by default 1 region per 5GB. The more regions got, the more reducers would be utilized. If you observe the reducer's number is small and performance is poor, you can set the following parameters in "conf/kylin.properties" to smaller, as follows:

```
kylin.hbase.region.cut=2
kylin.hbase.hfile.size.gb=1
```

If you're not sure what size a region should be, contact your HBase administrator. 


## Load HFile to HBase Table

This step uses HBase API to load the HFile to region servers, it is lightweight and fast.



## Update Cube Info

After loading data into HBase, Kylin marks this Cube segment as ready in metadata. This step is very fast.



## Cleanup

Drop the intermediate table from Hive. This step doesn't block anything as the segment has been marked ready in the previous step. If this step gets error, no need to worry, the garbage can be collected later when Kylin executes the [StorageCleanupJob](howto_cleanup_storage.html).


## Summary
There are also many other methods to boost the performance. If you have practices to share, welcome to discuss in [dev@kylin.apache.org](mailto:dev@kylin.apache.org).
