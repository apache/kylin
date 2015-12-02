---
layout: post-blog
title:  Fast Cubing on Spark in Apache Kylin
date:   2015-09-09 15:28:00
author: Qianhao Zhou
categories: blog
---

## Preparation

In order to make POC phase as simple as possible, a standalone spark cluster is the best choice.
So the environment setup is as below:

1. hadoop sandbox (hortonworks hdp 2.2.0)
	
	(8 cores, 16G) * 1

2. spark (1.4.1)

	master:(4 cores, 8G)
	
	worker:(4 cores, 8G) * 2
	
The hadoop conf should also be in the SPARK_HOME/conf


## Fast Cubing Implementation on Spark

Spark as a computation framework has provided much richer operators than map-reduce. And some of them are quite suitable for the cubing algorithm, for instance **aggregate**.

As the [Fast cubing algorithm](http://kylin.apache.org/blog/2015/08/15/fast-cubing/ "Fast Cubing Algorithm in Apache Kylin"), it contains several steps:

1. build dictionary
2. calculate region split for hbase
3. build & output cuboid data

----

**build dictionary**

In order to build dictionary, distinct values of the column are needed, which new API ***DataFrame*** has already provided(since spark 1.3.0).

So after got the data from the hive through SparkSQL, it is quite natural to directly use the api to build dictionary.

----

**calculate region split**

In order to calculate the distribution of all cuboids, Kylin use a HyperLogLog implementation. And each record will have a counter, whose size is by default 16KB each. So if the counter shuffles across the cluster, that will be very expensive.

Spark has provided an operator ***aggregate*** to reduce shuffle size. It first does a map-reduce phase locally, and then another round of reduce to merge the data from each node.

----

**build & output cuboid data**

In order to build cube, Kylin requires a small batch which can fit into memory in the same time.

Previously in map-reduce implementation, Kylin leverage the life-cycle callback **cleanup** to gather all the input together as a batch. This cannot be directly applied in the map reduce operator in spark which we don't have such life-cycle callback.

However spark has provided an operator ***glom*** which coalescing all elements within each partition into an array which is exactly Kylin want to build a small batch.

Once the batch data is ready, we can just apply the Fast Cubing algorithm. 

Then spark api ***saveAsNewAPIHadoopFile*** allow us to write hfile to hdfs and bulk load to HBase.


## Statistics

We use the sample data Kylin provided to build cube, total record count is 10000.

Below are results(system environments are mentioned above)
<table>
    <tr>
        <td></td>
        <td>Spark</td>
        <td>MR</td>
    </tr>
    <tr>
        <td>Duration</td>
        <td>5.5 min</td>
        <td>10+ min</td>
    </tr>
</table>

## Issues

Since hdp 2.2+ requires Hive 0.14.0 while spark 1.3.0 only supports Hive 0.13.0. There are several compatibility problems in hive-site.xml we need to fix.

1. some time-related settings

    There are several settings, whose default value in hive 0.14.0 cannot be parsed in 0.13.0. Such as **hive.metastore.client.connect.retry.delay**, its default value is **5s**. And in hive 0.13.0, this value can only be in the format of Long value. So you have to manually change to from **5s** to **5**.

2. hive.security.authorization.manager

    If you have enabled this configuration, its default value is **org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory** which is newly introduced in hive 0.14.0, it means you have to use the another implementation, such as **org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider**

3. hive.execution.engine

    In hive 0.14.0, the default value of **hive.execution.engine** is **tez**, change it to **mr** in the Spark classpath, otherwise there will be NoClassDefFoundError.

NOTE: Spark 1.4.0 has a [bug](https://issues.apache.org/jira/browse/SPARK-8368) which will lead to ClassNotFoundException. And it has been fixed in Spark 1.4.1. So if you are planning to run on Spark 1.4.0, you may need to upgrade to 1.4.1

Last but not least, when you trying to run Spark application on YARN, make sure that you have hive-site.xml and hbase-site.xml in the  HADDOP_CONF_DIR or YARN_CONF_DIR. Since by default HDP lays these conf in separate directories.




## Next move

Clearly above is not a fair competition. The environment is not the same, test data size is too small, etc.

However it showed that it is practical to migrate from MR to Spark, while some useful operators in Spark will save us quite a few codes.

So the next move for us is to setup a cluster, do the benchmark on real data set for both MR and Spark.

We will update the benchmark once we finished, please stay tuned.