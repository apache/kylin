---
layout: docs21
title:  Build Cube with Spark (beta)
categories: tutorial
permalink: /docs21/tutorial/cube_spark.html
---
Kylin v2.0 introduces the Spark cube engine, it uses Apache Spark to replace MapReduce in the build cube step; You can check [this blog](/blog/2017/02/23/by-layer-spark-cubing/) for an overall picture. The current document uses the sample cube to demo how to try the new engine.

**Please note, this toturial is based on Kylin 2.0 + Spark 1.6; Now Kylin v2.1 has upgraded Spark to 2.1.1. This document is out of date but the main steps are very similar.**

## Preparation
To finish this tutorial, you need a Hadoop environment which has Kylin v2.0.0 or above installed. Here we will use Hortonworks HDP 2.4 Sandbox VM, the Hadoop components as well as Hive/HBase has already been started. 

## Install Kylin v2.0.0

Download the Kylin v2.0.0 for HBase 1.x from Kylin's download page, and then uncompress the tar ball into */usr/local/* folder:

{% highlight Groff markup %}

wget http://www-us.apache.org/dist/kylin/apache-kylin-2.0.0/apache-kylin-2.0.0-bin-hbase1x.tar.gz -P /tmp

tar -zxvf /tmp/apache-kylin-2.0.0-bin-hbase1x.tar.gz -C /usr/local/

export KYLIN_HOME=/usr/local/apache-kylin-2.0.0-bin
{% endhighlight %}

## Prepare "kylin.env.hadoop-conf-dir"

To run Spark on Yarn, need specify **HADOOP_CONF_DIR** environment variable, which is the directory that contains the (client side) configuration files for Hadoop. In many Hadoop distributions the directory is "/etc/hadoop/conf"; But Kylin not only need access HDFS, Yarn and Hive, but also HBase, so the default directory might not have all necessary files. In this case, you need create a new directory and then copying or linking those client files (core-site.xml, hdfs-site.xml, yarn-site.xml, hive-site.xml and hbase-site.xml) there. In HDP 2.4, there is a conflict between hive-tez and Spark, so need change the default engine from "tez" to "mr" when copy for Kylin.

{% highlight Groff markup %}

mkdir $KYLIN_HOME/hadoop-conf
ln -s /etc/hadoop/conf/core-site.xml $KYLIN_HOME/hadoop-conf/core-site.xml 
ln -s /etc/hadoop/conf/hdfs-site.xml $KYLIN_HOME/hadoop-conf/hdfs-site.xml 
ln -s /etc/hadoop/conf/yarn-site.xml $KYLIN_HOME/hadoop-conf/yarn-site.xml 
ln -s /etc/hbase/2.4.0.0-169/0/hbase-site.xml $KYLIN_HOME/hadoop-conf/hbase-site.xml 
cp /etc/hive/2.4.0.0-169/0/hive-site.xml $KYLIN_HOME/hadoop-conf/hive-site.xml 
vi $KYLIN_HOME/hadoop-conf/hive-site.xml (change "hive.execution.engine" value from "tez" to "mr")

{% endhighlight %}

Now, let Kylin know this directory with property "kylin.env.hadoop-conf-dir" in kylin.properties:

{% highlight Groff markup %}
kylin.env.hadoop-conf-dir=/usr/local/apache-kylin-2.0.0-bin/hadoop-conf
{% endhighlight %}

If this property isn't set, Kylin will use the directory that "hive-site.xml" locates in; while that folder may have no "hbase-site.xml", will get HBase/ZK connection error in Spark.

## Check Spark configuration

Kylin embedes a Spark binary (v1.6.3) in $KYLIN_HOME/spark, all the Spark configurations can be managed in $KYLIN_HOME/conf/kylin.properties with prefix *"kylin.engine.spark-conf."*. These properties will be extracted and applied when runs submit Spark job; E.g, if you configure "kylin.engine.spark-conf.spark.executor.memory=4G", Kylin will use "--conf spark.executor.memory=4G" as parameter when execute "spark-submit".

Before you run Spark cubing, suggest take a look on these configurations and do customization according to your cluster. Below is the default configurations, which is also the minimal config for a sandbox (1 executor with 1GB memory); usually in a normal cluster, need much more executors and each has at least 4GB memory and 2 cores:

{% highlight Groff markup %}
kylin.engine.spark-conf.spark.master=yarn
kylin.engine.spark-conf.spark.submit.deployMode=cluster
kylin.engine.spark-conf.spark.yarn.queue=default
kylin.engine.spark-conf.spark.executor.memory=1G
kylin.engine.spark-conf.spark.executor.cores=2
kylin.engine.spark-conf.spark.executor.instances=1
kylin.engine.spark-conf.spark.eventLog.enabled=true
kylin.engine.spark-conf.spark.eventLog.dir=hdfs\:///kylin/spark-history
kylin.engine.spark-conf.spark.history.fs.logDirectory=hdfs\:///kylin/spark-history
#kylin.engine.spark-conf.spark.yarn.jar=hdfs://namenode:8020/kylin/spark/spark-assembly-1.6.3-hadoop2.6.0.jar
#kylin.engine.spark-conf.spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec

## uncomment for HDP
#kylin.engine.spark-conf.spark.driver.extraJavaOptions=-Dhdp.version=current
#kylin.engine.spark-conf.spark.yarn.am.extraJavaOptions=-Dhdp.version=current
#kylin.engine.spark-conf.spark.executor.extraJavaOptions=-Dhdp.version=current

{% endhighlight %}

For running on Hortonworks platform, need specify "hdp.version" as Java options for Yarn containers, so please uncommment the last three lines in kylin.properties. 

Besides, in order to avoid repeatedly uploading Spark assembly jar to Yarn, you can manually do that once, and then configure the jar's HDFS location; Please note, the HDFS location need be full qualified name.

{% highlight Groff markup %}
hadoop fs -mkdir -p /kylin/spark/
hadoop fs -put $KYLIN_HOME/spark/lib/spark-assembly-1.6.3-hadoop2.6.0.jar /kylin/spark/
{% endhighlight %}

After do that, the config in kylin.properties will be:
{% highlight Groff markup %}
kylin.engine.spark-conf.spark.yarn.jar=hdfs://sandbox.hortonworks.com:8020/kylin/spark/spark-assembly-1.6.3-hadoop2.6.0.jar
kylin.engine.spark-conf.spark.driver.extraJavaOptions=-Dhdp.version=current
kylin.engine.spark-conf.spark.yarn.am.extraJavaOptions=-Dhdp.version=current
kylin.engine.spark-conf.spark.executor.extraJavaOptions=-Dhdp.version=current
{% endhighlight %}

All the "kylin.engine.spark-conf.*" parameters can be overwritten at Cube or Project level, this gives more flexibility to the user.

## Create and modify sample cube

Run the sample.sh to create the sample cube, and then start Kylin server:

{% highlight Groff markup %}

$KYLIN_HOME/bin/sample.sh
$KYLIN_HOME/bin/kylin.sh start

{% endhighlight %}

After Kylin is started, access Kylin web, edit the "kylin_sales" cube, in the "Advanced Setting" page, change the "Cube Engine" from "MapReduce" to "Spark (Beta)":


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/1_cube_engine.png)

Click "Next" to the "Configuration Overwrites" page, click "+Property" to add property "kylin.engine.spark.rdd-partition-cut-mb" with value "500" (reasons below):

   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/2_overwrite_partition.png)

The sample cube has two memory hungry measures: a "COUNT DISTINCT" and a "TOPN(100)"; Their size estimation can be inaccurate when the source data is small: the estimized size is much larger than the real size, that causes much more RDD partitions be splitted, which slows down the build. Here 100 is a more reasonable number for it. Click "Next" and "Save" to save the cube.


## Build Cube with Spark

Click "Build", select current date as the build end date. Kylin generates a build job in the "Monitor" page, in which the 7th step is the Spark cubing. The job engine starts to execute the steps in sequence. 


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/2_job_with_spark.png)


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/3_spark_cubing_step.png)

When Kylin executes this step, you can monitor the status in Yarn resource manager. Click the "Application Master" link will open Spark web UI, it shows the progress of each stage and the detailed information.


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/4_job_on_rm.png)


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/5_spark_web_gui.png)


After all steps be successfully executed, the Cube becomes "Ready" and you can query it as normal.

## Troubleshooting

When getting error, you should check "logs/kylin.log" firstly. There has the full Spark command that Kylin executes, e.g:

{% highlight Groff markup %}
2017-03-06 14:44:38,574 INFO  [Job 2d5c1178-c6f6-4b50-8937-8e5e3b39227e-306] spark.SparkExecutable:121 : cmd:export HADOOP_CONF_DIR=/usr/local/apache-kylin-2.0.0-bin/hadoop-conf && /usr/local/apache-kylin-2.0.0-bin/spark/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry  --conf spark.executor.instances=1  --conf spark.yarn.jar=hdfs://sandbox.hortonworks.com:8020/kylin/spark/spark-assembly-1.6.3-hadoop2.6.0.jar  --conf spark.yarn.queue=default  --conf spark.yarn.am.extraJavaOptions=-Dhdp.version=current  --conf spark.history.fs.logDirectory=hdfs:///kylin/spark-history  --conf spark.driver.extraJavaOptions=-Dhdp.version=current  --conf spark.master=yarn  --conf spark.executor.extraJavaOptions=-Dhdp.version=current  --conf spark.executor.memory=1G  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=hdfs:///kylin/spark-history  --conf spark.executor.cores=2  --conf spark.submit.deployMode=cluster --files /etc/hbase/2.4.0.0-169/0/hbase-site.xml --jars /usr/local/apache-kylin-2.0.0-bin/spark/lib/spark-assembly-1.6.3-hadoop2.6.0.jar,/usr/hdp/2.4.0.0-169/hbase/lib/htrace-core-3.1.0-incubating.jar,/usr/hdp/2.4.0.0-169/hbase/lib/hbase-client-1.1.2.2.4.0.0-169.jar,/usr/hdp/2.4.0.0-169/hbase/lib/hbase-common-1.1.2.2.4.0.0-169.jar,/usr/hdp/2.4.0.0-169/hbase/lib/hbase-protocol-1.1.2.2.4.0.0-169.jar,/usr/hdp/2.4.0.0-169/hbase/lib/metrics-core-2.2.0.jar,/usr/hdp/2.4.0.0-169/hbase/lib/guava-12.0.1.jar, /usr/local/apache-kylin-2.0.0-bin/lib/kylin-job-2.0.0.jar -className org.apache.kylin.engine.spark.SparkCubingByLayer -hiveTable kylin_intermediate_kylin_sales_cube_555c4d32_40bb_457d_909a_1bb017bf2d9e -segmentId 555c4d32-40bb-457d-909a-1bb017bf2d9e -confPath /usr/local/apache-kylin-2.0.0-bin/conf -output hdfs:///kylin/kylin_metadata/kylin-2d5c1178-c6f6-4b50-8937-8e5e3b39227e/kylin_sales_cube/cuboid/ -cubename kylin_sales_cube

{% endhighlight %}

You can copy the cmd to execute manually in shell and then tunning the parameters quickly; During the execution, you can access Yarn resource manager to check more. If the job has already finished, you can check the history info in Spark history server. 

By default Kylin outputs the history to "hdfs:///kylin/spark-history", you need start Spark history server on that directory, or change to use your existing Spark history server's event directory in conf/kylin.properties with parameter "kylin.engine.spark-conf.spark.eventLog.dir" and "kylin.engine.spark-conf.spark.history.fs.logDirectory".

The following command will start a Spark history server instance on Kylin's output directory, before run it making sure you have stopped the existing Spark history server in sandbox:

{% highlight Groff markup %}
$KYLIN_HOME/spark/sbin/start-history-server.sh hdfs://sandbox.hortonworks.com:8020/kylin/spark-history 
{% endhighlight %}

In web browser, access "http://sandbox:18080" it shows the job history:

   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/9_spark_history.png)

Click a specific job, there you will see the detail runtime information, that is very helpful for trouble shooting and performance tuning.

## Go further

If you're a Kylin administrator but new to Spark, suggest you go through [Spark documents](https://spark.apache.org/docs/2.1.0/), and don't forget to update the configurations accordingly. You can enable Spark [Dynamic Resource Allocation](https://spark.apache.org/docs/2.1.0/job-scheduling.html#dynamic-resource-allocation) so that it can auto scale/shrink for different work load. Spark's performance relies on Cluster's memory and CPU resource, while Kylin's Cube build is a heavy task when having a complex data model and a huge dataset to build at one time. If your cluster resource couldn't fulfill, errors like "OutOfMemorry" will be thrown in Spark executors, so please use it properly. For Cube which has UHC dimension, many combinations (e.g, a full cube with more than 12 dimensions), or memory hungry measures (Count Distinct, Top-N), suggest to use the MapReduce engine. If your Cube model is simple, all measures are SUM/MIN/MAX/COUNT, source data is small to medium scale, Spark engine would be a good choice. Besides, Streaming build isn't supported in this engine so far (KYLIN-2484).

Now the Spark engine is in public beta; If you have any question, comment, or bug fix, welcome to discuss in dev@kylin.apache.org.
