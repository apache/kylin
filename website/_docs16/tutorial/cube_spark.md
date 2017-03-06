---
layout: docs16
title:  Build Cube with Spark (beta)
categories: tutorial
permalink: /docs16/tutorial/cube_spark.html
---
Kylin v2.0 introduces the Spark cube engine, it uses Apache Spark to replace MapReduce in the build cube step; You can check [this blog](/blog/2017/02/23/by-layer-spark-cubing/) for the high level design. The current document uses the sample cube to demo how to try the new engine.

## Preparation
To finish this tutorial, you need a Hadoop environment which has Kylin v2.0.0 or above installed. Here we will use Hortonworks HDP 2.4 Sandbox VM, the Hadoop platform as well as HBase has already been started. 

## Install Kylin v2.0.0 beta

Download the Kylin v2.0.0 beta for HBase 1.x from Kylin's download page, and then uncompress the tar ball in */usr/local/* folder:

{% highlight Groff markup %}

wget https://dist.apache.org/repos/dist/dev/kylin/apache-kylin-2.0.0-beta/apache-kylin-2.0.0-beta-hbase1x.tar.gz -P /tmp

tar -zxvf /tmp/apache-kylin-2.0.0-beta-hbase1x.tar.gz -C /usr/local/

export KYLIN_HOME=/usr/local/apache-kylin-2.0.0-SNAPSHOT-bin
{% endhighlight %}

## Prepare "kylin.env.hadoop-conf-dir"

To run Spark on Yarn, need specify *HADOOP_CONF_DIR* environment variable, which is the directory that contains the (client side) configuration files for the Hadoop cluster. In many Hadoop distributions these files are in "/etc/hadoop/conf"; But Kylin not only need access HDFS, Hive, but also HBase, so the default path might not have all necessary files. In this case, you need create a new directory and then copying or linking all the client files (core-site.xml, yarn-site.xml, hive-site.xml and hbase-site.xml) there. In HDP 2.4, there is a conflict between hive-tez and Spark, so need change the default engine from *tez* to *mr* when copy for Kylin.

{% highlight Groff markup %}

mkdir $KYLIN_HOME/hadoop-conf
ln -s /etc/hadoop/conf/core-site.xml $KYLIN_HOME/hadoop-conf/core-site.xml 
ln -s /etc/hadoop/conf/yarn-site.xml $KYLIN_HOME/hadoop-conf/yarn-site.xml 
ln -s /etc/hbase/2.4.0.0-169/0/hbase-site.xml $KYLIN_HOME/hadoop-conf/hbase-site.xml 
cp /etc/hive/2.4.0.0-169/0/hive-site.xml $KYLIN_HOME/hadoop-conf/hive-site.xml 
vi $KYLIN_HOME/hadoop-conf/hive-site.xml (change "hive.execution.engine" from "tez" to "mr")

{% endhighlight %}

Now, let Kylin know this directory with "kylin.env.hadoop-conf-dir" in kylin.properties:

{% highlight Groff markup %}
kylin.env.hadoop-conf-dir=/usr/local/apache-kylin-2.0.0-SNAPSHOT-bin/hadoop-conf
{% endhighlight %}

If this property wasn't set, Kylin will use the directory that "hive-site.xml" locates in; as that folder usually has no "hbase-site.xml", will get HBase/ZK connection error in Spark.

## Check Spark configuration

Kylin embedes a Spark binary (v1.6.3) in $KYLIN_HOME/spark, all the Spark configurations can be managed in $KYLIN_HOME/conf/kylin.properties with prefix *"kylin.engine.spark-conf."*. These properties will be extracted and applied when runs Spark; E.g, if you configure "kylin.engine.spark-conf.spark.executor.memory=4G", Kylin will use "--conf spark.executor.memory=4G" as parameter when execute "spark-submit".

Before you run Spark cubing, suggest take a look on these configurations and do customization according to your cluster. Below is the default configurations, which is also the minimal config for sandbox (1 executor with 1GB memory); usually in a normal cluster, need much more executors and each has at least 4GB memory and 2 cores:

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

Besides, in order to avoid repeatedly uploading Spark assembly jar to Yarn, you can manually do that once, and then specify the jar's HDFS location; Please note, the HDFS location need be the full qualified name.

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

All the "kylin.engine.spark-conf.*" parameters can be overwritten at Cube level, this gives more flexibility to user.

## Create and modify sample cube

Run the sample.sh to create the sample cube, and then start Kylin server:

{% highlight Groff markup %}

$KYLIN_HOME/bin/sample.sh
$KYLIN_HOME/bin/kylin.sh start

{% endhighlight %}

After Kylin is started, access Kylin web, edit the "kylin_sales" cube, in the "Advanced Setting" page, change the "Cube Engine" from "MapReduce" to "Spark (Beta)":


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/1_cube_engine.png)

Click "Next" to the "Configuration Overwrites" page, click "+Property" to add property "kylin.engine.spark.rdd-partition-cut-mb" with value "100" (reasons below):

   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/2_overwrite_partition.png)

The sample cube has two memory hungry measures: a "COUNT DISTINCT" and a "TOPN(100)"; Their size estimation can be inaccurate especially when the source data is small. The estimized size is much larger than the real size, that causes much more RDD partitions be splitted than expected. Here 100 is a more reasonable number. Click "Next" and "Save" to save the cube.


## Build Cube with Spark

Click "Build", select current date as the end date to proceed. Kylin generates a build job in the "Monitor" page, in which the 7th step is the Spark cubing. The job engine starts to execute the steps in sequence. 


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/2_job_with_spark.png)


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/3_spark_cubing_step.png)

When Kylin executes this step, you can monitor the status in Yarn resource manager. Click the "Application Master" link will open Spark web UI, it shows much more detailed information.


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/4_job_on_rm.png)


   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/5_spark_web_gui.png)


After all steps be successfully executed, the Cube becomes "Ready" and you can query it as normal.

## Trouble shotting

When getting error, you should check "logs/kylin.log" firstly. There has the full Spark command that Kylin executes, e.g:

{% highlight Groff markup %}
2017-03-06 14:44:38,574 INFO  [Job 2d5c1178-c6f6-4b50-8937-8e5e3b39227e-306] spark.SparkExecutable:121 : cmd:export HADOOP_CONF_DIR=/usr/local/apache-kylin-2.0.0-SNAPSHOT-bin/hadoop-conf && /usr/local/apache-kylin-2.0.0-SNAPSHOT-bin/spark/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry  --conf spark.executor.instances=1  --conf spark.yarn.jar=hdfs://sandbox.hortonworks.com:8020/kylin/spark/spark-assembly-1.6.3-hadoop2.6.0.jar  --conf spark.yarn.queue=default  --conf spark.yarn.am.extraJavaOptions=-Dhdp.version=current  --conf spark.history.fs.logDirectory=hdfs:///kylin/spark-history  --conf spark.driver.extraJavaOptions=-Dhdp.version=current  --conf spark.master=yarn  --conf spark.executor.extraJavaOptions=-Dhdp.version=current  --conf spark.executor.memory=1G  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=hdfs:///kylin/spark-history  --conf spark.executor.cores=2  --conf spark.submit.deployMode=cluster --files /etc/hbase/2.4.0.0-169/0/hbase-site.xml --jars /usr/local/apache-kylin-2.0.0-SNAPSHOT-bin/spark/lib/spark-assembly-1.6.3-hadoop2.6.0.jar,/usr/hdp/2.4.0.0-169/hbase/lib/htrace-core-3.1.0-incubating.jar,/usr/hdp/2.4.0.0-169/hbase/lib/hbase-client-1.1.2.2.4.0.0-169.jar,/usr/hdp/2.4.0.0-169/hbase/lib/hbase-common-1.1.2.2.4.0.0-169.jar,/usr/hdp/2.4.0.0-169/hbase/lib/hbase-protocol-1.1.2.2.4.0.0-169.jar,/usr/hdp/2.4.0.0-169/hbase/lib/metrics-core-2.2.0.jar,/usr/hdp/2.4.0.0-169/hbase/lib/guava-12.0.1.jar, /usr/local/apache-kylin-2.0.0-SNAPSHOT-bin/lib/kylin-job-2.0.0-SNAPSHOT.jar -className org.apache.kylin.engine.spark.SparkCubingByLayer -hiveTable kylin_intermediate_kylin_sales_cube_555c4d32_40bb_457d_909a_1bb017bf2d9e -segmentId 555c4d32-40bb-457d-909a-1bb017bf2d9e -confPath /usr/local/apache-kylin-2.0.0-SNAPSHOT-bin/conf -output hdfs:///kylin/kylin_metadata/kylin-2d5c1178-c6f6-4b50-8937-8e5e3b39227e/kylin_sales_cube/cuboid/ -cubename kylin_sales_cube

{% endhighlight %}

You can copy the cmd to execute manually in shell and then tunning the parameters quickly; During the execution, you can access Yarn resource manager to see the resource allocation status and the Spark web GUI. If the job has already finished, you can check the history info in Spark history server. 

As Kylin outputs the history to "hdfs:///kylin/spark-history", you need start Spark history server on that folder, or change to your existing Spark history server's location in conf/kylin.properties with "kylin.engine.spark-conf.spark.eventLog.dir" and "kylin.engine.spark-conf.spark.history.fs.logDirectory".

This command will start a Spark history server instance on Kylin's output folder, before run it making sure you have stopped the existing Spark history server from Ambari:

{% highlight Groff markup %}
$KYLIN_HOME/spark/sbin/start-history-server.sh hdfs://sandbox.hortonworks.com:8020/kylin/spark-history 
{% endhighlight %}

In web browser, access "http://sandbox:18080" it shows the job history:

   ![](/images/tutorial/2.0/Spark-Cubing-Tutorial/9_spark_history.png)

Click a specific Cube job, there you will see the detail runtime information, that is very helpful for trouble shooting and performance tunning.

## Go further

If you're a Kylin administrator but new to Spark, suggest you check [Spark document](https://spark.apache.org/docs/1.6.3/), and then update your configurations accordingly. Spark's performance relies on Cluster's memory and CPU resource, while Kylin's Cube build is a heavy task when have a huge dataset and complex data model, which may exceed your cluster's capacity and then cause OutOfMemory error, so please use it carefully. For Cube which has many dimensions (>10) or has memory hungry measures (Count Distinct, TOPN), suggest using the MapReduce engine. 

Please send your questions, feedbacks to dev@kylin.apache.org.
