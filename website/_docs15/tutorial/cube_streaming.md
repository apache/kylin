---
layout: docs15
title:  Kylin Cube from Streaming (Kafka)
categories: tutorial
permalink: /docs15/tutorial/cube_streaming.html
---
Kylin v1.5 releases the experimental streaming cubing feature. This is a step by step tutorial, illustrating how to create and build a cube from streaming; 

## Preparation
To finish this tutorial, you need a Hadoop environment which has kylin v1.5.2 installed, and also have Kafka be ready to use; Previous Kylin version has a couple issues so please upgrade your Kylin instance at first.

In this tutorial, we will use Hortonworks HDP 2.2.4 Sandbox VM as the Hadoop environment.

## Create sample Kafka topic and populate data

Firstly, we need a Kafka topic for the incoming data; A sample topic "kylin_demo" will be created here:

{% highlight Groff markup %}
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kylin_demo
Created topic "kylin_demo".
{% endhighlight %}

Secondly, we need put sample data to this topic; Kylin has a utility class which can do this; Assume Kylin is installed in /root/apache-kylin-1.5.2-bin:

{% highlight Groff markup %}
export KYLIN_HOME='/root/apache-kylin-1.5.2-bin'

cd $KYLINN_HOME
./bin/kylin.sh org.apache.kylin.source.kafka.util.KafkaSampleProducer --topic kylin_demo --broker sandbox:6667 —delay 0
{% endhighlight %}

It will send 1 record to Kafka every 2 second, with "delay" be 0, the "order_time" will be the same as the timestamp that the message be created. Please don't press CTRL+C before finishing this tutorial, otherwise the streaming will be stopped.

Thirdly, you can check the sample message with kafka-console-consumer.sh, with another shell session:

{% highlight Groff markup %}
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper sandbox.hortonworks.com:2181 --topic kylin_demo --from-beginning
{"amount":4.036149489293339,"category":"ELECTRONIC","order_time":1462465632689,"device":"Windows","qty":4,"currency":"USD","country":"AUSTRALIA"}
{"amount":83.74699855368388,"category":"CLOTH","order_time":1462465635214,"device":"iOS","qty":8,"currency":"USD","country":"INDIA"}

 {% endhighlight %}

## Define a table from streaming
Start Kylin server, login Kylin web GUI, select or create a project; Click "Model" -> "Data Source", then click the icon "Add Streaming Table"; 

   ![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/1 Add streaming table.png)

In the pop-up dialogue, enter a sample record which you got from the kafka-console-consumer, click the ">>" button, Kylin parses the JSON message and list all the properties; 

You need give a logic table name for this streaming data source; The name will be used for SQL query later; here please enter "STREAMING_SALES_TABLE" in the "Table Name" field. 

You also need select a timestamp property which will be used to identify the time of a message; Kylin can derive other time columns like "year_start", "quarter_start" from this time column, which can give your more flexibility on build and query the cube. Here let's check "order_time". You can deselect those properties which not need be built into cube. In this tutorial let's keep all columns. 

   ![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/2 Define streaming table.png)


Click "Next". On this page, you need provide the Kafka cluster information; Enter "kylin_demo" for "Topic"; The cluster has 1 broker, whose host is "sandbox", port is "6667", click "Save".

   ![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/3 Kafka setting.png)

In "Advanced setting" section, the "timeout" and "buffer size" are the configurations for connecting with Kafka. The "Margin" is the window margin that Kylin will fetch data from Kafaka, as the message may arrive earlier or later than expected, Kylin can fetch more data and then do a filtering to allow such advance or latency. Default margin is 5 minutes, you can customize if needed.


Click "Submit" to save the configurations. Now a "streaming" table is created in Kylin.


![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/4 Streaming table.png)


## Create data model
With the table defined in previous step, let's create the data model. The step is pretty the same as you create a common data model, but please note:

* for a streaming cube, it doesn't support join with lookup tables. So when define the data model, only select "DEFAULT.STREAMING_SALES_TABLE " as the fact table, no lookups;
* Select "MINUTE_START" as the cube's partition date column, as we will do incremental build at minutes level.

Here we pick 8 dimension and 2 measure columns:
 
![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/5 Data model dimension.png)
 	
![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/6 Data model measure.png)
 	
	
Save the data model.

## Create cube

The streaming cube is almost the same as a normal cube. a couple of points need get your attention:

* don't use "order\_time" as dimension as that is pretty fine-grained; suggest to use "mintue\_start", "hour\_start" or other, depends on how you will inspect the data.
* In the "refersh setting" step, create more merge ranges, like 0.5 hour, 4 hours, 1 day, and then 7 days; This will help to control the cube segment number.
* In the "rowkeys" section, drag the "minute\_start" to the head position, as for streaming queries, the time condition is always appeared; putting it to head will help to narrow down the scan range.

	![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/8 Cube dimension.png)
	
	![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/9 Cube measure.png)
		
	![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/10 Auto merge.png)

	![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/11 Rowkey.png)

Save the cube.

## Run a micro-batch build

Now the cube is created. The streaming cube's build is different with a normal cube which uses Hive as source. To trigger a build, we need run a micro-batch command:

 {% highlight Groff markup %}
 $KYLIN_HOME/bin/streaming_build.sh STREAMING_CUBE 300000 0
streaming started name: STREAMING_CUBE id: 1462471500000_1462471800000
  {% endhighlight %}

The build is triggered, a separate log file will be created in $KYLIN_HOME/logs/ folder, e.g, streaming_STREAMING_CUBE_1462471500000_1462471800000.log; As the delay is 0, margin is 5 minutes, the build will take a while as most of time is waiting for message to arrive. After about 7 to 10 mintues, the build will finish. 

Go to Kylin web GUI, refresh the page, and then click the cube, you should see its "source records" is a positive number now (usually be 150; 30 records per minute); Click the "HBase" tab you should see a segment is created, with a start time and end time (range is 5 mintues).

![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/12 One micro batch.png)
	
As the streaming micro-batch will not automatically enable the cube. You need click "Action -> Enable" to enable it manually.

Click the "Insight" tab, write a SQL to run, e.g:

 {% highlight Groff markup %}
select minute_start, count(*), sum(amount), sum(qty) from streaming_sales_table group by minute_start 
 {% endhighlight %}
 
 You would see the result as below.
 
 	![](/images/tutorial/1.5/Kylin-Cube-Streaming-Tutorial/13 Query result.png)
 
## Automate the micro-batch

Once the micro-batch build and query are successfully, you can schedule the build at a fixed frequency. In this case, as the interval is 5 minutes, we need schedule the build every 5 minute. It can be implemented easily with Linux crontab or other scheduling services; 

As the kylin.sh need "KYLIN_HOME" env variable be set, we need set it in somewhere like /etc/profile or ~/.bash_profile; 

{% highlight Groff markup %}
 vi ~/.bash_profile
 ## add the KYLIN_HOME here
 export KYLIN_HOME="/root/apache-kylin-1.5.2-bin"
{% endhighlight %}
 
 Then add a cron job for your cube:
  {% highlight Groff markup %}
crontab -e
*/5 * * * * sh /root/apache-kylin-1.5.2-bin/bin/streaming_build.sh STREAMING_CUBE 300000 0
 {% endhighlight %}

Now you can site down and watch the cube be automatically built from streaming. And when the cube segments accumulates, Kylin will automatically run job to merge them into a bigger one. The merge jobs are MapReduce jobs, which can be checked from Kylin's "Monitor" page.

### Furthermore

Something you need know about streaming cubing:

* If a merge job is failed, the auto-merge for that cube will stop; You need check and fix that failure and then resume the job to make auto-merge back to working; 
* Some error like system shutdown may cause segment gaps left in the cube. For example, we have segment A which is from 1:00 AM to 1:05 AM; But at 1:06 the system is unavailable until 1:12, then we have next segment B from 1:10 to 1:15; So there is a gap for 1:05 to 1:10 AM because the Linux cron doesn't check the execution history; For such case Kylin provide a shell script to check and then fill the gaps, you can schedule it with a lower frequency like every 2 hours:
 
  {% highlight Groff markup %}
0 */2 * * * sh /root/apache-kylin-1.5.2-bin/bin/streaming_fillgap.sh STREAMING_CUBE 300000 0
 {% endhighlight %}


