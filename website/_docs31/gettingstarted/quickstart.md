---
layout: docs31
title:  Quick Start
categories: start
permalink: /docs31/gettingstarted/kylin-quickstart.html
since: v0.6.x
---

This guide aims to provide novice Kylin users with a complete process guide from download and installation to a sub-second query experience. The guide is divided into two parts, which respectively introduce the installation and use of Kylin in two scenarios – with an installation based on a Hadoop environment and installation from Docker image without a Hadoop environment.

Users can follow these steps to get an initial understanding of how to use Kylin, master the basic skills of Kylin and then use Kylin to design models and speed up queries based on their own business scenarios.

### 01 Install Kylin From a Docker Image

In order to make it easy for users to try out Kylin, Zhu Weibin of Ant Financial has contributed “Kylin Docker Image” to the community. In this image, various services that Kylin depends on have been installed and deployed, including:

- JDK 1.8
- Hadoop 2.7.0
- Hive 1.2.1
- Hbase 1.1.2 (with Zookeeper)
- Spark 2.3.1
- Kafka 1.1.1
- MySQL 5.1.73

We have uploaded the user facing Kylin image to the Docker repository. Users do not need to build the image locally; they only need to install Docker to experience Kylin’s one-click installation.

#### Step1
First, execute the following command to pull the image from the Docker repository:

```
docker pull apachekylin/apache-kylin-standalone:3.1.0
```

The image here contains the latest version of Kylin: Kylin v3.1.0. This image contains all of the big data components that Kylin depends on, so it takes a long time to pull the image – please be patient. After the pull is successful, it is displayed as follows:

![](/images/docs/quickstart/pull_docker.png)

#### Step2
Execute the following command to start the container:

```
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 16010:16010 \
apachekylin/apache-kylin-standalone:3.1.0
```

The container will start shortly. Since the specified port in the container has been mapped to the local port, you can directly open the pages of each service in the local browser, such as:
- Kylin Page: http://127.0.0.1:7070/kylin/
- HDFS NameNode Page: http://127.0.0.1:50070
- Yarn ResourceManager Page: http://127.0.0.1:8088
- HBase Page: http://127.0.0.1:60010

When the container starts, the following services are automatically started:
- NameNode, DataNode
- ResourceManager, NodeManager
- HBase
- Kafka
- Kylin

It will also automatically run $ KYLIN_HOME / bin / sample.sh and create a kylin_streaming_topic in Kafka and continue to send data to that topic to allow users to experience building and querying cubes in batches and streams as soon as the container is launched.

Users can enter the container through the docker exec command. The relevant environment variables in the container are as follows:
- JAVA_HOME = /home/admin/jdk1.8.0_141
- HADOOP_HOME = /home/admin/hadoop-2.7.0
- KAFKA_HOME = /home/admin/kafka_2.11-1.1.1
- SPARK_HOME = /home/admin/spark-2.3.1-bin-hadoop2.6
- HBASE_HOME = /home/admin/hbase-1.1.2
- HIVE_HOME = /home/admin/apache-hive-1.2.1-bin
- KYLIN_HOME = /home/admin/apache-kylin-3.1.0-bin-hbase1x

After logging in to Kylin with user/password of ADMIN/KYLIN, users can use the sample cube to experience the construction and query of the cube, or they can create and query their own models and cubes by following the tutorial from Step 8 in “Install and Use Kylin Based on a Hadoop Environment” below.

### 02 Install and Use Kylin Based on a Hadoop Environment

Users who already have a stable Hadoop environment can download Kylin’s binary package and deploy it on their Hadoop cluster. Before installation, check the environment according to the following requirements.

#### Environmental Inspection

(1) Pre-Conditions: Kylin relies on a Hadoop cluster to process large data sets. You need to prepare a Hadoop cluster configured with HDFS, YARN, MapReduce, Hive, HBase, Zookeeper and other services for Kylin to run.

Kylin can be started on any node of a Hadoop cluster. For your convenience, you can run Kylin on the master node, but for better stability, we recommend that you deploy Kylin on a clean Hadoop client node. The Hive, HBase, HDFS and other command lines have been installed on the node and the client configuration (such as core-site.xml, hive-site.xml, hbase-site.xml and others) have been properly configured and they can automatically synchronize with other nodes.

The Linux account running Kylin must have access to the Hadoop cluster, including permissions to create/write HDFS folders, Hive tables, HBase tables and submit MapReduce tasks.

 

(2) Hardware Requirements: The server running Kylin is recommended to have a minimum configuration of 4 core CPU, 16 GB memory and 100 GB disk.

 

(3) Operating System Requirements: CentOS 6.5+ or Ubuntu 16.0.4+

 

(4) Software Requirements: Hadoop 2.7+, 3.0-3.1; Hive 0.13+, 1.2.1+; HBase 1.1+, 2.0 (supported since Kylin 2.5); JDK: 1.8+

 

It is recommended to use an integrated Hadoop environment for Kylin installation and testing, such as Hortonworks HDP or Cloudera CDH. Before Kylin was released, Hortonworks HDP 2.2-2.6 and 3.0, Cloudera CDH 5.7-5.11 and 6.0, AWS EMR 5.7-5.10, and Azure HDInsight 3.5-3.6 passed the test.

#### Install and Use
When your environment meets the above prerequisites, you can install and start using Kylin.

#### Step1. Download the Kylin Archive
Download a binary for your version of Hadoop from [Apache Kylin Download Site](https://kylin.apache.org/download/). Currently, the latest versions are Kylin 3.1.0 and Kylin 2.6.6, of which, version 3.0 supports the function of ingesting data in real time for pre-calculation. If your Hadoop environment is CDH 5.7, you can download Kylin 3.1.0 using the following command line:

```
cd /usr/local/
wget http://apache.website-solution.net/kylin/apache-kylin-3.1.0/apache-kylin-3.1.0-bin-cdh57.tar.gz
```

#### Step2. Extract Kylin
Extract the downloaded Kylin archive and configure the environment variable KYLIN_HOME to point to the extracted directory:

```
tar -zxvf  apache-kylin-3.1.0-bin-cdh57.tar.gz
cd apache-kylin-3.1.0-bin-cdh57
export KYLIN_HOME=`pwd`
```

#### Step3. Download Spark
Since Kylin checks the Spark environment when it starts, you need to set SPARK_HOME:

```
export SPARK_HOME=/path/to/spark
```

If you don’t have a Spark environment already downloaded, you can also download Spark using Kylin’s own script:

```
$KYLIN_HOME/bin/download-spark.sh
```

The script will place the decompressed Spark in the $ KYLIN_HOME directory. If SPARK_HOME is not set in the system, the Spark in the $ KYLIN_HOME directory will be found automatically when Kylin is started.

#### Step4. Environmental Inspection
Kylin runs on a Hadoop cluster and has certain requirements for the version, access permissions and CLASSPATH of each component. 
To avoid encountering various environmental problems, you can run the $ KYLIN_HOME / bin / check-env.sh script to perform an environment check to see if there are any problems. 
The script will print out detailed error messages if any errors are identified. If there is no error message, your environment is suitable for Kylin operation.

#### Step5. Start Kylin
Run 

```
$KYLIN_HOME/bin/kylin.sh
```

Start script to start Kylin. If the startup is successful, the following will be output at the end of the command line:

```
A new Kylin instance is started by root. To stop it, run 'kylin.sh stop'
Check the log at /usr/local/apache-kylin-3.1.0-bin-cdh57/logs/kylin.log
Web UI is at http://<hostname>:7070/kylin
```

The default port started by Kylin is 7070. You can use $ KYLIN_HOME/bin/kylin-port-replace-util.sh set number to modify the port. The modified port is 7070 + number.

#### Step6. Visit Kylin
After Kylin starts, you can access it through your browser: http://<hostname>:port/kylin – where <hostname> is the specific machine name, IP address or domain name, port is the Kylin port and the default is 7070. 
The initial username and password are ADMIN/KYLIN. After the server starts, you can get the runtime log by looking at $ KYLIN_HOME/logs/kylin.log.

#### Step7. Create Sample Cube
Kylin provides a script to create a sample cube for users to quickly experience Kylin. Run from the command line:

```
$KYLIN_HOME/bin/sample.sh
```

After completing, log in to Kylin, click System -> Configuration -> Reload Metadata to reload the metadata.

After the metadata is reloaded, you can see a project named learn_kylin in Project in the upper left corner. 
This contains kylin_sales_cube and kylin_streaming_cube, which are a batch cube and a streaming cube, respectively. 
You can build the kylin_sales_cube directly and you can query it after the build is completed. 
For kylin_streaming_cube, you need to set KAFKA_HOME and then execute $ {KYLIN_HOME} /bin/sample-streaming.sh. 
This script will create a Kafka Topic named kylin_streaming_topic in the localhost: 9092 broker and it will also randomly send 100 messages to kylin_streaming_topic, then you can build kylin_streaming_cube.

For sample cube, you can refer to:[Sample Cube](/docs31/tutorial/kylin_sample.html)

Of course, you can also try to create your own cube based on the following tutorial.

#### Step8. Create Project 
After logging in to Kylin, click the + in the upper left corner to create a Project.

![](/images/docs/quickstart/create_project.png)

#### Step9. Load Hive Table
Click Model -> the Data Source -> the Load the From the Table Tree. 
Kylin reads the Hive data source table and displays it in a tree. You can choose the tables you would like to add to models and then click Sync. The selected tables will then be loaded into Kylin.

![](/images/docs/quickstart/load_hive_table.png)

They then appear in the Tables directory of the data source.

#### Step10. Create the Model
Click Model -> New -> New Model:

![](/images/docs/quickstart/create_model.png)

Enter the Model Name and click Next, then select Fact Table and Lookup Table. You need to set the JOIN condition with the fact table when adding Lookup Table.

![](/images/docs/quickstart/add_lookup_table.png)

Then click Next to select Dimension:

![](/images/docs/quickstart/model_add_dimension.png)

Next, Select Measure:

![](/images/docs/quickstart/model_add_measure.png)

The next step is to set the time partition column and filter conditions. The time partition column is used to select the time range during incremental construction. If no time partition column is set, it means that the cubes under this model are all built. The filter condition is used for the where condition when flattening the table.

![](/images/docs/quickstart/set_partition_column.png)

Then, click Save to save the model.

#### Step11. Create Cube

Model -> New -> New Cube:

![](/images/docs/quickstart/create_cube.png)

Click Next to add Dimension. The dimensions of the Lookup Table can be set to Normal or Derived. The default setting is derived dimension. Derived dimension means that the column can be derived from the primary key of the dimension table. In fact, only the primary key column will be calculated by the cube.

![](/images/docs/quickstart/cube_add_dimension.png)

Click Next and click + Measure to add a pre-calculated measure.

Kylin creates a Count (1) metric by default. Kylin supports eight metrics: SUM, MIN, MAX, COUNT, COUNT_DISTINCT, TOP_N, EXTENDED_COLUMN and PERCENTILE.

Please select the appropriate return type for COUNT_DISTINCT and TOP_N, which is related to the size of the cube. 
Click OK after the addition is complete and the measure will be displayed in the Measures list.

![](/images/docs/quickstart/cube_add_measure.png)

After adding all of the measures, click Next to proceed. This page is about the settings for cube data refresh. 
Here you can set the threshold for automatic merge (Auto Merge Thresholds), the minimum time for data retention (Retention Threshold) and the start time of the first segment.

![](/images/docs/quickstart/segment_auto_merge.png)

Click Next to continue going through the Advanced Settings. 
Here you can set the aggregation group, RowKeys, Mandatory Cuboids, Cube Engine, etc.
 
For more information about Advanced Settings, you can refer to Step 5 on the [create_cube](/docs31/tutorial/create_cube.html), which details the settings for additional options. 

For more dimensional optimization, you can read: [aggregation-group](/blog/2016/02/18/new-aggregation-group/).

![](/images/docs/quickstart/advance_setting.png)

If you are not familiar with Advanced Settings, you can keep the default settings first. Click Next to jump to the Kylin Properties page. Here you can override the cube-level Kylin configuration items and define the properties to be covered. 
For configuration items, please refer to: [configuration](/docs31/install/configuration.html).

![](/images/docs/quickstart/properties.png)

After the configuration is complete, click the Next button to the next page. 
Here you can preview the basic information of the cube you are creating and you can return to the previous steps to modify it. 
If you don’t need to make any changes, you can click the Save button to complete the cube creation. 
After that, this cube will appear in your cube list.

![](/images/docs/quickstart/cube_list.png)

#### Step12. Build Cube

The cube created in the previous step has definitions but no calculated data. Its status is “DISABLED” and it cannot be queried. If you want the cube to have data, you need to build it. Cubes are usually built in one of two ways: full builds or incremental builds.

Click the Action under the Actions column of the cube to be expanded. Select Build. 

If the time partition column is not set in the model to which the cube belongs, the default is to build in full.

Click Submit to submit the build task directly. If a time partition column is set, the following page will appear, where you will need to select the start and end time for building the data.

![](/images/docs/quickstart/cube_build.png)

After setting the start and end time, click Submit to submit the build task. 
You can then observe the status of the build task on the Monitor page. 
Kylin displays the running status of each step on the page, the output log and MapReduce tasks. 
You can view more detailed log information in ${KYLIN_HOME}/logs/kylin.log.

![](/images/docs/quickstart/job_monitor.png)

After the job is built, the status of the cube will change to READY and you can see the segment information.

![](/images/docs/quickstart/segment_info.png)

#### Step13. Query Cube
After the cube is built, you can see the table of the built cube and query it under the Tables list on the Insight page. 
After the query hits the cube, it returns the pre-calculated results stored in HBase.

![](/images/docs/quickstart/query_cube.png)

Congratulations, you have already acquired the basic skills for using Kylin and you can now discover and explore more and more powerful functions.



 
