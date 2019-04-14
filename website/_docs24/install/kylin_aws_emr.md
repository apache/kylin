---
layout: docs24
title:  "Install Kylin on AWS EMR"
categories: install
permalink: /docs24/install/kylin_aws_emr.html
---

Many users run Hadoop on public Cloud like AWS today. Apache Kylin, compiled with standard Hadoop/HBase API, support most main stream Hadoop releases; The current version Kylin v2.2, supports AWS EMR 5.0 to 5.10. This document introduces how to run Kylin on EMR.

### Recommended Version
* AWS EMR 5.7 (for EMR 5.8 and above, please check [KYLIN-3129](https://issues.apache.org/jira/browse/KYLIN-3129))
* Apache Kylin v2.2.0 or above for HBase 1.x

### Start EMR cluster

Launch an EMR cluser with AWS web console, command line or API. Select "**HBase**" in the applications as Kylin need HBase service. 

You can select "HDFS" or "S3" as the storage for HBase, depending on whether you need Cube data be persisted after shutting down the cluster. EMR HDFS uses the local disk of EC2 instances, which will erase the data when cluster is stopped, then Kylin metadata and Cube data can be lost.

If you use "S3" as HBase's storage, you need customize its configuration for "**hbase.rpc.timeout**", because the bulk load to S3 is a copy operation, when data size is huge, HBase region server need wait much longer to finish than on HDFS.

```
[  {
    "Classification": "hbase-site",
    "Properties": {
      "hbase.rpc.timeout": "3600000",
      "hbase.rootdir": "s3://yourbucket/EMRROOT"
    }
  },
  {
    "Classification": "hbase",
    "Properties": {
      "hbase.emr.storageMode": "s3"
    }
  }
]
```

### Install Kylin

When EMR cluser is in "Waiting" status, you can SSH into its master  node, download Kylin and then uncompress the tar ball:

```
sudo mkdir /usr/local/kylin
sudo chown hadoop /usr/local/kylin
cd /usr/local/kylin
wget http://www-us.apache.org/dist/kylin/apache-kylin-2.2.0/apache-kylin-2.2.0-bin-hbase1x.tar.gz 
tar –zxvf apache-kylin-2.2.0-bin-hbase1x.tar.gz
```

### Configure Kylin

Before start Kylin, you need do a couple of configurations:

- Copy "hbase.zookeeper.quorum" property from /etc/hbase/conf/hbase-site.xml to $KYLIN\_HOME/conf/kylin\_job\_conf.xml, like this:


```
<property>
  <name>hbase.zookeeper.quorum</name>
  <value>ip-nn-nn-nn-nn.ap-northeast-2.compute.internal</value>
</property>
```

- Use HDFS as "kylin.env.hdfs-working-dir" (Recommended)

EMR recommends to **"use HDFS for intermediate data storage while the cluster is running and Amazon S3 only to input the initial data and output the final results"**. Kylin's 'hdfs-working-dir' is for putting the intermediate data for Cube building, cuboid files and also some metadata files (like dictionary and table snapshots which are not good in HBase); so it is best to configure HDFS for this. 

If using HDFS as Kylin working directory, you just leave configurations unchanged as EMR's default FS is HDFS:

```
kylin.env.hdfs-working-dir=/kylin
```

Before you shutdown/restart the cluster, you must backup the "/kylin" data on HDFS to S3 with [S3DistCp](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html), or you may lost data and couldn't recover the cluster later.

- Use S3 as "kylin.env.hdfs-working-dir" 

If you want to use S3 as storage (assume HBase is also on S3), you need configure the following parameters:

```
kylin.env.hdfs-working-dir=s3://yourbucket/kylin
kylin.storage.hbase.cluster-fs=s3://yourbucket
kylin.source.hive.redistribute-flat-table=false
```

The intermediate file and the HFile will all be written to S3. The build performance would be slower than HDFS. Make sure you have a good understanding about the difference between S3 and HDFS. Read the following articles from AWS:

[Input and Output Errors](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-errors-io.html)
[Are you having trouble loading data to or from Amazon S3 into Hive](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-error-hive.html#emr-troubleshoot-error-hive-3)


- Hadoop configurations

Some Hadoop configurations need be applied for better performance and data consistency on S3, according to [emr-troubleshoot-errors-io](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-errors-io.html)

```
<property>
  <name>io.file.buffer.size</name>
  <value>65536</value>
</property>
<property>
  <name>mapred.map.tasks.speculative.execution</name>
  <value>false</value>
</property>
<property>
  <name>mapred.reduce.tasks.speculative.execution</name>
  <value>false</value>
</property>
<property>
  <name>mapreduce.map.speculative</name>
  <value>false</value>
</property>
<property>
  <name>mapreduce.reduce.speculative</name>
  <value>false</value>
</property>

```


- Create the working-dir folder if it doesn't exist

```
hadoop fs -mkdir /kylin 
```

or

```
hadoop fs -mkdir s3://yourbucket/kylin
```

### Start Kylin

The start is the same as on normal Hadoop:

```
export KYLIN_HOME=/usr/local/kylin/apache-kylin-2.2.0-bin
$KYLIN_HOME/bin/sample.sh
$KYLIN_HOME/bin/kylin.sh start
```

Don't forget to enable the 7070 port access in the security group for EMR master - "ElasticMapReduce-master", or with SSH tunnel to the master node, then you can access Kylin Web GUI at http://\<master\-dns\>:7070/kylin

Build the sample Cube, and then run queries when the Cube is ready. You can browse S3 to see whether the data is safely persisted.

### Spark Configuration

EMR's Spark version may be incompatible with Kylin, so you couldn't directly use EMR's Spark. You need to set "SPARK_HOME" environment variable to Kylin's Spark folder (KYLIN_HOME/spark) before start Kylin. To access files on S3 or EMRFS, we need to copy EMR's implementation jars to Spark.

```
export SPARK_HOME=$KYLIN_HOME/spark

cp /usr/lib/hadoop-lzo/lib/*.jar $KYLIN_HOME/spark/jars/
cp /usr/share/aws/emr/emrfs/lib/emrfs-hadoop-assembly-*.jar $KYLIN_HOME/spark/jars/
cp /usr/lib/hadoop/hadoop-common*-amzn-*.jar $KYLIN_HOME/spark/jars/

$KYLIN_HOME/bin/kylin.sh start
```

You can also copy EMR's spark-defauts configuration to Kylin's spark for a better utilization of the cluster resources.

### Shut down EMR Cluster

Before you shut down EMR cluster, we suggest you take a backup for Kylin metadata and upload it to S3.

To shut down an Amazon EMR cluster without losing data that hasn't been written to Amazon S3, the MemStore cache needs to flush to Amazon S3 to write new store files. To do this, you can run a shell script provided on the EMR cluster. 

```
bash /usr/lib/hbase/bin/disable_all_tables.sh
```

To restart a cluster with the same HBase data, specify the same Amazon S3 location as the previous cluster either in the AWS Management Console or using the "hbase.rootdir" configuration property. For more information about EMR HBase, refer to [HBase on Amazon S3](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hbase-s3.html)

	
## Deploy Kylin in a dedicated EC2 

Running Kylin in a dedicated client node (not master, core or task) is recommended. You can start a separate EC2 instance within the same VPC and subnet as your EMR, copy the Hadoop clients from master node to it, and then install Kylin in it. This can improve the stability of services in master node as well as Kylin itself. 
	