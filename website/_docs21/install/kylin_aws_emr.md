---
layout: docs21
title:  "Install Kylin on AWS EMR"
categories: install
permalink: /docs21/install/kylin_aws_emr.html
---

Many users run Hadoop on public Cloud like AWS today. Apache Kylin, compiled with standard Hadoop/HBase API, support most main stream Hadoop releases; The current version Kylin v2.2, supports AWS EMR 5.0 to 5.7. This document introduces how to run Kylin on EMR.

For Chinese reader, you can also refer to [this AWS blog](https://aws.amazon.com/cn/blogs/china/using-apache-kylin-and-amazon-emr-to-proceed-olap-analysis-on-cloudc/), which was written with Kylin 2.0, but the main steps are the same.

### Recommended Version
* AWS EMR 5.7
* Apache Kylin v2.2.0 for HBase 1.x

### Start EMR cluster

Launch an EMR cluser with AWS web console, command line or API. Select "**HBase**" in the applications as Kylin need HBase service. 

You can select "HDFS" or "S3" as the storage for HBase, depending on whether you need Cube data be persisted after shutting down the cluster. EMR HDFS uses the local disk of EC2 instances, which will erase the data when cluster is stopped, then Kylin metadata and Cube data can be lost.

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

- Use HDFS as "kylin.env.hdfs-working-dir"

If using HDFS as Kylin working directory, you can leave configurations unchanged as EMR's default FS is HDFS:

```
kylin.env.hdfs-working-dir=/kylin
```

This will be very similar as on-premises deployment.

- Use S3 as "kylin.env.hdfs-working-dir"

Configure the following 2 parameters:

```
kylin.env.hdfs-working-dir=s3://yourbucket/kylin
kylin.storage.hbase.cluster-fs=s3://yourbucket

```
Then Kylin will use S3 for Cube building, big metadata file and Cube. The performance might be slower than HDFS.

- Create the working-dir folder if it doesn't exist

```
hadoop fs -mkdir /kylin 
or
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

### Shut down EMR Cluster

Before you shut down EMR cluster, we suggest you take a backup for Kylin metadata and upload it to S3.

To shut down an Amazon EMR cluster without losing data that hasn't been written to Amazon S3, the MemStore cache needs to flush to Amazon S3 to write new store files. To do this, you can run a shell script provided on the EMR cluster. 

```
bash /usr/lib/hbase/bin/disable_all_tables.sh
```

To restart a cluster with the same HBase data, specify the same Amazon S3 location as the previous cluster either in the AWS Management Console or using the "hbase.rootdir" configuration property. For more information about EMR HBase, refer to [HBase on Amazon S3](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hbase-s3.html)

	
## Deploy Kylin in a dedicated EC2 

Running Kylin in a dedicated client node (not master, core or task) is recommended. You can start a separate EC2 instance within the same VPC and subnet as your EMR, copy the Hadoop clients from master node to it, and then install Kylin in it. This can improve the stability of services in master node as well as Kylin itself. 
	
## Known issues on EMR
* [KYLIN-3028](https://issues.apache.org/jira/browse/KYLIN-3028)
* [KYLIN-3032](https://issues.apache.org/jira/browse/KYLIN-3032)
