---
layout: docs31
title:  "Install Kylin on AWS EMR"
categories: install
permalink: /docs31/install/kylin_aws_emr.html
---

This document introduces how to run Kylin on EMR.

### Recommended Version

* AWS EMR 5.27 or later 
* Apache Kylin v3.0.0 or above for HBase 1.x


### Start EMR cluster

Launch an EMR cluster with AWS web console, command line or API. Select *HBase* in the applications as Kylin need HBase service. 

You can choose "HDFS" or "S3" as the storage for HBase, depending on whether you need Cube data be persisted after shutting down the cluster. EMR HDFS uses the local disk of EC2 instances, which will erase the data when cluster is stopped, then Kylin metadata and Cube data will be lost.
If you use S3 as HBase's storage, you need customize its configuration for `hbase.rpc.timeout`, because the bulk load to S3 is a copy operation, when data size is huge, HBase region server need wait much longer to finish than on HDFS.
If you want your metadata of Hive is persisted outside of EMR cluster, you can choose AWS Glue or RDS of the metadata of Hive. Thus you can build a state-less OLAP service by Kylin in cloud.

Let create a demo EMR cluster via AWS CLI，with 
1. S3 as HBase storage (optional)
2. Glue as Hive Metadata (optional)
3. Enable consist metadata of S3 to make sure data wouldn't lose (optional)

```
aws emr create-cluster --applications Name=Hadoop Name=Hive Name=Pig Name=HBase Name=Spark Name=Sqoop Name=Tez  Name=ZooKeeper \
	--release-label emr-5.28.0 \
	--instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":50,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Worker Node"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":100,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Node"}]' \
	--configurations '[{"Classification":"hbase","Properties":{"hbase.emr.storageMode":"s3"}},{"Classification":"hbase-site","Properties":{"hbase.rootdir":"s3://{S3_BUCKET}/hbase/data","hbase.rpc.timeout": "3600000"}},{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
	--name 'Kylin3.0Cluster_Original' \
	--emrfs Consistent=true \
	--region cn-northwest-1
```

### Support Glue as metadata of Hive

If you want to enable support read metadata from Glue, please refer to `https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore` and build two jars.

1. aws-glue-datacatalog-client-common-xxx.jar
2. aws-glue-datacatalog-hive2-client-xxx.jar

### Install Kylin

When EMR cluster is in "Waiting" status, you can SSH into its master  node, download Kylin and then uncompress the tar-ball file:

```sh
sudo mkdir /usr/local/kylin
sudo chown hadoop /usr/local/kylin
cd /usr/local/kylin
wget http://mirror.bit.edu.cn/apache/kylin/apache-kylin-3.0.0/apache-kylin-3.0.0-bin-hbase1x.tar.gz
tar -zxvf apache-kylin-3.0.0-bin-hbase1x.tar.gz
```

### Configure Kylin

Before start Kylin, you need do a couple of configurations:

- Copy `hbase.zookeeper.quorum` property from `/etc/hbase/conf/hbase-site.xml` to `$KYLIN_HOME/conf/kylin_job_conf.xml`, like this:


```xml
<property>
  <name>hbase.zookeeper.quorum</name>
  <value>ip-nn-nn-nn-nn.ap-northeast-2.compute.internal</value>
</property>
```

- Use HDFS as `kylin.env.hdfs-working-dir` (Recommended)

EMR recommends to *use HDFS for intermediate data storage while the cluster is running and Amazon S3 only to input the initial data and output the final results*. Kylin's 'hdfs-working-dir' is for putting the intermediate data for Cube building, cuboid files and also some metadata files (like dictionary and table snapshots which are not good in HBase); so it is best to configure HDFS for this. 

If using HDFS as Kylin working directory, you just leave configurations unchanged as EMR's default FS is HDFS:

```properties
kylin.env.hdfs-working-dir=/kylin
```

Before you shutdown/restart the cluster, you must backup the "/kylin" data on HDFS to S3 with [S3DistCp](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html), or you may lost data and couldn't recover the cluster later.

- Use S3 as `kylin.env.hdfs-working-dir`

If you want to use S3 as storage (assume HBase is also on S3), you need configure the following parameters:

```properties
kylin.env.hdfs-working-dir=s3://yourbucket/kylin
kylin.storage.hbase.cluster-fs=s3://yourbucket
kylin.source.hive.redistribute-flat-table=false
```

The intermediate file and the HFile will all be written to S3. The build performance would be slower than HDFS. Make sure you have a good understanding about the difference between S3 and HDFS. Read the following articles from AWS:

[Input and Output Errors](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-errors-io.html)
[Are you having trouble loading data to or from Amazon S3 into Hive](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-error-hive.html#emr-troubleshoot-error-hive-3)


- Hadoop configurations

Some Hadoop configurations need be applied for better performance and data consistency on S3, according to [emr-troubleshoot-errors-io](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-errors-io.html)

```xml
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

### Solve jar conflict
- Add following env variable in ~/.bashrc

```sh
export HIVE_HOME=/usr/lib/hive
export HADOOP_HOME=/usr/lib/hadoop
export HBASE_HOME=/usr/lib/hbase
export SPARK_HOME=/usr/lib/spark

export KYLIN_HOME=/home/ec2-user/apache-kylin-3.0.0-SNAPSHOT-bin
export HCAT_HOME=/usr/lib/hive-hcatalog
export KYLIN_CONF_HOME=$KYLIN_HOME/conf
export tomcat_root=$KYLIN_HOME/tomcat
export hive_dependency=$HIVE_HOME/conf:$HIVE_HOME/lib/:$HIVE_HOME/lib/hive-hcatalog-core.jar:$SPARK_HOME/jars/
export PATH=$KYLIN_HOME/bin:$PATH

export hive_dependency=$HIVE_HOME/conf:$HIVE_HOME/lib/*:$HIVE_HOME/lib/hive-hcatalog-core.jar:/usr/share/aws/hmclient/lib/*:$SPARK_HOME/jars/*:$HBASE_HOME/lib/*.jar:$HBASE_HOME/*.jar
```

- Remove joda.jar

```sh
mv $HIVE_HOME/lib/jackson-datatype-joda-2.4.6.jar $HIVE_HOME/lib/jackson-datatype-joda-2.4.6.jar.backup
```

- Modify bin/kylin.sh
Add following content on the top of bin/kylin.sh

```sh
export HBASE_CLASSPATH_PREFIX=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*:$hive_dependency:$HBASE_CLASSPATH_PREFIX
```

### Enable glue as metadata for Hive(Optional)
1. Put `aws-glue-datacatalog-client-common-xxx.jar` and `aws-glue-datacatalog-hive2-client-xxx.jar` under $KYLIN_HOME/lib.
2. Set `kylin.source.hive.metadata-type=gluecatalog` in `kylin.properties`

### Configure Spark

- Build a Spark's flat jar 

```sh
rm -rf $KYLIN_HOME/spark_jars
mkdir $KYLIN_HOME/spark_jars
cp /usr/lib/spark/jars/*.jar $KYLIN_HOME/spark_jars
cp -f /usr/lib/hbase/lib/*.jar $KYLIN_HOME/spark_jars

rm -f netty-3.9.9.Final.jar 
rm -f netty-all-4.1.8.Final.jar

 jar cv0f spark-libs.jar -C $KYLIN_HOME/spark_jars .
aws s3 cp spark-libs.jar s3://{YOUR_BUCKET}/kylin/package/  # You choose s3 as your working-dir
hadoop fs -put spark-libs.jar hdfs://kylin/package/  # You choose hdfs as your working-dir
```
- Set `kylin.engine.spark-conf.spark.yarn.archive=PATH_TO_SPARK_LIB` in `kylin.properties`


### Start Kylin

The start is the same as on normal Hadoop:

```sh
$KYLIN_HOME/bin/sample.sh
$KYLIN_HOME/bin/kylin.sh start
```

Don't forget to enable the 7070 port access in the security group for EMR master - "ElasticMapReduce-master", or with SSH tunnel to the master node, then you can access Kylin Web GUI at http://\<master\-dns\>:7070/kylin

Build the sample Cube, and then run queries when the Cube is ready. You can browse S3 to see whether the data is safely persisted.

### Shut down EMR Cluster

Before you shut down EMR cluster, we suggest you take a backup for Kylin metadata and upload it to S3.

To shut down an Amazon EMR cluster without losing data that hasn't been written to Amazon S3, the MemStore cache needs to flush to Amazon S3 to write new store files. To do this, you can run a shell script provided on the EMR cluster. 

```sh
bash /usr/lib/hbase/bin/disable_all_tables.sh
```

To restart a cluster with the same HBase data, specify the same Amazon S3 location as the previous cluster either in the AWS Management Console or using the "hbase.rootdir" configuration property. For more information about EMR HBase, refer to [HBase on Amazon S3](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hbase-s3.html)
	
### Deploy Kylin in a dedicated EC2 

Running Kylin in a dedicated client node (not master, core or task) is recommended. You can start a separate EC2 instance within the same VPC and subnet as your EMR, copy the Hadoop clients from master node to it, and then install Kylin in it. This can improve the stability of services in master node as well as Kylin itself. 
	
### Trouble shotting

- If you set S3 as your working dir and find some "Wrong FS" exception in kylin.log(if you enable shrunken dictionary), please try to modify $KYLIN_HOME/conf/kylin_hive_conf.xml, /etc/hive/conf/hive-site.xml, /etc/hadoop/conf/core-site.xml.

```xml
  <property>
    <name>fs.defaultFS</name>
    <value>s3://{YOUR_BUCKET}</value>
    <!--<value>hdfs://ip-172-31-6-58.cn-northwest-1.compute.internal:8020</value>-->
  </property>
```