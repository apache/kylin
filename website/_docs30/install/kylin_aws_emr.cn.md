---
layout: docs30-cn
title:  "在 AWS EMR 上安装 Kylin"
categories: install
permalink: /cn/docs30/install/kylin_aws_emr.html
---

本文档介绍了在 EMR 上如何运行 Kylin。



### 推荐版本
* AWS EMR 5.27
* Apache Kylin v3.0.0 or above for HBase 1.x



### 启动 EMR 集群

使用 AWS 网页控制台，命令行或 API 运行一个 EMR 集群。在 Kylin 需要 HBase 服务的应用中选择 **HBase**。 

您可以选择 HDFS 或者 S3 作为 HBase 的存储，这取决于您在关闭集群之后是否需要将 Cube 数据进行存储。EMR HDFS 使用 EC2 实例的本地磁盘，当集群停止后数据将被清除，Kylin 元数据和 Cube 数据将会丢失。

如果您使用 S3 作为 HBase 的存储，您需要自定义配置为 `hbase.rpc.timeout`，由于 S3 的大容量负载是一个复制操作，当数据规模比较大时，HBase Region 服务器比在 HDFS 上将花费更多的时间等待其完成。

如果您希望EMR的Hive使用一个外部的元数据，您可以考虑使用RDS或者AWS Glue。那样您就可以在云上环境构建一个stateless的OLAP服务了。

让我们通过AWS CLI创建一个EMR 集群，并且开启（当然以下几项是可选的）
1. S3作为HBase数据存储
2. AWS Glue作为Hive元数据
3. 开启S3元数据一致性以防止数据文件丢失

```
aws emr create-cluster --applications Name=Hadoop Name=Hive Name=Pig Name=HBase Name=Spark Name=Sqoop Name=Tez  Name=ZooKeeper \
	--release-label emr-5.28.0 \
	--instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":50,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Worker Node"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":100,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Node"}]' \
	--configurations '[{"Classification":"hbase","Properties":{"hbase.emr.storageMode":"s3"}},{"Classification":"hbase-site","Properties":{"hbase.rootdir":"s3://{S3_BUCKET}/hbase/data","hbase.rpc.timeout": "3600000"}},{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
	--name 'Kylin3.0Cluster_Original' \
	--emrfs Consistent=true \
	--region cn-northwest-1
```

### 支持AWS Glue作为Hive元数据存储

如果你需要开启Glue作为Hive元数据, 请参考`https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore` 来进行打包。你需要获取以下jar：

1. aws-glue-datacatalog-client-common-xxx.jar
2. aws-glue-datacatalog-hive2-client-xxx.jar


### 安装 Kylin

当 EMR 集群处于 "Waiting" 状态，您可以 SSH 到 master 节点，下载 Kylin 然后解压 tar 包:

```sh
sudo mkdir /usr/local/kylin
sudo chown hadoop /usr/local/kylin
cd /usr/local/kylin
wget http://mirror.bit.edu.cn/apache/kylin/apache-kylin-3.0.0/apache-kylin-3.0.0-bin-hbase1x.tar.gz
tar -zxvf apache-kylin-3.0.0-bin-hbase1x.tar.gz
```

### 配置 Kylin

启动 Kylin 前，您需要进行一组配置:

- 从 `/etc/hbase/conf/hbase-site.xml` 复制 `hbase.zookeeper.quorum` 属性到 `$KYLIN_HOME/conf/kylin_job_conf.xml`，例如:


```xml
<property>
  <name>hbase.zookeeper.quorum</name>
  <value>ip-nn-nn-nn-nn.ap-northeast-2.compute.internal</value>
</property>
```

- 使用 HDFS 作为 `kylin.env.hdfs-working-dir` (推荐)

EMR 建议 **当集群运行时使用 HDFS 作为中间数据的存储而 Amazon S3 只用来输入初始的数据和输出的最终结果**。Kylin 的 `hdfs-working-dir` 用来存放 Cube 构建时的中间数据，cuboid 数据文件和一些元数据文件 (例如不便于在 HBase 中存储的 `/dictionary` 和 `/table snapshots`)，因此最好为其配置 HDFS。 

如果使用 HDFS 作为 Kylin 的工作目录，您无需做任何修改，因为 EMR 的默认文件系统是 HDFS:

```properties
kylin.env.hdfs-working-dir=/kylin
```

关闭/重启集群前，您必须用 [S3DistCp](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html) 备份 HDFS 上 `/kylin` 路径下的数据到 S3，否则您可能丢失数据且之后不能恢复集群。

- 使用 S3 作为 `kylin.env.hdfs-working-dir`

如果您想使用 S3 作为存储 (假设 HBase 也在 S3 上)，您需要配置下列参数:

```properties
kylin.env.hdfs-working-dir=s3://yourbucket/kylin
kylin.storage.hbase.cluster-fs=s3://yourbucket
kylin.source.hive.redistribute-flat-table=false
```

中间文件和 HFile 也都会写入 S3。构建性能将会比 HDFS 慢。
为了很好地理解 S3 和 HDFS 的区别，请参考如下来自 AWS 的两篇文章：

[Input and Output Errors](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-errors-io.html)
[Are you having trouble loading data to or from Amazon S3 into Hive](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-error-hive.html#emr-troubleshoot-error-hive-3)


- Hadoop 配置

根据 [emr-troubleshoot-errors-io](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-errors-io.html)，为在 S3 上获得更好的性能和数据一致性需要应用一些 Hadoop 配置。 

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


- 如果不存在创建工作目录文件夹

```sh
hadoop fs -mkdir /kylin 
```

或

```sh
hadoop fs -mkdir s3://yourbucket/kylin
```

### 解决包冲突

- 将以下内容添加到 ~/.bashrc

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

- 暂时删除 joda.jar

```sh
mv $HIVE_HOME/lib/jackson-datatype-joda-2.4.6.jar $HIVE_HOME/lib/jackson-datatype-joda-2.4.6.jar.backup
```

- 修改 bin/kylin.sh

将以下内容添加到 bin/kylin.sh的 开始

```sh
export HBASE_CLASSPATH_PREFIX=${tomcat_root}/bin/bootstrap.jar:${tomcat_root}/bin/tomcat-juli.jar:${tomcat_root}/lib/*:$hive_dependency:$HBASE_CLASSPATH_PREFIX
```

### 开启支持Glue作为Hive数据源(可选的)
- 把`aws-glue-datacatalog-client-common-xxx.jar`和`aws-glue-datacatalog-hive2-client-xxx.jar`放到 `$KYLIN_HOME/lib`目录下
- 在`kylin.properties`中修改`kylin.source.hive.metadata-type=gluecatalog`

### 配置 Spark

- 对Spark进行打包

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

- 在 `kylin.properties`设置`kylin.engine.spark-conf.spark.yarn.archive=PATH_TO_SPARK_LIB` 

### 启动 Kylin

启动和在普通 Hadoop 上一样:

```sh
$KYLIN_HOME/bin/sample.sh
$KYLIN_HOME/bin/kylin.sh start
```

别忘记在 EMR master - "ElasticMapReduce-master" 的安全组中启用 7070 端口访问，或使用 SSH 连接 master 节点，然后您可以使用 `http://<master-dns>:7070/kylin` 访问 Kylin Web GUI。

Build 同一个 Cube，当 Cube 准备好后运行查询。您可以浏览 S3 查看数据是否安全的持久化了。

### 关闭 EMR 集群

关闭 EMR 集群前，我们建议您为 Kylin metadata 做备份且将其上传到 S3。

为了在关闭 Amazon EMR 集群时不丢失没写入 Amazon S3 的数据，MemStore cache 需要刷新到 Amazon S3 写入新的 store 文件。您可以运行 EMR 集群上提供的 shell 脚本来完成这个需求。 

```sh
bash /usr/lib/hbase/bin/disable_all_tables.sh
```

为了用同样的 Hbase 数据重启一个集群，可在 AWS Management Console 中指定和之前集群相同的 Amazon S3 位置或使用 `hbase.rootdir` 配置属性。更多的 EMR HBase 信息，参考 [HBase on Amazon S3](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hbase-s3.html)

	
### 在专用的 EC2 上部署 Kylin 

推荐在专门的 client 节点上运行 Kylin (而不是 master，core 或 task)。启动一个和您 EMR 有同样 VPC 与子网的独立 EC2 实例，从 master 节点复制 Hadoop clients 到该实例，然后在其中安装 Kylin。这可提升 Kylin 自身与 master 节点中服务的稳定性。 
	
### 其他问题

如果将S3配置为您的working-dir，并且发现了"Wrong FS"异常，请尝试修改 `$KYLIN_HOME/conf/kylin_hive_conf.xml`，`/etc/hive/conf/hive-site.xml`，`/etc/hadoop/conf/core-site.xml`。 

```xml
  <property>
    <name>fs.defaultFS</name>
    <value>s3://{YOUR_BUCKET}</value>
    <!--<value>hdfs://ip-172-31-6-58.cn-northwest-1.compute.internal:8020</value>-->
  </property>
```