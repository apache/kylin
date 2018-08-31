---
layout: docs-cn
title:  "在 AWS EMR 上安装 Kylin"
categories: install
permalink: /cn/docs/install/kylin_aws_emr.html
---

今天许多用户将 Hadoop 运行在像 AWS 这样的公有云上。Apache Kylin，由标准的 Hadoop/HBase API 编译，支持多数主流的 Hadoop 发布；现在的版本是 Kylin v2.2，支持 AWS EMR 5.0 - 5.10。本文档介绍了在 EMR 上如何运行 Kylin。

### 推荐版本
* AWS EMR 5.7 (EMR 5.8 及以上，请查看 [KYLIN-3129](https://issues.apache.org/jira/browse/KYLIN-3129))
* Apache Kylin v2.2.0 or above for HBase 1.x

### 启动 EMR 集群

使用 AWS 网页控制台，命令行或 API 运行一个 EMR 集群。在 Kylin 需要 HBase 服务的应用中选择 "**HBase**"。 

您可以选择 "HDFS" 或者 "S3" 作为 HBase 的存储，这取决于您在关闭集群之后是否需要将 Cube 数据进行存储。EMR HDFS 使用 EC2 实例的本地磁盘，当集群停止后数据将被清除，Kylin metadata 和 Cube 数据将会丢失。

如果您使用 "S3" 作为 HBase 的存储，您需要自定义配置为 "**hbase.rpc.timeout**"，由于 S3 的大容量负载是一个复制操作，当数据规模比较大时，HBase region 服务器比在 HDFS 上将花费更多的时间等待其完成。

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

### 安装 Kylin

当 EMR 集群处于 "Waiting" 状态，您可以 SSH 到 master 节点，下载 Kylin 然后解压 tar 包:

```
sudo mkdir /usr/local/kylin
sudo chown hadoop /usr/local/kylin
cd /usr/local/kylin
wget http://www-us.apache.org/dist/kylin/apache-kylin-2.2.0/apache-kylin-2.2.0-bin-hbase1x.tar.gz 
tar –zxvf apache-kylin-2.2.0-bin-hbase1x.tar.gz
```

### 配置 Kylin

启动 Kylin 前，您需要进行一组配置:

- 从 /etc/hbase/conf/hbase-site.xml 复制 "hbase.zookeeper.quorum" 属性到 $KYLIN\_HOME/conf/kylin\_job\_conf.xml，例如:


```
<property>
  <name>hbase.zookeeper.quorum</name>
  <value>ip-nn-nn-nn-nn.ap-northeast-2.compute.internal</value>
</property>
```

- 使用 HDFS 作为 "kylin.env.hdfs-working-dir" (推荐)

EMR 建议 **"当集群运行时使用 HDFS 作为中间数据的存储而 Amazon S3 只用来输入初始的数据和输出的最终结果"**。Kylin 的 'hdfs-working-dir' 用来存放 Cube building 时的中间数据，cuboid 文件和一些 metadata 文件 (例如在 Hbase 中不好的 dictionary 和 table snapshots)；因此最好为其配置 HDFS。 

如果使用 HDFS 作为 Kylin 的工作目录，您无需做任何修改，因为 EMR 的默认文件系统是 HDFS:

```
kylin.env.hdfs-working-dir=/kylin
```

关闭/重启集群前，您必须用 [S3DistCp](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html) 备份 HDFS 上 "/kylin" 路径下的数据到 S3，否则您可能丢失数据且之后不能恢复集群。

- 使用 S3 作为 "kylin.env.hdfs-working-dir" 

如果您想使用 S3 作为存储 (假设 HBase 也在 S3 上)，您需要配置下列参数:

```
kylin.env.hdfs-working-dir=s3://yourbucket/kylin
kylin.storage.hbase.cluster-fs=s3://yourbucket
kylin.source.hive.redistribute-flat-table=false
```

中间文件和 HFile 也都会写入 S3。Build 性能将会比 HDFS 慢。确保您很好的理解了 S3 和 HDFS 的区别。阅读下列来自 AWS 的文章:

[Input and Output Errors](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-errors-io.html)
[Are you having trouble loading data to or from Amazon S3 into Hive](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-error-hive.html#emr-troubleshoot-error-hive-3)


- Hadoop 配置

根据 [emr-troubleshoot-errors-io](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-errors-io.html)，为在 S3 上获得更好的性能和数据一致性需要应用一些 Hadoop 配置。 

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


- 如果不存在创建工作目录文件夹

```
hadoop fs -mkdir /kylin 
```

或

```
hadoop fs -mkdir s3://yourbucket/kylin
```

### 启动 Kylin

启动和在普通 Hadoop 上一样:

```
export KYLIN_HOME=/usr/local/kylin/apache-kylin-2.2.0-bin
$KYLIN_HOME/bin/sample.sh
$KYLIN_HOME/bin/kylin.sh start
```

别忘记在 EMR master - "ElasticMapReduce-master" 的安全组中启用 7070 端口访问，或使用 SSH 连接 master 节点，然后您可以使用 http://\<master\-dns\>:7070/kylin 访问 Kylin Web GUI。

Build 同一个 Cube，当 Cube 准备好后运行查询。您可以浏览 S3 查看数据是否安全的持久化了。

### Spark 配置

EMR 的 Spark 版本很可能与 Kylin 编译的版本不一致，因此您通常不能直接使用 EMR 打包的 Spark 用于 Kylin 的任务。 您需要在启动 Kylin 之前，将 "SPARK_HOME" 环境变量设置指向 Kylin 的 Spark 子目录 (KYLIN_HOME/spark) 。此外，为了从 Spark 中访问 S3 或 EMRFS 上的文件，您需要将 EMR 的扩展类从 EMR 的目录拷贝到 Kylin 的 Spark 下。

```
export SPARK_HOME=$KYLIN_HOME/spark

cp /usr/lib/hadoop-lzo/lib/*.jar $KYLIN_HOME/spark/jars/
cp /usr/share/aws/emr/emrfs/lib/emrfs-hadoop-assembly-*.jar $KYLIN_HOME/spark/jars/
cp /usr/lib/hadoop/hadoop-common*-amzn-*.jar $KYLIN_HOME/spark/jars/

$KYLIN_HOME/bin/kylin.sh start
```

您也可以参考 EMR Spark 的 spark-defauts 来设置 Kylin 的 Spark 配置，以获得更好的对集群资源的适配。

### 关闭 EMR 集群

关闭 EMR 集群前，我们建议您为 Kylin metadata 做备份且将其上传到 S3。

为了在关闭 Amazon EMR 集群时不丢失没写入 Amazon S3 的数据，MemStore cache 需要刷新到 Amazon S3 写入新的 store 文件。您可以运行 EMR 集群上提供的 shell 脚本来完成这个需求。 

```
bash /usr/lib/hbase/bin/disable_all_tables.sh
```

为了用同样的 Hbase 数据重启一个集群，可在 AWS Management Console 中指定和之前集群相同的 Amazon S3 位置或使用 "hbase.rootdir" 配置属性。更多的 EMR HBase 信息，参考 [HBase on Amazon S3](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hbase-s3.html)

	
## 在专用的 EC2 上部署 Kylin 

推荐在专门的 client 节点上运行 Kylin (而不是 master，core 或 task)。启动一个和您 EMR 有同样 VPC 与子网的独立 EC2 实例，从 master 节点复制 Hadoop clients 到该实例，然后在其中安装 Kylin。这可提升 Kylin 自身与 master 节点中服务的稳定性。 
	