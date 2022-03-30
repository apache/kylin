## Prepare files manually

#### **(Optional)** II. Manually Download packages & Upload them to the S3 path which suffix is `*/tar`

> Note:
>
> ​	This step will automatically do by tools. So you can skip this step, or you want to check packages by yourself.



> Create the directory named `tar` **in the path which was created by yourself**.  For example, the full path would be `s3://.../kylin4-aws-test/tar`.
>

1. Download the Kylin4 package on the [official website](https://kylin.apache.org/download/).
2. Download Hadoop, [version 3.2.0](https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz).
3. Download Spark with hadoop3.2, [version 3.1.1](https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz).
4. Download Hive, [version 2.3.9](https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz).
5. Download Zookeeper, [version 3.4.9.](https://archive.apache.org/dist/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz)
6. Download JDK, [version 1.8_301](https://www.oracle.com/java/technologies/javase/javase8u211-later-archive-downloads.html).
7. Download Node Exporter, [version 1.3.1.](https://github.com/prometheus/node_exporter/releases/download/v1.3.1/node_exporter-1.3.1.linux-amd64.tar.gz)
8. Download Prometheus Server, [version 2.31.1](https://github.com/prometheus/prometheus/releases/download/v2.31.1/prometheus-2.31.1.linux-amd64.tar.gz).
9. Download the Kylin4 package with local cache + soft affinity feature by the [public website.](https://s3.cn-north-1.amazonaws.com.cn/public.kyligence.io/kylin/tar/apache-kylin-4.0.0-bin-spark3-soft.tar.gz)



> Note:
> 	If you want to use Kylin4 with a local cache + soft affinity feature, please download the `experimental` package above.

![tars](../images/tars.png)



#### (Optional) III. Upload  `backup/jars/*` to the S3 Path which suffix is `*/jars`

> Note:
>
> ​	This step will automatically do by tools. So you can skip this step, or you want to check jars by yourself.



> Create the directory named `jars` **in the path which was created by yourself**.  For example, the full path would be `s3://.../kylin4-aws-test/jars`.
>

Kylin4 needed extra jars

- Basic jars
  - commons-configuration-1.3.jar
  - mysql-connector-java-5.1.40.jar
- Local Cache + Soft Affinity feature needed jars
  - alluxio-2.6.1-client.jar
  - kylin-soft-affinity-cache-4.0.0-SNAPSHOT.jar

![jars](../images/jars.png)

#### (Optional) IV. Upload `backup/scripts/*` to the S3 Path which suffix is `*/scripts`

> Note:
>
> ​	This step will automatically do by tools. So you can skip this step, or you want to check scripts by yourself.



> Create the directory named `scripts` **in the path which was created by yourself**.  For example, the full path would be `s3://.../kylin4-aws-test/scripts`.
>

Scripts:

- prepare-ec2-env-for-kylin4.sh
- prepare-ec2-env-for-spark-master.sh
- prepare-ec2-env-for-spark-slave.sh
- prepare-ec2-env-for-static-services.sh
- prepare-ec2-env-for-zk.sh

![scripts](../images/scripts.png)
