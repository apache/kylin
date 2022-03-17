---
layout: post-blog
title: 安排！Kylin 4 现已支持 AWS Glue Catalog
date: 2022-03-17 11:00:00
author: Xiaoxiang Yu
categories: cn_blog
---

## 为什么在 EMR 部署 Kylin 需要支持 Glue ？

### 什么是 AWS Glue？

AWS Glue 是一项完全托管的 ETL（提取、转换和加载）服务，使 AWS 用户能够轻松而经济高效地对数据进行分类、清理和扩充，并在各种数据存储之间可靠地移动数据。AWS Glue 由一个称为 AWS Glue 数据目录的中央元数据存储库、一个自动生成代码的 ETL 引擎以及一个处理依赖项解析、作业监控和重试的灵活计划程序组成。AWS Glue 是无服务器服务，因此无需设置或管理基础设施。

### Kylin 为什么需要支持 AWS Glue Catalog？

目前社区有很多 Kylin 用户在使用 AWS EMR，组件主要包括 Hadoop、Spark、Hive、Presto 等，如果没有配置使用 AWS Glue data Catalog，那么在各个数据仓库组件如 Hive、Spark、Presto 建的数据表，在其它组件上是找不到的，也就不能使用，公司底层的数据仓库是提供给各个业务部门来进行使用，为了解决这个问题，在创建 AWS EMR 集群时就可以使用 AWS Glue data Catalog 来存储元数据，对各个组件共享数据源，对各个业务部门进行共享数据源，将各个业务部门的数据构建成一个大的数据立方体，能够快速响应公司高速发展的业务需求。
现代公司的数据都是基于云平台搭建，大数据团队使用的 AWS EMR 来进行数据加工、数据分析、以及模型训练，随着数据暴增带来提数慢、提数难，EMR/Spark/Hive 很难满足数据分析师、运营人员、销售的快速查询数据的需求，于是一些用户选择了 Apache Kylin 作为开源 OLAP 解决方案。
但是最近社区用户联系到我们，告知 Kylin 4 还不支持从 Glue 读取表元数据，所以我们和社区用户合作一起检查这里遇到的问题并最终解决了问题，从而使得 Kylin 4 支持了 AWS Glue Catalog，这样带来的好处在于 Hive、Presto、Spark、Kylin 中可以共享表和数据，使得每个主题都串联起来形成一个大的数据分析平台，打破元数据障碍。

### Apache Kylin 支持 AWS Glue 吗？

|                                 | 支持 Glue 的 Kylin 版本                | Issue Link                                                   |
| ------------------------------- | ------------------------------------ | ------------------------------------------------------------ |
| Kylin on HBase (Before Kylin 4) | 2.6.6 or higher<br/> 3.1.0 or higher | https://issues.apache.org/jira/browse/KYLIN-4206<br />https://zhuanlan.zhihu.com/p/99481373 |
| Kylin on Parquet                | 4.0.1 or higher                      | 本文。                                           |



## 部署前准备

### 软件信息一览

| **Software** | **Version**                           | Reference                                                    |
| ------------ | ------------------------------------- | ------------------------------------------------------------ |
| Apache Kylin | 4.0.1 or higher                       | 必须是 4.0.1 以及上，详情参考 [KIP 10 refactor hive and hadoop dependency](https://cwiki.apache.org/confluence/display/KYLIN/KIP+10+refactor+hive+and+hadoop+dependency). |
| AWS EMR      | 6.5.0 or higher<br />5.33.1 or higher | 覆盖EMR 6 / EMR 5 的较新版本，[Amazon EMR release 6.5.0 - Amazon EMR](https://docs.amazonaws.cn/en_us/emr/latest/ReleaseGuide/emr-650-release.html). |

### 准备 Glue 数据库和表

![](/images/blog/kylin4_support_aws_glue/1_prepare_aws_glue_table_en.png)

![](/images/blog/kylin4_support_aws_glue/2_prepare_aws_glue_table_en.png)

- 创建 AWS EMR 集群。

这里启动一个 EMR 的集群，需要注意的是，这里通过配置 `hive.metastore.client.factory.class` 启动了 Glue 外部元数据。以下命令可以作为参考。

```shell
aws emr create-cluster --applications Name=Hadoop Name=Hive Name=Spark Name=ZooKeeper Name=Tez Name=Ganglia \
  --ec2-attributes ${} \
  --release-label emr-6.5.0 \
  --log-uri ${} \
  --instance-groups ${} \
  --configurations '[{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
  --auto-scaling-role EMR_AutoScaling_DefaultRole \
  --ebs-root-volume-size 100 \
  --service-role EMR_DefaultRole \
  --enable-debugging \
  --name 'Kylin4_on_EMR65_with_Glue' \
  --region cn-northwest-1
```

- 登录 Master 节点，并且检查 Hadoop 版本 和 Hadoop 集群是否启动成功。

![](/images/blog/kylin4_support_aws_glue/3_prepare_hadoop_cluster_en.png)

![](/images/blog/kylin4_support_aws_glue/4_prepare_hadoop_cluster_en.png)

### 获取环境信息（Optional）

> 如果你使用 RDS 或者其他元数据存储，请酌情跳过此步。

由于 Kylin 4.X 推荐使用 RDBMS 作为元数据存储，处于测试目的，这里使用 Master 节点自带的 MariaDB 作为元数据存储；关于 MariaDB 的主机名称、账号、密码等信息，可以从 `/etc/hive/conf/hive-site.xml` 获取。

```shell
kylin.metadata.url=kylin4_on_cloud@jdbc,url=jdbc:mysql://${HOSTNAME}:3306/hue,username=hive,password=${PASSWORD},maxActive=10,maxIdle=10,driverClassName=org.mariadb.jdbc.Driver  
kylin.env.zookeeper-connect-string=${HOSTNAME}
```

获取这些信息后，并且替换以上 Kylin 配置项里面的变量，如 `${PASSWORD}`，保存到本地，供下一步启动 Kylin 进程使用。

### 测试 Spark SQL 和 AWS Glue 的连通性

通过 spark-sql 来测试 AWS 的 Spark SQL 是否能够通过 Glue 获取数据库和表的元数据，首次会发现启动报错失败。

![](/images/blog/kylin4_support_aws_glue/5_test_sparksql_glue_en.png)

其通过以下命令替换 Spark 使用的 `hive-site.xml`。

```shell
cd /etc/spark/conf
sudo mv hive-site.xml hive-site.xml.bak
sudo cp /etc/hive/conf/hive-site.xml .
```

并且修改 `/etc/spark/conf/hive-site.xml` 文件中 `hive.execution.engine` 的值为`mr`，再次尝试启动 Spark-SQL CLI，验证对 Glue 的表数据执行查询成功。

![](/images/blog/kylin4_support_aws_glue/6_test_sparksql_glue_en.png)

![](/images/blog/kylin4_support_aws_glue/7_test_sparksql_glue_en.png)

### 准备 kylin-spark-engine.jar（Optional）

> 如果 Apache Kylin 4.0.2 已经发布，那么应该已经修改该问题，可以跳过此步。否则请参考以下步骤，替换 `kylin-spark-engine.jar`：

参考下面的命令，克隆 kylin 仓库，执行 `mvn clean package -DskipTests`，获取 `kylin-spark-project/kylin-spark-engine/target/kylin-spark-engine-4.0.0-SNAPSHOT.jar` 。

```shell
git clone https://github.com/hit-lacus/kylin.git
cd kylin
git checkout KYLIN-5160
mvn clean package -DskipTests

# find -name kylin-spark-engine-4.0.0-SNAPSHOT.jar kylin-spark-project/kylin-spark-engine/target
```

Patch link: [https://github.com/apache/kylin/pull/1819](https://github.com/apache/kylin/pull/1819)

## 部署 Kylin 并连接 Glue

### 下载 Kylin

1. 下载并解压 Kylin ，请根据 EMR 的版本选择对应的 Kylin package，具体来说，EMR 5.X 使用 spark2 的 package，EMR 6.X 使用 spark3 的 package。
    ```shell
    # aws s3 cp s3://${BUCKET}/apache-kylin-4.0.1-bin-spark3.tar.gz .
    # wget apache-kylin-4.0.1-bin-spark3.tar.gz
    tar zxvf apache-kylin-4.0.1-bin-spark3.tar.gz .
    cd apache-kylin-4.0.1-bin-spark3
    export KYLIN_HOME=/home/hadoop/apache-kylin-4.0.1-bin-spark3
    ```

2. 获取 RDBMS 的 驱动 jar（Optional）

    > 如果你是用别的 RDBMS 作为元数据存储，请跳过此步骤。
    
    ```shell
    cd $KYLIN_HOME
    mkdir ext
    cp /usr/lib/hive/lib/mariadb-connector-java.jar $KYLIN_HOME/ext
    ```

### 准备 Spark

由于 AWS Spark 内置对 AWS Glue 的支持，所以 **加载表元数据和执行构建需要使用 AWS Spark**；但是考虑到 Kylin 4.0.1 是支持 Apache Spark，并且 AWS Spark 相对 Apache Spark 有比较大的代码修改，两者兼容性较差，所以**查询 Cube 需要使用 Apache Spark**。综上所述，需要根据 Kylin 需要执行查询任务还是构建任务，来切换所使用的的 Spark。

- 准备 AWS Spark

```shell
cd $KYLIN_HOME
mkdir ext
cp /usr/lib/hive/lib/mariadb-connector-java.jar $KYLIN_HOME/ext
```

- 准备 Apache Spark
    - 请根据 EMR 的版本选择对应的 Spark  版本安装包，具体来说，EMR 5.X 使用 `Spark 2.4.7` 的 Spark 安装包，EMR 6.X 使用 `Spark 3.1.2` 的 Spark 安装包。
```shell
cd $KYLIN_HOME
aws s3 cp s3://${BUCKET}/spark-2.4.7-bin-hadoop2.7.tgz $KYLIN_HOME # Or downloads spark-2.4.7-bin-hadoop2.7.tgz from offical website
tar zxvf spark-2.4.7-bin-hadoop2.7.tgz
mv spark-2.4.7-bin-hadoop2.7 spark-apache
```

- 因为要先加载 Glue 表，所以这里通过软链接将`$KYLIN_HOME/spark`指向 AWS Spark；请注意无需设置 `SPARK_HOME`，因为在 `$KYLIN_HOME/spark` 存在并且 `SPARK_HOME` 未设置的情况下，Kylin 会默认使用 `$KYLIN_HOME/spark` 。
  
```shell
ln -s spark-aws spark
```

### 修改 Kylin 启动脚本

1. 启动 Spark SQL CLI，不退出
2. 通过 `jps -ml ${PID}` 获取 `SparkSQLCLIDriver` 的 PID，然后获取 Driver 的 `spark.driver.extraClasspath`。或者也可以从 `/etc/spark/conf/spark-defaults.conf` 获取。
    ```shell
    jps -ml | grep SparkSubmit
    jinfo ${PID} | grep "spark.driver.extraClassPath"
    ```
    ![](/images/blog/kylin4_support_aws_glue/8_kylin_start_up_script_en.png)

3. 编辑 `bin/kylin.sh`，修改 `KYLIN_TOMCAT_CLASSPATH` 变量，追加 `kylin_driver_classpath` ；保存好 `bin/kylin.sh` 后退出 Spark SQL CLI

- 修改前的 kylin.sh

![](/images/blog/kylin4_support_aws_glue/9_kylin_start_up_script_en.png)

- 针对 EMR 6.5.0，修改后的 kylin.sh：`kylin_driver_classpath` 放到最后。

![](/images/blog/kylin4_support_aws_glue/10_kylin_start_up_script_en.png)

- 针对 EMR 5.33.1，修改后的 kylin.sh：`kylin_driver_classpath` 放到 `$SPARK_HOME/jars` 之前。

![](/images/blog/kylin4_support_aws_glue/11_kylin_start_up_script_en.png)

### 配置 Kylin

```shell
cd $KYLIN_HOME
vim conf/kylin.properties 
```

#### Minimal Kylin Configuration

| Property Key                                        | Property Value(Example)                                      | Notes                                                        |
| --------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| kylin.metadata.url                                  | kylin4_on_cloud@jdbc,url=jdbc:mysql://${HOSTNAME}:3306/hue,username=hive,password=${PASSWORD},maxActive=10,maxIdle=10,driverClassName=org.mariadb.jdbc.Driver | N/A                                                          |
| kylin.env.zookeeper-connect-string                  | ${HOSTNAME}                                                  | N/A                                                          |
| kylin.engine.spark-conf.spark.driver.extraClassPath | /usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/goodies/lib/emr-spark-goodies.jar:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar | Copied from spark.driver.extraClasspath in /etc/spark/conf/spark-default.conf |

### 启动 Kylin 并验证构建

#### 启动 Kylin

```shell
cd $KYLIN_HOME
ln -s spark spark_aws # skip this step if soft link 'spark' exists 
bin/kylin.sh restart
```

![](/images/blog/kylin4_support_aws_glue/12_start_kylin_en.png)

![](/images/blog/kylin4_support_aws_glue/13_start_kylin_en.png)

#### 替换 kylin-spark-engine.jar (Optional)

> 仅对于 4.0.1 需要操作该步骤。

```shell
cd $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/
mv kylin-spark-engine-4.0.1.jar kylin-spark-engine-4.0.1.jar.bak # remove old one 
cp kylin-spark-engine-4.0.0-SNAPSHOT.jar  .

bin/kylin.sh restart # restart kylin to make new jar be loaded
```

#### 加载 Glue 表、构建

- 加载 Glue 表元数据

![](/images/blog/kylin4_support_aws_glue/14_load_glue_meta_en.png)

![](/images/blog/kylin4_support_aws_glue/15_load_glue_meta_en.png)

- 创建 Model 和 Cube，然后触发构建

![](/images/blog/kylin4_support_aws_glue/16_load_glue_meta_en.png)

### 验证查询

切换 Kylin 使用的 Spark，重启 Kylin。

```shell
cd $KYLIN_HOME
rm spark # 'spark' is a soft link, it is point to aws spark
ln -s spark_apache spark # switch from aws spark to apache spark
bin/kylin.sh restart
```

执行测试查询，查询成功

![](/images/blog/kylin4_support_aws_glue/17_verify_query_en.png)

## 讨论和问答

### 为什么必须使用两个 Spark（AWS Spark & Apache Spark）？

由于 AWS Spark 内置对 AWS Glue Catalog 的支持，并且加载表和构建引擎需要获取表，所以**加载表元数据和执行构建需要使用 AWS Spark**；但是考虑到 Kylin 4.0.1 是支持 Apache Spark，并且 AWS Spark 相对 Apache Spark 有比较大的代码修改，造成两者兼容性较差，所以**查询 Cube 需要使用 Apache Spark**。综上所述，需要根据 Kylin 需要执行查询任务还是构建任务，来切换所使用的的 Spark。
在实际使用过程中，可以考虑 Job Node（构建任务）使用 AWS Spark，Query Node（查询任务）使用 Apache Spark。

### 为什么需要修改 kylin.sh？

Kylin 进程作为 Spark Driver 需要通过`aws-glue-datacatalog-spark-client.jar`加载表元数据，所以这块需要修改 kylin.sh，将相关 jar 加载到 Kylin 进程的 classpath。
