---
layout: post-blog
title: Kylin 4 now is supporting AWS Glue Catalog
date: 2022-03-17 11:00:00
author: Xiaoxiang Yu
categories: blog
---

## Why does installing Kylin on EMR need to support AWS Glue?

### What is AWS Glue?

AWS Glue is a fully hosted ETL (Extract, Transform, and Load) service that enables AWS users to easily and cost-effectively classify, cleanse, enrich data and move data between various data storages. AWS Glue consists of a central metastore called AWS Glue Data Catalog, an ETL engine that can automatically generate code and a flexible scheduler that can handle dependency resolution, monitor jobs and retry. AWS Glue is a serverless service, so there is no infrastructure to set up or manage.

### Why does Kylin need AWS Glue Catalog?

At present, many users in the Kylin community use AWS EMR for running large-scale distributed data processing jobs on Hadoop, Spark, Hive, Presto, etc. Without AWS Glue Data Catalog, tables built on these data warehouse components (like Hive, Spark and Presto) can not be used by any other components. As the data warehouse needs to answer requirements from various business departments, they use AWS Glue Data Catalog for metadata storage when creating the AWS EMR clusters, to share the data sources among different components and business departments. That is, to build one data cube with data from each business department, so they can provide quick responses to different business requirements.
In modern companies, data is saved on cloud object storage and big data teams use AWS EMR for data processing, data analysis and model training. But with data explosion, it becomes really difficult to extract data and the response time is too long. In other words, the solution of EMR + Spark/Hive cannot meet the speedy data query requirements from data analysts, O&M personnel and sales. So some users turn to Apache Kylin as their open-source OLAP solution.
Recently, our users approached us with the request that Kylin 4 could directly read table metadata from AWS Glue. After some collaboration, now Kylin 4 supports AWS Glue Catalog, making it possible for tables and data to be shared among Hive, Presto, Spark and Kylin. This helps to break down the metadata barrier, so different topics can be combined to form a big data analysis platform.

### Does Kylin support AWS Glue?

|                                 | Kylin version which supports Glue    | Issue Link                                                   |
| ------------------------------- | ------------------------------------ | ------------------------------------------------------------ |
| Kylin on HBase (Before Kylin 4) | 2.6.6 or higher<br />3.1.0 or higher | https://issues.apache.org/jira/browse/KYLIN-4206<br />https://zhuanlan.zhihu.com/p/99481373 |
| Kylin on Parquet                | 4.0.1 or higher                      | This article.                                                |



## Prerequisites for deployment

### Software Version

| **Software** | **Version**                           | Reference                                                    |
| ------------ | ------------------------------------- | ------------------------------------------------------------ |
| Apache Kylin | 4.0.1 or higher                       | [KIP 10 refactor hive and hadoop dependency](https://cwiki.apache.org/confluence/display/KYLIN/KIP+10+refactor+hive+and+hadoop+dependency). |
| AWS EMR      | 6.5.0 or higher<br />5.33.1 or higher | [Amazon EMR release 6.5.0 - Amazon EMR](https://docs.amazonaws.cn/en_us/emr/latest/ReleaseGuide/emr-650-release.html). |

### Prepare AWS Glue database and tables

![](/images/blog/kylin4_support_aws_glue/1_prepare_aws_glue_table_en.png)

![](/images/blog/kylin4_support_aws_glue/2_prepare_aws_glue_table_en.png)

- Create an EMR cluster.

Note: Parameter hive.metastore.client.factory.class is configured to enable AWS Glue. For details, you may refer to the commands below. 

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

- Log in to the Master node. Check the Hadoop version and whether the Hadoop cluster is successfully started.

![](/images/blog/kylin4_support_aws_glue/3_prepare_hadoop_cluster_en.png)

![](/images/blog/kylin4_support_aws_glue/4_prepare_hadoop_cluster_en.png)

### (Optional)Get environmental information

> If you are using RDS or other metadata storage, you may skip this step.

RDBMS is recommended for metastore in Kylin 4. So for testing purposes, in this article, we use MariaDB which comes with the Master node for metastore; for hostname, account and password of MariaDB, see `/etc/hive/conf/hive-site.xml`.

```shell
kylin.metadata.url=kylin4_on_cloud@jdbc,url=jdbc:mysql://${HOSTNAME}:3306/hue,username=hive,password=${PASSWORD},maxActive=10,maxIdle=10,driverClassName=org.mariadb.jdbc.Driver  
kylin.env.zookeeper-connect-string=${HOSTNAME}
```

Configure the variables as per the actual information, for example, replace  ${PASSWORD} with the real password, save it locally and it will be used to start Kylin.

### Test the connectivity between Spark SQL and AWS Glue

Test whether AWS Spark SQL can access databases and table metadata through AWS Glue with Spark-SQL. For the first test, you will find that the startup fails with an error.

![](/images/blog/kylin4_support_aws_glue/5_test_sparksql_glue_en.png)

Replace `hive-site.xml` used by Spark with the following commands.

```shell
cd /etc/spark/conf
sudo mv hive-site.xml hive-site.xml.bak
sudo cp /etc/hive/conf/hive-site.xml .
```

Then change the value of `hive.execution.engine` in file `/etc/spark/conf/hive-site.xml` to `mr`, restart Spark-SQL CLI and verify whether the query for AWS Glue's table data is successful.

![](/images/blog/kylin4_support_aws_glue/6_test_sparksql_glue_en.png)

![](/images/blog/kylin4_support_aws_glue/7_test_sparksql_glue_en.png)

### (Optional) Prepare kylin-spark-engine.jar

> This issue will be fixed in Apache Kylin 4.0.2. So you can skip this step after updating to Apache Kylin 4.0.2. For users with Kylin 4.0.1, please refer to the following steps to replace kylin-spark-engine.jar:

Clone Kylin git repository, execute `mvn clean package -DskipTests` to build a new `kylin-spark-project/kylin-spark-engine/target/kylin-spark-engine-4.0.0-SNAPSHOT.jar` .

```shell
git clone https://github.com/hit-lacus/kylin.git
cd kylin
git checkout KYLIN-5160
mvn clean package -DskipTests

# find -name kylin-spark-engine-4.0.0-SNAPSHOT.jar kylin-spark-project/kylin-spark-engine/target
```

Patch link: [https://github.com/apache/kylin/pull/1819](https://github.com/apache/kylin/pull/1819)

## Deploy Kylin and connect to AWS Glue

### Download Kylin

1. Download and decompress Kylin. Please download the corresponding Kylin package according to your EMR version. That is, with EMR 5.X you can download Spark 2 package; with EMR 6.X you can download Spark 3 package.
    ```shell
    # aws s3 cp s3://${BUCKET}/apache-kylin-4.0.1-bin-spark3.tar.gz .
    # wget apache-kylin-4.0.1-bin-spark3.tar.gz
    tar zxvf apache-kylin-4.0.1-bin-spark3.tar.gz .
    cd apache-kylin-4.0.1-bin-spark3
    export KYLIN_HOME=/home/hadoop/apache-kylin-4.0.1-bin-spark3
    ```

2. (Optional) Get MariaDB driver jar
    > If you are using other databases for metastore, please skip this step.
    
    ```shell
    cd $KYLIN_HOME
    mkdir ext
    cp /usr/lib/hive/lib/mariadb-connector-java.jar $KYLIN_HOME/ext
    ```

### Prepare Spark

AWS Spark has built-in support of AWS Glue, so you will use AWS Spark when loading table metadata and building jobs. Kylin 4.0.1 supports Apache Spark officially. Because the compatibility between Apache Spark and AWS Spark is not very good, we will use Apache Spark for cube queries. To sum up, you need to switch between AWS Spark and Apache Spark according to your task (query task or build task). 

- Prepare AWS Spark

```shell
cd $KYLIN_HOME
mkdir ext
cp /usr/lib/hive/lib/mariadb-connector-java.jar $KYLIN_HOME/ext
```

- Download Apache Spark
    - Please download the corresponding Spark installation package according to your EMR version. That is, with EMR 5.X you can download Spark 2.4.7 and with EMR 6.X you can download Spark 3.1.2.
```shell
cd $KYLIN_HOME
aws s3 cp s3://${BUCKET}/spark-2.4.7-bin-hadoop2.7.tgz $KYLIN_HOME # Or downloads spark-2.4.7-bin-hadoop2.7.tgz from offical website
tar zxvf spark-2.4.7-bin-hadoop2.7.tgz
mv spark-2.4.7-bin-hadoop2.7 spark-apache
```

- First, you need to load AWS Glue table, so direct `$KYLIN_HOME/spark` to AWS Spark with soft link. Note: you do not need to set up `SPARK_HOME`, because if `$KYLIN_HOME/spark` exists and `SPARK_HOME` is not set up, Kylin will use `$KYLIN_HOME/spark` as `SPARK_HOME` by default.
  
```shell
ln -s spark-aws spark
```

### Modify Kylin startup script

1. Start Spark SQL CLI and keep it in running status.
2. Acquire PID of `SparkSQLCLIDriver` with `jps -ml ${PID}`. Then acquire `spark.driver.extraClasspath` of **Driver**. Or, you can acquire these from /etc/spark/conf/spark-defaults.conf.
    ```shell
    jps -ml | grep SparkSubmit
    jinfo ${PID} | grep "spark.driver.extraClassPath"
    ```
    ![](/images/blog/kylin4_support_aws_glue/8_kylin_start_up_script_en.png)

3. Edit `bin/kylin.sh`, modify `KYLIN_TOMCAT_CLASSPATH`  and add `kylin_driver_classpath`; save bin/kylin.sh, then exit Spark SQL CLI.

- kylin.sh before modifying

![](/images/blog/kylin4_support_aws_glue/9_kylin_start_up_script_en.png)

- For EMR 6.5.0, in the modified `kylin.sh`, `kylin_driver_classpath` is at the end of the code.

![](/images/blog/kylin4_support_aws_glue/10_kylin_start_up_script_en.png)

- For EMR 5.33.1, in the modified `kylin.sh`, `kylin_driver_classpath` is placed before `$SPARK_HOME/jars`.

![](/images/blog/kylin4_support_aws_glue/11_kylin_start_up_script_en.png)

### Configure Kylin

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

### Start Kylin and verify the building job

#### Start Kylin

```shell
cd $KYLIN_HOME
ln -s spark spark_aws # skip this step if soft link 'spark' exists 
bin/kylin.sh restart
```

![](/images/blog/kylin4_support_aws_glue/12_start_kylin_en.png)

![](/images/blog/kylin4_support_aws_glue/13_start_kylin_en.png)

#### (Optional) Replace kylin-spark-engine.jar 

> This step is only required for Kylin 4.0.1 users.

```shell
cd $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/
mv kylin-spark-engine-4.0.1.jar kylin-spark-engine-4.0.1.jar.bak # remove old one 
cp kylin-spark-engine-4.0.0-SNAPSHOT.jar  .

bin/kylin.sh restart # restart kylin to make new jar be loaded
```

#### Load AWS Glue table and build

- Load AWS Glue table metadata

![](/images/blog/kylin4_support_aws_glue/14_load_glue_meta_en.png)

![](/images/blog/kylin4_support_aws_glue/15_load_glue_meta_en.png)

- Create Model and Cube, then trigger a building job.

![](/images/blog/kylin4_support_aws_glue/16_load_glue_meta_en.png)

### Verify the query

Switch the Spark used by Kylin and restart Kylin.

```shell
cd $KYLIN_HOME
rm spark # 'spark' is a soft link, it is point to aws spark
ln -s spark_apache spark # switch from aws spark to apache spark
bin/kylin.sh restart
```

Perform a test query and this query is successful.

![](/images/blog/kylin4_support_aws_glue/17_verify_query_en.png)

## Discussion and Q&A

### Why we must use both AWS Spark and Apache Spark？

AWS Spark has built-in support for AWS Glue so you will use AWS Spark when loading table metadata and building jobs;  Kylin 4.0.1 supports Apache Spark.  Because the compatibility between Apache Spark and AWS Spark is not very good, we will use Apache Spark for cube query. To sum up, you need to switch between AWS Spark and Apache Spark according to your task (query task or build task). 

### Why do users need to modify kylin.sh?

As Spark Driver, Kylin needs to load table metadata through `aws-glue-datacatalog-spark-client.jar`, so you need to modify kylin.sh and load the relevant jar into classpath of Kylin process.

### If I faced more questions, where should I asked?

If you have any questions about using Kylin on AWS, please contact us via mailling list([user@kylin.apache.org](mailto:user@kylin.apache.org)), please check for detail [https://kylin.apache.org/community/](https://kylin.apache.org/community/) .
