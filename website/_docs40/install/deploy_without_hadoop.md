---
layout: docs40
title:  "Deploy kylin on AWS EC2 without hadoop"
categories: install
permalink: /docs40/install/deploy_without_hadoop.html
---

Compared with Kylin 3.x, Kylin 4.0 implements a new Spark build engine and parquet storage, making it possible for Kylin to deploy without Hadoop environment. Compared with deploying Kylin 3.x on AWS EMR, deploying kylin4 directly on AWS EC2 instances has the following advantages:
1. Cost saving. Compared with AWS EMR node, AWS EC2 node has lower cost.
2. More flexible. On the EC2 node, users can more independently select the services and components they need for installation and deployment.
3. Remove Hadoop dependency. Hadoop ecology is heavy and needs to be maintained at a certain labor cost. Remove hadoop can be closer to the cloud-native.

After realizing the feature of supporting build and query in Spark Standalone mode, we tried to deploy Kylin 4.0 without Hadoop on the EC2 instance of AWS, and successfully built the cube and query.

### Environment preparation
- Apply for AWS EC2 Linux instances as required
- Create Amazon RDS for MySQL as kylin and hive metabases
- S3 as kylin's storage

### Component version information
The component version information provided here is that we selected during the test. If users need to use other versions for deployment, you can replace them by yourself and ensure the compatibility between component versions.

* JDK 1.8
* Hive 2.3.9
* Zookeeper 3.4.13
* Kylin 4.0 for spark3
* Spark 3.1.1
* Hadoop 3.2.0（No startup required）

### Deployment process

#### 1 Configure environment variables
- Modify profile

  ```shell
  vim /etc/profile
  
  # Add the following at the end of the profile file
  export JAVA_HOME=/usr/local/java/jdk1.8.0_291
  export JRE_HOME=${JAVA_HOME}/jre
  export HADOOP_HOME=/etc/hadoop/hadoop-3.2.0
  export HIVE_HOME=/etc/hadoop/hive
  export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
  export PATH=$HIVE_HOME/bin:$HIVE_HOME/conf:${HADOOP_HOME}/bin:${JAVA_HOME}/bin:$PATH
  
  # Execute after saving the contents of the above file
  source /etc/profile
  ```

#### 2 Install JDK 1.8

- Download JDK1.8 to the prepared EC2 instance and unzip it to the `/usr/local/Java` directory:

  ```shell
  mkdir /usr/local/java
  tar -xvf java-1.8.0-openjdk.tar -C /usr/local/java
  ```
 

#### 3 Config Hadoop

- Download Hadoop and unzip it

  ```shell
  wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz
  mkdir /etc/hadoop
  tar -xvf hadoop-3.2.0.tar.gz -C /etc/hadoop
  ```

- Copy the jar package required by S3 to the Hadoop class loading path, otherwise an error of `ClassNotFound` type may occur

  ```shell
  cd /etc/hadoop
  cp hadoop-3.2.0/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar hadoop-3.2.0/share/hadoop/common/lib/
  cp hadoop-3.2.0/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar hadoop-3.2.0/share/hadoop/common/lib/
  ```

- Modify `core-site.xml`，config AWS account information and endpoint. The following is an example:

  ```
  <?xml version="1.0" encoding="UTF-8"?>
  <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
  <!--
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
  
      http://www.apache.org/licenses/LICENSE-2.0
  
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License. See accompanying LICENSE file.
  -->
  
  <!-- Put site-specific property overrides in this file. -->
  
  <configuration>
    <property>
      <name>fs.s3a.access.key</name>
      <value>SESSION-ACCESS-KEY</value>
    </property>
    <property>
      <name>fs.s3a.secret.key</name>
      <value>SESSION-SECRET-KEY</value>
    </property> 
    <property>
      <name>fs.s3a.endpoint</name>
      <value>s3.$REGION.amazonaws.com</value>
    </property>
  </configuration> 
  ```

#### 4 Install Hive

- Download Hive and unzip it

  ```shell
  wget https://downloads.apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz
  tar -xvf apache-hive-2.3.9-bin.tar.gz -C /etc/hadoop
  mv /etc/hadoop/apache-hive-2.3.9-bin /etc/hadoop/hive
  ```

- Configure environment variables

  ```shell
  vim /etc/profile
  
  # Add the following at the end of the profile file
  export HIVE_HOME=/etc/hadoop/hive
  export PATH=$PATH:$HIVE_HOME/bin:$HIVE_HOME/conf
  
  # Execute after saving the contents of the above file
  source /etc/profile
  ```

- Modify hive-site.xml, `vim ${HIVE_HOME}/conf/hive-site.xml`. Please start Amazon RDS for MySQL database in advance to obtain the mysql connection URI, user name and password.

  Note: Please configure VPC and security group correctly to ensure that EC2 instances can access the database normally.

  The sample content of `hive-site.xml` is as follows:

  ```shell
  <?xml version="1.0" encoding="UTF-8" standalone="no"?>
  <?xml-stylesheet type="text/xsl" href="configuration.xsl"?><!--
     Licensed to the Apache Software Foundation (ASF) under one or more
     contributor license agreements.  See the NOTICE file distributed with
     this work for additional information regarding copyright ownership.
     The ASF licenses this file to You under the Apache License, Version 2.0
     (the "License"); you may not use this file except in compliance with
     the License.  You may obtain a copy of the License at
  
         http://www.apache.org/licenses/LICENSE-2.0
  
     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
     limitations under the License.
  --><configuration>
    <!-- WARNING!!! This file is auto generated for documentation purposes ONLY! -->
    <!-- WARNING!!! Any changes you make to this file will be ignored by Hive.   -->
    <!-- WARNING!!! You must make your changes in hive-site.xml instead.         -->
    <!-- Hive Execution Parameters -->
    <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>password</value>
      <description>password to use against metastore database</description>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:mysql://host-name:3306/hive?createDatabaseIfNotExist=true</value>
      <description>JDBC connect string for a JDBC metastore</description>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.jdbc.Driver</value>
      <description>Driver class name for a JDBC metastore</description>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>admin</value>
      <description>Username to use against metastore database</description>
    </property>
    <property>
      <name>hive.metastore.schema.verification</name>
      <value>false</value>
      <description>
        Enforce metastore schema version consistency.
        True: Verify that version information stored in metastore matches with one from Hive jars.  Also disable automatic
              schema migration attempt. Users are required to manually migrate schema after Hive upgrade which ensures
              proper metastore schema migration. (Default)
        False: Warn if the version information stored in metastore doesn't match with one from in Hive jars.
      </description>
    </property>
  </configuration>
  ```

- Hive metadata initialization

  ```shell
  # Download the jar package of MySQL JDBC and place it in $HIVE_HOME/lib directory
  cp mysql-connector-java-5.1.47.jar $HIVE_HOME/lib
  bin/schematool -dbType mysql -initSchema
  mkdir $HIVE_HOME/logs
  nohup $HIVE_HOME/bin/hive --service metastore >> $HIVE_HOME/logs/hivemetastorelog.log 2>&1 &
  ```

  Note：If the following error is reported in this step:

  ```shell
  java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(ZLjava/lang/String;Ljava/lang/Object;)V
  ```

  This is caused by the inconsistency between the guava version in hive2 and the guava version in Hadoop3. Please replace the guava jar in directory `$HIVE_HOME/lib` with the guava jar in directory `$HADOOP_HOME/share/hadoop/common/lib/`.
  
- To prevent jar package conflicts in the subsequent process, you need to remove some spark and scala related jar packages from hive's class loading path:

  ```shell
  mkdir $HIVE_HOME/spark_jar
  mv $HIVE_HOME/lib/spark-* $HIVE_HOME/spark_jar
  mv $HIVE_HOME/lib/jackson-module-scala_2.11-2.6.5.jar $HIVE_HOME/spark_jar
  ```

  Note: Here just lists the conflicting jar packages encountered during the test. If users encounter problems similar to jar package conflicts, you can judge which jar packages have conflicts according to the class loading path and remove the relevant jar packages. It is recommended to keep the jar package version under the spark class loading path when the same jar package has version conflicts.
  
#### 5 Deploy Spark Standalone

- Download Spark 3.1.1 and unzip it

  ```shell
  wget http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
  tar -xvf spark-3.1.1-bin-hadoop3.2.tgz -C /etc/hadoop
  mv /etc/hadoop/spark-3.1.1-bin-hadoop3.2 /etc/hadoop/spark 
  export SPARK_HOME=/etc/hadoop/spark 
  ```

- Copy jar package required by S3:

  ```shell
  cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar $SPARK_HOME/jars
  cp $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar $SPARK_HOME/jars
  cp mysql-connector-java-5.1.47.jar $SPARK_HOME/jars
  ```

- Copy hive-site.xml and mysql-jdbc 

  ```
  cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf
  ```

- Setup Spark master and worker

  ```
  $SPARK_HOME/bin/start-master.sh
  $SPARK_HOME/bin/start-worker.sh spark://hostname:7077
  ```

#### 6 Deploy Zookeeper

- Download zookeeper and unzip it

  ```shell
  wget http://archive.apache.org/dist/zookeeper/zookeeper-3.4.13/zookeeper-3.4.13.tar.gz
  tar -xvf zookeeper-3.4.13.tar.gz -C /etc/hadoop
  mv /etc/hadoop/zookeeper-3.4.13 /etc/hadoop/zookeeper
  ```

- Preparing the zookeeper configuration file. Since only one EC2 node is used in the test, the zookeeper pseudo cluster is deployed here.

  ```shell
  cp /etc/hadoop/zookeeper/conf/zoo_sample.cfg /etc/hadoop/zookeeper/conf/zoo1.cfg
  cp /etc/hadoop/zookeeper/conf/zoo_sample.cfg /etc/hadoop/zookeeper/conf/zoo2.cfg
  cp /etc/hadoop/zookeeper/conf/zoo_sample.cfg /etc/hadoop/zookeeper/conf/zoo3.cfg
  ```

- Modify the above three configuration files in sequence and add the following contents, note that change the directory name to a different directory:

  ```shell
  server.1=localhost:2287:3387
  server.2=localhost:2288:3388
  server.3=localhost:2289:3389
  dataDir=/tmp/zookeeper/zk1/data
  dataLogDir=/tmp/zookeeper/zk1/log
  clientPort=2181
  ```

- Create the required folders and files:

  ```shell
  mkdir /tmp/zookeeper/zk1/data
  mkdir /tmp/zookeeper/zk1/log
  mkdir /tmp/zookeeper/zk2/data
  mkdir /tmp/zookeeper/zk2/log
  mkdir /tmp/zookeeper/zk3/data
  mkdir /tmp/zookeeper/zk3/log
  vim /tmp/zookeeper/zk1/data/myid 
  vim /tmp/zookeeper/zk2/data/myid 
  vim /tmp/zookeeper/zk3/data/myid 
  ```

- Setup zookeeper cluster

  ```shell
  /etc/hadoop/zookeeper/bin/zkServer.sh start /etc/hadoop/zookeeper/conf/zoo1.cfg
  /etc/hadoop/zookeeper/bin/zkServer.sh start /etc/hadoop/zookeeper/conf/zoo2.cfg
  /etc/hadoop/zookeeper/bin/zkServer.sh start /etc/hadoop/zookeeper/conf/zoo3.cfg
  ```

#### 7 Setup kylin

- Download kylin 4.0 binary package and unzip it

  ```shell
  wget https://mirror-hk.koddos.net/apache/kylin/apache-kylin-4.0.0/apache-kylin-4.0.0-bin.tar.gz
  tar -xvf apache-kylin-4.0.0-bin.tar.gz /etc/hadoop
  export KYLIN_HOME=/etc/hadoop/apache-kylin-4.0.0-bin
  mkdir $KYLIN_HOME/ext
  cp mysql-connector-java-5.1.47.jar $KYLIN_HOME/ext
  ```

- Modify kylin.properties `vim $KYLIN_HOME/conf/kylin.properties`

  ```shell
  kylin.metadata.url=kylin_metadata@jdbc,url=jdbc:mysql://hostname:3306/kylin,username=root,password=password,maxActive=10,maxIdle=10
  kylin.env.zookeeper-connect-string=hostname
  kylin.engine.spark-conf.spark.master=spark://hostname:7077
  kylin.engine.spark-conf.spark.submit.deployMode=client
  kylin.env.hdfs-working-dir=s3://bucket/kylin
  kylin.engine.spark-conf.spark.eventLog.dir=s3://bucket/kylin/spark-history
  kylin.engine.spark-conf.spark.history.fs.logDirectory=s3://bucket/kylin/spark-history
  kylin.engine.spark-conf.spark.yarn.jars=s3://bucket/spark2_jars/*
  kylin.query.spark-conf.spark.master=spark://hostname:7077
  kylin.query.spark-conf.spark.yarn.jars=s3://bucket/spark2_jars/*
  ```

- Execute `bin/kylin.sh start`

- Kylin may encounter ClassNotFound type errors during startUp. Please refer to the following methods to restart kylin:

  ```shell
  # Download commons-collections-3.2.2.jar 
  cp commons-collections-3.2.2.jar $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/
  # Download commons-configuration-1.3.jar
  cp commons-configuration-1.3.jar $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/
  cp $HADOOP_HOME/share/hadoop/common/lib/aws-java-sdk-bundle-1.11.563.jar $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/
  cp $HADOOP_HOME/share/hadoop/common/lib/hadoop-aws-3.2.2.jar $HADOOP_HOME/tomcat/webapps/kylin/WEB-INF/lib/
  ```