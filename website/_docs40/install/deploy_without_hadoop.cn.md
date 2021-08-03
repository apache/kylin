---
layout: docs40-cn
title:  "无 Hadoop 环境部署 Kylin4"
categories: install
permalink: /cn/docs40/install/deploy_without_hadoop.html
---

相比于 Kylin3，Kylin4.0 实现了全新 spark 构建引擎和 parquet 存储，使 kylin 不依赖 hadoop 环境部署成为可能。与在 AWS EMR 之上部署 Kylin3 相比，直接在 AWS EC2 实例上部署 Kylin4 存在以下优势：
1. 节省成本。相比 AWS EMR 节点，AWS EC2 节点的成本更低。
2. 更加灵活。在 EC2 节点上，用户可以更加自主选择自己所需的服务以及组件进行安装部署。
3. 去 Hadoop。Hadoop 生态比较重，需要投入一定的人力成本进行维护，去 Hadoop 可以更加贴近云原生。

在实现了支持在 spark standalone 模式下进行构建和查询的功能之后，我们在 AWS 的 EC2 实例上对无 hadoop 部署 Kylin4 做了尝试，并成功构建 cube 和进行了查询。

### 环境准备

- 按照需求申请 AWS EC2 Linux 实例
- 创建 Amazon RDS for Mysql 作为 Kylin 以及 Hive 元数据库
- S3 作为 Kylin 存储

### 组件版本信息
此处提供的版本信息是我们在测试时选用的版本信息，如果用户需要使用其他的版本进行部署，可以自行更换，保证组件版本之间兼容即可。

* JDK 1.8
* Hive 2.3.9
* Zookeeper 3.4.13
* Kylin 4.0 for spark3
* Spark 3.1.1
* Hadoop 3.2.0（不需要启动）

### 安装过程

#### 1 配置环境变量

- 配置环境变量并使其生效

  ```shell
  vim /etc/profile
  
  # 在 profile 文件末尾添加以下内容
  export JAVA_HOME=/usr/local/java/jdk1.8.0_291
  export JRE_HOME=${JAVA_HOME}/jre
  export HADOOP_HOME=/etc/hadoop/hadoop-3.2.0
  export HIVE_HOME=/etc/hadoop/hive
  export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
  export PATH=$HIVE_HOME/bin:$HIVE_HOME/conf:${HADOOP_HOME}/bin:${JAVA_HOME}/bin:$PATH
  
  # 保存以上文件内容后执行以下命令
  source /etc/profile
  ```

#### 2 安装 JDK 1.8

- 下载 jdk1.8 到准备好的 EC2 实例，解压到 `/usr/local/java` 目录：

  ```shell
  mkdir /usr/local/java
  tar -xvf jdk-8u291-linux-x64.tar -C /usr/local/java
  ``` 

#### 3 配置 Hadoop

- 下载 Hadoop 并解压

  ```shell
  wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz
  mkdir /etc/hadoop
  tar -xvf hadoop-3.2.0.tar.gz -C /etc/hadoop
  ```

- copy 连接 S3 所需 jar 包到 hadoop 类加载路径，否则可能会出现 ClassNotFound 类型报错

  ```shell
  cd /etc/hadoop
  cp hadoop-3.2.0/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar hadoop-3.2.0/share/hadoop/common/lib/
  cp hadoop-3.2.0/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar hadoop-3.2.0/share/hadoop/common/lib/
  ```

- 修改 `core-site.xml`，配置 aws 账号信息以及 endpoint，以下为示例内容

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

#### 4 安装 Hive

- 下载 Hive 并解压

  ```shell
  wget https://downloads.apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz
  tar -xvf apache-hive-2.3.9-bin.tar.gz -C /etc/hadoop
  mv /etc/hadoop/apache-hive-2.3.9-bin /etc/hadoop/hive
  ```

- 编辑 hive 配置文件 `vim ${HIVE_HOME}/conf/hive-site.xml`，请提前启动 Amazon RDS for Mysql database，获取连接 URI、用户名和密码。

  注意：正确配置 VPC 和安全组，以保证 EC2 实例可以正常访问数据库。

  `hive-site.xml`文件示例内容如下：

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

- Hive 元数据初始化

  ```shell
  # 下载 mysql-jdbc 的 jar 包放置在 $HIVE_HOME/lib 目录下
  cp mysql-connector-java-5.1.47.jar $HIVE_HOME/lib
  bin/schematool -dbType mysql -initSchema
  mkdir $HIVE_HOME/logs
  nohup $HIVE_HOME/bin/hive --service metastore >> $HIVE_HOME/logs/hivemetastorelog.log 2>&1 &
  ```

  注意：如果在这个步骤中出现了如下报错：

  ```shell
  java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(ZLjava/lang/String;Ljava/lang/Object;)V
  ```

  这是由于 hive2 中 guava 包版本与 hadoop3 的 guava 版本不一致导致的，请使用 `$HADOOP_HOME/share/hadoop/common/lib/` 目录下的 guava jar 替换 $HIVE_HOME/lib 目录中的 guava jar。

- 为防止后续过程中出现 jar 包冲突，需要从 hive 的类加载路径中移除一些 spark 以及 scala 相关的 jar 包

  ```shell
  rm $HIVE_HOME/lib/spark-* $HIVE_HOME/spark_jar
  rm $HIVE_HOME/lib/jackson-module-scala_2.11-2.6.5.jar
  ```

  注：此处只列出了我们在测试过程中遇到的产生冲突的 jar 包，如果用户在遇到类似 jar 包冲突的问题，可以根据类加载路径判断哪些 jar 包产生了冲突并移除相关 jar 包。建议当相同 jar 包产生版本冲突时，保留 spark 类加载路径下的 jar 包版本。

#### 5 部署 Spark Standalone

- 下载 Spark 3.1.1 并解压

  ```shell
  wget http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
  tar -xvf spark-3.1.1-bin-hadoop3.2.tgz -C /etc/hadoop
  mv /etc/hadoop/spark-3.1.1-bin-hadoop3.2 /etc/hadoop/spark 
  export SPARK_HOME=/etc/hadoop/spark 
  ```

- Copy 连接 S3 所需 jar 包

  ```shell
  cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar $SPARK_HOME/jars
  cp $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar $SPARK_HOME/jars
  cp mysql-connector-java-5.1.47.jar $SPARK_HOME/jars
  ```

- Copy hive 配置文件及 mysql-jdbc 

  ```
  cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf
  ```

- 启动 Spark master 和 worker

  ```
  $SPARK_HOME/bin/start-master.sh
  $SPARK_HOME/bin/start-worker.sh spark://hostname:7077
  ```

#### 6 部署 Zookeeper 伪集群

- 下载 zookeeper 安装包并解压

  ```shell
  wget http://archive.apache.org/dist/zookeeper/zookeeper-3.4.13/zookeeper-3.4.13.tar.gz
  tar -xvf zookeeper-3.4.13.tar.gz -C /etc/hadoop
  mv /etc/hadoop/zookeeper-3.4.13 /etc/hadoop/zookeeper
  ```

- 修改 zookeeper 配置文件，启动三节点 zookeeper 伪集群

  ```shell
  cp /etc/hadoop/zookeeper/conf/zoo_sample.cfg /etc/hadoop/zookeeper/conf/zoo1.cfg
  cp /etc/hadoop/zookeeper/conf/zoo_sample.cfg /etc/hadoop/zookeeper/conf/zoo2.cfg
  cp /etc/hadoop/zookeeper/conf/zoo_sample.cfg /etc/hadoop/zookeeper/conf/zoo3.cfg
  ```

- 依次修改上述三个配置文件，添加如下内容：

  ```shell
  server.1=localhost:2287:3387
  server.2=localhost:2288:3388
  server.3=localhost:2289:3389
  dataDir=/tmp/zookeeper/zk1/data
  dataLogDir=/tmp/zookeeper/zk1/log
  clientPort=2181
  ```

- 创建所需文件夹和文件

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

- 启动 zookeeper 集群

  ```shell
  /etc/hadoop/zookeeper/bin/zkServer.sh start /etc/hadoop/zookeeper/conf/zoo1.cfg
  /etc/hadoop/zookeeper/bin/zkServer.sh start /etc/hadoop/zookeeper/conf/zoo2.cfg
  /etc/hadoop/zookeeper/bin/zkServer.sh start /etc/hadoop/zookeeper/conf/zoo3.cfg
  ```

#### 7 启动 kylin

- 下载 kylin 4.0 二进制包并解压

  ```shell
  wget https://mirror-hk.koddos.net/apache/kylin/apache-kylin-4.0.0/apache-kylin-4.0.0-bin.tar.gz
  tar -xvf apache-kylin-4.0.0-bin.tar.gz /etc/hadoop
  export KYLIN_HOME=/etc/hadoop/apache-kylin-4.0.0-bin
  mkdir $KYLIN_HOME/ext
  cp mysql-connector-java-5.1.47.jar $KYLIN_HOME/ext
  ```

- 修改配置文件 `vim $KYLIN_HOME/conf/kylin.properties`

  ```shell
  kylin.metadata.url=kylin_metadata@jdbc,url=jdbc:mysql://hostname:3306/kylin,username=root,password=password,maxActive=10,maxIdle=10
  kylin.env.zookeeper-connect-string=hostname
  kylin.engine.spark-conf.spark.master=spark://hostname:7077
  # 当 spark.submit.deployMode 配置为 cluster 时，需要额外配置 kylin.engine.spark.standalone.master.httpUrl
  kylin.engine.spark-conf.spark.submit.deployMode=client/cluster
  kylin.env.hdfs-working-dir=s3://bucket/kylin
  kylin.engine.spark-conf.spark.eventLog.dir=s3://bucket/kylin/spark-history
  kylin.engine.spark-conf.spark.history.fs.logDirectory=s3://bucket/kylin/spark-history
  kylin.query.spark-conf.spark.master=spark://hostname:7077
  ```

- 执行 `bin/kylin.sh start`

- Kylin 启动时可能会遇到 ClassNotFound 类型报错，可参考以下方法解决后重启 kylin：

  ```shell
  # 下载 commons-collections-3.2.2.jar 
  cp commons-collections-3.2.2.jar $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/
  # 下载 commons-configuration-1.3.jar
  cp commons-configuration-1.3.jar $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/
  cp $HADOOP_HOME/share/hadoop/common/lib/aws-java-sdk-bundle-1.11.563.jar $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/
  cp $HADOOP_HOME/share/hadoop/common/lib/hadoop-aws-3.2.2.jar $HADOOP_HOME/tomcat/webapps/kylin/WEB-INF/lib/
  ```