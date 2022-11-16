---
title: Prerequisite
language: en
sidebar_label: Prerequisite
pagination_label: Prerequisite
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: intro
pagination_next: null
keywords: 
  - prerequisite
draft: false
last_update:
    date: 09/13/2022
---

To ensure system performance and stability, we recommend you run Kylin on a dedicated Hadoop cluster.

Prior to installing Kylin, please check the following prerequisites are met.

- Environment
    - [Supported Hadoop Distributions](#hadoop)
    - [Java Environment](#java)
    - [Account Authority](#account)
    - [Metastore Configuration](#metadata)
    - [Check Zookeeper](#zookeeper)
- Recommended Resource and Configuration
    - [Hadoop Cluster Resource Allocation](#resource)
    - [Recommended Hardware Configuration](#hardware)
    - [Recommended Linux Distribution](#linux)
    - [Recommended Client Configuration](#client)



### <span id="hadoop">Supported Hadoop Distributions</span>

The following Hadoop distributions are verified to run on Kylin.

- [Apache Hadoop 3.2.1](installation/platform/install_on_apache_hadoop.md)


Kylin requires some components, please make sure each server has the following components.

- Hive
- HDFS
- Yarn
- ZooKeeper

**Note:** Spark is shipped in the binary package, so you don't need to install it in advance. If for security and compliance reasons, you want to replace the shipped Spark and Hadoop dependency with existing ones in your environment, please contact Kylin Community. 

### <span id="java">Java Environment</span>

Kylin requires:

- Requires your environment's default JDK version is 8 （JDK 1.8_162 or above small version）

```shell
java -version
```

You can use the following command to check the JDK version of your existing environment, for example, the following figure shows JDK 8

![JDK version](images/jdk.png)

### <span id="account">Account Authority</span>

The Linux account running Kylin must have the required access permissions to the cluster. These permissions include:

* Read/Write permission of HDFS
* Create/Read/Write permission of Hive table

Verify the user has access to the Hadoop cluster with account `KyAdmin`. Test using the steps below:

1. Verify the user has HDFS read and write permissions

   Assuming the HDFS storage path for model data is `/kylin`, set it in `conf/kylin.properties` as:

   ```properties
   kylin.env.hdfs-working-dir=/kylin
   ```

   The storage folder must be created and granted with permissions. You may have to switch to HDFS administrator (usually the `hdfs` user),  to do this:

   ```shell
   su hdfs
   hdfs dfs -mkdir /kylin
   hdfs dfs -chown KyAdmin /kylin
   hdfs dfs -mkdir /user/KyAdmin 
   hdfs dfs -chown KyAdmin /user/KyAdmin
   ```
   Verify the `KyAdmin` user has read and write permissions

   ```shell
   hdfs dfs -put <any_file> /kylin
   hdfs dfs -put <any_file> /user/KyAdmin   
   ```

2. Verify the `KyAdmin` user has Hive read and write permissions

   Let's say you want to store a Hive table `t1` in Hive database `kylinDB`, The `t1` table contains two fields `id, name`.

   Then verify the Hive permissions:

   ```shell
   #hive
   hive> show databases;
   hive> use kylinDB;
   hive> show tables;
   hive> insert into t1 values(1, "kylin");
   hive> select * from t1;
   ```

### <span id="metadata">Metastore Configuration</span>

A configured metastore is required for this product.

We recommend using PostgreSQL 10.7 as the metastore, which is provided in our package. Please refer to [Use PostgreSQL as Metastore (Default)](./rdbms_metastore/postgresql/default_metastore.md) for installation steps and details.

If you want to use your own PostgreSQL database, the supported versions are below:

- PostgreSQL 9.1 or above

You can also choose to use MySQL but we currently don't provide a MySQL installation package or JDBC driver. Therefore, you need to finish all the prerequisites before setting up. Please refer to [Use MySQL as Metastore](./rdbms_metastore/mysql/mysql_metastore.md) for installation steps and details. The supported MySQL database versions are below:

- MySQL 5.1-5.7
- MySQL 5.7 (recommended)

### <span id="zookeeper">Check Zookeeper</span>

The following steps can be used to quickly verify the connectivity between ZooKeeper and Kylin after Kerberos is enabled.
1. Find the ZooKeeper working directory on the node where the ZooKeeper Client is deployed
2. Add or modify the Client section to the `conf/jaas.conf` file:

   ```shell
   Client {
     com.sun.security.auth.module.Krb5LoginModule required
     useKeyTab=true
     keyTab="/path/to/keytab_assigned_to_kylin"
     storeKey=true
     useTicketCache=false
     principal="principal_assigned_to_kylin";
   };
   ```
3. `export JVMFLAGS="-Djava.security.auth.login.config=/path/to/jaas.conf"`
4. `bin/zkCli.sh -server ${kylin.env.zookeeper-connect-string}`
5. Verify that the ZooKeeper node can be viewed normally, for example: `ls /`
6. Clean up the new Client section in step 2 and the environment variables `unset JVMFLAGS` declared in step 3

If you download ZooKeeper from the non-official website, you can consult the operation and maintenance personnel before performing the above operations.

### <span id="resource">Hadoop Cluster Resource Allocation</span>

To ensure Kylin works efficiently, please ensure the Hadoop cluster configurations satisfy the following conditions:

* `yarn.nodemanager.resource.memory-mb` configuration item bigger than 8192 MB
* `yarn.scheduler.maximum-allocation-mb` configuration item bigger than 4096 MB
* `yarn.scheduler.maximum-allocation-vcores` configuration item bigger than 5

If you need to run Kylin in a sandbox or other virtual machine environment, please make sure the virtual machine environment has the following resources:

- No less than 4 processors

- Memory is no less than 10 GB

- The value of the configuration item `yarn.nodemanager.resource.cpu-vcores` is no less than 8

### <span id="hardware">Recommended Hardware Configuration</span>

We recommend the following hardware configuration to install Kylin:

- 32 vCore, 128 GB memory
- At least one 1TB SAS HDD (3.5 inches), 7200RPM, RAID1
- At least two 1GbE Ethernet ports. For network port requirements, please refer to the [Network Port Requirements](./network_port_requirements.md) chapter.

### <span id="linux">Recommended Linux Distribution</span>

We recommend using the following version of the Linux operating system:

- Ubuntu 18.04 + (recommend LTS version)
- Red Hat Enterprise Linux 6.4+ or 7.x 
- CentOS 6.4+ or 7.x

### <span id="client">Recommended Client Configuration</span>

- CPU: 2.5 GHz Intel Core i7
- Operating System: macOS / Windows 7 / Windows 10
- RAM: 8G or above
- Browser version:
    + Chrome 45 or above
    + Internet Explorer 11 or above
