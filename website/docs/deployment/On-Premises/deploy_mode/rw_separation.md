---
title: Read/Write Separation Deployment
language: en
sidebar_label: Read/Write Separation Deployment
pagination_label: Read/Write Separation Deployment
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
   - read/write separation
   - separation
draft: false
last_update:
   date: 08/12/2022
---

Kylin's tasks based on Hadoop are mainly divided into two types: build and query. If these two tasks use the same set of Hadoop resources, resource preemption may occur between the build and the query, which makes them not stable and fast.

Kylin allows you to finish building and query tasks on different Hadoop clusters. There are many write operations in the former, known as the **Build Cluster**, while the latter is dominated by read-only operations, called **Query Cluster**. The build task will be sent to the build cluster. After the build is completed, the system will send the data into the query cluster so as to executing query tasks.

With a read/write separation deployment, you can completely isolate both build and query workloads, allowing them to run independently, avoiding improper interactions between them and possible performance instability.

### Prerequisites

Due to the involvement of two Hadoop environments, please read and comply with the following environmental checks.

1. Please confirm the Hadoop versions of build cluster and query cluster are identical, and they're supported version by Kylin.

2. Please confirm that the Hadoop client of build cluster and query cluster is installed and configured on the **Kylin server**. Check commands like `hdfs`、`hive` are all working properly and can access cluster resources.

3. If the two clusters have enabled the HDFS NameNode HA, please check and make sure their HDFS nameservice names are different. If they are the same, please change one of them to avoid conflict.

4. Please check the two clusters can access each other without manually inputting any user credential.

   > **Tips**: As a test, on any build cluster try to copy some HDFS files from/to the query cluster. The copy must succeed without any extra manual interaction.

5. Please make sure the network latency between the two clusters is low enough, as there will be a large number of data moved back and forth during model build process.

6. If Kerberos is enabled, please check the following:

   - The build cluster and the query cluster belong to different realms.
   - The cross-realm trust between the two clusters is configured properly.

### Install and Configure Read/Write Separation Deployment

You can follow the below instructions to finish Kylin read/write separation deployment based on hadoop clusters.

1. First of all, on **Kylin server**, uncompress Kylin software package to the same location. This location will be referenced as `$KYLIN_HOME` later.

2. Secondly, you should prepare the hadoop conf file of the two clusters. The hadoop configuration of query cluster should be put into `$KYLIN_HOME/hadoop_conf` directory, while that of build query will be put into `$KYLIN_HOME/write_hadoop_conf` directory. What's more, the `hive-site.xml` of the build cluster should be put into the above two directories, make two copies of hive-site.xml and name them hiveserver2-site.xml and hivemetastore-site.xml respectively. 
   
   If Kerberos authentication enabled, you need to copy the krb5.conf file of build cluster to the `$KYLIN_HOME/write_hadoop_conf` directory and copy the krb5.conf file of query cluster to the `$KYLIN_HOME/hadoop_conf` directory.

3. Set the configuration:

   ```properties
   kylin.engine.submit-hadoop-conf-dir=$KYLIN_HOME/write_hadoop_conf

   ## Working path of Kylin instance on HDFS. Please replace {working_dir} with the real working path, and use the absolute path, such as hdfs://kylin
   kylin.env.hdfs-working-dir={working_dir}
   ```

4. If Kerberos is enabled, you will need to do additional configuration:

   ```properties
   kylin.storage.columnar.spark-conf.spark.yarn.access.hadoopFileSystems=hdfs://readcluster,hdfs://writecluster
   kylin.engine.spark-conf.spark.yarn.access.hadoopFileSystems=hdfs://readcluster,hdfs://writecluster
   ```
5. If the Kerberos authentication mechanism enabled, the following checks need to be done to avoid errors in the segment build job:
   
   ```
   java.lang.IllegalArgumentException: Can't get Kerberos realm
   ...
   Caused by: KrbException: Cannot locate default realm
   ```
   
   1. Make sure the `keytab` file and `krb5.conf` file required for authentication exist in the directory `$KYLIN_HOME/conf`
   2. If the directory `$KYLIN_HOME/hadoop_conf` exists, make sure there is a `krb5.conf` file in this directory
   3. If the directory `$KYLIN_HOME/write_hadoop_conf` exists, make sure there is a `krb5.conf` file in this directory

Now read/write separation deployment is configured. 

**Note:**

* `$KYLIN_HOME/bin/check-env.sh` and `$KYLIN_HOME/bin/sample.sh` are not available in this deployment mode.

* In this mode, `kylin.engine.spark-conf.spark.yarn.queue` in `kylin.properties` should be configured as the queue of the build cluster.


