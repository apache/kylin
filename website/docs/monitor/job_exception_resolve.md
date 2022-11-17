---
title: Common Job Error Causes and Solutions
language: en
sidebar_label: Common Job Error Causes and Solutions
pagination_label: Common Job Error Causes and Solutions
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - common job error causes and solutions
draft: false
last_update:
    date: 08/19/2022
---


Various problems may occur during the execution of building jobs which cause the job to fail. Usually, the brief error cause and description are directly displayed in the job details. This article will summarize some common error causes and solutions to assist solving problems.

#### <span id="date_format_not_match">Incrementally build model, the time format of the time partition column is wrong</span>

- **ErrorCode：** KE-030001003
- **Description：**

  In the time partition setting of model, the time format of the time partition column is inconsistent with the actual time format in the data source. The key information in the log is:`date format not match`。

- **Solution：**

    1. Modify the time format of the model time partition column to be consistent with the actual time format in the data source:

       Please refer to [Design a Data Model](../modeling/manual_modeling.md#step-4-save-the-model-and-set-the-loading-method) *Step 4. Save the model and set the loading method* modify the time format of the model time partition column。

    2. 2. If you insist on using this format, you can choose to disable checking the time partition column by modifying the system parameter in `kylin.properties` to `kylin.engine.check-partition-col-enabled=false`.
       Notice: Although this method can bypass the time format verification here, it may cause other problems. Please use it with caution.

#### <span id="oom">OOM exception occurred during building</span>

- **ErrorCode：** KE-030001004
- **Description：** Spark Driver/Executor has OOM during building. The key information in the log is: `OutOfMemoryError`.
- **Solution：**

    1. Adjust spark.sql.shuffle.partitions

        During the build process, if there are MetadataFetchFailedException, executor lost, oom problems, you can try to adjust the following parameters:
        - kylin.engine.spark-conf.spark.sql.shuffle.partitions

       This parameter determines the number of partitions during aggregate or join execution, and the default is 200.

    2. Improve build resources

       In general, using more resources can significantly improve performance and fault tolerance by tuning cores and memory used by builds with the following parameter :

        - kylin.engine.spark-conf.spark.executor.instances
        - kylin.engine.spark-conf.spark.executor.cores
        - kylin.engine.spark-conf.spark.executor.memory
        - kylin.engine.spark-conf.spark.executor.memoryOverhead

#### <span id="no_space_left_on_device">No space left on device during building</span>

- **ErrorCode：** KE-030001005
- **Description：** The building job reports an error: no space left on device. The key information in the log is: `No space left on device`.
- **Solution：**

    1. Please check Kylin and the cluster disk space used for building, clean up invalid files or expand capacity in time.
    2. Try to clean up Kylin's inefficient storage.
    3. For `shuffle no left space on device` problem, you can appropriately increase the number of executor instances to use more computing resources.

        - spark.executor.cores
        - spark.executor.instances

#### <span id="class_not_found">Class not found when building</span>

- **ErrorCode：** KE-030001006
- **Description：** The class was not found during building. The key information in the log is: `ClassNotFoundException`.
- **Solution：**

    1. Missing Mysql driver(`java.lang.ClassNotFoundException: com.mysql.jdbc.Driver`)

       Please refer to [Use MySQL as Metastore](../deployment/on-premises/rdbms_metastore/mysql/mysql_metastore.md) set up Mysql as metabase.

#### <span id="kerberos_realm_not_found">Kerberos realm not found when building</span>

- **ErrorCode：** KE-030001007
- **Description：** Kerberos is not configured correctly, resulting Kerberos realm being not found. The key information in the log is: `Can't get Kerberos realm`.
- **Solution：**

    1. Double check Kerberos configuration
       1. For both Yarn Cluster and Yarn Client modes, the krb5.conf file in {KYLIN_HOME}/conf/ and {KYLIN_HOME}/hadoop_conf/ should be checked to prevent any failure related to Kerberos realm.
       2. If Yarn Cluster mode is chosen, please pay more attention to the Kerberos config in {KYLIN_HOME}/spark/conf/spark-env.sh file.
