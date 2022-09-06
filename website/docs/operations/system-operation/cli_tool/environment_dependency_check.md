---
title: Environment Dependency Check
language: en
sidebar_label: Environment Dependency Check
pagination_label: Environment Dependency Check
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - environment dependency check
draft: false
last_update:
    date: 08/16/2022
---

Before you start Kylin, we provide an environment dependency checking tool to help you spot the potential problems in advance. This tool will be automatically executed by startup script when you run Kylin at the first time.

### How To Use

As said above, if you start Kylin at the first time, the startup script will automatically run this tool. If it check failed, this tool will be executed again when you start this product. Once successfully passed this check, the tool will not be executed automatically. 

If you need to check the environment dependency manually, just run the below command:

```sh
$KYLIN_HOME/bin/check-env.sh
```

### What To Check

The following table describes what will be checked in the tool.

|     Check Item        | Description                                                         |
| ---------------------- | ------------------------------------------------------------ |
| Kerberos               | To check whether user enable Kerberos in the settings. If not, the check will be skipped. Otherwise, it will execute the following operations: <br /> 1. check if Kerberos command exists <br /> 2. initialize Kerberos|
| OS version and command | Kylin only supports Linux operating systems. Besides operating system, this tool will also check if `hadoop` and `yarn` commands exist. If these two commands are not available, please make sure Hadoop cluster whether is available. |
| Hadoop configuration files            | Kylin copies Hadoop configuration files to Kylin installation directory `$KYLIN_HOME/hadoop_conf`. For instance, `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `hive-site.xml`, etc. This tool will check if `$KYLIN_HOME/hadoop_conf` exists and contains necessary configuration files. |
| HDFS working directory             | 1. Check if HDFS working directory exists <br /> 2. If yes, check whether current user has write privilege |
| Java version               | Currently, we only support Java versions above 1.8 |
| Server port              | Check if the port is in use |
| Spark      | 1. Check if the configured resource size exceeds the cluster's actual resource size, such as, executor cores and executor instances. <br /> 2. Check if Spark is available  <br /> 3. Check if the configured yarn queues for submitting query jobs and build jobs are legal  4. Check if the configured driver host address is legal|
| Spark log directory    | Users can configure a HDFS directory to store Spark logs, so it checks if the directory exists and current user has read and write privileges. |
| Metastore | Check if the metastore is accessible and current user can perform necessary operations on metadata. |
| InfluxDB                  | 1. Check if InfluxDB is accessible <br /> 2. Check if current user has read and write privileges  |
| ZooKeeper              | Check if the service discovery is available. |
| KylinConfig | Checking kylin config, must starts with kylin / spring / server. |
| Query history | Check whether the current user has permissions of reading and writing on the `query_history` and `query_history_realization` tables in the RDBMS database|
