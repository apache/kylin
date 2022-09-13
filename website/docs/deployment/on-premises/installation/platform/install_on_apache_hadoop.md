---
title: Install on Apache Hadoop Platform
language: en
sidebar_label: Install on Apache Hadoop Platform
pagination_label: Install on Apache Hadoop Platform
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - install
    - hadoop
draft: false
last_update:
    date: 08/12/2022
---


### Prepare Environment

First, **make sure you allocate sufficient resources for the environment**. Please refer to [Prerequisites](docs/deployment/on-premises/prerequisite.md) for detailed resource requirements for Kylin. Moreover, please ensure that `HDFS`, `YARN`, `Hive`, `ZooKeeper` and other components are in normal state without any warning information.



#### Apache Hadoop Supported Version

Following Apache Hadoop versions are supported by Kylin:

- Apache Hadoop 3.2.1

**Note**：The Apache Hadoop 3.2.1 environment with Kerberos is not currently supported.

#### Additional configuration required for Apache Hadoop version

Add the following two configurations in `$KYLIN_HOME/conf/kylin.properties`:

- `kylin.env.apache-hadoop-conf-dir` Hadoop conf directory in Hadoop environment
- `kylin.env.apache-hive-conf-dir` Hive conf directory in Hadoop environment



#### Jar package required by Apache Hadoop version

In Apache Hadoop 3.2.1, you also need to prepare the MySQL JDBC driver in the operating environment of Kylin.

Here is a download link for the jar file package of the MySQL 8.0 JDBC driver：https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar. You need to prepare the other versions of the driver yourself.Please place the JDBC driver of the corresponding version of MySQL in the `$KYLIN_HOME/lib/ext` directory.



### Install Kylin

After setting up the environment, please refer to [Quick Start](docs/quickstart/quick_start.md) to continue.
