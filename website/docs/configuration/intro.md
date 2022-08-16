---
title: System Configuration
language: en
sidebar_label: System Configuration
pagination_label: System Configuration
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - system configuration
draft: false
last_update:
    date: 08/16/2022
---

After deploying Kylin on your cluster, configure Kylin so that it can interact with Apache Hadoop and Apache Hive. You can also optimize the performance of Kylin by configuring to your own environment.

This chapter introduces some configurations for Kylin.

### Kylin Configuration File List

| Component            | File                        | Description                                                  |
| -------------------- | --------------------------- | ------------------------------------------------------------ |
| Kylin                | conf/kylin.properties                   | This is the global configuration file, with all configuration properties about Kylin in it. Details will be discussed in the subsequent chapter [Basic Configuration](configuration.md). |
| Hadoop               | hadoop_conf/core-site.xml               | Global configuration file used by Hadoop, which defines system-level parameters such as HDFS URLs and Hadoop temporary directories, etc. |
| Hadoop               | hadoop_conf/hdfs-site.xml               | HDFS configuration file, which defines HDFS parameters such as the storage location of NameNode and DataNode and the number of file copies, etc. |
| Hadoop               | hadoop_conf/yarn-site.xml               | Yarn configuration file,which defines Hadoop cluster resource management system parameters, such as ResourceManager, NodeManager communication port and web monitoring port, etc. |
| Hadoop               | hadoop_conf/mapred-site.xml             | Map Reduce configuration file used in Hadoop,which defines the default number of reduce tasks, the default upper and lower limits of the memory that the task can use, etc. |
| Hive                 | hadoop_conf/hive-site.xml               | Hive configuration file, which defines Hive parameters such as hive data storage directory and database address, etc. |

>Note:
>
>+ Unless otherwise specified, the configuration file `kylin.properties` mentioned in this manual refers to the corresponding configuration file in the list.
