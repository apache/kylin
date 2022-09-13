---
title: Network Port Requirements
language: en
sidebar_label: Network Port Requirements
pagination_label: Network Port Requirements
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
   - Network Port
draft: false
last_update:
    date: 08/11/2022
---

Kylin needs to communicate with different components. The following are the ports that need to be opened to Kylin. This table only includes the default configuration of the Hadoop environment, and does not include the configuration differences between Hadoop platforms.

| Component            | Port          | Function                                                     | Required |
| -------------------- | ------------- | ------------------------------------------------------------ | -------- |
| SSH                  | 22            | SSH to connect to the port of the virtual machine where Kylin is located | Y        |
| Kylin                | 7070          | Kylin access port                                            | Y        |
| Kylin                | 7443          | Kylin HTTPS access port                                      | N        |
| HDFS                 | 8020          | HDFS receives client connection RPC port                     | Y        |
| HDFS                 | 50010         | Access HDFS DataNode, data transmission port                 | Y        |
| Hive                 | 10000         | HiveServer2 access port                                      | N        |
| Hive                 | 9083          | Hive Metastore access port                                   | Y        |
| Zookeeper            | 2181          | Zookeeper access port                                        | Y        |
| Yarn                 | 8088          | Yarn Web UI access port                                      | Y        |
| Yarn                 | 8090          | Yarn Web UI HTTPS access port                                | N        |
| Yarn                 | 8050 / 8032   | Yarn ResourceManager communication port                      | Y        |
| Spark                | 4041          | Kylin query engine Web UI default port        | Y        |
| Spark                | 18080         | Spark History Server port                                    | N        |
| Spark                | (1024, 65535] | The ports occupied by Spark Driver and Executor are random   | Y        |
| Influxdb             | 8086          | Influxdb HTTP port                                           | N        |
| Influxdb             | 8088          | Influxdb RPC port                                            | N        |
| PostgreSQL           | 5432          | PostgreSQL access port                                       | Y        |
| MySQL                | 3306          | MySQL access port                                            | Y        |

