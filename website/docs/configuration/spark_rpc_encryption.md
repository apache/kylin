---
title: Spark RPC Communication Encryption
language: en
sidebar_label: Spark RPC Communication Encryption
pagination_label: Spark RPC Communication Encryption
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - spark rpc communication encryption
draft: false
last_update:
    date: 08/16/2022
---

Kylin supports the configuration of communication encryption between Spark nodes, which can improve the security of internal communication and prevent specific security attacks.

For more details about Spark RPC communication encryption, please see [Spark Security](http://spark.apache.org/docs/1.6.2/job-scheduling.html#dynamic-resource-allocation).

This function is disabled by default. If you need to enable it, please refer to the following method for configuration.

### Spark RPC Communication Encryption Configuration

1、Please refer to [Spark Security](http://spark.apache.org/docs/1.6.2/job-scheduling.html#dynamic-resource-allocation) to ensure that RPC communication encryption is enabled in the Spark cluster.

2、Add the following configurations in `$KYLIN_HOME/conf/kylin.properties`, to To enable Kylin nodes and Spark cluster communication encryption

```
### spark rpc encryption for build jobs
kylin.storage.columnar.spark-conf.spark.authenticate=true
kylin.storage.columnar.spark-conf.spark.authenticate.secret=kylin
kylin.storage.columnar.spark-conf.spark.network.crypto.enabled=true
kylin.storage.columnar.spark-conf.spark.network.crypto.keyLength=256
kylin.storage.columnar.spark-conf.spark.network.crypto.keyFactoryAlgorithm=PBKDF2WithHmacSHA256

### spark rpc encryption for query jobs
kylin.engine.spark-conf.spark.authenticate=true
kylin.engine.spark-conf.spark.authenticate.secret=kylin
kylin.engine.spark-conf.spark.network.crypto.enabled=true
kylin.engine.spark-conf.spark.network.crypto.keyLength=256
kylin.engine.spark-conf.spark.network.crypto.keyFactoryAlgorithm=PBKDF2WithHmacSHA256
```

### Spark RPC Communication Encryption Cerification
After the configuration is complete, start Kylin and verify that the query and build tasks can be executed normally.
