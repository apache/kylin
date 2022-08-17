---
title: Service Discovery
language: en
sidebar_label: Service Discovery
pagination_label: Service Discovery
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - service discovery
    - ha
draft: false
last_update:
    date: 08/12/2022
---

Multiple Kylin instances can work together as a cluster. When a Kylin instance is started, stopped, or lost, other instances in the cluster will be updated automatically. Kylin has a new implementation based on Apache Curator framework, which is more convenient and more stable. 

If you want to enable service discovery, please set the correct ZooKeeper address in the configuration file `$KYLIN_HOME/conf/kylin.properties`. For example: 

```properties
kylin.env.zookeeper-connect-string=host1:2181,host2:2181,host3:2181
```

After the configuration, please restart Kylin. Each Kylin instance will register itself in ZooKeeper, and each one will discover other instances from ZooKeeper. 

By default, Kylin will use hostname as the node name. If the cluster you run Kylin on cannot parses the host name to IP address through DNS and other services, you could set an additional configuration in  `$KYLIN_HOME/conf/kylin.properties` as below (this can avoid communication errors between Kylin servers). For example:

```properties
spring.cloud.zookeeper.discovery.instance-host=10.1.2.3
```

This config enforces Kylin to use IP address for service registry, making sure that other instances can find it by IP address instead of hostname. In case you use this config, please specify this config for **each of the Kylin instance** in `kylin.properties`. 

**Note:** Please replace the IP address with actual IP address on each server.
