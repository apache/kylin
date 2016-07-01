---
layout: docs15
title:  "Hadoop Environment"
categories: install
permalink: /docs15/install/hadoop_env.html
---

Kylin need run in a Hadoop node, to get better stability, we suggest you to deploy it a pure Hadoop client machine, on which it the command lines like `hive`, `hbase`, `hadoop`, `hdfs` already be installed and configured. The Linux account that running Kylin has got permission to the Hadoop cluster, including create/write hdfs, hive tables, hbase tables and submit MR jobs. 

## Recommended Hadoop Versions

* Hadoop: 2.6 - 2.7
* Hive: 0.13 - 1.2.1
* HBase: 0.98 - 0.99, 1.x
* JDK: 1.7+

_Tested with Hortonworks HDP 2.2 and Cloudera Quickstart VM 5.1_

To make things easier we strongly recommend you try Kylin with an all-in-one sandbox VM, like [HDP sandbox](http://hortonworks.com/products/hortonworks-sandbox/). In the following tutorial we'll go with **Hortonworks Sandbox 2.1** and **Cloudera QuickStart VM 5.1**. 

To avoid permission issue in the sandbox, you can use its `root` account. The password for **Hortonworks Sandbox 2.1** is `hadoop` , for **Cloudera QuickStart VM 5.1** is `cloudera`.

We also suggest you using bridged mode instead of NAT mode in Virtual Box settings. Bridged mode will assign your sandbox an independent IP address so that you can avoid issues like [this](https://github.com/KylinOLAP/Kylin/issues/12).

### Start Hadoop
Use ambari helps to launch hadoop:

```
ambari-agent start
ambari-server start
```

With both command successfully run you can go to ambari homepage at <http://your_sandbox_ip:8080> (user:admin,password:admin) to check everything's status. **By default hortonworks ambari disables Hbase, you need manually start the `Hbase` service at ambari homepage.**

![start hbase in ambari](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/installation/starthbase.png)

**Additonal Info for setting up Hortonworks Sandbox on Virtual Box**

	Please make sure Hbase Master port [Default 60000] and Zookeeper [Default 2181] is forwarded to Host OS.
 
