---
layout: docs
title:  "Installation Guide"
categories: install
permalink: /cn/docs15/install/index.html
version: v0.7.2
since: v0.7.1
---

### Environment

Kylin requires a properly setup hadoop environment to run. Following are the minimal request to run Kylin, for more detial, please check this reference: [Hadoop Environment](hadoop_env.html).

## Prerequisites on Hadoop

* Hadoop: 2.4+
* Hive: 0.13+
* HBase: 0.98+, 1.x
* JDK: 1.7+  
_Tested with Hortonworks HDP 2.2 and Cloudera Quickstart VM 5.1_


It is most common to install Kylin on a Hadoop client machine. It can be used for demo use, or for those who want to host their own web site to provide Kylin service. The scenario is depicted as:

![On-Hadoop-CLI-installation](/images/install/on_cli_install_scene.png)

For normal use cases, the application in the above picture means Kylin Web, which contains a web interface for cube building, querying and all sorts of management. Kylin Web launches a query engine for querying and a cube build engine for building cubes. These two engines interact with the Hadoop components, like hive and hbase.

Except for some prerequisite software installations, the core of Kylin installation is accomplished by running a single script. After running the script, you will be able to build sample cube and query the tables behind the cubes via a unified web interface.

### Install Kylin

1. Download latest Kylin binaries at [http://kylin.apache.org/download](http://kylin.apache.org/download)
2. Export KYLIN_HOME pointing to the extracted Kylin folder
3. Make sure the user has the privilege to run hadoop, hive and hbase cmd in shell. If you are not so sure, you can run **bin/check-env.sh**, it will print out the detail information if you have some environment issues.
4. To start Kylin, simply run **bin/kylin.sh start**
5. To stop Kylin, simply run **bin/kylin.sh stop**

> If you want to have multiple Kylin nodes please refer to [this](kylin_cluster.html)

After Kylin started you can visit <http://your_hostname:7070/kylin>. The username/password is ADMIN/KYLIN. It's a clean Kylin homepage with nothing in there. To start with you can:

1. [Quick play with a sample cube](../tutorial/kylin_sample.html)
2. [Create and Build your own cube](../tutorial/create_cube.html)
3. [Kylin Web Tutorial](../tutorial/web.html)

