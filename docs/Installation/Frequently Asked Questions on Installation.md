Frequently Asked Questions on Installation
---
* Some NPM error causes ERROR exit (中国大陆地区用户请特别注意此问题)?
> Check out https://github.com/KylinOLAP/Kylin/issues/35

* Can't get master address from ZooKeeper" when installing Kylin on Hortonworks Sandbox
> Check out https://github.com/KylinOLAP/Kylin/issues/9.

* Install scripted finished in my virtual machine, but cannot visit via http://localhost:9080
> Check out https://github.com/KylinOLAP/Kylin/issues/12.

* Map Reduce Job information can't display on sandbox deployment
> Check out https://github.com/KylinOLAP/Kylin/issues/40

* Install Kylin on CDH 5.2 or Hadoop 2.5.x
> Check out discussion: https://groups.google.com/forum/?utm_medium=email&utm_source=footer#!msg/kylin-olap/X0GZfsX1jLc/nzs6xAhNpLkJ
> 
```
I was able to deploy Kylin with following option in POM.
<hadoop2.version>2.5.0</hadoop2.version>
<yarn.version>2.5.0</yarn.version>
<hbase-hadoop2.version>0.98.6-hadoop2</hbase-hadoop2.version>
<zookeeper.version>3.4.5</zookeeper.version>
<hive.version>0.13.1</hive.version>
My Cluster is running on Cloudera Distribution CDH 5.2.0.
```
