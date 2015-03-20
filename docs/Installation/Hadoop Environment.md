## Hadoop Environment

Kylin requires you having access to a hadoop CLI, where you have full permissions to hdfs, hive, hbase and map-reduce. To make things easier we strongly recommend you starting with running Kylin on a hadoop sandbox, like <http://hortonworks.com/products/hortonworks-sandbox/>. In the following tutorial we'll go with **Hortonworks Sandbox 2.1** and **Cloudera QuickStart VM 5.1**. 

To avoid permission issue, we suggest you using `root` account. The password for **Hortonworks Sandbox 2.1** is `hadoop` , for **Cloudera QuickStart VM 5.1** is `cloudera`.

We also suggest you using bridged mode instead of NAT mode in your virtual box settings. Bridged mode will assign your sandbox an independent IP so that you can avoid issues like https://github.com/KylinOLAP/Kylin/issues/12

### Start Hadoop

Please make sure Hive, HDFS and HBase are available on our CLI machine.
If you don't know how, here's a simple tutorial for hortonworks sanbox:

Use ambari helps to launch hadoop:

	ambari-agent start
	ambari-server start
	
With both command successfully run you can go to ambari homepage at <http://your_sandbox_ip:8080> (user:admin,password:admin) to check everything's status. **By default hortonworks ambari disables Hbase, you'll need manually start the `Hbase` service at ambari homepage.**

![start hbase in ambari](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/installation/starthbase.png)