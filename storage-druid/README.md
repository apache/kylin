### How to compile

It's the same as Kylin on HBase

### How to deploy

It's the same as Kylin on HBase

### The config for Kylin on Druid

```
#support multi coordinators failover
kylin.storage.druid.coordinator-addresses=http://host1:port,http://host2:port

kylin.storage.druid.broker-host=host:port

#the druid segments path in HDFS
kylin.storage.druid.hdfs-location=/druid/segments

#the mysql metastore info for druid, must same as Druid config
kylin.storage.druid.mysql-url=jdbc:mysql://host:port/db?characterEncoding=UTF-8
kylin.storage.druid.mysql-user=xxx
kylin.storage.druid.mysql-password=xxx
kylin.storage.druid.mysql-seg-table=segments

kylin.storage.druid.max-shard-count=500

#avoid dependency conflicts
kylin.engine.mr.druid-config-override.mapreduce.job.user.classpath.first=true

#Druid need JDK8
kylin.engine.mr.druid-config-override.yarn.app.mapreduce.am.env=JAVA_HOME=/usr/local/java18
kylin.engine.mr.druid-config-override.mapred.child.env=JAVA_HOME=/usr/local/java18

kylin.engine.mr.druid-config-override.mapreduce.reduce.memory.mb=3500
kylin.engine.mr.druid-config-override.mapreduce.reduce.speculative=false
```

### How to use Kylin Druid Storage
when create or update cube, change the "Cube Storage" to Druid in "Advanced Setting"