---
layout: docs31
title:  "Advanced Settings"
categories: install
permalink: /docs31/install/advance_settings.html
---

## Overwrite default kylin.properties at Cube level
In `conf/kylin.properties` there are many parameters, which control/impact on Kylin's behaviors; Most parameters are global configs like security or job related; while some are Cube related; These Cube related parameters can be customized at each Cube level, so you can control the behaviors more flexibly. The GUI to do this is in the "Configuration Overwrites" step of the Cube wizard, as the screenshot below.

![]( /images/install/overwrite_config_v2.png)

Here take two example: 

 * `kylin.cube.algorithm`: it defines the Cubing algorithm that the job engine will select; Its default value is "auto", means the engine will dynamically pick an algorithm ("layer" or "inmem") by sampling the data. If you knows Kylin and your data/cluster well, you can set your preferred algorithm directly.   

 * `kylin.storage.hbase.region-cut-gb`: it defines how big a region is when creating the HBase table. The default value is "5" (GB) per region. It might be too big for a small or medium cube, so you can give it a smaller value to get more regions created, then can gain better query performance.

## Overwrite default Hadoop job conf at Cube level
The `conf/kylin_job_conf.xml` and `conf/kylin_job_conf_inmem.xml` manage the default configurations for Hadoop jobs. If you have the need to customize the configs by cube, you can achieve that with the similar way as above, but need adding a prefix `kylin.engine.mr.config-override.`; These configs will be parsed out and then applied when submitting jobs. See two examples below:

 * If want a cube's job getting more memory from Yarn, you can define: `kylin.engine.mr.config-override.mapreduce.map.java.opts=-Xmx7g` and `kylin.engine.mr.config-override.mapreduce.map.memory.mb=8192`
 * If want a cube's job going to a different Yarn resource queue, you can define: `kylin.engine.mr.config-override.mapreduce.job.queuename=myQueue` ("myQueue" is just a sample, change to your queue name)

## Overwrite default Hive job conf at Cube level

The `conf/kylin_hive_conf.xml` manages the default configurations when running Hive job (like creating intermediate flat hive table). If you have the need to customize the configs by cube, you can achieve that with the similar way as above, but need using another prefix `kylin.source.hive.config-override.`; These configs will be parsed out and then applied when running "hive -e" or "beeline" commands. See example below:

 * If want hive goes to a different Yarn resource queue, you can define: `kylin.source.hive.config-override.mapreduce.job.queuename=myQueue` ("myQueue" is just a sample, change to your queue name)

## Overwrite default Spark conf at Cube level

 The configurations for Spark are managed in `conf/kylin.properties` with prefix `kylin.engine.spark-conf.`. For example, if you want to use job queue "myQueue" to run Spark, setting "kylin.engine.spark-conf.spark.yarn.queue=myQueue" will let Spark get "spark.yarn.queue=myQueue" feeded when submitting applications. The parameters can be configured at Cube level, which will override the default values in `conf/kylin.properties`. 

## Enable compression

By default, Kylin does not enable compression, this is not the recommend settings for production environment, but a tradeoff for new Kylin users. A suitable compression algorithm will reduce the storage overhead. But unsupported algorithm will break the Kylin job build also. There are three kinds of compression used in Kylin, HBase table compression, Hive output compression and MR jobs output compression. 

* HBase table compression
The compression settings define in `kyiln.properties` by `kylin.hbase.default.compression.codec`, default value is *none*. The valid value includes *none*, *snappy*, *lzo*, *gzip* and *lz4*. Before changing the compression algorithm, please make sure the selected algorithm is supported on your HBase cluster. Especially for snappy, lzo and lz4, not all Hadoop distributions include these. 

* Hive output compression
The compression settings define in `kylin_hive_conf.xml`. The default setting is empty which leverages the Hive default configuration. If you want to override the settings, please add (or replace) the following properties into `kylin_hive_conf.xml`. Take the snappy compression for example:
{% highlight Groff markup %}
    <property>
        <name>mapreduce.map.output.compress.codec</name>
        <value>org.apache.hadoop.io.compress.SnappyCodec</value>
        <description></description>
    </property>
    <property>
        <name>mapreduce.output.fileoutputformat.compress.codec</name>
        <value>org.apache.hadoop.io.compress.SnappyCodec</value>
        <description></description>
    </property>
{% endhighlight %}

* MR jobs output compression
The compression settings define in `kylin_job_conf.xml` and `kylin_job_conf_inmem.xml`. The default setting is empty which leverages the MR default configuration. If you want to override the settings, please add (or replace) the following properties into `kylin_job_conf.xml` and `kylin_job_conf_inmem.xml`. Take the snappy compression for example:
{% highlight Groff markup %}
    <property>
        <name>mapreduce.map.output.compress.codec</name>
        <value>org.apache.hadoop.io.compress.SnappyCodec</value>
        <description></description>
    </property>
    <property>
        <name>mapreduce.output.fileoutputformat.compress.codec</name>
        <value>org.apache.hadoop.io.compress.SnappyCodec</value>
        <description></description>
    </property>
{% endhighlight %}

Compression settings only take effect after restarting Kylin server instance.

## Allocate more memory to Kylin instance

Open `bin/setenv.sh`, which has two sample settings for `KYLIN_JVM_SETTINGS` environment variable; The default setting is small (4GB at max.), you can comment it and then un-comment the next line to allocate 16GB:

{% highlight Groff markup %}
export KYLIN_JVM_SETTINGS="-Xms1024M -Xmx4096M -Xss1024K -XX:MaxPermSize=128M -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$KYLIN_HOME/logs/kylin.gc.$$ -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=64M"
# export KYLIN_JVM_SETTINGS="-Xms16g -Xmx16g -XX:MaxPermSize=512m -XX:NewSize=3g -XX:MaxNewSize=3g -XX:SurvivorRatio=4 -XX:+CMSClassUnloadingEnabled -XX:+CMSParallelRemarkEnabled -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:CMSInitiatingOccupancyFraction=70 -XX:+DisableExplicitGC -XX:+HeapDumpOnOutOfMemoryError"
{% endhighlight %}

## Enable multiple job engines (HA)
Since Kylin 2.0, Kylin support multiple job engines running together, which is more extensible, available and reliable than the default job scheduler.

To enable the distributed job scheduler, you need to set or update the configs in the kylin.properties:

```
kylin.job.scheduler.default=2
kylin.job.lock=org.apache.kylin.storage.hbase.util.ZookeeperJobLock
```
Please add all job servers and query servers to the `kylin.server.cluster-servers`.

## Enable LDAP or SSO authentication

Check [How to Enable Security with LDAP and SSO](../howto/howto_ldap_and_sso.html)


## Enable email notification

Kylin can send email notification on job complete/fail; To enable this, edit `conf/kylin.properties`, set the following parameters:
{% highlight Groff markup %}
mail.enabled=true
mail.host=your-smtp-server
mail.username=your-smtp-account
mail.password=your-smtp-pwd
mail.sender=your-sender-address
kylin.job.admin.dls=adminstrator-address
{% endhighlight %}

Restart Kylin server to take effective. To disable, set `mail.enabled` back to `false`.

Administrator will get notifications for all jobs. Modeler and Analyst need enter email address into the "Notification List" at the first page of cube wizard, and then will get notified for that cube.


## Enable MySQL as Kylin metadata storage (beta)

Kylin can use MySQL as the metadata storage, for the scenarios that HBase is not the best option; To enable this, you can perform the following steps: 

* Install a MySQL server, e.g, v5.1.17;
* Create a new MySQL database for Kylin metadata, for example "kylin_metadata";
* Download and copy MySQL JDBC connector "mysql-connector-java-<version>.jar" to $KYLIN_HOME/ext (if the folder does not exist, create it yourself);
* Edit `conf/kylin.properties`, set the following parameters:
{% highlight Groff markup %}
kylin.metadata.url={your_metadata_tablename}@jdbc,url=jdbc:mysql://localhost:3306/kylin,username={your_username},password={your_password},driverClassName=com.mysql.jdbc.Driver
kylin.metadata.jdbc.dialect=mysql
kylin.metadata.jdbc.json-always-small-cell=true
kylin.metadata.jdbc.small-cell-meta-size-warning-threshold=100mb
kylin.metadata.jdbc.small-cell-meta-size-error-threshold=1gb
kylin.metadata.jdbc.max-cell-size=1mb
{% endhighlight %}
In "kylin.metadata.url" more configuration items can be added; The `url`, `username`, and `password` are required items. If not configured, the default configuration items will be used:
{% highlight Groff markup %}
url: the JDBC connection URL;
username: JDBC user name
password: JDBC password, if encryption is selected, please put the encrypted password here;
driverClassName: JDBC driver class name, the default value is com.mysql.jdbc.Driver
maxActive: the maximum number of database connections, the default value is 5;
maxIdle: the maximum number of connections waiting, the default value is 5;
maxWait: The maximum number of milliseconds to wait for connection. The default value is 1000.
removeAbandoned: Whether to automatically reclaim timeout connections, the default value is true;
removeAbandonedTimeout: the number of seconds in the timeout period, the default is 300;
passwordEncrypted: Whether the JDBC password is encrypted or not, the default is false;
{% endhighlight %}

* You can encrypt your password:
{% highlight Groff markup %}
cd $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib
java -classpath kylin-server-base-\<version\>.jar:kylin-core-common-\<version\>.jar:spring-beans-4.3.10.RELEASE.jar:spring-core-4.3.10.RELEASE.jar:commons-codec-1.7.jar org.apache.kylin.rest.security.PasswordPlaceholderConfigurer AES <your_password>
{% endhighlight %}

* Start Kylin

**Note: The feature is in beta now.**

## Use SparkSql to create intermediate flat Hive table 

**Note: There will be an issue when connecting thriftserver again, detail info, please check [https://issues.apache.org/jira/browse/SPARK-21067](https://issues.apache.org/jira/browse/SPARK-21067)**

Kylin can use SparkSql to create intermediate flat Hive table; Before enable this: 

- Make sure the following parameters exist in hive-site.xml:

{% highlight Groff markup %}

<property>
  <name>hive.security.authorization.sqlstd.confwhitelist</name>
  <value>mapred.*|hive.*|mapreduce.*|spark.*</value>
</property>

<property>
  <name>hive.security.authorization.sqlstd.confwhitelist.append</name>
  <value>mapred.*|hive.*|mapreduce.*|spark.*</value>
</property>
    
{% endhighlight %}
- Change `hive.execution.engine` to mr (Optional), if you want to use tez, please make sure the dependency of tez has been added
- Copy the hive-site.xml to $SPARK_HOME/conf
- Make sure the environmental variable HADOOP_CONF_DIR has been set
- Use `sbin/start-thriftserver.sh --master spark://sparkmasterip:sparkmasterport` to start the thriftserver, usually port is 7077
- Edit `conf/kylin.properties`, set the following parameters:
{% highlight Groff markup %}
kylin.source.hive.enable-sparksql-for-table-ops=true
kylin.source.hive.sparksql-beeline-shell=/path/to/spark-client/bin/beeline
kylin.source.hive.sparksql-beeline-params=-n root -u 'jdbc:hive2://thriftserverip:thriftserverport'
{% endhighlight %}

Restart Kylin server to take effective. To disable, set `kylin.source.hive.enable-sparksql-for-table-ops` back to `false`.


