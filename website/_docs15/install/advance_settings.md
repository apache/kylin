---
layout: docs15
title:  "Advanced Settings"
categories: install
permalink: /docs15/install/advance_settings.html
---

## Overwrite default kylin.properties at Cube level
In `conf/kylin.properties` there are many parameters, which control/impact on Kylin's behaviors; Most parameters are global configs like security or job related; while some are Cube related; These Cube related parameters can be customized at each Cube level, so you can control the behaviors more flexibly. The GUI to do this is in the "Configuration Overwrites" step of the Cube wizard, as the screenshot below.

![]( /images/install/overwrite_config.png)

Here take two example: 

 * `kylin.cube.algorithm`: it defines the Cubing algorithm that the job engine will select; Its default value is "auto", means the engine will dynamically pick an algorithm ("layer" or "inmem") by sampling the data. If you knows Kylin and your data/cluster well, you can set your preferred algorithm directly (usually "inmem" has better performance but will request more memory).   

 * `kylin.hbase.region.cut`: it defines how big a region is when creating the HBase table. The default value is "5" (GB) per region. It might be too big for a small or medium cube, so you can give it a smaller value to get more regions created, then can gain better query performance.

## Overwrite default Hadoop job conf at Cube level
The `conf/kylin_job_conf.xml` and `conf/kylin_job_conf_inmem.xml` manage the default configurations for Hadoop jobs. If you have the need to customize the configs by cube, you can achieve that with the similar way as above, but need adding a prefix `kylin.job.mr.config.override.`; These configs will be parsed out and then applied when submitting jobs. See two examples below:

 * If want a cube's job getting more memory from Yarn, you can define: `kylin.job.mr.config.override.mapreduce.map.java.opts=-Xmx7g` and `kylin.job.mr.config.override.mapreduce.map.memory.mb=8192`
 * If want a cube's job going to a different Yarn resource queue, you can define: `kylin.job.mr.config.override.mapreduce.job.queuename=myQueue` (note: "myQueue" is just a sample)

 ## Overwrite default Hive job conf at Cube level
The `conf/kylin_hive_conf.xml` manage the default configurations when running Hive job (like creating intermediate flat hive table). If you have the need to customize the configs by cube, you can achieve that with the similar way as above, but need using another prefix `kylin.hive.config.override.`; These configs will be parsed out and then applied when running "hive -e" or "beeline" commands. See example below:

 * If want hive goes a different Yarn resource queue, you can define: `kylin.hive.config.override.mapreduce.job.queuename=myQueue` (note: "myQueue" is just a sample)

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
