---
layout: docs15
title:  "Advance Settings of Kylin Environment"
categories: install
permalink: /docs15/install/advance_settings.html
---

## Settings for compression

By default, Kylin does not enable compression, this is not the recommend settings for production environment, but a tradeoff for new Kylin users. A suitable compression algorithm will reduce the storage overhead. But unsupported algorithm will break the Kylin job build also. There are three kinds of compression used in Kylin, HBase table compression, Hive output compression and MR jobs output compression. Compression settings only take effect after restarting Kylin server instance (by `./kylin.sh start` and `./kylin.sh stop`).

### HBase table compression
The compression settings define in `kyiln.properties` by `kylin.hbase.default.compression.codec`, default value is *none*. The valid value includes *none*, *snappy*, *lzo*, *gzip* and *lz4*. Before changing the compression algorithm, please make sure the selected algorithm is supported on your HBase cluster. Especially for snappy, lzo and lz4, not all Hadoop distributions include these. 

### Hive output compression
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

### MR jobs output compression
The compression settings define in `kylin_job_conf.xml`. The default setting is empty which leverages the MR default configuration. If you want to override the settings, please add (or replace) the following properties into `kylin_job_conf.xml`. Take the snappy compression for example:
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

### LZO compression

#### Make sure LZO is working in your environment

There is a simple tool to test whether LZO is well installed on EVERY SERVER in HBase cluster ( http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.2.4/bk_installing_manually_book/content/ch_install_hdfs_yarn_chapter.html#install-snappy-man-install ), and restart the cluster.
To test it on the hadoop CLI that you deployed Kylin, Just run

{% highlight Groff markup %}
hbase org.apache.hadoop.hbase.util.CompressionTest file:///PATH-TO-A-LOCAL-TMP-FILE lzo
{% endhighlight %}

If no exception is printed, you're good to go. Otherwise you'll need to first install LZO properly on this server.
To test if the HBase cluster is ready to create LZO compressed tables, test following HBase command:

{% highlight Groff markup %}
create 'lzoTable', {NAME => 'colFam',COMPRESSION => 'LZO'}
{% endhighlight %}

#### Use LZO compression

Compression settings only take effect after restarting Kylin server instance (by `./kylin.sh start` and `./kylin.sh stop`). To use LZO for compressing MR jobs you need to modify $KYLIN_HOME/conf/kylin_job_conf.xml by replacing compression configuration entries to com.hadoop.compression.lzo. You can refer to other documents for details: http://xiaming.me/posts/2014/05/03/enable-lzo-compression-on-hadoop-pig-and-spark.  To use LZO for compressing HBase tables you need to open $KYLIN_HOME/conf/kylin.properties, change kylin.hbase.default.compression.codec=lzo.

## Enable LDAP or SSO authentication

Check [How to Enable Security with LDAP and SSO](../howto/howto_ldap_and_sso.html)
