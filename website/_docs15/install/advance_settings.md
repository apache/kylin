---
layout: docs15
title:  "Advance Settings of Kylin Environment"
categories: install
permalink: /docs15/install/advance_settings.html
---

## Settings for compression

### Disable compression (Easiest way to address compression related issues)

By default Kylin leverages snappy compression to compress the output of MR jobs, as well as hbase table storage, to reduce the storage overhead. We do not choose LZO compression in Kylin because hadoop venders tend to not include LZO in their distributions due to license(GPL) issues. If you compression related issues happened in your cubing job, you have two options: 1. Disable compression 2. Choose other compression algorithms like LZO. 

#### Disable HBase compression

Compression settings only take effect after restarting Kylin server instance (by `./kylin.sh start` and `./kylin.sh stop`). To disable compressing MR jobs you need to modify $KYLIN_HOME/conf/kylin_job_conf.xml by removing all configuration entries related to compression(Just grep the keyword "compress"). To disable compressing hbase tables you need to open $KYLIN_HOME/conf/kylin.properties and remove the line starting with kylin.hbase.default.compression.codec.

### LZO compression

#### Make sure LZO is working in your environment

We have a simple tool to test whether LZO is well installed on EVERY SERVER in hbase cluster ( http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.2.4/bk_installing_manually_book/content/ch_install_hdfs_yarn_chapter.html#install-snappy-man-install ), and restart the cluster.
To test it on the hadoop CLI that you deployed Kylin, Just run

{% highlight Groff markup %}
hbase org.apache.hadoop.hbase.util.CompressionTest file:///PATH-TO-A-LOCAL-TMP-FILE lzo
{% endhighlight %}

If no exception is printed, you're good to go. Otherwise you'll need to first install LZO properly on this server.
To test if the hbase cluster is ready to create LZO compressed tables, test following hbase command:

{% highlight Groff markup %}
create 'lzoTable', {NAME => 'colFam',COMPRESSION => 'LZO'}
{% endhighlight %}

#### Use LZO compression

Compression settings only take effect after restarting Kylin server instance (by `./kylin.sh start` and `./kylin.sh stop`). To use LZO for compressing MR jobs you need to modify $KYLIN_HOME/conf/kylin_job_conf.xml by replacing configuration entries related to compression from org.apache.hadoop.io.compress.SnappyCodec to com.hadoop.compression.lzo. You can refer to other documents for details: http://xiaming.me/posts/2014/05/03/enable-lzo-compression-on-hadoop-pig-and-spark.  To use LZO for compressing hbase tables you need to open $KYLIN_HOME/conf/kylin.properties, change kylin.hbase.default.compression.codec=snappy to kylin.hbase.default.compression.codec=lzo.

## Enable LDAP or SSO authentication

Check [How to Enable Security with LDAP and SSO](../howto/howto_ldap_and_sso.html)
