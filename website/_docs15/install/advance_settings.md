---
layout: docs15
title:  "Advance Settings of Kylin Environment"
categories: install
permalink: /docs15/install/advance_settings.html
---

## Enable LZO compression

By default Kylin leverages snappy compression to compress the output of MR jobs, as well as hbase table storage, reducing the storage overhead. We do not choose LZO compression in Kylin because hadoop venders tend to not include LZO in their distributions due to license(GPL) issues. To enable LZO in Kylin, follow these steps:

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

#### Use LZO for HBase compression

You'll need to stop Kylin first by running `./kylin.sh stop`, and then modify $KYLIN_HOME/conf/kylin_job_conf.xml by uncommenting some configuration entries related to LZO compression. 
After this, you need to run `./kylin.sh start` to start Kylin again. Now Kylin will use LZO to compress MR outputs and hbase tables.

Goto $KYLIN_HOME/conf/kylin.properties, change kylin.hbase.default.compression.codec=snappy to kylin.hbase.default.compression.codec=lzo

#### Use LZO for MR jobs

Modify $KYLIN_HOME/conf/kylin_job_conf.xml by changing all org.apache.hadoop.io.compress.SnappyCodec to com.hadoop.compression.lzo.LzoCodec. 

Start Kylin again. Now Kylin will use LZO to compress MR outputs and HBase tables.

## Enable LDAP or SSO authentication

Check [How to Enable Security with LDAP and SSO](../howto/howto_ldap_and_sso.html)
