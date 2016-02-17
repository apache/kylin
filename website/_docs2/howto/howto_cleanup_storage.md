---
layout: docs2
title:  How to Cleanup Storage (HDFS & HBase Tables)
categories: howto
permalink: /docs2/howto/howto_cleanup_storage.html
version: v0.7.2
since: v0.7.1
---

Kylin will generate intermediate files in HDFS during the cube building; Besides, when purge/drop/merge cubes, some HBase tables may be left in HBase and will no longer be queried; Although Kylin has started to do some 
automated garbage collection, it might not cover all cases; You can do an offline storage cleanup periodically:

Steps:
1. Check which resources can be cleanup, this will not remove anything:
{% highlight Groff markup %}
hbase org.apache.hadoop.util.RunJar ${KYLIN_HOME}/lib/kylin-job-(version).jar org.apache.kylin.job.hadoop.cube.StorageCleanupJob --delete false
{% endhighlight %}
Here please replace (version) with the specific Kylin jar version in your installation;
2. You can pickup 1 or 2 resources to check whether they're no longer be referred; Then add the "--delete true" option to start the cleanup:
{% highlight Groff markup %}
hbase org.apache.hadoop.util.RunJar ${KYLIN_HOME}/lib/kylin-job-(version).jar org.apache.kylin.job.hadoop.cube.StorageCleanupJob --delete true
{% endhighlight %}
On finish, the intermediate HDFS location and HTables will be dropped;