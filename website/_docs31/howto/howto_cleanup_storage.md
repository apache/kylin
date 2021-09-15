---
layout: docs31
title:  Cleanup Storage
categories: howto
permalink: /docs31/howto/howto_cleanup_storage.html
---

Kylin will generate intermediate files in HDFS during the cube building; Besides, when purge/drop/merge cubes, some HBase tables may be left in HBase and will no longer be queried; Although Kylin has started to do some 
automated garbage collection, it might not cover all cases; You can do an offline storage cleanup periodically:

Steps:
1. Check which resources can be cleanup, this will not remove anything:
{% highlight Groff markup %}
export KYLIN_HOME=/path/to/kylin_home
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete false
{% endhighlight %}
Here please replace (version) with the specific Kylin jar version in your installation;
2. You can pickup 1 or 2 resources to check whether they're no longer be referred; Then add the "--delete true" option to start the cleanup:
{% highlight Groff markup %}
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete true
{% endhighlight %}
On finish, the intermediate Hive tables, HDFS location and HTables should be dropped;
3. If you want to delete all resources, then add the "--force true" option to start the cleanup:
{% highlight Groff markup %}
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --force true --delete true
{% endhighlight %}
On finish, all the intermediate Hive tables, HDFS location and HTables should be dropped;
