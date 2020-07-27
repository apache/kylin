---
layout: docs40
title:  Cleanup Storage
categories: howto
permalink: /docs40/howto/howto_cleanup_storage.html
---

Kylin will generate intermediate files in HDFS during the cube building; Besides, when purge/drop/merge cubes, some Parquet file may be left and will no longer be queried; Although Kylin has started to do some 
automated garbage collection, it might not cover all cases; You can do an offline storage cleanup periodically:
Which can be deleted:
- temp job files
`hdfs:///kylin/${metadata_url}/${project}/job_tmp`
- none used segment cuboid files
`hdfs:///kylin/${metadata_url}/${project}/${cube_name}/${non_used_segment}`

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
On finish, the temp job files and none used segment cuboid files should be dropped;

