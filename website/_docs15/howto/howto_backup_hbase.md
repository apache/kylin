---
layout: docs15
title:  Clean/Backup HBase Tables
categories: howto
permalink: /docs15/howto/howto_backup_hbase.html
since: v0.7.1
---

Kylin persists all data (meta data and cube) in HBase; You may want to export the data sometimes for whatever purposes 
(backup, migration, troubleshotting etc); This page describes the steps to do this and also there is a Java app for you to do this easily.

Steps:

1. Cleanup unused cubes to save storage space (be cautious on production!): run the following command in hbase CLI: 
{% highlight Groff markup %}
hbase org.apache.hadoop.util.RunJar /${KYLIN_HOME}/lib/kylin-job-(version).jar org.apache.kylin.job.hadoop.cube.StorageCleanupJob --delete true
{% endhighlight %}
2. List all HBase tables, iterate and then export each Kylin table to HDFS; 
See [https://hbase.apache.org/book/ops_mgt.html#export](https://hbase.apache.org/book/ops_mgt.html#export)

3. Copy the export folder from HDFS to local file system, and then archive it;

4. (optional) Download the archive from Hadoop CLI to local;

5. Cleanup the export folder from CLI HDFS and local file system;

Kylin provide the "ExportHBaseData.java" (currently only exist in "minicluster" branch) for you to do the 
step 2-5 in one run; Please ensure the correct path of "kylin.properties" has been set in the sys env; This Java uses the sandbox config by default;
