---
layout: post
title:  "Kylin Metadata Store"
date:   2015-03-05
author: hongbin ma
categories: howto
---

Kylin organizes all of its metadata(including cube descriptions and instances, projects, inverted index description and instances, jobs, tables and dictionaries) as a hierarchy file system. However, Kylin uses hbase to store it, rather than normal file system. If you check your kylin configuration file(kylin.properties) you will find such a line:

{% highlight Groff markup %}
# The metadata store in hbase
kylin.metadata.url=kylin_metadata_qa@hbase:sandbox.hortonworks.com:2181:/hbase-unsecure
{% endhighlight %}


This indicates that the metadata will be saved as a htable called `kylin_metadata_qa`. You can scan the htable in hbase shell to check it out.

# Backup Metadata Store

Sometimes you need to backup the Kylin's Metadata Store from hbase to your disk file system.
In such cases, assuming you're on the hadoop CLI(or sandbox) where you deployed Kylin, you can use:

{% highlight Groff markup %}
mkdir ~/meta_dump

hbase  org.apache.hadoop.util.RunJar  PATH_TO_KYLIN_JOB_JAR_FOLDER/kylin-job-x.x.x-SNAPSHOT-job.jar com.kylinolap.common.persistence.ResourceTool  copy PATH_TO_KYLIN_CONFIG/kylin.properties ~/meta_dump
{% endhighlight %}

to dump your metadata to your local folder ~/meta_dump.

# Restore Metadata Store

In case you find your metadata store messed up, and you want to restore to a previous backup:

first clean up the metadata store:

{% highlight Groff markup %}
hbase  org.apache.hadoop.util.RunJar PATH_TO_KYLIN_JOB_JAR_FOLDER/kylin-job-x.x.x-SNAPSHOT-job.jar com.kylinolap.common.persistence.ResourceTool  reset 
{% endhighlight %}

then upload the backup metadata in ~/meta_dump to Kylin's metadata store:

{% highlight Groff markup %}
hbase  org.apache.hadoop.util.RunJar  PATH_TO_KYLIN_JOB_JAR_FOLDER/kylin-job-x.x.x-SNAPSHOT-job.jar com.kylinolap.common.persistence.ResourceTool  copy ~/meta_dump PATH_TO_KYLIN_CONFIG/kylin.properties
{% endhighlight %}

