---
layout: docs
title:  "Kylin Metadata Store"
categories: development
permalink: /docs/development/metadata_store.html
version: v0.7.2
since: v0.7.1
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
export KYLIN_HOME=PATH_TO_KYLIN_HOME
hbase org.apache.hadoop.util.RunJar $KYLIN_HOME/lib/kylin-job-x.x.x.jar com.kylinolap.common.persistence.ResourceTool download ~/meta_dump
{% endhighlight %}

to dump your metadata to your local folder ~/meta_dump.

# Restore Metadata Store

In case you find your metadata store messed up, and you want to restore to a previous backup:

Firstly, reset the metadata store:

{% highlight Groff markup %}
hbase  org.apache.hadoop.util.RunJar $KYLIN_HOME/lib/kylin-job-x.x.x.jar com.kylinolap.common.persistence.ResourceTool  reset
{% endhighlight %}

Then upload the backup metadata in ~/meta_dump to Kylin's metadata store:

{% highlight Groff markup %}
hbase org.apache.hadoop.util.RunJar  $KYLIN_HOME/lib/kylin-job-x.x.x.jar com.kylinolap.common.persistence.ResourceTool  copy ~/meta_dump PATH_TO_KYLIN_CONFIG/kylin.properties
{% endhighlight %}

# Cleanup unused resources from Metadata Store (available since 0.7.3)
As time goes on, some resources like dictionary, table snapshots became useless (as the cube segment be dropped or merged), but they still take space there; You can run command to find and cleanup them from metadata store:

Firstly, run a check, this is safe as it will not change anything:
{% highlight Groff markup %}
hbase org.apache.hadoop.util.RunJar $KYLIN_HOME/lib/kylin-job-x.x.x.jar org.apache.kylin.job.hadoop.cube.MetadataCleanupJob
{% endhighlight %}

The resources that will be dropped will be listed;

Next, add the "--delete true" parameter to cleanup those resources; before this, make sure you have made a backup of the metadata store;
{% highlight Groff markup %}
hbase org.apache.hadoop.util.RunJar $KYLIN_HOME/lib/kylin-job-x.x.x.jar org.apache.kylin.job.hadoop.cube.MetadataCleanupJob --delete true
{% endhighlight %}
