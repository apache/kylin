---
layout: docs
title:  How to Backup Metadata
categories: howto
permalink: /docs/howto/howto_backup_metadata.html
version: v0.8
since: v0.7.1
---

Kylin organizes all of its metadata (including cube descriptions and instances, projects, inverted index description and instances, jobs, tables and dictionaries) as a hierarchy file system. However, Kylin uses hbase to store it, rather than normal file system. If you check your kylin configuration file(kylin.properties) you will find such a line:

{% highlight Groff markup %}
## The metadata store in hbase
kylin.metadata.url=kylin_metadata_qa@hbase
{% endhighlight %}

This indicates that the metadata will be saved as a htable called `kylin_metadata_qa`. You can scan the htable in hbase shell to check it out.

## Backup Metadata Store with binary package

Sometimes you need to backup the Kylin's Metadata Store from hbase to your disk file system.
In such cases, assuming you're on the hadoop CLI(or sandbox) where you deployed Kylin, you can go to KYLIN_HOME and run :

{% highlight Groff markup %}
bin/metastore.sh backup
{% endhighlight %}

to dump your metadata to your local folder a folder under KYLIN_HOME/metadata_backps, the folder is named after current time with the syntax: KYLIN_HOME/meta_backups/meta_year_month_day_hour_minute_second

## Restore Metadata Store with binary package

In case you find your metadata store messed up, and you want to restore to a previous backup:

Firstly, reset the metadata store (this will clean everything of the Kylin metadata store in hbase, make sure to backup):

{% highlight Groff markup %}
bin/metastore.sh reset
{% endhighlight %}

Then upload the backup metadata to Kylin's metadata store:
{% highlight Groff markup %}
bin/metastore.sh restore $KYLIN_HOME/meta_backups/meta_xxxx_xx_xx_xx_xx_xx
{% endhighlight %}

## Backup/Restore metadata in development env (available since 0.7.3)

When developing/debugging Kylin, typically you have a dev machine with an IDE, and a backend sandbox. Usually you'll write code and run test cases at dev machine. It would be troublesome if you always have to put a binary package in the sandbox to check the metadata. There is a helper class called SandboxMetastoreCLI to help you download/upload metadata locally at your dev machine. Follow the Usage information and run it in your IDE.

## Cleanup unused resources from Metadata Store (available since 0.7.3)
As time goes on, some resources like dictionary, table snapshots became useless (as the cube segment be dropped or merged), but they still take space there; You can run command to find and cleanup them from metadata store:

Firstly, run a check, this is safe as it will not change anything:
{% highlight Groff markup %}
bin/metastore.sh clean
{% endhighlight %}

The resources that will be dropped will be listed;

Next, add the "--delete true" parameter to cleanup those resources; before this, make sure you have made a backup of the metadata store;
{% highlight Groff markup %}
bin/metastore.sh clean --delete true
{% endhighlight %}
