---
layout: docs40
title:  Backup Metadata
categories: howto
permalink: /docs40/howto/howto_backup_metadata.html
---

Kylin organizes all of its metadata (including cube descriptions and instances, projects, inverted index description and instances, jobs, tables and dictionaries) as a hierarchy file system. However, Kylin uses mysql to store it, rather than normal file system. If you check your kylin configuration file(kylin.properties) you will find such a line:

{% highlight Groff markup %}
## The metadata store in mysql
kylin.metadata.url=kylin_metadata@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://localhost:3306/kylin_database,username=,password=
{% endhighlight %}

This indicates that the metadata will be saved as a table called `kylin_metadata` in mysql database `kylin_database`. 

## Metadata directory

Kylin metastore use `resource root path + resource name + resource suffix` as key to store metadata. You can refer to the following table to use `./bin/metastore.sh`.
 
| Resource root path  | resource name         | resource suffix
| --------------------| :---------------------| :--------------|
| /cube               | /cube name            | .json |
| /cube_desc          | /cube name            | .json |
| /cube_statistics    | /cube name/uuid       | .seq |
| /model_desc         | /model name           | .json |
| /project            | /project name         | .json |
| /table              | /DATABASE.TABLE--project name | .json |
| /table_exd          | /DATABASE.TABLE--project name | .json |
| /execute            | /job id               |  |
| /execute_output     | /job id-step index    |  |
| /user               | /user name            |  |

## View metadata

If you want to view some metadata, you can run:

{% highlight Groff markup %}
./bin/metastore.sh list /path/to/store/metadata
{% endhighlight %}

to list the entity stored in specified directory, and then run: 

{% highlight Groff markup %}
./bin/metastore.sh cat /path/to/store/entity/metadata.
{% endhighlight %}

to view one entity metadata.

## Backup metadata with binary package

Sometimes you need to backup the Kylin's metadata store from mysql to your disk file system.
In such cases, assuming you're on the hadoop CLI(or sandbox) where you deployed Kylin, you can go to KYLIN_HOME and run :

{% highlight Groff markup %}
./bin/metastore.sh backup
{% endhighlight %}

to dump your metadata to your local folder a folder under KYLIN_HOME/metadata_backps, the folder is named after current time with the syntax: KYLIN_HOME/meta_backups/meta_year_month_day_hour_minute_second

In addition, you can run:

{% highlight Groff markup %}
./bin/metastore.sh fetch /path/to/store/metadata
{% endhighlight %}

to dump metadata selectively. For example, run `./bin/metastore.sh fetch /cube_desc/` to get all cube desc metadata, or run `./bin/metastore.sh fetch /cube_desc/kylin_sales_cube.json` to get single cube desc metadata.

## Restore metadata with binary package

In case you find your metadata store messed up, and you want to restore to a previous backup:

Firstly, reset the metadata store (this will clean everything of the Kylin metadata store in mysql, make sure to backup):

{% highlight Groff markup %}
./bin/metastore.sh reset
{% endhighlight %}

Then upload the backup metadata to Kylin's metadata store:
{% highlight Groff markup %}
./bin/metastore.sh restore $KYLIN_HOME/meta_backups/meta_xxxx_xx_xx_xx_xx_xx
{% endhighlight %}

## Restore metadata selectively (Recommended)
If only changes a couple of metadata files, the administrator can just pick these files to restore, without having to cover all the metadata. Compared to the full recovery, this approach is more efficient, safer, so it is recommended.

Create a new empty directory, and then create subdirectories in it according to the location of the metadata files to restore; for example, to restore a Cube instance, you should create a "cube" subdirectory：

{% highlight Groff markup %}
mkdir /path/to/restore_new
mkdir /path/to/restore_new/cube
{% endhighlight %}

Copy the metadata file to be restored to this new directory:

{% highlight Groff markup %}
cp meta_backups/meta_2016_06_10_20_24_50/cube/kylin_sales_cube.json /path/to/restore_new/cube/
{% endhighlight %}

At this point, you can modify/fix the metadata manually.

Restore from this directory:

{% highlight Groff markup %}
cd $KYLIN_HOME
./bin/metastore.sh restore /path/to/restore_new
{% endhighlight %}

Only the files in the folder will be uploaded to Kylin metastore. Similarly, after the recovery is finished, click Reload Metadata button on the Web UI to flush cache.

## Backup/restore metadata in development env 

When developing/debugging Kylin, typically you have a dev machine with an IDE, and a backend sandbox. Usually you'll write code and run test cases at dev machine. It would be troublesome if you always have to put a binary package in the sandbox to check the metadata. There is a helper class called SandboxMetastoreCLI to help you download/upload metadata locally at your dev machine. Follow the Usage information and run it in your IDE.
