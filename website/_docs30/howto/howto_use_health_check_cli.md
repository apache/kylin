---
layout: docs30
title:  Kylin Health Check(NEW)
categories: howto
permalink: /docs30/howto/howto_use_health_check_cli.html
---

## Get started
In kylin 3.0, we add a health check job of Kylin which help to detect whether your Kylin is in good state. This will help to reduce manually work for Kylin's Administrator. If you have hundreds of cubes and thousands of building job every day, this feature help you quickly find failed job and segment which lost file or hbase table, or cube with too high expansion rate. 

Use this feature by adding following to *kylin.properties* if you are using 126.com:
{% highlight Groff markup %}
kylin.job.notification-enabled=true
kylin.job.notification-mail-enable-starttls=true
kylin.job.notification-mail-host=smtp.126.com
kylin.job.notification-mail-username=hahaha@126.com
kylin.job.notification-mail-password=hahaha
kylin.job.notification-mail-sender=hahaha@126.com
kylin.job.notification-admin-emails=hahaha@kyligence.io,hahaha@126.com
{% endhighlight %} 
After start the Kylin process, you should execute following command and get email received. In production env, it should be scheduled by crontab etc.
{% highlight Groff markup %}
sh bin/kylin.sh org.apache.kylin.tool.KylinHealthCheckJob
{% endhighlight %} 
You will receive email in your mailbox.

## Detail of health check step

### Checking metadata
This part will try record all path of entry which Kylin process failed to load from Metadata(ResourceStore). This maybe a signal of health state for Kylin's Metadata Store.

If find any error, it will be sent via email as following.
{% highlight Groff markup %}
Error loading CubeDesc at ${PATH} ...
Error loading DataModelDesc at ${PATH} ...
{% endhighlight %}

### Fix missing HDFS path of segments
This part will try to visit all segments and check whether segment file exists in HDFS.  

If find any error, it will be sent via email as following.
{% highlight Groff markup %}
Project: ${PROJECT} cube: ${CUBE} segment: ${SEGMENT} cube id data: ${SEGMENT_PATH} don't exist and need to rebuild it
{% endhighlight %}

### Checking HBase Table of segments
This part will check whether HTable belong to each segment exists and state is Enable, you may need to rebuild them or re-enable them if find any.

If find any error, it will be sent via email as following.
{% highlight Groff markup %}
HBase table: {TABLE_NAME} not exist for segment: {SEGMENT}, project: {PROJECT}
{% endhighlight %}

### Checking holes of Cubes
This part will try to check segment holes of each cube, so lost segments need to be rebuilt if find any.

If find any error, it will be sent via email as following.
{% highlight Groff markup %}
{COUNT_HOLE} holes in cube: {CUBE_NAME}, project: {PROJECT_NAME}
{% endhighlight %}

### Checking too many segments of Cubes
This part will try to check cube which have too many segments, so they need to merged.

If find any error, it will be sent via email as following.
{% highlight Groff markup %}
Too many segments: {COUNT_OF_SEGMENT} for cube: {CUBE_NAME}, project: {PROJECT_NAME}, please merge the segments
{% endhighlight %}

The threshold is decided by `kylin.tool.health-check.warning-segment-num`, default value is `-1`, which means skip check.

### Checking out-of-date Cubes
This part will try to find cube which have not been built for a long duration, so maybe you don't really need them. 

If find any error, it will be sent via email as following.
{% highlight Groff markup %}
Ready Cube: {CUBE_NAME} in project: {PROJECT_NAME} is not built more then {DAYS} days, maybe it can be disabled
Disabled Cube: {CUBE_NAME} in project: {PROJECT_NAME} is not built more then {DAYS} days, maybe it can be deleted
{% endhighlight %}

The threshold is decided by `kylin.tool.health-check.stale-cube-threshold-days`, default value is `100`.

### Check data expansion rate
This part will try to check cube have high expansion rate, so you may consider optimize them. 

If find any error, it will be sent via stdout as following.
{% highlight Groff markup %}
Cube: {CUBE_NAME} in project: {PROJECT_NAME} with too large expansion rate: {RATE}, cube data size: {SIZE}G
{% endhighlight %}

The expansion rate warning threshold is decided by `kylin.tool.health-check.warning-cube-expansion-rate`.
The cube-size warning threshold is decided by `kylin.tool.health-check.expansion-check.min-cube-size-gb`.

### Check cube configuration

This part will try to check cube has been set with auto merge & retention configuration. 

If find any error, it will be sent via stdout as following.
{% highlight Groff markup %}
Cube: {CUBE_NAME} in project: {PROJECT_NAME} with no auto merge params
Cube: {CUBE_NAME} in project: {PROJECT_NAME} with no retention params
{% endhighlight %} 

### Cleanup stopped job

Stopped and Error jobs which have not be repaired in time will be alarmed if find any.

{% highlight Groff markup %}
Should discard job: {}, which in ERROR/STOPPED state for {} days
{% endhighlight %} 

The duration is set by `kylin.tool.health-check.stale-job-threshold-days`, default is `30`.


----

For the detail of HealthCheck, please check code at *org.apache.kylin.rest.job.KylinHealthCheckJob* in Github Repo. 
If you have more suggestion or want to add more check rule, please submit a PR to master branch.
