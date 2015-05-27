---
layout: post
title:  "Advance settings of Kylin environment"
date:   2015-01-22
author: hongbin ma
categories: installation
---

## Enabling LZO compression

LZO compression can be leveraged to compress the output of MR jobs, as well as hbase table storage, reducing the storage overhead. By default we do not enable LZO compression in Kylin because hadoop sandbox venders tend to not include LZO in their distributions due to license(GPL) issues.

To enable LZO in Kylin, there are three steps:

### Make sure LZO is working in your environment

we have a simple tool to test whether LZO is well installed and configured in your environment(we only test it on the hadoop CLI that you deployed Kylin), Just run

{% highlight Groff markup %}
hbase org.apache.hadoop.util.RunJar kylin-job-latest.jar com.kylinolap.job.tools.LZOSupportnessChecker
{% endhighlight %}

If the program prints "LZO supported by current env? true", you're good to go. Otherwise you'll need to first install LZO properly.

### Modify kylin_job_conf.xml

You'll need to stop Kylin first by running `./kylin.sh stop`, and then modify /etc/kylin/kylin_job_conf.xml by uncommenting some configuration entries related to LZO compression. 

### export KYLIN_LD_LIBRARY_PATH to carry your native library paths

Before running `./kylin.sh start` again, you'll need to export KYLIN_LD_LIBRARY_PATH to carry your LZO native library paths. If you are not sure where it is, you can simply find it in /tmp/kylin_retrieve.sh, Kylin retrieved this from hbase, normally this should work. Here's an example for hdp 2.1:

{% highlight Groff markup %}
export KYLIN_LD_LIBRARY_PATH=::/usr/lib/hadoop/lib/native/Linux-amd64-64:/usr/lib/hadoop/lib/native
{% endhighlight %}

After exporting, you need to run `./kylin.sh start` to start Kylin again. Now Kylin will use LZO to compress MR outputs and hbase tables.
