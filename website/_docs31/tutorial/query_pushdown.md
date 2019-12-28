---
layout: docs31
title:  Enable Query Pushdown
categories: tutorial
permalink: /docs31/tutorial/query_pushdown.html
since: v2.1
---

### Introduction

If a query can not be answered by any cube, Kylin supports pushing down such query to backup query engines like Hive, SparkSQL, Impala through JDBC.


### Query Pushdown config

#### Pushdown to single engine
1. In Kylin's installation directory, uncomment configuration item `kylin.query.pushdown.runner-class-name` of config file `kylin.properties`, and set it to `org.apache.kylin.query.adhoc.PushDownRunnerJdbcImpl`


2. Add configuration items below in config file `kylin.properties`. 

   - *kylin.query.pushdown.jdbc.url*: Hive JDBC's URL.

   - *kylin.query.pushdown.jdbc.driver*: Hive Jdbc's driver class name.

   - *kylin.query.pushdown.jdbc.username*: Hive Jdbc's user name.

   - *kylin.query.pushdown.jdbc.password*: Hive Jdbc's password.

   - *kylin.query.pushdown.jdbc.pool-max-total*: Hive Jdbc's connection pool's max connected connection number, default value is 8

   - *kylin.query.pushdown.jdbc.pool-max-idle*: Hive Jdbc's connection pool's max waiting connection number, default value is 8

   - *kylin.query.pushdown.jdbc.pool-min-idle*: Hive Jdbc's connection pool's min connected connection number, default value is 0

Here is a sample configuration; remember to change host "hiveserver" and port "10000" with your cluster configuraitons.

Then, restart Kylin.

{% highlight Groff markup %}
kylin.query.pushdown.runner-class-name=org.apache.kylin.query.adhoc.PushDownRunnerJdbcImpl
kylin.query.pushdown.jdbc.url=jdbc:hive2://hiveserver:10000/default
kylin.query.pushdown.jdbc.driver=org.apache.hive.jdbc.HiveDriver
kylin.query.pushdown.jdbc.username=hive
kylin.query.pushdown.jdbc.password=
kylin.query.pushdown.jdbc.pool-max-total=8
kylin.query.pushdown.jdbc.pool-max-idle=8
kylin.query.pushdown.jdbc.pool-min-idle=0

{% endhighlight %}

#### Pushdown to multi engines
Since v3.0.0, Kylin supports pushdown query to multiple engines through JDBC.
You can specify multiple engine ids by configuring `kylin.query.pushdown.runner.ids`, separated by `,`, such as:

{% highlight Groff markup %}
kylin.query.pushdown.runner.ids=id1,id2,id3
{% endhighlight %}

Three depression engines are specified. These three engines can be the same type or different types.

Multi-engine pushdown also supports specifying specific jdbc parameters. The meaning of the parameters is the same as the single engine pushdown described above. Please see the configuration below:

{% highlight Groff markup %}
kylin.query.pushdown.{id}.jdbc.url
kylin.query.pushdown.{id}.jdbc.driver
kylin.query.pushdown.{id}.jdbc.username
kylin.query.pushdown.{id}.jdbc.password
kylin.query.pushdown.{id}.jdbc.pool-max-total
kylin.query.pushdown.{id}.jdbc.pool-max-idle
kylin.query.pushdown.{id}.jdbc.pool-min-idle
{% endhighlight %}

When specifying a specific jdbc parameter for an engine, please replace the above `{id}` with the real engine id, such as the configuration of `id1`:

{% highlight Groff markup %}
kylin.query.pushdown.id1.jdbc.url
kylin.query.pushdown.id1.jdbc.driver
kylin.query.pushdown.id1.jdbc.username
kylin.query.pushdown.id1.jdbc.password
kylin.query.pushdown.id1.jdbc.pool-max-total
kylin.query.pushdown.id1.jdbc.pool-max-idle
kylin.query.pushdown.id1.jdbc.pool-min-idle
{% endhighlight %}


### Do Query Pushdown

After Query Pushdown is configured, user is allowed to do flexible queries to the imported tables without available cubes.

   ![](/images/tutorial/2.1/push_down/push_down_1.png)

If query is answered by backup engine, `Is Query Push-Down` is set to `true` in the log.

   ![](/images/tutorial/2.1/push_down/push_down_2.png)
# 
