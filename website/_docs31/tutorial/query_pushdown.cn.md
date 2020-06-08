---
layout: docs31-cn
title:  查询下压
categories: tutorial
permalink: /cn/docs31/tutorial/query_pushdown.html
since: v2.1
---

### Kylin 支持查询下压

对于没有cube能查得结果的sql，Kylin支持将这类查询通过JDBC下压至备用查询引擎如Hive, SparkSQL, Impala等来查得结果。

### 查询下压配置

#### 下压至单个引擎
以Hive为例说明开启步骤

1. 修改配置文件`kylin.properties`打开Query Pushdown注释掉的配置项`kylin.query.pushdown.runner-class-name`，设置为`org.apache.kylin.query.adhoc.PushDownRunnerJdbcImpl`


2. 在配置文件`kylin.properties`添加如下配置项。若不设置，将使用默认配置项。请不要忘记将"hiveserver"和"10000"替换成环境中Hive运行的主机和端口。

    - *kylin.query.pushdown.jdbc.url*：Hive JDBC的URL.

    - *kylin.query.pushdown.jdbc.driver*：Hive Jdbc的driver类名
      
    - *kylin.query.pushdown.jdbc.username*：Hive Jdbc对应数据库的用户名

    - *kylin.query.pushdown.jdbc.password*：Hive Jdbc对应数据库的密码

    - *kylin.query.pushdown.jdbc.pool-max-total*：Hive Jdbc连接池的最大连接数，默认值为8

    - *kylin.query.pushdown.jdbc.pool-max-idle*：Hive Jdbc连接池的最大等待连接数，默认值为8
    
    - *kylin.query.pushdown.jdbc.pool-min-idle*：Hive Jdbc连接池的最小连接数，默认值为0

下面是一个样例设置; 请记得将主机名"hiveserver"以及端口"10000"修改为您的集群设置。

{% highlight Groff markup %} kylin.query.pushdown.runner-class-name=org.apache.kylin.query.adhoc.PushDownRunnerJdbcImpl kylin.query.pushdown.jdbc.url=jdbc:hive2://hiveserver:10000/default kylin.query.pushdown.jdbc.driver=org.apache.hive.jdbc.HiveDriver kylin.query.pushdown.jdbc.username=hive kylin.query.pushdown.jdbc.password= kylin.query.pushdown.jdbc.pool-max-total=8 kylin.query.pushdown.jdbc.pool-max-idle=8 kylin.query.pushdown.jdbc.pool-min-idle=0

{% endhighlight %}

然后，重启Kylin

#### 下压至多个引擎
自 v3.0.0 起，开始支持通过JDBC下压至多个引擎进行查询。
通过配置 `kylin.query.pushdown.runner.ids` 来指定多个引擎id，id 之间以 `,` 进行分隔，如：

{% highlight Groff markup %}
kylin.query.pushdown.runner.ids=id1,id2,id3
{% endhighlight %}

指定了三个下压引擎，这三个引擎可以是同类型或不同类型的引擎。

多引擎下压同样支持指定具体的 jdbc 参数，参数含义和上述单个引擎下压相同，请看下面的配置

{% highlight Groff markup %}
kylin.query.pushdown.{id}.jdbc.url
kylin.query.pushdown.{id}.jdbc.driver
kylin.query.pushdown.{id}.jdbc.username
kylin.query.pushdown.{id}.jdbc.password
kylin.query.pushdown.{id}.jdbc.pool-max-total
kylin.query.pushdown.{id}.jdbc.pool-max-idle
kylin.query.pushdown.{id}.jdbc.pool-min-idle
{% endhighlight %}

当要为某个引擎指定具体的 jdbc 参数时，请将上面的 `{id}` 替换为真实的引擎 id，如替换为 `id1` 的各配置为:

{% highlight Groff markup %}
kylin.query.pushdown.id1.jdbc.url
kylin.query.pushdown.id1.jdbc.driver
kylin.query.pushdown.id1.jdbc.username
kylin.query.pushdown.id1.jdbc.password
kylin.query.pushdown.id1.jdbc.pool-max-total
kylin.query.pushdown.id1.jdbc.pool-max-idle
kylin.query.pushdown.id1.jdbc.pool-min-idle
{% endhighlight %}

### 进行查询下压

开启查询下压后，即可按同步的表进行灵活查询，而无需根据查询构建对应Cube。

   ![](/images/tutorial/2.1/push_down/push_down_1.png)

用户在提交查询时，若查询下压发挥作用，则在log里有相应的记录。

   ![](/images/tutorial/2.1/push_down/push_down_2.png)

### Pushdown to Presto 

如果你希望查询下压到Presto，你可以在 Project 级别配置以下参数以启用 Presto 查询下压 (通过 KYLIN-4491 引入)。

{% highlight Groff markup %}
kylin.query.pushdown.runner-class-name=org.apache.kylin.query.pushdown.PushdownRunnerSDKImpl
kylin.source.jdbc.dialect=presto
kylin.source.jdbc.adaptor=org.apache.kylin.sdk.datasource.adaptor.PrestoAdaptor
kylin.query.pushdown.jdbc.url={YOUR_URL}
kylin.query.pushdown.jdbc.driver=com.facebook.presto.jdbc.PrestoDriver
kylin.query.pushdown.jdbc.username={USER_NAME}
kylin.query.pushdown.jdbc.password={PASSWORD}
{% endhighlight %}    