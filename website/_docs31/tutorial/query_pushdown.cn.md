---
layout: docs31-cn
title:  查询下压
categories: tutorial
permalink: /cn/docs31/tutorial/query_pushdown.html
since: v2.1
---

### Kylin 支持查询下压

对于没有cube能查得结果的sql，Kylin支持将这类查询通过JDBC下压至备用查询引擎如Hive, SparkSQL, Impala等来查得结果。以下以Hive为例说明开启步骤，由于Kylin本事就将Hive作为数据源，作为Query Pushdown引擎也更易使用与配置。

### 查询下压配置

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

3. 重启Kylin

### 进行查询下压

开启查询下压后，即可按同步的表进行灵活查询，而无需根据查询构建对应Cube。

   ![](/images/tutorial/2.1/push_down/push_down_1.png)

用户在提交查询时，若查询下压发挥作用，则在log里有相应的记录。

   ![](/images/tutorial/2.1/push_down/push_down_2.png)
