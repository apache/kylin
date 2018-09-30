---
layout: docs
title:  "FAQ"
categories: gettingstarted
permalink: /docs/gettingstarted/faq.html
since: v0.6.x
---

#### Is Kylin a generic SQL engine for big data?

  * No, Kylin is an OLAP engine with SQL interface. The SQL queries need be matched with the pre-defined OLAP model.

#### How to compare Kylin with other SQL engines like Hive, Presto, Spark SQL, Impala?

  * They answer a query in different ways. Kylin is not a replacement for them, but a supplement (query accelerator). Many users run Kylin together with other SQL engines. For the high frequent query patterns, building Cubes can greatly improve the performance and also offload cluster workloads. For less queried patterns or ad-hoc queries, ther MPP engines are more flexible.

#### What's a typical scenario to use Apache Kylin?

  * Kylin can be the best option if you have a huge table (e.g., >100 million rows), join with lookup tables, while queries need be finished in the second level (dashboards, interactive reports, business intelligence, etc), and the concurrent users can be dozens or hundreds.

#### How large a data scale can Kylin support? How about the performance?

  * Kylin can supports second level query performance at TB to PB level dataset. This has been verified by users like eBay, Meituan, Toutiao. Take Meituan's case as an example (till 2018-08), 973 cubes, 3.8 million queries per day, raw data 8.9 trillion, total cube size 971 TB (original data is bigger), 50% queries finished in < 0.5 seconds, 90% queries < 1.2 seconds.

#### Who are using Apache Kylin?

  * You can find a list in Kylin's [powered by page](/community/poweredby.html). If you want to be added, please email to dev@kylin.apache.org with your use case.

#### What's the expansion rate of Cube (compared with raw data)?

  * It depends on a couple of factors, for example, dimension/measure number, dimension cardinality, cuboid number, compression algorithm, etc. You can optimize the cube expansion in many ways to control the size.

#### How to compare Kylin with Druid?

  * Druid is more suitable for real-time analysis. Kylin is more focus on OLAP case. Druid has good integration with Kafka as real-time streaming; Kylin fetches data from Hive or Kafka in batches. The real-time capability of Kylin is still under development.

  * Many internet service providers host both Druid and Kylin, serving different purposes (real-time and historical).

  * Some other Kylin's highlights: supports star & snowflake schema; ANSI-SQL support, JDBC/ODBC for BI integrations. Kylin also has a Web GUI with LDAP/SSO user authentication.

  * For more information, please do a search or check this [mail thread](https://mail-archives.apache.org/mod_mbox/kylin-dev/201503.mbox/%3CCAKmQrOY0fjZLUU0MGo5aajZ2uLb3T0qJknHQd+Wv1oxd5PKixQ@mail.gmail.com%3E).

#### How to quick start with Kylin?

  * To get a quick start, you can run Kylin in a Hadoop sandbox VM or in the cloud, for example, start a small AWS EMR or Azure HDInsight cluster and then install Kylin in one of the node.

#### How many nodes of the Hadoop are needed to run Kylin?

  * Kylin can run on a Hadoop cluster from only a couple nodes to thousands of nodes, depends on how much data you have. The architecture is horizontally scalable.

  * Because most of the computation is happening in Hadoop (MapReduce/Spark/HBase), usually you just need to install Kylin in a couple of nodes.

#### How many dimensions can be in a cube?

  * The max physical dimension number (exclude derived column in lookup tables) in a cube is 63; If you can normalize some dimensions to lookup tables, with derived dimensions, you can create a cube with more than 100 dimensions.

  * But a cube with > 30 physical dimensions is not recommended; You even couldn't save that in Kylin if you don't optimize the aggregation groups. Please search "curse of dimensionality".

#### Why I got an error when running a "select * " query?

  * The cube only has aggregated data, so all your queries should be aggregated queries ("GROUP BY"). You can use a SQL with all dimensions be grouped to get them as close as the detailed result, but that is not the raw data.

  * In order to be connected from some BI tools, Kylin tries to answer "select *" query but please aware the result might not be expected. Please make sure each query to Kylin is aggregated.

#### How can I query raw data from a cube?

  * Cube is not the right option for raw data.

But if you do want, there are some workarounds. 1) Add the primary key as a dimension, then the "group by pk" will return the raw data; 2) Configure Kylin to push down the query to another SQL engine like Hive, but the performance has no assurance.

#### What is the UHC dimension?

  * UHC means Ultra High Cardinality. Cardinality means the number of distinct values of a dimension. Usually, a dimension's cardinality is from tens to millions. If above million, we call it a UHC dimension, for example, user id, cell number, etc.

  * Kylin supports UHC dimension but you need to pay attention to UHC dimension, especially the encoding and the cuboid combinations. It may cause your Cube very large and query to be slow.

#### Can I specify a cube to answer my SQL statements?

  * No, you couldn't; Cube is transparent for the end user. If you have multiple Cubes for the same data models, separating them into different projects is a good idea.

#### Is there a REST API to create the project/model/cube?

  * Yes, but they are private APIs, incline to change over versions (without notification). By design, Kylin expects the user to create a new project/model/cube in Kylin's web GUI.

#### Where does the cube locate, can I directly read cube from HBase without going through Kylin API?

  * Cube is stored in HBase. Each cube segment is an HBase table. The dimension values will be composed as the row key. The measures will be serialized in columns. To improve the storage efficiency, both dimension and measure values will be encoded to bytes. Kylin will decode the bytes to origin values after fetching from HBase. Without Kylin's metadata, the HBase tables are not readable.

#### How to encrypt cube data?

  * You can enable encryption at HBase side. Refer https://hbase.apache.org/book.html#hbase.encryption.server for more details.

#### How to schedule the cube build at a fixed frequency, in an automatic way?

  * Kylin doesn't have a built-in scheduler for this. You can trigger that through Rest API from external scheduler services, like Linux cron job, Apache Airflow, etc.

#### Does Kylin support Hadoop 3 and HBase 2.0?

  * From v2.5.0, Kylin will provide a binary package for Hadoop 3 and HBase 2.

#### The Cube is ready, but why the table does not appear in the "Insight" tab?

  * Make sure the "kylin.server.cluster-servers" property in `conf/kylin.properties` is configured with EVERY Kylin node, all job and query nodes. Kylin nodes notify each other to flush cache with this configuration. And please ensure the network among them are healthy.

#### What should I do if I encounter a "java.lang.NoClassDefFoundError" error?

  * Kylin doesn't ship those Hadoop jars, because they should already exist in the Hadoop node. So Kylin will try to find them and then add to Kylin's classpath. Due to Hadoop's complexity, there might be some case a jar wasn't found. In this case please look at the "bin/find-*-dependency.sh" and "bin/kylin.sh", modify them to fit your environment.

#### How to query Kylin in Python?

  * Please check: [https://github.com/Kyligence/kylinpy]()

#### How to add dimension/measure to a cube?

  * Once a cube is built, its structure couldn't be modified. To add dimension/measure, you need to clone a new cube, and then add in it.

When the new cube is built, please disable or drop the old one.

If you can accept the absence of new dimensions for historical data, you can build the new cube since the end time of the old cube. And then create a hybrid model over the old and new cube.

#### The query result is not exactly matched with that in Hive, what's the possible reason?

  * Possible reasons:
a) Source data changed in Hive after built into the cube;
b) Cube's time range is not the same as in Hive;
c) Another cube answered your query;
d) The data model has inner joins, but the query doesn't join all tables;
e) Cube has some approximate measures like HyberLogLog, TopN;
f) In v2.3 and before, Kylin may have data loss when fetching from Hive, see KYLIN-3388.

#### What to do if the source data changed after being built into the cube?

  * You need to refresh the cube. If the cube is partitioned, you can refresh certain segments.

#### What is the possible reason for getting the error ‘bulk load aborted with some files not yet loaded’ in the ‘Load HFile to HBase Table’ step?

  * Kylin doesn't have permissions to execute HBase CompleteBulkLoad. Check whether the current user (that run Kylin service) has the permission to access HBase.

#### Why `bin/sample.sh` cannot create the `/tmp/kylin` folder on HDFS?

  * Run ./bin/find-hadoop-conf-dir.sh -v, check the error message, then check the environment according to the information reported.

#### In Chrome, web console shows net::ERR_CONTENT_DECODING_FAILED, what should I do?

  * Edit $KYLIN_HOME/tomcat/conf/server.xml, find the "compress=on", change it to off.

#### How to configure one cube to be built using a chosen YARN queue?

  * Set the YARN queue in Cube’s Configuration Overwrites page, then it will affect only one cube. Here are the three parameters:

  {% highlight Groff markup %}
kylin.engine.mr.config-override.mapreduce.job.queuename=YOUR_QUEUE_NAME
kylin.source.hive.config-override.mapreduce.job.queuename=YOUR_QUEUE_NAME
kylin.engine.spark-conf.spark.yarn.queue=YOUR_QUEUE_NAME
  {% endhighlight %}

#### How to add a new JDBC data source dialect?

  * That is easy to add a new type of JDBC data source. You can follow such steps:

1) Add the dialect in  source-hive/src/main/java/org/apache/kylin/source/jdbc/JdbcDialect.java

2) Implement a new IJdbcMetadata if {database that you want to add}'s metadata fetching is different with others and then register it in JdbcMetadataFactory

3) You may need to customize the SQL for creating/dropping table in JdbcExplorer for {database that you want to add}.

#### How to ask a question?

  * Check Kylin documents first. and do a Google search also can help. Sometimes the question has been answered so you don't need ask again. If no matching, please send your question to Apache Kylin user mailing list: user@kylin.apache.org; You need to drop an email to user-subscribe@kylin.apache.org to subscribe if you haven't done so. In the email content, please provide your Kylin and Hadoop version, specific error logs (as much as possible), and also the how to re-produce steps.  

#### "bin/find-hive-dependency.sh" can locate hive/hcat jars in local, but Kylin reports error like "java.lang.NoClassDefFoundError: org/apache/hive/hcatalog/mapreduce/HCatInputFormat" or "java.lang.NoClassDefFoundError: org/apache/hadoop/hive/ql/session/SessionState"

  * Kylin need many dependent jars (hadoop/hive/hcat/hbase/kafka) on classpath to work, but Kylin doesn't ship them. It will seek these jars from your local machine by running commands like `hbase classpath`, `hive -e set` etc. The founded jars' path will be appended to the environment variable *HBASE_CLASSPATH* (Kylin uses `hbase` shell command to start up, which will read this). But in some Hadoop distribution (like AWS EMR 5.0), the `hbase` shell doesn't keep the origin `HBASE_CLASSPATH` value, that causes the "NoClassDefFoundError".

  * To fix this, find the hbase shell script (in hbase/bin folder), and search *HBASE_CLASSPATH*, check whether it overwrite the value like :

  {% highlight Groff markup %}
  export HBASE_CLASSPATH=$HADOOP_CONF:$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$ZOOKEEPER_HOME/*:$ZOOKEEPER_HOME/lib/*
  {% endhighlight %}

  * If true, change it to keep the origin value like:

   {% highlight Groff markup %}
  export HBASE_CLASSPATH=$HADOOP_CONF:$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$ZOOKEEPER_HOME/*:$ZOOKEEPER_HOME/lib/*:$HBASE_CLASSPATH
  {% endhighlight %}

#### Get "java.lang.IllegalArgumentException: Too high cardinality is not suitable for dictionary -- cardinality: 5220674" in "Build Dimension Dictionary" step

  * Kylin uses "Dictionary" encoding to encode/decode the dimension values (check [this blog](/blog/2015/08/13/kylin-dictionary/)); Usually a dimension's cardinality is less than millions, so the "Dict" encoding is good to use. As dictionary need be persisted and loaded into memory, if a dimension's cardinality is very high, the memory footprint will be tremendous, so Kylin add a check on this. If you see this error, suggest to identify the UHC dimension first and then re-evaluate the design (whether need to make that as dimension?). If must keep it, you can by-pass this error with couple ways: 1) change to use other encoding (like `fixed_length`, `integer`) 2) or set a bigger value for `kylin.dictionary.max.cardinality` in `conf/kylin.properties`.


#### How to Install Kylin on CDH 5.2 or Hadoop 2.5.x

  * Check out discussion: [https://groups.google.com/forum/?utm_medium=email&utm_source=footer#!msg/kylin-olap/X0GZfsX1jLc/nzs6xAhNpLkJ](https://groups.google.com/forum/?utm_medium=email&utm_source=footer#!msg/kylin-olap/X0GZfsX1jLc/nzs6xAhNpLkJ)

  {% highlight Groff markup %}
  I was able to deploy Kylin with following option in POM.
  <hadoop2.version>2.5.0</hadoop2.version>
  <yarn.version>2.5.0</yarn.version>
  <hbase-hadoop2.version>0.98.6-hadoop2</hbase-hadoop2.version>
  <zookeeper.version>3.4.5</zookeeper.version>
  <hive.version>0.13.1</hive.version>
  My Cluster is running on Cloudera Distribution CDH 5.2.0.
  {% endhighlight %}


#### SUM(field) returns a negative result while all the numbers in this field are > 0
  * If a column is declared as integer in Hive, the SQL engine (calcite) will use column's type (integer) as the data type for "SUM(field)", while the aggregated value on this field may exceed the scope of integer; in that case the cast will cause a negtive value be returned; The workaround is, alter that column's type to BIGINT in hive, and then sync the table schema to Kylin (the cube doesn't need rebuild); Keep in mind that, always declare as BIGINT in hive for an integer column which would be used as a measure in Kylin; See hive number types: [https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-NumericTypes](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-NumericTypes)

#### Why Kylin need extract the distinct columns from Fact Table before building cube?
  * Kylin uses dictionary to encode the values in each column, this greatly reduce the cube's storage size. To build the dictionary, Kylin need fetch the distinct values for each column.

#### Why Kylin calculate the HIVE table cardinality?
  * The cardinality of dimensions is an important measure of cube complexity. The higher the cardinality, the bigger the cube, and thus the longer to build and the slower to query. Cardinality > 1,000 is worth attention and > 1,000,000 should be avoided at best effort. For optimal cube performance, try reduce high cardinality by categorize values or derive features.

#### How to add new user or change the default password?
  * Kylin web's security is implemented with Spring security framework, where the kylinSecurity.xml is the main configuration file:

   {% highlight Groff markup %}
   ${KYLIN_HOME}/tomcat/webapps/kylin/WEB-INF/classes/kylinSecurity.xml
   {% endhighlight %}

  * The password hash for pre-defined test users can be found in the profile "sandbox,testing" part; To change the default password, you need generate a new hash and then update it here, please refer to the code snippet in: [https://stackoverflow.com/questions/25844419/spring-bcryptpasswordencoder-generate-different-password-for-same-input](https://stackoverflow.com/questions/25844419/spring-bcryptpasswordencoder-generate-different-password-for-same-input)
  * When you deploy Kylin for more users, switch to LDAP authentication is recommended.

#### Using sub-query for un-supported SQL

{% highlight Groff markup %}
Original SQL:
select fact.slr_sgmt,
sum(case when cal.RTL_WEEK_BEG_DT = '2015-09-06' then gmv else 0 end) as W36,
sum(case when cal.RTL_WEEK_BEG_DT = '2015-08-30' then gmv else 0 end) as W35
from ih_daily_fact fact
inner join dw_cal_dt cal on fact.cal_dt = cal.cal_dt
group by fact.slr_sgmt
{% endhighlight %}

{% highlight Groff markup %}
Using sub-query
select a.slr_sgmt,
sum(case when a.RTL_WEEK_BEG_DT = '2015-09-06' then gmv else 0 end) as W36,
sum(case when a.RTL_WEEK_BEG_DT = '2015-08-30' then gmv else 0 end) as W35
from (
    select fact.slr_sgmt as slr_sgmt,
    cal.RTL_WEEK_BEG_DT as RTL_WEEK_BEG_DT,
    sum(gmv) as gmv36,
    sum(gmv) as gmv35
    from ih_daily_fact fact
    inner join dw_cal_dt cal on fact.cal_dt = cal.cal_dt
    group by fact.slr_sgmt, cal.RTL_WEEK_BEG_DT
) a
group by a.slr_sgmt
{% endhighlight %}

#### Build kylin meet NPM errors (中国大陆地区用户请特别注意此问题)

  * Please add proxy for your NPM:  
  `npm config set proxy http://YOUR_PROXY_IP`

  * Please update your local NPM repository to using any mirror of npmjs.org, like Taobao NPM (请更新您本地的NPM仓库以使用国内的NPM镜像，例如淘宝NPM镜像) :  
  [http://npm.taobao.org](http://npm.taobao.org)

#### Failed to run BuildCubeWithEngineTest, saying failed to connect to hbase while hbase is active
  * User may get this error when first time run hbase client, please check the error trace to see whether there is an error saying couldn't access a folder like "/hadoop/hbase/local/jars"; If that folder doesn't exist, create it.

#### Kylin JDBC driver returns a different Date/time than the REST API, seems it add the timezone to parse the date.
  * Please check the [post in mailing list](http://apache-kylin.74782.x6.nabble.com/JDBC-query-result-Date-column-get-wrong-value-td5370.html)


#### How to update the default password for 'ADMIN'?
  * By default, Kylin uses a simple, configuration based user registry; The default administrator 'ADMIN' with password 'KYLIN' is hard-coded in `kylinSecurity.xml`. To modify the password, you need firstly get the new password's encrypted value (with BCrypt), and then set it in `kylinSecurity.xml`. Here is a sample with password 'ABCDE'
  
{% highlight Groff markup %}

cd $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib

java -classpath kylin-server-base-2.3.0.jar:spring-beans-4.3.10.RELEASE.jar:spring-core-4.3.10.RELEASE.jar:spring-security-core-4.2.3.RELEASE.jar:commons-codec-1.7.jar:commons-logging-1.1.3.jar org.apache.kylin.rest.security.PasswordPlaceholderConfigurer BCrypt ABCDE

BCrypt encrypted password is:
$2a$10$A7.J.GIEOQknHmJhEeXUdOnj2wrdG4jhopBgqShTgDkJDMoKxYHVu

{% endhighlight %}

Then you can set it into `kylinSecurity.xml'

{% highlight Groff markup %}

vi ./tomcat/webapps/kylin/WEB-INF/classes/kylinSecurity.xml

{% endhighlight %}

Replace the origin encrypted password with the new one: 
{% highlight Groff markup %}

        <bean class="org.springframework.security.core.userdetails.User" id="adminUser">
            <constructor-arg value="ADMIN"/>
            <constructor-arg
                    value="$2a$10$A7.J.GIEOQknHmJhEeXUdOnj2wrdG4jhopBgqShTgDkJDMoKxYHVu"/>
            <constructor-arg ref="adminAuthorities"/>
        </bean>
        
{% endhighlight %}

Restart Kylin to take effective. If you have multiple Kylin server as a cluster, do the same on each instance. 

#### What kind of data be left in 'kylin.env.hdfs-working-dir' ? We often execute kylin cleanup storage command, but now our working dir folder is about 300 GB size, can we delete old data manually?

The data in 'hdfs-working-dir' ('hdfs:///kylin/kylin_metadata/' by default) includes intermediate files (will be GC) and Cuboid data (won't be GC). The Cuboid data is kept for the further segments' merge, as Kylin couldn't merge from HBase. If you're sure those segments won't be merged, you can move them to other paths or even delete.

Please pay attention to the "resources" sub-folder under 'hdfs-working-dir', which persists some big metadata files like  dictionaries and lookup tables' snapshots. They shouldn't be moved.
