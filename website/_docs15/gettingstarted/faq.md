---
layout: docs15
title:  "FAQ"
categories: gettingstarted
permalink: /docs15/gettingstarted/faq.html
since: v0.6.x
---

#### 1. "bin/find-hive-dependency.sh" can locate hive/hcat jars in local, but Kylin reports error like "java.lang.NoClassDefFoundError: org/apache/hive/hcatalog/mapreduce/HCatInputFormat"

  * Kylin need many dependent jars (hadoop/hive/hcat/hbase/kafka) on classpath to work, but Kylin doesn't ship them. It will seek these jars from your local machine by running commands like `hbase classpath`, `hive -e set` etc. The founded jars' path will be appended to the environment variable *HBASE_CLASSPATH* (Kylin uses `hbase` shell command to start up, which will read this). But in some Hadoop distribution (like EMR 5.0), the `hbase` shell doesn't keep the origin `HBASE_CLASSPATH` value, that causes the "NoClassDefFoundError".

  To fix this, find the hbase shell script (in hbase/bin or hbase/conf folder), and search *HBASE_CLASSPATH*, check whether it overwrite the value like :

```
  export HBASE_CLASSPATH=$HADOOP_CONF:$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$ZOOKEEPER_HOME/*:$ZOOKEEPER_HOME/lib/*
```

  If true, change it to keep the origin value like:

```
  export HBASE_CLASSPATH=$HADOOP_CONF:$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$ZOOKEEPER_HOME/*:$ZOOKEEPER_HOME/lib/*:$HBASE_CLASSPATH
```

  * For EMR5.0, need change the hbase-env.sh

```
 sudo vi /usr/lib/hbase/conf/hbase-env.sh
```

 in around line 30 it was :

```
export HBASE_CLASSPATH=/etc/hadoop/conf
```

change to :

```
export HBASE_CLASSPATH=/etc/hadoop/conf:$HBASE_CLASSPATH
```

then restart Kylin.

#### 2. Get "java.lang.IllegalArgumentException: Too high cardinality is not suitable for dictionary -- cardinality: 5220674" in "Build Dimension Dictionary" step

  * Kylin uses "Dictionary" encoding to encode/decode the dimension values (check [this blog](/blog/2015/08/13/kylin-dictionary/)); Usually a dimension's cardinality is less than millions, so the "Dict" encoding is good to use. As dictionary need be persisted and loaded into memory, if a dimension's cardinality is very high, the memory footprint will be tremendous, so Kylin add a check on this. If you see this error, suggest to identify the UHC dimension first and then re-evaluate the design (whether need to make that as dimension?). If must keep it, you can by-pass this error with couple ways: 
    * change to use other encoding (like `fixed_length`, `integer`) 
    * or set a bigger value for `kylin.dictionary.max.cardinality` in `conf/kylin.properties`.

#### 3. Build cube failed due to "error check status"

  * Check if `kylin.log` contains *yarn.resourcemanager.webapp.address:http://0.0.0.0:8088* and *java.net.ConnectException: Connection refused*
  * If yes, then the problem is the address of resource manager was not available in yarn-site.xml
  * A workaround is update `kylin.properties`, set `kylin.job.yarn.app.rest.check.status.url=http://YOUR_RM_NODE:8088/ws/v1/cluster/apps/${job_id}?anonymous=true`

#### 4. HBase cannot get master address from ZooKeeper on Hortonworks Sandbox
   
  * By default hortonworks disables hbase, you'll have to start hbase in ambari homepage first.

#### 5. Map Reduce Job information cannot display on Hortonworks Sandbox
   
  * Check out [https://github.com/KylinOLAP/Kylin/issues/40](https://github.com/KylinOLAP/Kylin/issues/40)

#### 6. How to Install Kylin on CDH 5.2 or Hadoop 2.5.x

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


#### 7. SUM(field) returns a negtive result while all the numbers in this field are > 0
  * If a column is declared as integer in Hive, the SQL engine (calcite) will use column's type (integer) as the data type for "SUM(field)", while the aggregated value on this field may exceed the scope of integer; in that case the cast will cause a negtive value be returned; The workround is, alter that column's type to BIGINT in hive, and then sync the table schema to Kylin (the cube doesn't need rebuild); Keep in mind that, always declare as BIGINT in hive for an integer column which would be used as a measure in Kylin; See hive number types: [https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-NumericTypes](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-NumericTypes)

#### 8. Why Kylin need extract the distinct columns from Fact Table before building cube?
  * Kylin uses dictionary to encode the values in each column, this greatly reduce the cube's storage size. To build the dictionary, Kylin need fetch the distinct values for each column.

#### 9. Why Kylin calculate the HIVE table cardinality?
  * The cardinality of dimensions is an important measure of cube complexity. The higher the cardinality, the bigger the cube, and thus the longer to build and the slower to query. Cardinality > 1,000 is worth attention and > 1,000,000 should be avoided at best effort. For optimal cube performance, try reduce high cardinality by categorize values or derive features.

#### 10. How to add new user or change the default password?
  * Kylin web's security is implemented with Spring security framework, where the kylinSecurity.xml is the main configuration file:

   {% highlight Groff markup %}
   ${KYLIN_HOME}/tomcat/webapps/kylin/WEB-INF/classes/kylinSecurity.xml
   {% endhighlight %}

  * The password hash for pre-defined test users can be found in the profile "sandbox,testing" part; To change the default password, you need generate a new hash and then update it here, please refer to the code snippet in: [https://stackoverflow.com/questions/25844419/spring-bcryptpasswordencoder-generate-different-password-for-same-input](https://stackoverflow.com/questions/25844419/spring-bcryptpasswordencoder-generate-different-password-for-same-input)
  * When you deploy Kylin for more users, switch to LDAP authentication is recommended.

#### 11. Using sub-query for un-supported SQL

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

#### 12. Build kylin meet NPM errors (中国大陆地区用户请特别注意此问题)

  * Please add proxy for your NPM:  
  `npm config set proxy http://YOUR_PROXY_IP`

  * Please update your local NPM repository to using any mirror of npmjs.org, like Taobao NPM (请更新您本地的NPM仓库以使用国内的NPM镜像，例如淘宝NPM镜像) :  
  [http://npm.taobao.org](http://npm.taobao.org)

#### 13. Failed to run BuildCubeWithEngineTest, saying failed to connect to hbase while hbase is active
  * User may get this error when first time run hbase client, please check the error trace to see whether there is an error saying couldn't access a folder like "/hadoop/hbase/local/jars"; If that folder doesn't exist, create it.




