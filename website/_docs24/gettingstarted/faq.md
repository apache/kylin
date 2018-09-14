---
layout: docs
title:  "FAQ"
categories: gettingstarted
permalink: /docs24/gettingstarted/faq.html
since: v0.6.x
---

#### 1. "bin/find-hive-dependency.sh" can locate hive/hcat jars in local, but Kylin reports error like "java.lang.NoClassDefFoundError: org/apache/hive/hcatalog/mapreduce/HCatInputFormat" or "java.lang.NoClassDefFoundError: org/apache/hadoop/hive/ql/session/SessionState"

  * Kylin need many dependent jars (hadoop/hive/hcat/hbase/kafka) on classpath to work, but Kylin doesn't ship them. It will seek these jars from your local machine by running commands like `hbase classpath`, `hive -e set` etc. The founded jars' path will be appended to the environment variable *HBASE_CLASSPATH* (Kylin uses `hbase` shell command to start up, which will read this). But in some Hadoop distribution (like AWS EMR 5.0), the `hbase` shell doesn't keep the origin `HBASE_CLASSPATH` value, that causes the "NoClassDefFoundError".

  * To fix this, find the hbase shell script (in hbase/bin folder), and search *HBASE_CLASSPATH*, check whether it overwrite the value like :

  {% highlight Groff markup %}
  export HBASE_CLASSPATH=$HADOOP_CONF:$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$ZOOKEEPER_HOME/*:$ZOOKEEPER_HOME/lib/*
  {% endhighlight %}

  * If true, change it to keep the origin value like:

   {% highlight Groff markup %}
  export HBASE_CLASSPATH=$HADOOP_CONF:$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$ZOOKEEPER_HOME/*:$ZOOKEEPER_HOME/lib/*:$HBASE_CLASSPATH
  {% endhighlight %}

#### 2. Get "java.lang.IllegalArgumentException: Too high cardinality is not suitable for dictionary -- cardinality: 5220674" in "Build Dimension Dictionary" step

  * Kylin uses "Dictionary" encoding to encode/decode the dimension values (check [this blog](/blog/2015/08/13/kylin-dictionary/)); Usually a dimension's cardinality is less than millions, so the "Dict" encoding is good to use. As dictionary need be persisted and loaded into memory, if a dimension's cardinality is very high, the memory footprint will be tremendous, so Kylin add a check on this. If you see this error, suggest to identify the UHC dimension first and then re-evaluate the design (whether need to make that as dimension?). If must keep it, you can by-pass this error with couple ways: 1) change to use other encoding (like `fixed_length`, `integer`) 2) or set a bigger value for `kylin.dictionary.max.cardinality` in `conf/kylin.properties`.


#### 3. How to Install Kylin on CDH 5.2 or Hadoop 2.5.x

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


#### 4. SUM(field) returns a negtive result while all the numbers in this field are > 0
  * If a column is declared as integer in Hive, the SQL engine (calcite) will use column's type (integer) as the data type for "SUM(field)", while the aggregated value on this field may exceed the scope of integer; in that case the cast will cause a negtive value be returned; The workround is, alter that column's type to BIGINT in hive, and then sync the table schema to Kylin (the cube doesn't need rebuild); Keep in mind that, always declare as BIGINT in hive for an integer column which would be used as a measure in Kylin; See hive number types: [https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-NumericTypes](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-NumericTypes)

#### 5. Why Kylin need extract the distinct columns from Fact Table before building cube?
  * Kylin uses dictionary to encode the values in each column, this greatly reduce the cube's storage size. To build the dictionary, Kylin need fetch the distinct values for each column.

#### 6. Why Kylin calculate the HIVE table cardinality?
  * The cardinality of dimensions is an important measure of cube complexity. The higher the cardinality, the bigger the cube, and thus the longer to build and the slower to query. Cardinality > 1,000 is worth attention and > 1,000,000 should be avoided at best effort. For optimal cube performance, try reduce high cardinality by categorize values or derive features.

#### 7. How to add new user or change the default password?
  * Kylin web's security is implemented with Spring security framework, where the kylinSecurity.xml is the main configuration file:

   {% highlight Groff markup %}
   ${KYLIN_HOME}/tomcat/webapps/kylin/WEB-INF/classes/kylinSecurity.xml
   {% endhighlight %}

  * The password hash for pre-defined test users can be found in the profile "sandbox,testing" part; To change the default password, you need generate a new hash and then update it here, please refer to the code snippet in: [https://stackoverflow.com/questions/25844419/spring-bcryptpasswordencoder-generate-different-password-for-same-input](https://stackoverflow.com/questions/25844419/spring-bcryptpasswordencoder-generate-different-password-for-same-input)
  * When you deploy Kylin for more users, switch to LDAP authentication is recommended.

#### 8. Using sub-query for un-supported SQL

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

#### 9. Build kylin meet NPM errors (中国大陆地区用户请特别注意此问题)

  * Please add proxy for your NPM:  
  `npm config set proxy http://YOUR_PROXY_IP`

  * Please update your local NPM repository to using any mirror of npmjs.org, like Taobao NPM (请更新您本地的NPM仓库以使用国内的NPM镜像，例如淘宝NPM镜像) :  
  [http://npm.taobao.org](http://npm.taobao.org)

#### 10. Failed to run BuildCubeWithEngineTest, saying failed to connect to hbase while hbase is active
  * User may get this error when first time run hbase client, please check the error trace to see whether there is an error saying couldn't access a folder like "/hadoop/hbase/local/jars"; If that folder doesn't exist, create it.

#### 11. Kylin JDBC driver returns a different Date/time than the REST API, seems it add the timezone to parse the date.
  * Please check the [post in mailing list](http://apache-kylin.74782.x6.nabble.com/JDBC-query-result-Date-column-get-wrong-value-td5370.html)


#### 12. How to update the default password for 'ADMIN'?
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

#### 13. What kind of data be left in 'kylin.env.hdfs-working-dir' ? We often execute kylin cleanup storage command, but now our working dir folder is about 300 GB size, can we delete old data manually?

The data in 'hdfs-working-dir' ('hdfs:///kylin/kylin_metadata/' by default) includes intermediate files (will be GC) and Cuboid data (won't be GC). The Cuboid data is kept for the further segments' merge, as Kylin couldn't merge from HBase. If you're sure those segments won't be merged, you can move them to other paths or even delete.

Please pay attention to the "resources" sub-folder under 'hdfs-working-dir', which persists some big metadata files like  dictionaries and lookup tables' snapshots. They shouldn't be moved.