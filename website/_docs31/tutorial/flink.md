---
layout: docs31
title:  Apache Flink
categories: tutorial
permalink: /docs31/tutorial/flink.html
---


### Introduction

This document describes how to use Kylin as a data source in Apache Flink; 

There were several attempts to do this in Scala and JDBC, but none of them works: 

* [attempt1](http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/JDBCInputFormat-preparation-with-Flink-1-1-SNAPSHOT-and-Scala-2-11-td5371.html)  
* [attempt2](http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Type-of-TypeVariable-OT-in-class-org-apache-flink-api-common-io-RichInputFormat-could-not-be-determi-td7287.html)  
* [attempt3](http://stackoverflow.com/questions/36067881/create-dataset-from-jdbc-source-in-flink-using-scala)  
* [attempt4](https://codegists.com/snippet/scala/jdbcissuescala_zeitgeist_scala); 

We will try use CreateInput and [JDBCInputFormat](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/batch/index.html) in batch mode and access via JDBC to Kylin. But it isn’t implemented in Scala, is only in Java [MailList](http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/jdbc-JDBCInputFormat-td9393.html). This doc will go step by step solving these problems.

### Pre-requisites

* Need an instance of Kylin, with a Cube; [Sample Cube](kylin_sample.html) will be good enough.
* [Scala](http://www.scala-lang.org/) and [Apache Flink](http://flink.apache.org/) Installed
* [IntelliJ](https://www.jetbrains.com/idea/) Installed and configured for Scala/Flink (see [Flink IDE setup guide](https://ci.apache.org/projects/flink/flink-docs-release-1.1/internals/ide_setup.html) )

### Used software:

* [Apache Flink](http://flink.apache.org/downloads.html) v1.2-SNAPSHOT
* [Apache Kylin](http://kylin.apache.org/download/) v1.5.2 (v1.6.0 also works)
* [IntelliJ](https://www.jetbrains.com/idea/download/#section=linux)  v2016.2
* [Scala](downloads.lightbend.com/scala/2.11.8/scala-2.11.8.tgz)  v2.11

### Starting point:

This can be out initial skeleton: 

{% highlight Groff markup %}
import org.apache.flink.api.scala._
val env = ExecutionEnvironment.getExecutionEnvironment
val inputFormat = JDBCInputFormat.buildJDBCInputFormat()
  .setDrivername("org.apache.kylin.jdbc.Driver")
  .setDBUrl("jdbc:kylin://172.17.0.2:7070/learn_kylin")
  .setUsername("ADMIN")
  .setPassword("KYLIN")
  .setQuery("select count(distinct seller_id) as sellers from kylin_sales group by part_dt order by part_dt")
  .finish()
  val dataset =env.createInput(inputFormat)
{% endhighlight %}

The first error is: ![alt text](/images/Flink-Tutorial/02.png)

Add to Scala: 
{% highlight Groff markup %}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
{% endhighlight %}

Next error is  ![alt text](/images/Flink-Tutorial/03.png)

We can solve dependencies [(mvn repository: jdbc)](https://mvnrepository.com/artifact/org.apache.flink/flink-jdbc/1.1.2); Add this to your pom.xml:
{% highlight Groff markup %}
<dependency>
   <groupId>org.apache.flink</groupId>
   <artifactId>flink-jdbc</artifactId>
   <version>${flink.version}</version>
</dependency>
{% endhighlight %}

## Solve dependencies of row 

Similar to previous point we need solve dependencies of Row Class [(mvn repository: Table) ](https://mvnrepository.com/artifact/org.apache.flink/flink-table_2.10/1.1.2):

  ![](/images/Flink-Tutorial/03b.png)


* In pom.xml add:
{% highlight Groff markup %}
<dependency>
   <groupId>org.apache.flink</groupId>
   <artifactId>flink-table_2.10</artifactId>
   <version>${flink.version}</version>
</dependency>
{% endhighlight %}

* In Scala: 
{% highlight Groff markup %}
import org.apache.flink.api.table.Row
{% endhighlight %}

## Solve RowTypeInfo property (and their new dependencies)

This is the new error to solve:

  ![](/images/Flink-Tutorial/04.png)


* If check the code of [JDBCInputFormat.java](https://github.com/apache/flink/blob/master/flink-batch-connectors/flink-jdbc/src/main/java/org/apache/flink/api/java/io/jdbc/JDBCInputFormat.java#L69), we can see [this new property](https://github.com/apache/flink/commit/09b428bd65819b946cf82ab1fdee305eb5a941f5#diff-9b49a5041d50d9f9fad3f8060b3d1310R69) (and mandatory) added on Apr 2016 by [FLINK-3750](https://issues.apache.org/jira/browse/FLINK-3750)  Manual [JDBCInputFormat](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/api/java/io/jdbc/JDBCInputFormat.html) v1.2 in Java

   Add the new Property: **setRowTypeInfo**
   
{% highlight Groff markup %}
val inputFormat = JDBCInputFormat.buildJDBCInputFormat()
  .setDrivername("org.apache.kylin.jdbc.Driver")
  .setDBUrl("jdbc:kylin://172.17.0.2:7070/learn_kylin")
  .setUsername("ADMIN")
  .setPassword("KYLIN")
  .setQuery("select count(distinct seller_id) as sellers from kylin_sales group by part_dt order by part_dt")
  .setRowTypeInfo(DB_ROWTYPE)
  .finish()
{% endhighlight %}

* How can configure this property in Scala? In [Attempt4](https://codegists.com/snippet/scala/jdbcissuescala_zeitgeist_scala), there is an incorrect solution
   
   We can check the types using the intellisense: ![alt text](/images/Flink-Tutorial/05.png)
   
   Then we will need add more dependences; Add to scala:

{% highlight Groff markup %}
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
{% endhighlight %}

   Create a Array or Seq of TypeInformation[ ]

  ![](/images/Flink-Tutorial/06.png)


   Solution:
   
{% highlight Groff markup %}
   var stringColum: TypeInformation[String] = createTypeInformation[String]
   val DB_ROWTYPE = new RowTypeInfo(Seq(stringColum))
{% endhighlight %}

## Solve ClassNotFoundException

  ![](/images/Flink-Tutorial/07.png)

Need find the kylin-jdbc-x.x.x.jar and then expose to Flink

1. Find the Kylin JDBC jar

   From Kylin [Download](http://kylin.apache.org/download/) choose **Binary** and the **correct version of Kylin and HBase**
   
   Download & Unpack: in ./lib: 
   
  ![](/images/Flink-Tutorial/08.png)


2. Make this JAR accessible to Flink

   If you execute like service you need put this JAR in you Java class path using your .bashrc 

  ![](/images/Flink-Tutorial/09.png)


  Check the actual value: ![alt text](/images/Flink-Tutorial/10.png)
  
  Check the permission for this file (Must be accessible for you):

  ![](/images/Flink-Tutorial/11.png)

 
  If you are executing from IDE, need add your class path manually:
  
  On IntelliJ: ![alt text](/images/Flink-Tutorial/12.png)  > ![alt text](/images/Flink-Tutorial/13.png) > ![alt text](/images/Flink-Tutorial/14.png) > ![alt text](/images/Flink-Tutorial/15.png)
  
  The result, will be similar to: ![alt text](/images/Flink-Tutorial/16.png)
  
## Solve "Couldn’t access resultSet" error

  ![](/images/Flink-Tutorial/17.png)


It is related with [Flink 4108](https://issues.apache.org/jira/browse/FLINK-4108)  [(MailList)](http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/jdbc-JDBCInputFormat-td9393.html#a9415) and Timo Walther [make a PR](https://github.com/apache/flink/pull/2619)

If you are running Flink <= 1.2 you will need apply this path and make clean install

## Solve the casting error

  ![](/images/Flink-Tutorial/18.png)

In the error message you have the problem and solution …. nice ;)  ¡¡

## The result

The output must be similar to this, print the result of query by standard output:

  ![](/images/Flink-Tutorial/19.png)


## Now, more complex

Try with a multi-colum and multi-type query:

{% highlight Groff markup %}
select part_dt, sum(price) as total_selled, count(distinct seller_id) as sellers 
from kylin_sales 
group by part_dt 
order by part_dt
{% endhighlight %}

Need changes in DB_ROWTYPE:

  ![](/images/Flink-Tutorial/20.png)


And import lib of Java, to work with Data type of Java ![alt text](/images/Flink-Tutorial/21.png)

The new result will be: 

  ![](/images/Flink-Tutorial/23.png)


## Error:  Reused Connection


  ![](/images/Flink-Tutorial/24.png)

Check if your HBase and Kylin is working. Also you can use Kylin UI for it.


## Error:  java.lang.AbstractMethodError:  ….Avatica Connection

See [Kylin 1898](https://issues.apache.org/jira/browse/KYLIN-1898) 

It is a problem with kylin-jdbc-1.x.x. JAR, you need use Calcite 1.8 or above; The solution is to use Kylin 1.5.4 or above.

  ![](/images/Flink-Tutorial/25.png)



## Error: can't expand macros compiled by previous versions of scala

Is a problem with versions of scala, check in with "scala -version" your actual version and choose your correct POM.

Perhaps you will need a IntelliJ > File > Invalidates Cache > Invalidate and Restart.

I added POM for Scala 2.11


## Final Words

Now you can read Kylin’s data from Apache Flink, great!

[Full Code Example](https://github.com/albertoRamon/Flink/tree/master/ReadKylinFromFlink/flink-scala-project)

Solved all integration problems, and tested with different types of data (Long, BigDecimal and Dates). The patch has been committed at 15 Oct, then, will be part of Flink 1.2.
