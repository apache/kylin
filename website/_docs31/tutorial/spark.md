---
layout: docs31
title:  Apache Spark
categories: tutorial
permalink: /docs31/tutorial/spark.html
---


### Introduction

Apache Kylin provides JDBC driver to query the Cube data, and Apache Spark supports JDBC data source. With it, you can connect with Kylin from your Spark application and then do the analysis over a very huge data set in an interactive way.

Please keep in mind, Kylin is an OLAP system, which already aggregated the raw data by the given dimensions. If you simply load the source table like a normal database, you may not gain the benefit of Cubes, and it may crash your application.

The right way is to start from a summarized view (e.g., a query with "group by"), loading it as a data frame, and then do the transformation and other actions.

This document describes how to use Kylin as a data source in Apache Spark. You need to install Kylin, build a Cube before run it. And remember to put Kylin's JDBC driver (in the 'lib' folder of Kylin binary package) onto Spark's class path. 

### The wrong way

The below Python application tries to directly load Kylin's table as a data frame, and then to get the total row count with "df.count()", but the result is incorrect.

{% highlight Groff markup %}

conf = SparkConf() 
conf.setMaster('yarn')
conf.setAppName('Kylin jdbc example')

sc = SparkContext(conf=conf)
sqlContext = SQLContext(self.sc)

url='jdbc:kylin://sandbox:7070/default'
df = self.sqlContext.read.format('jdbc').options(
    url=url, user='ADMIN', password='KYLIN',
    driver='org.apache.kylin.jdbc.Driver',
    dbtable='kylin_sales').load()

print df.count()

    
{% endhighlight %}

The output is:
{% highlight Groff markup %}
132

{% endhighlight %}


The result "132" is not the total count of the origin table. Because Spark didn't send a "select count(*)" query to Kylin as you thought, but send a "select * " and then try to count within Spark; This would be inefficient and, as Kylin doesn't have the raw data, the "select * " query will be answered with the base Cuboid (summarized by all dimensions). The "132" is the row number of the base Cuboid, not original data. 


### The right way

The right behavior is to push down possible aggregations to Kylin, so that the Cube can be leveraged and the performance would be much better. Below is the correct code:

{% highlight Groff markup %}

conf = SparkConf() 
conf.setMaster('yarn')
conf.setAppName('Kylin jdbc example')

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
  
url='jdbc:kylin://sandbox:7070/default'
tab_name = '(select count(*) as total from kylin_sales) the_alias'

df = sqlContext.read.format('jdbc').options(
        url=url, user='ADMIN', password='KYLIN',
        driver='org.apache.kylin.jdbc.Driver',
        dbtable=tab_name).load()

df.show()

{% endhighlight %}

Here is the output, the result is correct as Spark push down the aggregation to Kylin:

{% highlight Groff markup %}
+-----+
|TOTAL|
+-----+
| 2000|
+-----+

{% endhighlight %}

Thanks for the input and sample code from Shuxin Yang (shuxinyang.oss@gmail.com).

