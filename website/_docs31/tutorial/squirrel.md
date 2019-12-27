---
layout: docs31
title:  SQuirreL
categories: tutorial
permalink: /docs31/tutorial/squirrel.html
---

### Introduction

[SQuirreL SQL](http://www.squirrelsql.org/) is a multi platform Universal SQL Client (GNU License). You can use it to access HBase + Phoenix and Hive. This document introduces how to connect to Kylin from SQuirreL.

### Used Software

* [Kylin v1.6.0](/download/) & ODBC 1.6
* [SquirreL SQL v3.7.1](http://www.squirrelsql.org/)

## Pre-requisites

* Find the Kylin JDBC driver jar
  From Kylin Download, Choose Binary and the **correct version of Kylin and HBase**
	Download & Unpack:  in **./lib**: 
  ![](/images/SQuirreL-Tutorial/01.png)


* Need an instance of Kylin, with a Cube; the [Sample Cube](kylin_sample.html) is enough.

  ![](/images/SQuirreL-Tutorial/02.png)


* [Dowload and install SquirreL](http://www.squirrelsql.org/#installation)

## Add Kylin JDBC Driver

On left menu: ![alt text](/images/SQuirreL-Tutorial/03.png) >![alt text](/images/SQuirreL-Tutorial/04.png)  > ![alt text](/images/SQuirreL-Tutorial/05.png)  > ![alt text](/images/SQuirreL-Tutorial/06.png)

And locate the JAR: ![alt text](/images/SQuirreL-Tutorial/07.png)

Configure this parameters:

* Put a name: ![alt text](/images/SQuirreL-Tutorial/08.png)
* Example URL ![alt text](/images/SQuirreL-Tutorial/09.png)

  jdbc:kylin://172.17.0.2:7070/learn_kylin
* Put Class Name: ![alt text](/images/SQuirreL-Tutorial/10.png)
	Tip:  If auto complete not work, type:  org.apache.kylin.jdbc.Driver 
	
Check the Driver List: ![alt text](/images/SQuirreL-Tutorial/11.png)

## Add Aliases

On left menu: ![alt text](/images/SQuirreL-Tutorial/12.png)  > ![alt text](/images/SQuirreL-Tutorial/13.png) : (Login pass by default: ADMIN / KYLIN)

  ![](/images/SQuirreL-Tutorial/14.png)


And automatically launch connection:

  ![](/images/SQuirreL-Tutorial/15.png)


## Connect and Execute

The startup window when connected:

  ![](/images/SQuirreL-Tutorial/16.png)


Choose Tab: and write a query  (whe use Kylin’s example cube):

  ![](/images/SQuirreL-Tutorial/17.png)


```
select part_dt, sum(price) as total_sold, count(distinct seller_id) as sellers 
from kylin_sales group by part_dt 
order by part_dt
```

Execute With: ![alt text](/images/SQuirreL-Tutorial/18.png) 

  ![](/images/SQuirreL-Tutorial/19.png)


And it’s works!

## Tips:

SquirreL isn’t the most stable SQL Client, but it is very flexible and get a lot of info; It can be used for PoC and checking connectivity issues.

List of tables: 

  ![](/images/SQuirreL-Tutorial/21.png)


List of columns of table:

  ![](/images/SQuirreL-Tutorial/22.png)


List of column of Querie:

  ![](/images/SQuirreL-Tutorial/23.png)


Export the result of queries:

  ![](/images/SQuirreL-Tutorial/24.png)


 Info about time query execution:

  ![](/images/SQuirreL-Tutorial/25.png)
