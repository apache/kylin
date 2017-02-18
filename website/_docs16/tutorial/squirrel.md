---
layout: docs16
title:  SQuirreL
categories: tutorial
permalink: /docs16/tutorial/squirrel.html
---

### Intro
SquirreL SQL is a multi platform Universal SQL Client (GNU License)
You can use it to access HBase + Phoenix and Hive

&nbsp;
### Used Software
* [Kylin v1.5.2](http://kylin.apache.org/download/) & ODBC 1.5
*  Update Dic 2016: Kylin 1.6.0 & ODBC 1.6 Works OK
* [SquirreL SQL v3.7.1](http://www.squirrelsql.org/)

&nbsp;
## Pre-requisites
* We need to find the JAR Class for the JDBC Connector

  From Kylin Download, Choose Binary and the **correct version of Kylin and HBase**
  
	Download & Unpack:  in **./lib**: 

  ![](/images/SQuirreL-Tutorial/01.png)


* We need an instance of Kylin, with a cube: Quick Start with Sample Cube, will be enough

  You can check: 

  ![](/images/SQuirreL-Tutorial/02.png)


* [Dowload and install SquirreL](http://www.squirrelsql.org/#installation), you will need Java

&nbsp;
## Add Kylin Driver
On left menu: ![alt text](/images/SQuirreL-Tutorial/03.png) >![alt text](/images/SQuirreL-Tutorial/04.png)  > ![alt text](/images/SQuirreL-Tutorial/05.png)  > ![alt text](/images/SQuirreL-Tutorial/06.png)

And locale your JAR: ![alt text](/images/SQuirreL-Tutorial/07.png)

Configure this parameters:

* Put a name: ![alt text](/images/SQuirreL-Tutorial/08.png)
* Example URL ![alt text](/images/SQuirreL-Tutorial/09.png)

  jdbc:kylin://172.17.0.2:7070/learn_kylin
* Put Class Name: ![alt text](/images/SQuirreL-Tutorial/10.png)
	TIP:  If auto complete not work, type you:  org.apache.kylin.jdbc.Driver 
	
Check in your Driver List: ![alt text](/images/SQuirreL-Tutorial/11.png)

&nbsp;
## Add Aliases
On left menu: ![alt text](/images/SQuirreL-Tutorial/12.png)  > ![alt text](/images/SQuirreL-Tutorial/13.png) : (Login pass by default: ADMIN / KYLIN)

  ![](/images/SQuirreL-Tutorial/14.png)


And automatically launch conection:

  ![](/images/SQuirreL-Tutorial/15.png)


&nbsp;
## Connect and Execute
The startup window when you are connect:

  ![](/images/SQuirreL-Tutorial/16.png)



Choose Tab:   and write your querie  (whe use Kylin’s example cube):

  ![](/images/SQuirreL-Tutorial/17.png)


```
select part_dt, sum(price) as total_selled, count(distinct seller_id) as sellers 
from kylin_sales group by part_dt 
order by part_dt
```

Execute With: ![alt text](/images/SQuirreL-Tutorial/18.png) 

  ![](/images/SQuirreL-Tutorial/19.png)


And it’s works,OK ![alt text](/images/SQuirreL-Tutorial/20.png) 

&nbsp;
## Extra: Some tips:
SquirreL isn’t the most stable SQL Client, but its very flexible and get you a lot of info

I use it for PoC and try to solve / check connectivity problems

List of  Tables: 

  ![](/images/SQuirreL-Tutorial/21.png)


&nbsp;

List of Columns of table:

  ![](/images/SQuirreL-Tutorial/22.png)


&nbsp;

List of column of Querie:

  ![](/images/SQuirreL-Tutorial/23.png)


&nbsp;

Export the result of queries:

  ![](/images/SQuirreL-Tutorial/24.png)


&nbsp;

 Info about time query execution:

  ![](/images/SQuirreL-Tutorial/25.png)



