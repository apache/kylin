---
layout: docs31
title: Hue
categories: tutorial
permalink: /docs31/tutorial/hue.html
---
### Introduction
 In [Hue-2745](https://issues.cloudera.org/browse/HUE-2745) v3.10, add JDBC support like Phoenix, Kylin, Redshift, Solr Parallel SQL, …

However, there isn’t any manual to use with Kylin.

### Pre-requisites
Build a cube sample of Kylin with: [Quick Start with Sample Cube](http://kylin.apache.org/docs/tutorial/kylin_sample.html), will be enough.

You can check: 

  ![](/images/tutorial/2.0/hue/01.png)


### Used Software:
* [Hue](http://gethue.com/) v3.10.0
* [Apache Kylin](http://kylin.apache.org/) v1.5.2


### Install Hue
If you have Hue installed, you can skip this step.

To install Hue on Ubuntu 16.04 LTS. The [official Instructions](http://gethue.com/how-to-build-hue-on-ubuntu-14-04-trusty/) didn’t work but [this](https://github.com/cloudera/hue/blob/master/tools/docker/hue-base/Dockerfile) works fine:

There isn’t any binary package thus [pre-requisites](https://github.com/cloudera/hue#development-prerequisites) must be installed and compile with the command *make*

{% highlight Groff markup %}
    sudo apt-get install --fix-missing -q -y \
    git \
    ant \
    gcc \
    g++ \
    libkrb5-dev \
    libmysqlclient-dev \
    libssl-dev \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    libsqlite3-dev \
    libtidy-0.99-0 \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    make \
    maven \
    libldap2-dev \
    python-dev \
    python-setuptools \
    libgmp3-dev \
    libz-dev
{% endhighlight %}

Download and Compile:

{% highlight Groff markup %}
    git clone https://github.com/cloudera/hue.git
    cd hue
    make apps
{% endhighlight %}

Start and connect to Hue:

{% highlight Groff markup %}
    build/env/bin/hue runserver_plus localhost:8888
{% endhighlight %}
* runserver_plus: is like runserver with [debugger](http://django-extensions.readthedocs.io/en/latest/runserver_plus.html#usage)
* localIP: Port, usually Hue uses 8888

The output must be similar to:

  ![](/images/tutorial/2.0/hue/02.png)


Connect using your browser: http://localhost:8888

  ![](/images/tutorial/2.0/hue/03.png)


Important: The first time that you connect to hue, you set Login / Pass  for admin

We will use Hue / Hue as login / pass


**Issue 1:** Could not create home directory

  ![](/images/tutorial/2.0/hue/04.png)


   It is a permission problem of your current user, you can use: sudo to start Hue

**Issue 2:** Could not connect to … 

  ![](/images/tutorial/2.0/hue/05.png)

   If Hue’s code  had been downloaded from Git, Hive connection is active but not configured → skip this message  

**Issue 3:** Address already in use

  ![](/images/tutorial/2.0/hue/06.png)

   The port is in use or you have a Hive process running already

  You can use *ps -ef | grep hue*, to find the PID and kill


### Configure Hue for Apache Kylin
The purpose is to add a snipped in a notebook with Kylin queries

References:
* [Custom SQL Databases](http://gethue.com/custom-sql-query-editors/)	
* [Manual: Kylin JDBC Driver](http://kylin.apache.org/docs/howto/howto_jdbc.html)
* [GitHub: Kylin JDBC Driver](https://github.com/apache/kylin/tree/3b2ebd243cfe233ea7b1a80285f4c2110500bbe5/jdbc)

Register JDBC Driver

1. To find the JAR Class for the JDBC Connector

 From Kylin [Download](http://kylin.apache.org/download/)
Choose **Binary** and the **correct version of Kylin and HBase**

 Download & Unpack:  in ./lib: 

  ![](/images/tutorial/2.0/hue/07.png)


2. Place this JAR in Java ClassPATH using .bashrc

  ![](/images/tutorial/2.0/hue/08.png)


  check the actual value: ![alt text](/images/tutorial/2.0/hue/09.png)

  check the permission for this file (must be accessible to you):

  ![](/images/tutorial/2.0/hue/10.png)


3. Add this new interface to Hue.ini

  Where is the hue.ini ? 

 * If the code is downloaded from Git:  *UnzipPath/desktop/conf/pseudo-distributed.ini*

   (I shared my *INI* file in GitHub).

 * If you are using Cloudera: you must use Advanced Configuration Snippet

 * Other: find your actual *hue.ini*

 Add these lines in *[[interpreters]]*
{% highlight Groff markup %}
    [[[kylin]]]
    name=kylin JDBC
    interface=jdbc
    options='{"url": "jdbc:kylin://172.17.0.2:7070/learn_kylin","driver": "org.apache.kylin.jdbc.Driver", "user": "ADMIN", "password": "KYLIN"}'
{% endhighlight %}

4. Try to Start Hue and connect just like in ‘Start and connect’

TIP: One JDBC Source for each project is need


Register without a password, it can do use this other format:
{% highlight Groff markup %}
    options='{"url": "jdbc:kylin://172.17.0.2:7070/learn_kylin","driver": "org.apache.kylin.jdbc.Driver"}'
{% endhighlight %}

And when you open the Notebook, Hue prompts this:

  ![](/images/tutorial/2.0/hue/11.png)



**Issue 1:** Hue can’t Start

If you see this when you connect to Hue  ( http://localhost:8888 ):

  ![](/images/tutorial/2.0/hue/12.png)


Go to the last line ![alt text](/images/tutorial/2.0/hue/13.png) 

And launch Python Interpreter (see console icon on the right):

  ![](/images/tutorial/2.0/hue/14.png)

In this case: I've forgotten to close “ after learn_kylin

**Issue 2:** Password Prompting

In Hue 3.11 there is a bug [Hue 4716](https://issues.cloudera.org/browse/HUE-4716)

In Hue 3.10 with Kylin, I don’t have any problem   :)


## Test query example
Add Kylin JDBC as source in the Kylin’s notebook:

 ![alt text](/images/tutorial/2.0/hue/15.png) > ![alt text](/images/tutorial/2.0/hue/16.png)  > ![alt text](/images/tutorial/2.0/hue/17.png)  > ![alt text](/images/tutorial/2.0/hue/18.png) 


Write a query, like this:
{% highlight Groff markup %}
    select part_dt, sum(price) as total_selled, count(distinct seller_id) as sellers from kylin_sales group by part_dt order by part_dt
{% endhighlight %}

And Execute with: ![alt text](/images/tutorial/2.0/hue/19.png) 

  ![](/images/tutorial/2.0/hue/20.png)


 **Congratulations !!!**  you are connected to Hue with Kylin


**Issue 1:**  No suitable driver found for jdbc:kylin

  ![](/images/tutorial/2.0/hue/21.png)

There is a bug, not solved since 27 Aug 2016, nor in 3.10 and 3.11, but the solution is very easy:

[Link](https://github.com/cloudera/hue/pull/369): 
You only need to change 3 lines in  *<HuePath>/desktop/libs/librdbms/src/librdbms/jdbc.py*


## Limits
In Hue 3.10 and 3.11
* Auto-complete doesn’t work on JDBC interfaces
* Max 1000 records. There is a limitation on JDBC interfaces, because Hue does not support result pagination [Hue 3419](https://issues.cloudera.org/browse/HUE-3419). 


### Future Work

**Dashboards**
There is an amazing feature of Hue: [Search Dasboards](http://gethue.com/search-dashboards/) / [Dynamic Dashboards](http://gethue.com/hadoop-search-dynamic-search-dashboards-with-solr/). You can ‘play’ with this [Demo On-line](http://demo.gethue.com/search/admin/collections). But this only works with SolR.

There is a JIRA to solve this: [Hue 3228](https://issues.cloudera.org/browse/HUE-3228), is in roadmap for 4.1. Check Hue MailList[MailList](https://groups.google.com/a/cloudera.org/forum/#!topic/hue-user/B6FWBeoqK7I) and add Dashboards to JDBC connections.

**Chart & Dynamic Filter**
Nowadays, it isn’t compatible, you only can work with Grid.

**DB Query**
 DB Query does not yet support JDBC.
