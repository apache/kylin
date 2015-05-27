---
layout: post
title:  "How to install kylin in your Hadoop cluster"
date:   2014-11-14
author: xfhap
categories: howto
---

## 1 Environment
We can open the Kylin’ code directory and read the pom.xml, and find some components version:

### 1.1 Install hadoop cluster

{% highlight Groff markup %}
  <hadoop2.version>2.4.1</hadoop2.version>
  <hbase-hadoop2.version>0.98.0-hadoop2</hbase-hadoop2.version>
  <hive.version>0.13.0</hive.version> (ps: install mysql --use mysql to store metastore_db)
{% endhighlight %}

Attach:

1) When you build the cube and get this error: java.net.ConnectException: to 0.0.0.0:10020 failed on connection exception, you can try to change this file.

config ${hadoop install}/etc/hadoop/mapred-site.xml, and add this code:
{% highlight Groff markup %}
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>host:10020</value>
  </property>
{% endhighlight %}

Execute this cmd after the configuration:
$ mr-jobhistory-daemon.sh start historyserver

2) The version of hadoop which Hbase used must be same as the hadoop cluster.

For example:

In Hbase0.98.0-hadoop2 use hadoop 2.2.0, if your hadoop cluster is 2.4.1, then you need to replace all the hadoop jars from 2.2.0 to 2.4.1.

### 1.2	Install other components

Command hadoop, hive, hbase is workable on your hadoop cluster, this step can read this link [https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation)
{% highlight Groff markup %}
  <javaVersion>1.7</javaVersion>
  Maven
  Git
  Tomcat (CATALINA_HOME being set)
  Npm
{% endhighlight %}

## 2 Install Kylin

### 2.1	Run the deploy.sh
