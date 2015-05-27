---
layout: post
title:  "Off Hadoop CLI Installation (Dev Env Setup)"
date:   2015-02-15
author: SHAOFENG SHI
categories: installation
---

Off-Hadoop-CLI installation is usually for **development use**.

Developers want to run kylin test cases or applications at their development machine. The scenario is depicted at [https://github.com/KylinOLAP/Kylin#off-hadoop-cli-installation](https://github.com/KylinOLAP/Kylin#off-hadoop-cli-installation).


By following this tutorial, you will be able to build kylin test cubes by running a specific test case, and you can further run other test cases against the cubes having been built.


## Environment on the Hadoop CLI

Off-Hadoop-CLI installation requires you having a hadoop CLI machine (or a hadoop sandbox) as well as your local develop machine. To make things easier we strongly recommend you starting with running Kylin on a hadoop sandbox, like <http://hortonworks.com/products/hortonworks-sandbox/>. In the following tutorial we'll go with **Hortonworks Sandbox 2.2**. 

### Start Hadoop

In Hortonworks sandbox, ambari helps to launch hadoop:

{% highlight Groff markup %}
	ambari-agent start
	ambari-server start
{% endhighlight %}
	
With both command successfully run you can go to ambari home page at <http://yoursandboxip:8080> (user:admin,password:admin) to check everything's status. By default ambari disables Hbase, you'll need manually start the `Hbase` service.

For other hadoop distribution, basically start the hadoop cluster, make sure HDFS, YARN, Hive, HBase are running.


## Environment on the dev machine


### Install maven

The latest maven can be found at <http://maven.apache.org/download.cgi>, we create a symbolic so that `mvn` can be run anywhere.

{% highlight Groff markup %}
	cd ~
	wget http://xenia.sote.hu/ftp/mirrors/www.apache.org/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
	tar -xzvf apache-maven-3.2.5-bin.tar.gz
	ln -s /root/apache-maven-3.2.5/bin/mvn /usr/bin/mvn
{% endhighlight %}

### Compile

First clone the Kylin project to your local:

{% highlight Groff markup %}
	git clone https://github.com/KylinOLAP/Kylin.git
{% endhighlight %}
	
Install Kylin artifacts to the maven repo

{% highlight Groff markup %}
	mvn clean install -DskipTests
{% endhighlight %}

### Modify local configuration

Local configuration must be modified to point to your hadoop sandbox (or CLI) machine. 

* In **examples/test_case_data/sandbox/kylin.properties**
   * Find `sandbox` and replace with your hadoop hosts (if you're using HDP sandbox, this can be skipped)
   * Fine `kylin.job.run.as.remote.cmd` and change it to "true" (in code repository the default is false, which assume running it on hadoop CLI)
   * Find `kylin.job.remote.cli.username` and `kylin.job.remote.cli.password`, fill in the user name and password used to login hadoop cluster for hadoop command execution; If you're using HDP sandbox, the default username is "root" and password is "hadoop".

* In **examples/test_case_data/sandbox**
   * For each configuration xml file, find all occurrence of `sandbox` and replace with your hadoop hosts; (if you're using HDP sandbox, this can be skipped)

An alternative to the host replacement is updating your `hosts` file to resolve `sandbox` and `sandbox.hortonworks.com` to the IP of your sandbox machine.

### Run unit tests

Run a end-to-end cube building test
 
{% highlight Groff markup %}
	mvn test -Dtest=com.kylinolap.job.BuildCubeWithEngineTest -DfailIfNoTests=false
{% endhighlight %}
	
Run other tests, the end-to-end cube building test is exclueded

{% highlight Groff markup %}
	mvn test
{% endhighlight %}

### Launch Kylin Web Server

In your Eclipse IDE, launch `com.kylinolap.rest.DebugTomcat` with specifying VM arguments "-Dspring.profiles.active=sandbox". (By default Kylin server will listen on 7070 port; If you want to use another port, please specify it as a parameter when run `DebugTomcat)

Check Kylin Web available at http://localhost:7070/kylin (user:ADMIN,password:KYLIN)

