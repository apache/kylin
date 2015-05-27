---
layout: post
title:  "On Hadoop CLI installation"
date:   2015-03-13
author: Luke Han
categories: installation
---

>For new users: Please go with v0.7.x, binary package and installation guide here: [http://kylin.incubator.apache.org/download/](http://kylin.incubator.apache.org/download/)
> 
> For Version: v0.6.6 only, tested: HDP 2.1/2.2 sandbox, HDP 2.1 Cluster


On-Hadoop-CLI installation is for demo use, or for those who want to host their own web site to provide Kylin service. The scenario is depicted at <https://github.com/KylinOLAP/Kylin#on-hadoop-cli-installation>.

Except for some prerequisite software installations, the core of Kylin installation is accomplished by running a single script. After running the script, you will be able to build sample cube and query the tables behind the cubes via a unified web interface.

## Environment

Running On-Hadoop-CLI installation requires you having access to a hadoop CLI, where you have full permissions to hdfs, hive, hbase and map-reduce. To make things easier we strongly recommend you starting with running Kylin on a hadoop sandbox, like <http://hortonworks.com/products/hortonworks-sandbox/>. In the following tutorial we'll go with **Hortonworks Sandbox 2.1/2.2** and **Cloudera QuickStart VM 5.1/5.2**. 

To avoid permission issue, we suggest you using `root` account. The password for **Hortonworks Sandbox 2.1/2.2** is `hadoop` , for **Cloudera QuickStart VM 5.1/5.2** is `cloudera`.

For account other than `root`, please make sure your account has privileges to access relative folders and files. For example, if you are using `cloudera` account in Cloudera VM, please run follow command first before run Kylin deploy script:

{% highlight Groff markup %}
sudo mkdir /etc/kylin
sudo chmod -R 777 /etc/kylin
{% endhighlight %}

We also suggest you using bridged mode instead of NAT mode in your virtual box settings. Bridged mode will assign your sandbox an independent IP so that you can avoid issues like https://github.com/KylinOLAP/Kylin/issues/12

### Start Hadoop

For Hortonworks, ambari helps to launch hadoop:

{% highlight Groff markup %}
ambari-agent start
ambari-server start
{% endhighlight %}
	
With both command successfully run you can go to ambari homepage at <http://your_sandbox_ip:8080> (user:admin,password:admin) to check everything's status. **By default hortonworks ambari disables Hbase, you'll need manually start the `Hbase` service at ambari homepage.**

![start hbase in ambari]( /images/On-Hadoop-CLI-installation/starthbase.png)

For Cloudera, you can skip this step since they're by default activated.

### Install tomcat

The latest tomcat can be found at <http://tomcat.apache.org/download-70.cgi>, we need the variable `CATALINA_HOME` exported.

{% highlight Groff markup %}
cd ~
wget http://apache.mirrors.lucidnetworks.net/tomcat/tomcat-7/v7.0.59/bin/apache-tomcat-7.0.59.tar.gz
tar -xzvf apache-tomcat-7.0.59.tar.gz
export CATALINA_HOME=~/apache-tomcat-7.0.59
{% endhighlight %}

### Install maven:

For Cloudera, you can skip this step since maven is by default installed.

The latest maven can be found at <http://maven.apache.org/download.cgi>, we create a symbolic so that `mvn` can be run anywhere.

{% highlight Groff markup %}
cd ~
wget http://xenia.sote.hu/ftp/mirrors/www.apache.org/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
tar -xzvf apache-maven-3.2.5-bin.tar.gz 
ln -s ~/apache-maven-3.2.5/bin/mvn /usr/bin/mvn
{% endhighlight %}

### Install npm

Npm comes with latest versions of nodejs, just donwload nodejs from <http://nodejs.org/download/>, `npm` is located at the `/bin` folder of `nodejs`, we append the folder to `PATH` so that `npm` can be run anywhere.

{% highlight Groff markup %}
cd ~
wget http://nodejs.org/dist/v0.10.32/node-v0.10.32-linux-x64.tar.gz
tar -xzvf node-v0.10.32-linux-x64.tar.gz
export PATH=~/node-v0.10.32-linux-x64/bin:$PATH
{% endhighlight %}
	
## Build cubes & Query tables

First clone the Kylin project to your local:

{% highlight Groff markup %}
git clone https://github.com/KylinOLAP/Kylin.git
{% endhighlight %}
	
Go into the folder and run deploy script:

{% highlight Groff markup %}	
cd Kylin/
./deploy.sh
{% endhighlight %}

At this point, if you're installing Kylin on a cluster, please take a look at this:[a limitation](https://issues.apache.org/jira/browse/KYLIN-604), If you're using a sandbox, please just go ahead.

After successfully deployed, you can start run with:

{% highlight Groff markup %}	
cd Kylin/
./kylin.sh start
{% endhighlight %}

And later stop it by:
	
{% highlight Groff markup %}
cd Kylin/
./kylin.sh stop
{% endhighlight %}

If you meet any problems, please check [FAQ](https://github.com/KylinOLAP/Kylin/wiki/FAQ-on-Kylin-Installation-and-Usage) first.
This script will help to:

1. Check your environment
2. Build Kylin artifacts with Maven
3. Generate some sample tables
3. Create empty cubes for these tables
4. Lauch a one-stop website to build cubes & query tables

After successfully running the script, please navigate to <http://your_hostname:7070> to build your cube and query it. The username/password is ADMIN/KYLIN

If you want to check detail log, you can find it at ${CATALINA_HOME}/logs/kylin.log

### Build Cube in Cubes Tab

![build cube]( /images/On-Hadoop-CLI-installation/cube.png)

### Check Building Progress in Job Tab
![check job status]( /images/On-Hadoop-CLI-installation/job.png)

### Query Tables in Query Tab
![query tables]( /images/On-Hadoop-CLI-installation/query.png)

## What if I closed my VM?

If you shut down the VM and restarted it, Kylin will not automatically start. Depending on whether you succeed in running the deploy.sh, you should:

### If running deploy.sh failed last time
1. Kill it if any tomcat instance exist
2. Start over again

### If running deploy.sh succeeded last time
1. Kill it if any tomcat instance exist
2. Make sure Hbase is running
3. run `export CATALINA_HOME=~/apache-tomcat-7.0.56`
4. run `sudo -i "${CATALINA_HOME}/bin/startup.sh"`
