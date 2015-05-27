---
layout: post
title:  "Kylin Manual Installation Guide"
date:   2015-01-22
author: hongbin ma
categories: installation
---

## INTRODUCTION

In most cases our automated script [On-Hadoop-CLI-installation](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation) can help you launch Kylin in your hadoop sandbox and even your hadoop cluster. However, in case something went wrong in the deploy script, this article comes as an reference guide to fix your issues.

Basically this article explains every step in the automatic script. We assume that you are already very familiar with Hadoop operations on Linux. 

## PREREQUISITES
* Tomcat installed, with CATALINA_HOME exported. ([quick help](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation#install-tomcat))
* Maven installed, with `mvn` command accessible at shell. ([quick help](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation#install-maven)) 
* Npm installed, with command `npm` accessible at shell. ([quick help](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation#install-npm))
* Kylin git repo cloned to a folder later referenced as $KYLIN_HOME

## STEPS

### 1.Build kylin.war from source code

We have provided a script( $KYLIN_HOME/package.sh ) to help you automate this step. The kylin.war composes of two parts: (Take a look at package.sh to better understand)

* A REST server that accepts cube build requests, queries requests ,etc. This part is purely in Java, and is built by maven.
* An angular-based web application, this is the real website users see, and it will call the REST APIs in the above REST server. This part is purely html+js, and is built by npm.

Running package.sh will build both parts and combine them to a single kylin.war.

### 2.Customize kylin configuration files 

Kylin's configuration file includes a kylin.properties and a kylin_job_conf.xml for MR job settings. You can find a template for these two configuration files in $KYLIN_HOME/examples/test_case_data/sandbox/. They're almost ready to use, the only thing you need to do is copy them to a configuration folder(we suggest /etc/kylin, if you choose not to use it, you'll replace the "/etc/kylin" with yours in later occurrence) and replace every "sandbox" in kylin.properties with the real hostname of your server.

When you find you cubes's expansion rate too large, you'll need to consider enabling LZO compression by modifying kylin_job_conf.xml, checkout [this chapter ](https://github.com/KylinOLAP/Kylin/wiki/Advance-settings-of-Kylin-environment#enabling-lzo-compression) to find how. For test cubes you can safely ignore this.

### 3. Prepare tomcat
* Copy $KYLIN_HOME/server/target/kylin.war to $CATALINA_HOME/webapps/, change kylin.war's mode to 644 in case tomcat have no access to it.
* By default tomcat uses port 8080 as well as several other ports. However, these ports are already occupied in many hadoop sandboxes. For example, port 8080 is used by ambari on hortonworks sandbox. So you need to modify port if necessary. If you're not familiar with this, you can choose to overwrite $CATALINA_HOME/conf/server.xml with $KYLIN_HOME/deploy/server.xml. With our server.xml, kylin will run on port 7070.

### 4. Prepare Jars

There are two jars that Kylin will need to use, there two jars and configured in the default kylin.properties:

####  kylin.job.jar=/tmp/kylin/kylin-job-latest.jar

This is job jar that Kylin uses for MR jobs. You need to copy $KYLIN_HOME/job/target/kylin-job-latest.jar to /tmp/kylin/

#### kylin.coprocessor.local.jar=/tmp/kylin/kylin-coprocessor-latest.jar

This is a hbase coprocessor jar that Kylin will put on hbase. It is used for performance boosting. You need to copy $KYLIN_HOME/storage/target/kylin-coprocessor-latest.jar to /tmp/kylin/

### 5. Start Kylin

Start Kylin with

`./kylin.sh start`

and stop Kylin with

`./Kylin.sh stop`
