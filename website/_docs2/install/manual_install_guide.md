---
layout: docs2
title:  Manual Installation Guide
categories: install
permalink: /docs2/install/manual_install_guide.html
version: v0.7.2
since: v0.7.1
---

## INTRODUCTION

In most cases our automated script [Installation Guide](index.html) can help you launch Kylin in your hadoop sandbox and even your hadoop cluster. However, in case something went wrong in the deploy script, this article comes as an reference guide to fix your issues.

Basically this article explains every step in the automatic script. We assume that you are already very familiar with Hadoop operations on Linux. 

## PREREQUISITES
* Tomcat installed, with CATALINA_HOME exported. 
* Kylin binary pacakge copied to local and setup $KYLIN_HOME correctly

## STEPS

### 4. Prepare Jars

There are two jars that Kylin will need to use, there two jars and configured in the default kylin.properties:

```
kylin.job.jar=/tmp/kylin/kylin-job-latest.jar

```

This is job jar that Kylin uses for MR jobs. You need to copy $KYLIN_HOME/job/target/kylin-job-latest.jar to /tmp/kylin/

```
kylin.coprocessor.local.jar=/tmp/kylin/kylin-coprocessor-latest.jar

```

This is a hbase coprocessor jar that Kylin will put on hbase. It is used for performance boosting. You need to copy $KYLIN_HOME/storage/target/kylin-coprocessor-latest.jar to /tmp/kylin/

### 5. Start Kylin

Start Kylin with

`./kylin.sh start`

and stop Kylin with

`./Kylin.sh stop`
