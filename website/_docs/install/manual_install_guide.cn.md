---
layout: docs-cn
title:  "手动安装指南"
categories: 安装
permalink: /cn/docs/install/manual_install_guide.html
version: v0.7.2
since: v0.7.1
---

## 引言

在大多数情况下，我们的自动脚本[On-Hadoop-CLI-installation](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation)可以帮助你在你的hadoop sandbox甚至你的hadoop cluster中启动Kylin。但是，为防部署脚本出错，我们撰写本文作为参考指南来解决你的问题。

基本上本文解释了自动脚本中的每一步骤。我们假设你已经对Linux上的Hadoop操作非常熟悉。

## 前提条件
* 已安装Tomcat，输出到CATALINA_HOME（with CATALINA_HOME exported）。([quick help](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation#install-tomcat))
* 已安装Maven，shell可访问`mvn`指令（with `mvn` command accessible at shell）。([quick help](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation#install-maven)) 
* 已安装Npm，shell可访问`npm`指令（with command `npm` accessible at shell）。([quick help](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation#install-npm))
* Kylin git repo克隆到一个文件夹，之后使用$KYLIN_HOME引用

## 步骤

### 1.从源代码构建kylin.war

我们提供一个脚本（$KYLIN_HOME/package.sh）来帮助你自动操作这个步骤。kylin.war由两部分组成：（看看package.sh可以更好地理解）

* 一个REST服务器接收cube构建请求，查询请求等。这一部分是纯Java的，并由maven构建。
* 一个基于angular（angular-based）的网页应用，这是一个用户看到的真实网站，并且它会调用上述REST服务器的REST API。这一部分是纯html+js的，并由npm构建。

运行package.sh会构建上述两部分并将它们合并到单个kylin.war。

### 2.自定义Kylin配置文件

Kylin的配置文件包括一个kylin.properties和一个用于MR job设置的kylin_job_conf.xml。你可以在$KYLIN_HOME/examples/test_case_data/sandbox/找到这两个配置文件的模板。它们几乎可以直接使用，你唯一需要做的是将它们复制到一个配置文件夹（我们建议/etc/kylin，如果你选择不使用它，你将在之后出现"/etc/kylin"处使用你路径进行替换）并使用你服务器的实际主机名替换每个kylin.properties中的"sandbox"。

当你发现你的cube的膨胀率过大，你将需要考虑通过修改kylin_job_conf.xml启用LZO压缩，查看[this chapter ](https://github.com/KylinOLAP/Kylin/wiki/Advance-settings-of-Kylin-environment#enabling-lzo-compression)探讨如何修改。测试cube（For test cubes）可以安全地忽略这一点。

### 3. 准备tomcat
* 复制 $KYLIN_HOME/server/target/kylin.war 到 $CATALINA_HOME/webapps/, 将Kylin.war的模式修改为644以防tomcat无法访问它。
* tomcat默认使用端口8080与一些其他端口。但是，这些端口在许多hadoop sandbox中已经被占用了。例如，端口8080在hortonworks sandbox上被ambari使用。所以必要的话你需要修改端口。如果你对此不熟悉，你可以选择使用$KYLIN_HOME/deploy/server.xml覆盖$CATALINA_HOME/conf/server.xml。使用我们的server.xml，kylin会运行在端口7070.

### 4. 准备Jars

Kylin会需要使用两个jar包，两个jar包和配置在默认kylin.properties：（there two jars and configured in the default kylin.properties）

####  kylin.job.jar=/tmp/kylin/kylin-job-latest.jar

这是Kylin用于MR jobs的job jar包。你需要复制 $KYLIN_HOME/job/target/kylin-job-latest.jar 到 /tmp/kylin/

#### kylin.coprocessor.local.jar=/tmp/kylin/kylin-coprocessor-latest.jar

这是一个Kylin会放在hbase上的hbase协处理jar包。它用于提高性能。你需要复制 $KYLIN_HOME/storage/target/kylin-coprocessor-latest.jar 到 /tmp/kylin/

### 5. 启动Kylin

以`./kylin.sh start`

启动Kylin

并以`./Kylin.sh stop`

停止Kylin
