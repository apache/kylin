---
layout: docs15-cn
title:  "手动安装指南"
categories: 安装
permalink: /cn/docs15/install/manual_install_guide.html
version: v0.7.2
since: v0.7.1
---

## 引言

在大多数情况下，我们的自动脚本[Installation Guide](./index.html)可以帮助你在你的hadoop sandbox甚至你的hadoop cluster中启动Kylin。但是，为防部署脚本出错，我们撰写本文作为参考指南来解决你的问题。

基本上本文解释了自动脚本中的每一步骤。我们假设你已经对Linux上的Hadoop操作非常熟悉。

## 前提条件
* 已安装Tomcat，输出到CATALINA_HOME（with CATALINA_HOME exported). 
* Kylin 二进制文件拷贝至本地并解压，之后使用$KYLIN_HOME引用

## 步骤

### 准备Jars

Kylin会需要使用两个jar包，两个jar包和配置在默认kylin.properties：（there two jars and configured in the default kylin.properties）

```
kylin.job.jar=/tmp/kylin/kylin-job-latest.jar

```

这是Kylin用于MR jobs的job jar包。你需要复制 $KYLIN_HOME/job/target/kylin-job-latest.jar 到 /tmp/kylin/

```
kylin.coprocessor.local.jar=/tmp/kylin/kylin-coprocessor-latest.jar

```

这是一个Kylin会放在hbase上的hbase协处理jar包。它用于提高性能。你需要复制 $KYLIN_HOME/storage/target/kylin-coprocessor-latest.jar 到 /tmp/kylin/

### 启动Kylin

以`./kylin.sh start`

启动Kylin

并以`./Kylin.sh stop`

停止Kylin
