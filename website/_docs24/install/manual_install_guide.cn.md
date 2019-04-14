---
layout: docs24-cn
title:  "手动安装指南"
categories: 安装
permalink: /cn/docs24/install/manual_install_guide.html
version: v0.7.2
since: v0.7.1
---

## 引言

在大多数情况下，我们的自动脚本[Installation Guide](./index.html)可以帮助你在你的hadoop sandbox甚至你的hadoop cluster中启动Kylin。但是，为防部署脚本出错，我们撰写本文作为参考指南来解决你的问题。

基本上本文解释了自动脚本中的每一步骤。我们假设你已经对Linux上的Hadoop操作非常熟悉。

## 前提条件
* Kylin 二进制文件拷贝至本地并解压，之后使用$KYLIN_HOME引用
`export KYLIN_HOME=/path/to/kylin`
`cd $KYLIN_HOME`

### 启动Kylin

以`./bin/kylin.sh start`

启动Kylin

并以`./bin/Kylin.sh stop`

停止Kylin
