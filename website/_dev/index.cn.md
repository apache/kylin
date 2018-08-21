---
layout: dev-cn
title: 开发快速指南
permalink: /cn/development/index.html
---

Apache Kylin 一直寻求的不只是代码的贡献，还寻求使用文档，性能报告，问答等方面的贡献。所有类型的贡献都为成为 Kylin Committer 铺平了道路。每个人都有机会，尤其是那些有分析和解决方案背景的，因为缺少来自于用户和解决方案视角的内容。

### 如何贡献
查看[如何贡献](/cn/development/howto_contribute.html)文档。

### 源仓库
Apache Kylin™ 源码使用 Git version control 进行版本控制：
Commits [总结](https://github.com/apache/kylin/commits/master)  
源仓库：[https://github.com/apache/kylin ](https://github.com/apache/kylin )  
Gitbox 的镜像：[https://gitbox.apache.org/repos/asf?p=kylin.git ](https://gitbox.apache.org/repos/asf?p=kylin.git )

### CI 和代码分析
使用 JDK 1.7（过时）的 master 分支上的 UT：[Kylin-Master-JDK-1.7](https://builds.apache.org/job/Kylin-Master-JDK-1.7/) 
使用 JDK 1.8 的 master 分支上的 UT：[Kylin-Master-JDK-1.8](https://builds.apache.org/job/Kylin-Master-JDK-1.8/) 
静态代码分析：[SonarCube dashboard](https://builds.apache.org/analysis/overview?id=org.apache.kylin%3Akylin)

[![Build Status](https://travis-ci.org/apache/kylin.svg?branch=master)](https://travis-ci.org/apache/kylin)[![Codacy Badge](https://api.codacy.com/project/badge/Grade/74f0139786cd4e8a8ce69bb0c17c2e71)](https://www.codacy.com/app/kyligence-git/kylin?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=apache/kylin&amp;utm_campaign=Badge_Grade)
[![Quality Gate](https://sonarcloud.io/api/badges/gate?key=org.apache.kylin%3Akylin)](https://sonarcloud.io/dashboard/index/org.apache.kylin%3Akylin)[![SonarCloud Coverage](https://sonarcloud.io/api/badges/measure?key=org.apache.kylin%3Akylin&metric=coverage)](https://sonarcloud.io/component_measures/metric/coverage/list?id=org.apache.kylin%3Akylin)[![SonarCloud Bugs](https://sonarcloud.io/api/badges/measure?key=org.apache.kylin%3Akylin&metric=bugs)](https://sonarcloud.io/component_measures/metric/reliability_rating/list?id=org.apache.kylin%3Akylin)[![SonarCloud Vulnerabilities](https://sonarcloud.io/api/badges/measure?key=org.apache.kylin%3Akylin&metric=vulnerabilities)](https://sonarcloud.io/component_measures/metric/security_rating/list?id=org.apache.kylin%3Akylin)

### Issue 追踪  
在 Apache JIRA 上的 "Kylin" 项目追踪 issues（[浏览](http://issues.apache.org/jira/browse/KYLIN))。

### 路线图
- 支持 Hadoop 3.0（纠偏编码)
- 完全使用 Spark 的 Cube 引擎
- 接入更多的源（MySQL，Oracle，Spark SQL 等)
- 无需构建 Cube 的即席查询 
- 更好的存储引擎（Druid，Kudu，等)
- 支持实时数据分析的 Lambda 架构
