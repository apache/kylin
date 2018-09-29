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
使用 JDK 1.8 的 master 分支上的 UT：[Kylin-Master-JDK-1.8](https://builds.apache.org/job/Kylin-Master-JDK-1.8/) 
静态代码分析：[SonarCube dashboard](https://builds.apache.org/analysis/overview?id=org.apache.kylin%3Akylin)

[![Build Status](https://travis-ci.org/apache/kylin.svg?branch=master)](https://travis-ci.org/apache/kylin)[![Codacy Badge](https://api.codacy.com/project/badge/Grade/74f0139786cd4e8a8ce69bb0c17c2e71)](https://www.codacy.com/app/kyligence-git/kylin?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=apache/kylin&amp;utm_campaign=Badge_Grade)
[![Quality Gate](https://camo.githubusercontent.com/a9947cd56fb6e99807644f46830a35c1c4d4555e/68747470733a2f2f736f6e6172636c6f75642e696f2f6170692f70726f6a6563745f6261646765732f7175616c6974795f676174653f70726f6a6563743d6f72672e6170616368652e6b796c696e2533416b796c696e)](https://sonarcloud.io/dashboard/index/org.apache.kylin%3Akylin)[![SonarCloud Coverage](https://camo.githubusercontent.com/576fc0211eeafae5dd250ceaff1cf81135aae71a/68747470733a2f2f736f6e6172636c6f75642e696f2f6170692f70726f6a6563745f6261646765732f6d6561737572653f70726f6a6563743d6f72672e6170616368652e6b796c696e2533416b796c696e266d65747269633d636f766572616765)](https://sonarcloud.io/component_measures/metric/coverage/list?id=org.apache.kylin%3Akylin)[![SonarCloud Bugs](https://camo.githubusercontent.com/ce363d0c5f82c2ddc4bb5c2db4e0365354efb2cf/68747470733a2f2f736f6e6172636c6f75642e696f2f6170692f70726f6a6563745f6261646765732f6d6561737572653f70726f6a6563743d6f72672e6170616368652e6b796c696e2533416b796c696e266d65747269633d62756773)](https://sonarcloud.io/component_measures/metric/reliability_rating/list?id=org.apache.kylin%3Akylin)[![SonarCloud Vulnerabilities](https://camo.githubusercontent.com/24932e44a729982c780a9b230428f30d909657e3/68747470733a2f2f736f6e6172636c6f75642e696f2f6170692f70726f6a6563745f6261646765732f6d6561737572653f70726f6a6563743d6f72672e6170616368652e6b796c696e2533416b796c696e266d65747269633d76756c6e65726162696c6974696573)](https://sonarcloud.io/component_measures/metric/security_rating/list?id=org.apache.kylin%3Akylin)

### Issue 追踪  
在 Apache JIRA 上的 "Kylin" 项目追踪 issues（[浏览](http://issues.apache.org/jira/browse/KYLIN))。

### 路线图
- 支持 Hadoop 3.0（纠偏编码)
- 完全使用 Spark 的 Cube 引擎
- 接入更多的源（MySQL，Oracle，Spark SQL 等)
- 无需构建 Cube 的即席查询 
- 更好的存储引擎（Druid，Kudu，等)
- 支持实时数据分析的 Lambda 架构
