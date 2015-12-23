---
layout: download-cn
title: 下载
---

__最新发布(源代码)__  
最新发布的Apache Kylin™可以从ASF网站下载：:

* [Apache Kylin v1.2](http://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-1.2/)
* [发布日志](http://kylin.apache.org/docs/release_notes.html)
* Git 标签: [kylin-1.2](https://github.com/apache/kylin/tree/kylin-1.2)
* Git Commit: [c2589aee4ac5537b460b3b02fa89cdb3a922d64e](https://github.com/apache/kylin/commit/c2589aee4ac5537b460b3b02fa89cdb3a922d64e)

__二进制包 for HBase 0.98/0.99__
为方便使用，我们提供预打包的二进制安装包：

  * [apache-kylin-1.2-bin.tar.gz](https://dist.apache.org/repos/dist/release/kylin/apache-kylin-1.2/apache-kylin-1.2-bin.tar.gz)
  * [安装帮助](http://kylin.apache.org/docs/install)

__二进制包 for HBase 1.1.3及更高版本__
越来越多的用户开始部署使用HBase 1.1或更高版本，我们提供一个在HBase 1.1上编译的Kylin快照二进制包；
请注意此安装包需要HBase 1.1.3或更高版本，之前版本中有一个已知的关于fuzzy key过滤器的缺陷，会导致Kylin查询结果缺少记录: [HBASE-14269](https://issues.apache.org/jira/browse/HBASE-14269)
此外请注意，这不是一个正式的发布版，没有经过完整的测试。

  * [apache-kylin-1.2-HBase1.1-incubating-SNAPSHOT-bin.tar.gz](https://dist.apache.org/repos/dist/dev/kylin/apache-kylin-1.2-incubating-snapshot/apache-kylin-1.2-HBase1.1-incubating-SNAPSHOT-bin.tar.gz)
  * Git commit [3623dd6ff93d76141bb6a5fb623a3421ae78ca93](https://github.com/apache/kylin/commit/3623dd6ff93d76141bb6a5fb623a3421ae78ca93)

对于HBase 1.0用户，建议您升级到1.1.3或降级到0.98/0.99.

__构建二进制包__
可以从各个版本或当前最新的开发分支中生成二进制包，请参考这篇[帮助文档](https://kylin.apache.org/development/howto_package.html)

__以前的版本__
Apache Kylin的旧版本可以从[归档](https://archive.apache.org/dist/kylin/)中下载。

__ODBC 驱动__  
Kylin ODBC 驱动要求首先安装[Microsoft Visual C++ 2012 Redistributable]()。 
Kylin ODBC 驱动可以从这里下载:

  * [Kylin ODBC 驱动](http://kylin.apache.org/download/KylinODBCDriver.zip)
