---
layout: download-cn
title: 下载
---

__最新发布(源代码)__  
最新发布的Apache Kylin™可以从ASF网站下载：:

* [Apache Kylin v1.3.0](http://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-1.3.0/)
* [发布日志](http://kylin.apache.org/docs/release_notes.html)
* Git 标签: [kylin-1.3.0](https://github.com/apache/kylin/tree/kylin-1.3.0)
* Git Commit: [b95e47c4dde42ec752916013f67ed1221f092cb7](https://github.com/apache/kylin/commit/b95e47c4dde42ec752916013f67ed1221f092cb7)

__二进制包 for HBase 0.98/0.99__
为方便使用，我们提供预打包的二进制安装包：

  * [apache-kylin-1.3.0-bin.tar.gz](https://dist.apache.org/repos/dist/release/kylin/apache-kylin-1.3.0/apache-kylin-1.3.0-bin.tar.gz)
  * [安装帮助](http://kylin.apache.org/docs/install)

__1.2发布(源代码)__  
1.2发布的Apache Kylin™可以从ASF网站下载：:

* [Apache Kylin v1.2](http://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-1.2/)
* [发布日志](http://kylin.apache.org/docs/release_notes.html)
* Git 标签: [kylin-1.2](https://github.com/apache/kylin/tree/kylin-1.2)
* Git Commit: [c2589aee4ac5537b460b3b02fa89cdb3a922d64e](https://github.com/apache/kylin/commit/c2589aee4ac5537b460b3b02fa89cdb3a922d64e)

__1.2二进制包 for HBase 0.98/0.99__
为方便使用，我们提供预打包的二进制安装包：

  * [apache-kylin-1.2-bin.tar.gz](https://dist.apache.org/repos/dist/release/kylin/apache-kylin-1.2/apache-kylin-1.2-bin.tar.gz)
  * [安装帮助](http://kylin.apache.org/docs/install)

__特别二进制包 for HBase 1.1.3及更高版本__
越来越多的用户开始部署使用HBase 1.1或更高版本，我们提供一个在HBase 1.1上编译的Kylin快照二进制包；
请注意此安装包需要HBase 1.1.3或更高版本，之前版本中有一个已知的关于fuzzy key过滤器的缺陷，会导致Kylin查询结果缺少记录: [HBASE-14269](https://issues.apache.org/jira/browse/HBASE-14269)
此外请注意，这不是一个正式的发布版(每隔几周rebase KYLIN 1.3.x 分支上最新的改动)，没有经过完整的测试。

  * [apache-kylin-1.3-HBase-1.1-SNAPSHOT-bin.tar.gz](https://dist.apache.org/repos/dist/dev/kylin/apache-kylin-1.3-snapshot/apache-kylin-1.3-HBase-1.1-SNAPSHOT-bin.tar.gz)
  * Git commit [a3b8eb04955310abec158ea30f61deb0119679d1](https://github.com/apache/kylin/commit/a3b8eb04955310abec158ea30f61deb0119679d1) 

对于HBase 1.0用户，建议您升级到1.1.3或降级到0.98/0.99.

__构建二进制包__
可以从各个版本或当前最新的开发分支中生成二进制包，请参考这篇[帮助文档](https://kylin.apache.org/development/howto_package.html)

__以前的版本__
Apache Kylin的旧版本可以从[归档](https://archive.apache.org/dist/kylin/)中下载。

__ODBC 驱动__  
Kylin ODBC 驱动要求首先安装[Microsoft Visual C++ 2012 Redistributable]()。 
Kylin ODBC 驱动可以从这里下载:

  * [Kylin ODBC 驱动 v1.2](http://kylin.apache.org/download/KylinODBCDriver-1.2.zip)
