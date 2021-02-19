---
layout: download-cn
title: 下载
---

您可以按照这些[步骤](https://www.apache.org/info/verification.html) 并使用这些[KEYS](https://www.apache.org/dist/kylin/KEYS)来验证下载文件的有效性.

#### v4.0.0-beta
- 这是 4.0.0-alpha 版本后的一个主要版本，包含25个新功能以及改进和14个问题的修复。关于具体内容请查看发布说明。
- [发布说明](/docs/release_notes.html), [安装指南](https://cwiki.apache.org/confluence/display/KYLIN/Installation+Guide) and [升级指南](https://cwiki.apache.org/confluence/display/KYLIN/How+to+upgrade)
- 源码下载: [apache-kylin-4.0.0-beta-source-release.zip](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-source-release.zip) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-source-release.zip.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-source-release.zip.sha256)\]
- Hadoop 2 和 Hadoop 3 二进制包 (请为 Kylin 4.X 使用指定版本的 Spark，版本为 Apache Spark 2.4.6, 而不是环境自带的 Spark):
  - [apache-kylin-4.0.0-beta-bin.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-bin.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-bin.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-bin.tar.gz.sha256)\] (已经在 CDH 5.7, CDH 6.2, AWS EMR 5.31, AWS EMR 6.0.0, HDP 2.4 环境下验证, Hadoop3 和 EMR 环境需做额外配置，请[查看安装指南](https://cwiki.apache.org/confluence/display/KYLIN/Installation+Guide))

#### v3.1.1
- 这是 3.1.0 版本后的一个bug-fix版本，包含58个问题的修复以及各种改进。关于具体内容请查看发布说明。
- [发布说明](/docs/release_notes.html), [安装指南](/docs/install/index.html) and [升级指南](/docs/howto/howto_upgrade.html)
- 源码下载: [apache-kylin-3.1.1-source-release.zip](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-source-release.zip) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-source-release.zip.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-source-release.zip.sha256)\]
- Hadoop 2 二进制包:
  - for HBase 1.x (includes HDP 2.3+, AWS EMR 5.0+, Azure HDInsight 3.4 - 3.6) - [apache-kylin-3.1.1-bin-hbase1x.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-hbase1x.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-hbase1x.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-hbase1x.tar.gz.sha256)\]
  - for Cloudera CDH 5.7+ - [apache-kylin-3.1.1-bin-cdh57.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-cdh57.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-cdh57.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-cdh57.tar.gz.sha256)\]
- Hadoop 3 二进制包:
  - for Hadoop 3.1 + HBase 2.0 (includes Hortonworks HDP 3.0) - [apache-kylin-3.1.1-bin-hadoop3.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-hadoop3.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-hadoop3.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-hadoop3.tar.gz.sha256)\]
  - for Cloudera CDH 6.0/6.1 (check [KYLIN-3564](https://issues.apache.org/jira/browse/KYLIN-3564) first) - [apache-kylin-3.1.1-bin-cdh60.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-cdh60.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-cdh60.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.1/apache-kylin-3.1.1-bin-cdh60.tar.gz.sha256)\]

#### JDBC 驱动程序

Kylin JDBC 驱动程序, `kylin-jdbc-<version>.jar`, 在 Kylin 二进制包的 "lib" 目录下.

#### ODBC 驱动程序

Apache Kylin 不再提供预先构建的 ODBC 驱动程序。你可以自行从源代码来编译构建 (在 "odbc" 子目录)，或者从第三方厂商获得。请阅读此[通知](http://apache-kylin.74782.x6.nabble.com/Kylin-ODBC-driver-is-removed-from-download-page-td12928.html)。

#### 以前的版本  
Apache Kylin的旧版本可以从 [https://archive.apache.org/dist/kylin/](https://archive.apache.org/dist/kylin/) 下载。
