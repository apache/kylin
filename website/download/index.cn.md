---
layout: download-cn
title: 下载
---

您可以按照这些[步骤](https://www.apache.org/info/verification.html) 并使用这些[KEYS](https://www.apache.org/dist/kylin/KEYS)来验证下载文件的有效性.

#### v4.0.3
- 这是 Kylin 4.0.2 版本之后的一个 bug-fix 版本，包含 4 个新功能和改进，以及 4 个问题的修复。关于具体内容请查看发布说明。
- [发布说明](/docs/release_notes.html), [安装指南](https://cwiki.apache.org/confluence/display/KYLIN/Installation+Guide) and [升级指南](/docs/howto/howto_upgrade.html)
- 源码下载: [apache-kylin-4.0.3-source-release.zip](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-4.0.3/apache-kylin-4.0.3-source-release.zip) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-4.0.3/apache-kylin-4.0.3-source-release.zip.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-4.0.3/apache-kylin-4.0.3-source-release.zip.sha256)\]
- 二进制包 (选择二进制包前请检查文档 [Hadoop 支持矩阵](https://cwiki.apache.org/confluence/display/KYLIN/Support+Hadoop+Version+Matrix+of+Kylin+4)):
  - for Apache Spark 3.1.x [apache-kylin-4.0.3-bin-spark3.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-4.0.3/apache-kylin-4.0.3-bin-spark3.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-4.0.3/apache-kylin-4.0.3-bin-spark3.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-4.0.3/apache-kylin-4.0.3-bin-spark3.tar.gz.sha256)\]

#### v3.1.3
- 这是 Kylin 3.1.2 版本后的一个 bug-fix 版本，包含 10 个新功能和改进，以及 14 个问题的修复。关于具体内容请查看发布说明。
- [发布说明](/docs31/release_notes.html), [安装指南](/docs31/install/index.html) and [升级指南](/docs31/howto/howto_upgrade.html)
- 源码下载: [apache-kylin-3.1.3-source-release.zip](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-source-release.zip) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-source-release.zip.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-source-release.zip.sha256)\]
- Hadoop 2 二进制包:
  - for HBase 1.x (includes HDP 2.3+, AWS EMR 5.0+, Azure HDInsight 3.4 - 3.6) - [apache-kylin-3.1.3-bin-hbase1x.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hbase1x.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hbase1x.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hbase1x.tar.gz.sha256)\]
  - for Cloudera CDH 5.7+ - [apache-kylin-3.1.3-bin-cdh57.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh57.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh57.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh57.tar.gz.sha256)\]
- Hadoop 3 二进制包:
  - for Hadoop 3.1 + HBase 2.0 (includes Hortonworks HDP 3.0) - [apache-kylin-3.1.3-bin-hadoop3.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hadoop3.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hadoop3.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hadoop3.tar.gz.sha256)\]
  - for Cloudera CDH 6.0/6.1 (check [KYLIN-3564](https://issues.apache.org/jira/browse/KYLIN-3564) first) - [apache-kylin-3.1.3-bin-cdh60.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh60.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh60.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh60.tar.gz.sha256)\]

#### JDBC 驱动程序

Kylin JDBC 驱动程序, `kylin-jdbc-<version>.jar`, 在 Kylin 二进制包的 "lib" 目录下.

#### ODBC 驱动程序

Apache Kylin 不再提供预先构建的 ODBC 驱动程序。你可以自行从源代码来编译构建 (在 "odbc" 子目录)，或者从第三方厂商获得。请阅读此[通知](http://apache-kylin.74782.x6.nabble.com/Kylin-ODBC-driver-is-removed-from-download-page-td12928.html)。

#### 以前的版本  
Apache Kylin的旧版本可以从 [https://archive.apache.org/dist/kylin/](https://archive.apache.org/dist/kylin/) 下载。
