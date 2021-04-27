---
layout: download
title: Download
permalink: /download/index.html
---

You can verify the download by following these [procedures](https://www.apache.org/info/verification.html) and using these [KEYS](https://www.apache.org/dist/kylin/KEYS).

#### v4.0.0-beta
- This is a major release after 4.0.0-alpha, with 25 new features/improvements and 14 bug fixes. Check the release notes.
- [Release notes](/docs/release_notes.html), [installation guide](https://cwiki.apache.org/confluence/display/KYLIN/Installation+Guide) and [upgrade guide](https://cwiki.apache.org/confluence/display/KYLIN/How+to+upgrade)
- Source download: [apache-kylin-4.0.0-beta-source-release.zip](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-source-release.zip) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-source-release.zip.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-source-release.zip.sha256)\]
- Binary for Apache Hadoop 2 and Hadoop 3 download (Please use the specified version Spark for Kylin 4.X, the version should be Apache Spark 2.4.6, not the Spark that provided by the environment):
  - [apache-kylin-4.0.0-beta-bin.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-bin.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-bin.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-4.0.0-beta/apache-kylin-4.0.0-beta-bin.tar.gz.sha256)\] (Verified on CDH 5.7, CDH 6.2, AWS EMR 5.31, AWS EMR 6.0.0, HDP 2.4, Hadoop 3 and EMR environments require additional configuration, please check the [installation guide](https://cwiki.apache.org/confluence/display/KYLIN/Installation+Guide))
  
#### v3.1.2
- This is a bug-fix release after 3.1.0, with 40 bug fixes and enhancement. Check the release notes.
- [Release notes](/docs/release_notes.html), [installation guide](/docs/install/index.html) and [upgrade guide](/docs/howto/howto_upgrade.html)
- Source download: [apache-kylin-3.1.2-source-release.zip](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-source-release.zip) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-source-release.zip.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-source-release.zip.sha256)\]
- Binary for Hadoop 2 download:
  - for HBase 1.x (includes HDP 2.3+, AWS EMR 5.0+, Azure HDInsight 3.4 - 3.6) - [apache-kylin-3.1.2-bin-hbase1x.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-hbase1x.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-hbase1x.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-hbase1x.tar.gz.sha256)\]
  - for Cloudera CDH 5.7+ - [apache-kylin-3.1.2-bin-cdh57.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-cdh57.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-cdh57.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-cdh57.tar.gz.sha256)\]

- Binary for Hadoop 3 download:
  - for Hadoop 3.1 + HBase 2.0 (includes Hortonworks HDP 3.0) - [apache-kylin-3.1.2-bin-hadoop3.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-hadoop3.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-hadoop3.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-hadoop3.tar.gz.sha256)\]
  - for Cloudera CDH 6.0/6.1 (check [KYLIN-3564](https://issues.apache.org/jira/browse/KYLIN-3564) first) - [apache-kylin-3.1.2-bin-cdh60.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-cdh60.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-cdh60.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.2/apache-kylin-3.1.2-bin-cdh60.tar.gz.sha256)\]

#### JDBC Driver

Kylin JDBC Driver, `kylin-jdbc-<version>.jar`, is in the "lib" folder of the binary package.

#### ODBC Driver

Apache Kylin no longer provides the download for pre-built ODBC driver binary package. You can compile it from the source code (in the "odbc" sub-folder), or get from a vendor. Read this [announcement](http://apache-kylin.74782.x6.nabble.com/Kylin-ODBC-driver-is-removed-from-download-page-td12928.html).

#### Previous Release

Older releases can be found in [https://archive.apache.org/dist/kylin/](https://archive.apache.org/dist/kylin/).
