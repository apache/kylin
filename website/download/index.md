---
layout: download
title: Download
permalink: /download/index.html
---

You can verify the download by following these [procedures](https://www.apache.org/info/verification.html) and using these [KEYS](https://www.apache.org/dist/kylin/KEYS).

#### v4.0.1
- This is a bug-fix release after Kylin 4.0.0, with 8 new features/improvements and 6 bug fixes. Check the release notes.
- [Release notes](/docs/release_notes.html), [installation guide](https://cwiki.apache.org/confluence/display/KYLIN/Installation+Guide) and [upgrade guide](https://cwiki.apache.org/confluence/display/KYLIN/How+to+upgrade)
- Source download: [apache-kylin-4.0.1-source-release.zip](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-4.0.1/apache-kylin-4.0.1-source-release.zip) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-4.0.1/apache-kylin-4.0.1-source-release.zip.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-4.0.1/apache-kylin-4.0.1-source-release.zip.sha256)\]
- Binary for the download (check this to see which binary you should choose [Hadoop Matrix supported](https://cwiki.apache.org/confluence/display/KYLIN/Support+Hadoop+Version+Matrix+of+Kylin+4)) :
  - for Apache Spark 2.4.7 [apache-kylin-4.0.1-bin-spark2.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-4.0.1/apache-kylin-4.0.1-bin-spark2.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-4.0.1/apache-kylin-4.0.1-bin-spark2.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-4.0.1/apache-kylin-4.0.1-bin-spark2.tar.gz.sha256)\] 
  - for Apache Spark 3.1.1 [apache-kylin-4.0.1-bin-spark3.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-4.0.1/apache-kylin-4.0.1-bin-spark3.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-4.0.1/apache-kylin-4.0.1-bin-spark3.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-4.0.1/apache-kylin-4.0.1-bin-spark3.tar.gz.sha256)\]

#### v3.1.3
- This is a bug-fix release after Kylin 3.1.1, with 10 new features/improvements and 14 bug fixes. Check the release notes.
- [Release notes](/docs/release_notes.html), [installation guide](/docs/install/index.html) and [upgrade guide](/docs/howto/howto_upgrade.html)
- Source download: [apache-kylin-3.1.3-source-release.zip](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-source-release.zip) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-source-release.zip.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-source-release.zip.sha256)\]
- Binary for Hadoop 2 download:
  - for HBase 1.x (includes HDP 2.3+, AWS EMR 5.0+, Azure HDInsight 3.4 - 3.6) - [apache-kylin-3.1.3-bin-hbase1x.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hbase1x.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hbase1x.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hbase1x.tar.gz.sha256)\]
  - for Cloudera CDH 5.7+ - [apache-kylin-3.1.3-bin-cdh57.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh57.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh57.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh57.tar.gz.sha256)\]

- Binary for Hadoop 3 download:
  - for Hadoop 3.1 + HBase 2.0 (includes Hortonworks HDP 3.0) - [apache-kylin-3.1.3-bin-hadoop3.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hadoop3.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hadoop3.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-hadoop3.tar.gz.sha256)\]
  - for Cloudera CDH 6.0/6.1 (check [KYLIN-3564](https://issues.apache.org/jira/browse/KYLIN-3564) first) - [apache-kylin-3.1.3-bin-cdh60.tar.gz](https://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh60.tar.gz) \[[asc](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh60.tar.gz.asc)\] \[[sha256](https://www.apache.org/dist/kylin/apache-kylin-3.1.3/apache-kylin-3.1.3-bin-cdh60.tar.gz.sha256)\]

#### JDBC Driver

Kylin JDBC Driver, `kylin-jdbc-<version>.jar`, is in the "lib" folder of the binary package.

#### ODBC Driver

Apache Kylin no longer provides the download for pre-built ODBC driver binary package. You can compile it from the source code (in the "odbc" sub-folder), or get from a vendor. Read this [announcement](http://apache-kylin.74782.x6.nabble.com/Kylin-ODBC-driver-is-removed-from-download-page-td12928.html).

#### Previous Release

Older releases can be found in [https://archive.apache.org/dist/kylin/](https://archive.apache.org/dist/kylin/).
