---
layout: download
title: Download
permalink: /download/index.html
---

__Latest Release(Source Code)__  
The latest release of Apache Kylin can be downloaded from the ASF:

* [Apache Kylin v1.1.1-incubating](http://www.apache.org/dyn/closer.cgi/incubator/kylin/apache-kylin-1.1.1-incubating/)
* [Release Notes](http://kylin.incubator.apache.org/docs/release_notes.html)
* Git Tag: [kylin-1.1.1-incubating](https://github.com/apache/incubator-kylin/tree/kylin-1.1.1-incubating)
* Git Commit: [6a9499b4d0fabb54211a8a536c2e18d3fe8b4a5d](https://github.com/apache/incubator-kylin/commit/6a9499b4d0fabb54211a8a536c2e18d3fe8b4a5d)

__Binary Package (for running on HBase 0.98/0.99)__
For convenience, there’s binary package also available: 

* [apache-kylin-1.1.1-incubating-bin.tar.gz](https://dist.apache.org/repos/dist/release/incubator/kylin/apache-kylin-1.1.1-incubating/apache-kylin-1.1.1-incubating-bin.tar.gz)
* [Installation Guide](http://kylin.incubator.apache.org/docs/install)

__Binary Package (for running on HBase 1.1.3 or above)__
As there are more and more HBase 1.1 deployments, an binary snapshot build for HBase 1.1.3+ is provided; 
Note the requirement of HBase version 1.1.3 (or above). There is a known bug in HBase earlier versions about fuzzy key filter that will cause
missing rows or lesser aggregations in Kylin query result: [HBASE-14269](https://issues.apache.org/jira/browse/HBASE-14269)
Also, please aware this is not a formal release, and it is not fully tested:

* [apache-kylin-1.2-HBase1.1-incubating-SNAPSHOT-bin.tar.gz](https://dist.apache.org/repos/dist/dev/incubator/kylin/apache-kylin-1.2-incubating-snapshot/apache-kylin-1.2-HBase1.1-incubating-SNAPSHOT-bin.tar.gz)
* Git commit [3623dd6ff93d76141bb6a5fb623a3421ae78ca93](https://github.com/apache/incubator-kylin/commit/3623dd6ff93d76141bb6a5fb623a3421ae78ca93) 

If you're using HBase 1.0, we suggest you to upgrade to 1.1.3+ or downgrade to 0.98/0.99.

__Build Binary Package__
To build binary package from any version even latest development branch, please refer to this [guide](https://kylin.incubator.apache.org/development/howto_package.html)

__Previous Release__  
 Older releases may be found in the [archives](https://dist.apache.org/repos/dist/release/incubator/kylin/).
    
__ODBC Driver__  
Kylin ODBC driver requires [Microsoft Visual C++ 2012 Redistributable](http://www.microsoft.com/en-us/download/details.aspx?id=30679) installed first. 
And Kylin ODBC Driver could be downloaded here: 

* [Kylin ODBC Driver](http://kylin.incubator.apache.org/download/KylinODBCDriver.zip)


