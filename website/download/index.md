---
layout: download
title: Download
permalink: /download/index.html
---

__Latest Release(Source Code)__  
The latest release of Apache Kylin™ can be downloaded from the ASF:

* [Apache Kylin v1.3.0](http://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-1.3.0/)
* [Release Notes](http://kylin.apache.org/docs/release_notes.html)
* Git Tag: [kylin-1.3.0](https://github.com/apache/kylin/tree/kylin-1.3.0)
* Git Commit: [b95e47c4dde42ec752916013f67ed1221f092cb7](https://github.com/apache/kylin/commit/b95e47c4dde42ec752916013f67ed1221f092cb7)

__Binary Package (for running on HBase 0.98/0.99)__
For convenience, there’s binary package also available: 

* [apache-kylin-1.3.0-bin.tar.gz](https://dist.apache.org/repos/dist/release/kylin/apache-kylin-1.3.0/apache-kylin-1.3.0-bin.tar.gz)
* [Installation Guide](http://kylin.apache.org/docs/install)

__1.2 Release(Source Code)__  
The 1.2 release of Apache Kylin™ can be downloaded from the ASF:

* [Apache Kylin v1.2](http://www.apache.org/dyn/closer.cgi/kylin/apache-kylin-1.2/)
* [Release Notes](http://kylin.apache.org/docs/release_notes.html)
* Git Tag: [kylin-1.2](https://github.com/apache/kylin/tree/kylin-1.2)
* Git Commit: [c2589aee4ac5537b460b3b02fa89cdb3a922d64e](https://github.com/apache/kylin/commit/c2589aee4ac5537b460b3b02fa89cdb3a922d64e)

__1.2 Release Binary Package (for running on HBase 0.98/0.99)__
For convenience, there’s binary package also available: 

* [apache-kylin-1.2-bin.tar.gz](https://dist.apache.org/repos/dist/release/kylin/apache-kylin-1.2/apache-kylin-1.2-bin.tar.gz)
* [Installation Guide](http://kylin.apache.org/docs/install)

__Special Binary Package (for running on HBase 1.1.3 or above)__
As there are more and more HBase 1.1 deployments, an binary snapshot build for HBase 1.1.3+ is provided; 
Note the requirement of HBase version 1.1.3 (or above). There is a known bug in HBase earlier versions about fuzzy key filter that will cause
missing rows or lesser aggregations in Kylin query result: [HBASE-14269](https://issues.apache.org/jira/browse/HBASE-14269)
Also, please aware this is not a formal release (rebasing latest changings on KYLIN 1.3.x branch every couple of weeks), and it is not fully tested:

* [apache-kylin-1.3-HBase-1.1-SNAPSHOT-bin.tar.gz](https://dist.apache.org/repos/dist/dev/kylin/apache-kylin-1.3-snapshot/apache-kylin-1.3-HBase-1.1-SNAPSHOT-bin.tar.gz)
* Git commit [a3b8eb04955310abec158ea30f61deb0119679d1](https://github.com/apache/kylin/commit/a3b8eb04955310abec158ea30f61deb0119679d1) 

If you're using HBase 1.0, we suggest you to upgrade to 1.1.3+ or downgrade to 0.98/0.99.

__Build Binary Package__
To build binary package from any version even latest development branch, please refer to this [guide](https://kylin.apache.org/development/howto_package.html)

__Previous Release__  
 Older releases may be found in the [archives](https://archive.apache.org/dist/kylin/).
    
__ODBC Driver__  
Kylin ODBC driver requires [Microsoft Visual C++ 2012 Redistributable](http://www.microsoft.com/en-us/download/details.aspx?id=30679) installed first. 
And Kylin ODBC Driver could be downloaded here: 

* [Kylin ODBC Driver v1.2](http://kylin.apache.org/download/KylinODBCDriver-1.2.zip)


