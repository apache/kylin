---
layout: dev
title: Development Quick Guide
permalink: /development/index.html
---

Apache Kylin is always looking for contributions of not only code, but also usage document, performance report, Q&A etc. All kinds of contributions pave the way towards a Kylin Committer. There is opportunity for everyone, especially for those come from analysis and solution background, due to the lacking of content from user and solution perspective.

### How to Contribute
Check out the [How to Contribute](/development/howto_contribute.html) document.

### Source Repository
Apache Kylin™ source code is version controlled using Git version control:
Commits [Summary](https://github.com/apache/kylin/commits/master)  
Source Repo: [https://github.com/apache/kylin ](https://github.com/apache/kylin )  
Mirrored to Gitbox: [https://gitbox.apache.org/repos/asf?p=kylin.git ](https://gitbox.apache.org/repos/asf?p=kylin.git )

### CI and Code Analysis
UT on master branch with JDK 1.7(deprecated): [Kylin-Master-JDK-1.7](https://builds.apache.org/job/Kylin-Master-JDK-1.7/) 
UT on master branch with JDK 1.8: [Kylin-Master-JDK-1.8](https://builds.apache.org/job/Kylin-Master-JDK-1.8/) 
Static Code Analysis: [SonarCube dashboard](https://builds.apache.org/analysis/overview?id=org.apache.kylin%3Akylin)

[![Build Status](https://travis-ci.org/apache/kylin.svg?branch=master)](https://travis-ci.org/apache/kylin)[![Codacy Badge](https://api.codacy.com/project/badge/Grade/74f0139786cd4e8a8ce69bb0c17c2e71)](https://www.codacy.com/app/kyligence-git/kylin?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=apache/kylin&amp;utm_campaign=Badge_Grade)
[![Quality Gate](https://sonarcloud.io/api/badges/gate?key=org.apache.kylin%3Akylin)](https://sonarcloud.io/dashboard/index/org.apache.kylin%3Akylin)[![SonarCloud Coverage](https://sonarcloud.io/api/badges/measure?key=org.apache.kylin%3Akylin&metric=coverage)](https://sonarcloud.io/component_measures/metric/coverage/list?id=org.apache.kylin%3Akylin)[![SonarCloud Bugs](https://sonarcloud.io/api/badges/measure?key=org.apache.kylin%3Akylin&metric=bugs)](https://sonarcloud.io/component_measures/metric/reliability_rating/list?id=org.apache.kylin%3Akylin)[![SonarCloud Vulnerabilities](https://sonarcloud.io/api/badges/measure?key=org.apache.kylin%3Akylin&metric=vulnerabilities)](https://sonarcloud.io/component_measures/metric/security_rating/list?id=org.apache.kylin%3Akylin)

### Issue Tracking  
Track issues on the "Kylin" Project on the Apache JIRA ([browse](http://issues.apache.org/jira/browse/KYLIN)).

### Roadmap
- Hadoop 3.0 support (Erasure Coding)
- Fully on Spark Cube engine
- Connect more data sources (MySQL, Oracle, SparkSQL, etc)
- Ad-hoc queries without Cubing
- Better storage (Druid, Kudu, etc)
- Real-time analytics with Lambda Architecture
