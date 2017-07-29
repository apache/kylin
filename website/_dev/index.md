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
Commits [Summary](https://git-wip-us.apache.org/repos/asf?p=kylin.git;a=summary)  
Source Repo: [git://git.apache.org/kylin.git](git://git.apache.org/kylin.git)  
Mirrored to Github: [https://github.com/apache/kylin](https://github.com/apache/kylin)

### CI and Code Analysis
UT on master branch with JDK 1.7: [Kylin-Master-JDK-1.7](https://builds.apache.org/job/Kylin-Master-JDK-1.7/) 
UT on master branch with JDK 1.8: [Kylin-Master-JDK-1.8](https://builds.apache.org/job/Kylin-Master-JDK-1.8/) 
Static Code Analysis: [SonarCube dashboard](https://builds.apache.org/analysis/overview?id=org.apache.kylin%3Akylin)

### Issue Tracking  
Track issues on the "Kylin" Project on the Apache JIRA ([browse](http://issues.apache.org/jira/browse/KYLIN)).

### Roadmap
- Hadoop 3.0 support (Erasure Coding)
- Spark cubing enhancement
- Connect more sources (JDBC, SparkSQL)
- Ad-hoc queries without cubing
- Better storage (Kudu?)
- Real-time analytics with Lambda Architecture
