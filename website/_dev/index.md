---
layout: dev
title: Development Quick Guide
permalink: /development/index.html
---

Apache Kylin is always looking for contributions of not only code, but also usage document, performance report, Q&A etc. All kinds of contributions pave the way towards a Kylin Committer. There is opportunity for everyone, especially for those come from analysis and solution background, due to the lacking of content from user and solution perspective.

Here is the development document for Apache kylin 3.x and earlier version. Check the development documents of other versions:
* [v4.x development documents](/development40/) 

### How to Contribute
Check out the [How to Contribute](/development/howto_contribute.html) document.

### Source Repository
Apache Kylin™ source code is version controlled using Git version control:
Commits [Summary](https://github.com/apache/kylin/commits/main)
Source Repo: [https://github.com/apache/kylin ](https://github.com/apache/kylin )  
Mirrored to Gitbox: [https://gitbox.apache.org/repos/asf?p=kylin.git ](https://gitbox.apache.org/repos/asf?p=kylin.git )

### CI and Code Analysis

Apache Kylin uses [GitHub Actions](https://github.com/apache/kylin/actions) to run the unit tests on Linux x86_64, and [TravisCI](https://app.travis-ci.com/github/apache/kylin) to test on Linux ARM64 and x86_64 architectures with JDK 1.8.

Static Code Analysis: [SonarCube dashboard](https://builds.apache.org/analysis/overview?id=org.apache.kylin%3Akylin). Currently disabled!

[![Build Status](https://travis-ci.org/apache/kylin.svg?branch=master)](https://travis-ci.org/apache/kylin)[![Codacy Badge](https://api.codacy.com/project/badge/Grade/74f0139786cd4e8a8ce69bb0c17c2e71)](https://www.codacy.com/app/kyligence-git/kylin?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=apache/kylin&amp;utm_campaign=Badge_Grade)
[![Quality Gate](https://camo.githubusercontent.com/a9947cd56fb6e99807644f46830a35c1c4d4555e/68747470733a2f2f736f6e6172636c6f75642e696f2f6170692f70726f6a6563745f6261646765732f7175616c6974795f676174653f70726f6a6563743d6f72672e6170616368652e6b796c696e2533416b796c696e)](https://sonarcloud.io/dashboard/index/org.apache.kylin%3Akylin)[![SonarCloud Coverage](https://camo.githubusercontent.com/576fc0211eeafae5dd250ceaff1cf81135aae71a/68747470733a2f2f736f6e6172636c6f75642e696f2f6170692f70726f6a6563745f6261646765732f6d6561737572653f70726f6a6563743d6f72672e6170616368652e6b796c696e2533416b796c696e266d65747269633d636f766572616765)](https://sonarcloud.io/component_measures/metric/coverage/list?id=org.apache.kylin%3Akylin)[![SonarCloud Bugs](https://camo.githubusercontent.com/ce363d0c5f82c2ddc4bb5c2db4e0365354efb2cf/68747470733a2f2f736f6e6172636c6f75642e696f2f6170692f70726f6a6563745f6261646765732f6d6561737572653f70726f6a6563743d6f72672e6170616368652e6b796c696e2533416b796c696e266d65747269633d62756773)](https://sonarcloud.io/component_measures/metric/reliability_rating/list?id=org.apache.kylin%3Akylin)[![SonarCloud Vulnerabilities](https://camo.githubusercontent.com/24932e44a729982c780a9b230428f30d909657e3/68747470733a2f2f736f6e6172636c6f75642e696f2f6170692f70726f6a6563745f6261646765732f6d6561737572653f70726f6a6563743d6f72672e6170616368652e6b796c696e2533416b796c696e266d65747269633d76756c6e65726162696c6974696573)](https://sonarcloud.io/component_measures/metric/security_rating/list?id=org.apache.kylin%3Akylin)

### Issue Tracking  
Track issues on the "Kylin" Project on the Apache JIRA ([browse](http://issues.apache.org/jira/browse/KYLIN)).

### Roadmap
- Hadoop 3.0 support (Erasure Coding) : DONE (v2.5)
- Fully on Spark Cube engine : DONE (v2.5)
- Real-time analytics with Lambda Architecture : DONE (v3.0)
- Connect more data sources (MySQL, SparkSQL, etc) : DONE (v2.6)
- Flink engine : Done (v3.1)
- Cloud-native storage (Parquet) : In-progress (v4.0)
- Distributed query execution engine (Spark) : In-progress, together with Parquet storage (v4.0)
- Containerization/Kubernetes support : Done (v3.1)
- Pushdown SDK with more engines (Presto, Clickhouse, etc) : In progress (Presto support in v3.1)
- Ad-hoc queries without Cubing
