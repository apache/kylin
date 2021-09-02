Apache Kylin
============

[![Build Status](https://travis-ci.org/apache/kylin.svg?branch=master)](https://travis-ci.org/apache/kylin)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Coverage Status](https://coveralls.io/repos/github/apache/kylin/badge.svg?branch=master)](https://coveralls.io/github/apache/kylin?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/74f0139786cd4e8a8ce69bb0c17c2e71)](https://www.codacy.com/app/kyligence-git/kylin?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=apache/kylin&amp;utm_campaign=Badge_Grade)
[![Quality Gate](https://sonarcloud.io/api/project_badges/quality_gate?project=org.apache.kylin%3Akylin)](https://sonarcloud.io/dashboard/index/org.apache.kylin%3Akylin)
[![SonarCloud Coverage](https://sonarcloud.io/api/project_badges/measure?project=org.apache.kylin%3Akylin&metric=coverage)](https://sonarcloud.io/component_measures/metric/coverage/list?id=org.apache.kylin%3Akylin)
[![SonarCloud Bugs](https://sonarcloud.io/api/project_badges/measure?project=org.apache.kylin%3Akylin&metric=bugs)](https://sonarcloud.io/component_measures/metric/reliability_rating/list?id=org.apache.kylin%3Akylin)
[![SonarCloud Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=org.apache.kylin%3Akylin&metric=vulnerabilities)](https://sonarcloud.io/component_measures/metric/security_rating/list?id=org.apache.kylin%3Akylin)

> Extreme OLAP Engine for Big Data

Apache Kylin is an open source Distributed Analytics Engine, contributed by eBay Inc., it provides a SQL interface and multi-dimensional analysis (OLAP) on Hadoop with support for extremely large datasets.

For more details, see the website [http://kylin.apache.org](http://kylin.apache.org), Chinese version:[http://kylin.apache.org/cn/](http://kylin.apache.org/cn/).

Develop
=============
Please refer to [https://github.com/Kyligence/kylin-on-parquet-v2/wiki/Development-document](https://github.com/Kyligence/kylin-on-parquet-v2/wiki/Development-document).

Get started with Kylin in 5 minutes with Docker
=============
In order to allow users to try Kylin easily, we provide a docker image for Kylin.

Just run the following commands in your terminal. After 3~5 mins, you can access Kylin WebUI http://127.0.0.1:7070/kylin/login in your browser.

1. pull docker image
```shell
docker pull apachekylin/apache-kylin-standalone:4.0.0
```

2. start the container
```shell
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 2181:2181 \
--name kylin-4.0.0 \
apachekylin/apache-kylin-standalone:4.0.0
```

You can learn more about this docker image on Kylin's [website](http://kylin.apache.org/docs40/install/kylin_docker.html).



Documentation
=============
Please refer to [http://kylin.apache.org/docs40/](http://kylin.apache.org/docs40/).

Get Help
============
The fastest way to get response from our developers is to send an email to our mail list <dev@kylin.apache.org>,   
and remember to subscribe our mail list via <dev-subscribe@kylin.apache.org>

License
============
Please refer to [LICENSE](https://github.com/apache/kylin/blob/master/LICENSE) file.





