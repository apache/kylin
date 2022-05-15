Apache Kylin
============

[![Build Status](https://travis-ci.org/apache/kylin.svg?branch=master)](https://travis-ci.org/apache/kylin)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Coverage Status](https://coveralls.io/repos/github/apache/kylin/badge.svg?branch=main)](https://coveralls.io/github/apache/kylin?branch=main)

> Extreme OLAP Engine for Big Data

Apache Kylin is an open source Distributed Analytics Engine, contributed by eBay Inc., it provides a SQL interface and multi-dimensional analysis (OLAP) on Hadoop with support for extremely large datasets.

For more details, see the website [http://kylin.apache.org](http://kylin.apache.org), Chinese version:[http://kylin.apache.org/cn/](http://kylin.apache.org/cn/).

Develop
=============
Please refer to [https://github.com/Kyligence/kylin-on-parquet-v2/wiki/Development-document](https://github.com/Kyligence/kylin-on-parquet-v2/wiki/Development-document).

Get started with Kylin in 5 minutes with Docker
=============
In order to allow users to try Kylin easily, we provide a docker image for Kylin.

Just run the following commands in your terminal. After 3~5 mins, you can access Kylin WebUI http://127.0.0.1:7070/kylin/login in your browser with ADMIN/KYLIN.

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





