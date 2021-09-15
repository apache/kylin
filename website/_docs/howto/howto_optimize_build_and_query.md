---
layout: docs
title:  Optimize Build and Query
categories: Optimize Build and Query
permalink: /docs/howto/howto_optimize_build_and_query.html
since: v4.0.0
---

Kylin 4 is a major architecture upgrade version, both cube building engine and query engine use spark as calculation engine, and cube data is stored in parquet files instead of HBase.So the build/query performance tuning is very different from [Kylin 3 tuning](http://kylin.apache.org/docs/howto/howto_optimize_build.html). 

About the build/query performance tuning of Apache Kylin4.0, Please refer to: 
[How to improve cube building and query performance of Apache Kylin4.0](https://cwiki.apache.org/confluence/display/KYLIN/How+to+improve+cube+building+and+query+performance).

At the same time, you can refer to kylin4.0 user's optimization practice blog:
[why did Youzan choose Kylin4](/blog/2021/06/17/Why-did-Youzan-choose-Kylin4/)