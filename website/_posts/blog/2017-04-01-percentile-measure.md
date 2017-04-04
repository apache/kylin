---
layout: post-blog
title:  A new measure for Percentile precalculation
date:   2017-04-01 22:22:22
author: Dong Li
categories: blog
---

## Introduction

Since Apache Kylin 2.0, there's a new measure for percentile precalculation, which aims at (sub-)second latency for **approximate** percentile analytics SQL queries. The implementation is based on [t-digest](https://github.com/tdunning/t-digest) library under Apachee 2.0 license, which provides a high-effecient data structure to save aggregation counters and algorithm to calculate approximate result of percentile.

### Percentile
*From [wikipedia](https://en.wikipedia.org/wiki/Percentile)*: A **percentile** (or a **centile**) is a measure used in statistics indicating the value below which a given percentage of observations in a group of observations fall. For example, the 20th percentile is the value (or score) below which 20% of the observations may be found.

In Apache Kylin, we support the similar SQL sytanx like Apache Hive, with a aggregation function called **percentile(\<Number Column\>, \<Double\>)**:

```sql
SELECT seller_id, percentile(price, 0.5)
FROM test_kylin_fact
GROUP BY seller_id
```

### How to use
If you know little about *Cubes*, please go to [QuickStart](http://kylin.apache.org/docs20/tutorial/kylin_sample.html) first to learn basic knowledge.

Firstly, you need to add this column as measure in data model.

![](/images/blog/percentile_1.png)

Secondly, create a cube and add a PERCENTILE measure.

![](/images/blog/percentile_2.png)

Finally, build the cube and try some query.

![](/images/blog/percentile_3.png)