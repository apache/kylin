---
layout: post-blog
title:  Retention Or Conversion Rate Analyze in Apache Kylin
date:   2016-11-28 13:30:00
author: Yerui Sun 
categories: blog
---

Since v.1.6.0

## Background
Retention or conversion rate is important in data analysis. In general, the value can be calculated based on the intersection of two data sets (uuid etc.), with some same dimensions (city, category, etc.) and one variety dimension (date etc.).
Apache Kylin has support retention calculation based on the Bitmap and UDAF intersect_count. This article introduced how to use this feature.

## Usage
To use retention calculation in Apache Kylin, must meet requirements as below:
* Only one dimension can be variety
* The measure to be calculated have defined precisely count distinct measure

The intersect_count usage is described below:

```
intersect_count(columnToCount, columnToFilter, filterValueList)
`columnToCount` the columnt to cacluate and distinct count
`columnToFilter` the variety dimension
`filterValueList` the values of variety dimension, should be array
```

Here's some examples:

```
intersect_count(uuid, dt, array['20161014', '20161015'])
The precisely distinct count of uuids shows up both in 20161014 and 20161015

intersect_count(uuid, dt, array['20161014', '20161015', '20161016'])
The precisely distinct count of uuids shows up all in 20161014, 20161015 and 20161016

intersect_count(uuid, dt, array['20161014'])
The precisely distinct count of uuids shows up in 20161014, equivalent to `count(distinct uuid)`
```

A complete sql statement example:

```
select city, version,
intersect_count(uuid, dt, array['20161014']) as first_day,
intersect_count(uuid, dt, array['20161015']) as second_day,
intersect_count(uuid, dt, array['20161016']) as third_day,
intersect_count(uuid, dt, array['20161014', '20161015']) as retention_oneday,
intersect_count(uuid, dt, array['20161014', '20161015', '20161016']) as retention_twoday
from visit_log
where dt in ('2016104', '20161015', '20161016')
group by city, version
```

## Conclusions
Based on Bitmap and UDAF intersect_count, we can do fast and convenient retention analyze on Apache Kylin. Compared with the traditional way, SQL in Apache Kylin can be much more simple and clearly, and more efficient.

