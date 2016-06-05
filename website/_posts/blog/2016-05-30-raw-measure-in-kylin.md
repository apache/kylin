---
layout: post-blog
title:  RAW measure in Apache Kylin
date:   2016-05-30 00:30:00
author: Xiaoyu Wang
categories: blog
---

## Introduction

 > `RAW` measure function is use to query the detail data on the measure column in Kylin.

Example data:

| DT         | SITE\_ID | SELLER\_ID | ITEM\_COUNT |
| :--------- | :------: | :--------: | :---------: |
| 2016-05-01 | 0        | SELLER-001 | 100         |
| 2016-05-01 | 0        | SELLER-002 | 200         |
| 2016-05-02 | 1        | SELLER-003 | 300         |
| 2016-05-02 | 1        | SELLER-004 | 400         |
| 2016-05-03 | 2        | SELLER-005 | 500         |

We design the cube desc is the `DT,SITE_ID` columns as dimensions, and `SUM(ITEM_COUNT)` as measure. So, the base cuboid data will like this:

| Rowkey of base cuboid | SUM(ITEM\_COUNT) |
| :-------------------- | :--------------: |
| 2016-05-01_0          | 300              |
| 2016-05-02_1          | 700              |
| 2016-05-03_2          | 500              |

For the first row in the base cuboid data, Kylin can extract the dimension column values `2016-05-01,0` from the HBase Rowkey, and in the measure cell will store the measure function's aggregated results `300`, we can't get the raw value `100` and `200` which before the aggregation on the `ITEM_COUNT` column.

The RAW function is use to make the SQL:

```
SELECT DT,SITE_ID,ITEM_COUNT FROM FACT_TABLE
```

to return the correct result:

| DT         | SITE\_ID | ITEM\_COUNT |
| :--------- | :------: | :---------: |
| 2016-05-01 | 0        | 100         |
| 2016-05-01 | 0        | 200         |
| 2016-05-02 | 1        | 300         |
| 2016-05-02 | 1        | 400         |
| 2016-05-03 | 2        | 500         |


## How to use

 - Choose the Kylin version `1.5.1+`.
 - Like the above case, we can make the `DT,SITE_ID` as dimensions, and `RAW(ITEM_COUNT)`as measure.
 - After the cube build, you can use the SQL to query the raw data:

```
SELECT DT,SITE_ID,ITEM_COUNT FROM FACT_TABLE WHERE SITE_ID = 0
```

## Optimize

The column which define `RAW` measure will be encoded with dictionary by default. So, you must know you data's cardinality and distribution characteristics.

 - As far as possible to define the value uniform distribution column to dimensions, this will make the measure cell value size more uniform and avoid data skew.
 - If choose the ultra high cardinality column to define `RAW` measure, you can try the following to avoid the dictionary build error:
   1. Cut a big segment into several segments, if you were trying to build a large data set at once;
   2. Set `kylin.dictionary.max.cardinality` in conf/kylin.properties to a bigger value (default is 5000000).


## To be improved

 - Now, the maximum storage 1M values of `RAW` measure in one cuboid. If exceed 1M values, it will throw `BufferOverflowException` in the cube build. This will be optimized in the later release.
 - Only dimension column can use in `WHERE` condition, `RAW` measure column is not support.


## Implement

 - Custom one aggregation function RAW implement, the function's return type depends on the column type.
 - Make the RAW aggregation function to save the column raw data in the base cuboid data.
 - The HBase value cell will store the dictionary id of the raw data to save space.
 - The SQL which contains the RAW measure column will be routed to the base cuboid query.
 - Extract the raw data from base cuboid data with dimension values to assemble into a complete row when query.
