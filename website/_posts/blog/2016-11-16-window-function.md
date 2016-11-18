---
layout: post-blog
title:  Use Window Function and Grouping Sets in Apache Kylin
date:   2016-11-16 23:30:00
author: Yerui Sun 
categories: blog
---

Since v.1.5.4

## Background
We've provided window function and grouping sets in Apache Kylin, to support more complicate query, keeping SQL statements simple and clearly. Here's the article about HOW TO use them.

## Window Function
Window function give us the ability to partition, order and aggregate on the query result sets. We can use window function to meet complicated query requirements with much more simple SQL statements. 
The window function syntax of Apache Kylin can be found in [calcite reference](http://calcite.apache.org/docs/reference.html#window-functions), and is similar with [Hive window function](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+WindowingAndAnalytics).
Here's some examples:

```
sum(col) over()
count(col) over(partition by col2)
row_number() over(partition by col order by col2)
first_value(col) over(partition by col2 order by col3 rows 2 preceding)
lag(col, 5, 0) over(partition by col2, order by col3 range 3 preceding 6 following)
```

## Grouping Sets
Sometimes we want aggregate data by different keys in one SQL statements. This is possible with the grouping sets feature.
Here's example, suppose we execute one query and get result as below:

```
select dim1, dim2, sum(col) as metric1 from table group by dim1, dim2
```

| dim1 | dim2 | metric1 |
| :--- | :--: | :-----: |
| A    | AA   | 10      |
| A    | BB   | 20      |
| B    | AA   | 15      |
| B    | BB   | 30      |

If we also want to see the result with dim2 rolled up, we can rewrite the sql and get result as below:

```
select dim1,
case grouping(dim2) when 1 then 'ALL' else dim2 end,
sum(col) as metric1
from table
group by grouping sets((dim1, dim2), (dim1))
```

| dim1 | dim2 | metric1 |
| :--- | :--: | :-----: |
| A    | ALL  | 30      |
| A    | AA   | 10      |
| A    | BB   | 20      |
| B    | ALL  | 45      |
| B    | AA   | 15      |
| B    | BB   | 30      |

Apache Kylin support cube/rollup/grouping sets, and grouping functions can be found [here](http://calcite.apache.org/docs/reference.html#grouping-functions). It's also similar with [Hive grouping sets](https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup).

