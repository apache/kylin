---
title: Sum Expression
language: en
sidebar_label: Sum Expression
pagination_label: Sum Expression
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - sum expression
draft: true
last_update:
    date: 08/19/2022
---


Sum(expression) is a common usage in SQL and is often needed by various data analysis scenarios.

In the previous versions, table index or computed column is required for sum(expression) to work. Since v5, Kylin can answer some kind of sum(expression) using model.

### How to Use

This feature is off by default. To enable it, please set the configuration in `$KYLIN_HOME/conf/kylin.properties`.

```properties
kylin.query.convert-sum-expression-enabled=true
```

Currently, four kinds of sum (expression) usages are supported, namely

- sum(case when)
- sum(column*constant)
- sum(constant)
- sum(cast(case when))

We will use the sample dataset to introduce the usage. Read more about the [Sample Dataset](../../../quickstart/sample_dataset.md).



**sum(case when) function**

For example:

```sql
select
  sum(case when LO_ORDERPRIOTITY='1-URGENT' then LO_ORDTOTALPRICE else null end)
from SSB.LO_LINEORDER
```

In order to run this SQL, set your model as below in addition to enable sum(expression):

- Define all columns in the `when` clause as dimensions, like the `LO_ORDERPRIOTITY` in this example.
- Define all columns in the `then` clause as Sum measure, like the `sum(LO_ORDTOTALPRICE)` in this example.

Then, the model will be able to run the above SQL.



**sum(column*constant) function**

For example:

```sql
select sum(LO_ORDTOTALPRICE * 3) from SSB.LO_LINEORDER
```

In order to run this SQL, set your model as below in addition to enable sum(expression):

- Define the column in the `sum` function as Sum measure, like the `sum(LO_ORDTOTALPRICE)` in this example.

Then, the model will be able to run the above SQL.



**sum(constant) function**

For example:

```sql
select sum(3) from P_LINEORDER
```

In order to run this SQL, just enable the sum(expression) feature. No other setting on model is needed.

**sum(cast(case when)) function**

For example:

```sql
select sum(cast((case when LO_ORDERPRIOTITY='1-URGENT' then LO_ORDTOTALPRICE else null end) as bigint)) from SSB.P_LINEORDER
```

In order to run this SQL, set your model as below in addition to enable sum(expression):

- Define all columns in the `when` clause as dimensions, like the `ORDERPRIOTITY` in this example.
- Define all columns in the `then` clause as Sum measure, like the `sum(ORDTOTALPRICE)` in this example.

Then, the model will be able to run the above SQL.



### Known Limitation

1. Due to the complexity of `null` value, `sum(column+column)` and `sum(column+constant)` are not supported yet. If you need use the above syntax, please use computed column or table index.
2. In the current version, `topN`  is not supported to use together with `sum(case when)`. `count(distinct)`, `collect_set`, `percentile` can be used with `sum(case when)`，but they can not be answered by single index.
