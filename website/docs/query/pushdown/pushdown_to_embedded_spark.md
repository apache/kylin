---
title: Pushdown to Embedded SparkSQL
language: en
sidebar_label: Pushdown to Embedded SparkSQL
pagination_label: Pushdown to Embedded SparkSQL
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - pushdown to embedded sparksql
draft: false
last_update:
    date: 08/17/2022
---


Kylin uses pre-calculation instead of online calculation to achieve sub-second query latency on big data. In general, the model with pre-calculated data is able to serve the most frequently-used queries. But if a query is beyond the model's definition, the system will route it to the Kylin smart pushdown engine. The embedded pushdown engine is Spark SQL.

> **Note**: In order to ensure data consistency, query cache is not available in pushdown.



#### Turn on and off

Kylin 5.x has an embedded Spark engine, so no 3rd party dependency is required to enable query pushdown. You can query on source tables directly after loading the data source (at least one table loaded).

By default, the query pushdown is turned on in a new project. If you need to turn it off, there are two ways:

**At project level:** As shown below, click the left navigation bar **Setting** tab and in the **Basic Settings -> Pushdown Setting** part, you can turn off the Pushdown Engine in the red frame. If this setting has never been modified at project level, it takes instance level setting as default value.

![Turn off Query Pushdown at Project Level](images/query_pushdown.png)

**At instance level:** Query pushdown is turned on by default, which corresponds to the configuration item `kylin.query.pushdown-enabled=true` in the configuration file `${KYLIN_HOME}/conf/kylin.properties`. To turn it off, add `kylin.query.pushdown-enabled=false` into the configuration file.



### Verify Query Pushdown

If you submit the query when there is no online model, the query pushdown will work. If your data source is HIVE, the query pushdown will show the result from HIVE, such as: `Answered By: HIVE`.

> **Tip**: If the query answered by models, the query history will be displayed as: `Answered By: {model_name}`.



### Float Type Notice

There are 2 things to notice when pushdown query has filter for `float` type column of datasource: literal type and precision.

* Literal Type: Specify literal data type manually with the same type as column like `'123.4f'`, for example:

```sql
SELECT * FROM table1 WHERE col1 > '123.4f'
```

* Precision: Do not exceed the precision range of `float / double` type


For example, datasource table `table1` has `float` type column `col1`, data in table:

```text
|-------|
| col1  |
|-------|
| 1.2   |
| 5.67  |
| 123.4 |
| 130.1 |
|-------|
```

A pushdown query which is:

```sql
SELECT * FROM table1 WHERE col1 > 123.4
```

Will get the following result:

```text
|-------|
| col1  |
|-------|
| 123.4 |
| 130.1 |
|-------|
```

As you can see the line `123.4` appears in the result even the `WHERE` filter uses the operation grater than (`>`).

The reason causes it is the two different data types between two sides of the filter operator, and it hits a rule of Spark optimizer:

`col1` is the type of `float`, and lteral value `123.4` is the type of `double` by default.

And this Spark optimizer will transform the filter in such a way (Notice the operator `>=` after transformation):

```text
cast(col1 to double) > 123.4  ===>  col1 >= cast(123.4 to float)
```

That causes the line `123.4` return.

The correct way is to specify **literal type** manually like the following:

```sql
SELECT * FROM table1 WHERE col1 > '123.4f'
```

And the result looks good now:

```text
|-------|
| col1  |
|-------|
| 130.1 |
|-------|
```

For **literal precision**, check out the following pushdown query:

```sql
SELECT * FROM table1 WHERE col1 > '123.3999f'
```

Which returns a correct result:

```text
|-------|
| col1  |
|-------|
| 123.4 |
| 130.1 |
|-------|
```

But the next pushdown query which has an unsuitable numeric precision may cause an unexpected result:

```sql
SELECT * FROM table1 WHERE col1 > '123.39999999999f'
```

The unexpected result:

```text
|-------|
| col1  |
|-------|
| 130.1 |
|-------|
```
