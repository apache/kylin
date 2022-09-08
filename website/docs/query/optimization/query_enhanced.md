---
title: Use the Left Join model to answer Inner Join queries with equivalent semantics
language: en
sidebar_label: Use the Left Join model to answer Inner Join queries with equivalent semantics
pagination_label: Use the Left Join model to answer Inner Join queries with equivalent semantics
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - query enhanced
draft: false
last_update:
    date: 08/17/2022
---

By default in Kylin, the relationship between tables in the query SQL must be consistent with the relationship between the fact tables and dimension tables defined in the model, that is, the model of `Left Join` cannot answer the query of `Inner Join`.

But in some cases,  part of `Left Join` queries can be semantically equivalently transformed into `Inner Join` queries, so we provide configuration parameters that allow users to use `Left Join`  model to answer equivalent semantics `Inner Join` query.

The configuration parameters starts to take effect from version of Kylin 5.0, which is closed by default.



### Scene one
`[Table A] Left Join [Table B] Inner Join [Table C]` is semantically equivalent to `[Table A] Inner Join [Table B] Inner Join [Table C]`.

The reason is that when `Inner Join` is performed after `Left Join`, the rows that do not match the last right table will be filtered out, so the above two expressions are semantically equivalent.

Using the `kylin.query.join-match-optimization-enabled=true` configuration, Kylin can support `Left Join` models to answer the above equivalent semantic `Inner Join` queries.

#### Example 1
The model is defined as `KYLIN_SALES Left Join KYLIN_ACCOUNT Inner Join KYLIN_COUNTRY`

The SQL is as follows:

```sql
select kylin_country.name
from kylin_sales inner join kylin_account on kylin_sales.buyer_id = kylin_account.account_id
inner join kylin_country on kylin_account.account_country = kylin_country.country
```

The above model can answer this SQL.

#### Example 2
The model is defined as `[Table A] Left Join [Table B] Left Join [Table C] Inner Join [Table D] Left Join [Table E]`

The SQL is as follows:
`[Table A] Inner Join [Table B] Inner Join [Table C] Inner Join [Table D] Left Join [Table E]`

The above model can answer this SQL.



### Scene Two

SQL has the following characteristics: `[Table A] Left Join [Table B]` and any column of `[Table B]` in the filter condition has a non-null constraint, then the SQL semantics is equivalent to `[Table A] Inner Join [Table B]`.

Using `kylin.query.join-match-optimization-enabled=true` configuration, Kylin can support `Left Join` model to answer the above equivalent semantic `Inner Join` query.

The columns with non-null constraints need to meet the condition: they should be `isNotNull` filter conditions, `isNotNull` corresponds to common comparison operators: `=`, `<>`, `>`, `<`, `<=`, `>=`, `like`, `In`, `not like`, `not in` etc.

`isNull` filter conditions like `IS NULL` do not have non-null constraint.

At the same time, the filter conditions `IS DISTINCT FROM` and `CASE WHEN` is not supported for the time being, thus will be automatically ignored. See the known limitation 1.

#### Example 1
The model is defined as: `TEST_ACCOUNT left join TEST_ORDER left join TEST_ACCOUNT`

There are SQL as follows, which can hit the above model because there is a non-null constraint filter condition.

```sql
select SUM(a.ITEM_COUNT) as SUM_ITEM_COUNT
from TEST_KYLIN_FACT a
left join TEST_ORDER b on a.ORDER_ID = b.ORDER_ID
inner join TEST_ACCOUNT c on b.BUYER_ID = c.ACCOUNT_ID
where c.ACCOUNT_COUNTRY ='CN' and (c.ACCOUNT_COUNTRY like'%US' or c.ACCOUNT_COUNTRY is null)
```

#### Example 2
The model is defined as: `[Table A] inner join [Table B]`

There are SQL as follows that can hit this model:

```sql
A left join B where B.nonfk = '123' and B.col1 in ('ab','ac')
A left join B where A.col is null and B.col1 like 'xxx'
A left join B where B.col between 100 and 100000
A left join B where A.fk is null and B.col1 ='something'
A left join B where b.col ='something' and (b.col ='xxx' or b.col is null)
A left join B where abs(b.col) = 123
A left join B where floor(b.col) = 123
```

The following SQL cannot hit this model:

```sql
A left join B where B.col1 = 'xx' or A.col2 = 'yy'
A left join B where B.col1 = 'xx' or B.col2 is null
```



### Scene Three

Now there is the model `[Table A] Left Join [Table B] left join [Table C]`.

The following SQL is as follows, you can hit this model:
```sql
A inner join B inner join C where C.col ='abc'
```

The reason is that the columns in table C have non-null constraints, so the query can be equivalent to:

```sql
A LEFT_OR_INNER join B LEFT_OR_INNER join C where C.col ='abc'
```

Scene three is a mixture of scene one and scene two.



### Known limitations

1. For Scene two, judgment of non-null constraints don't include `NOT SIMILAR TO`, `CASE WHEN` expressions.
2. Also for scene two, judgment of non-null constraints don't include aggregation functions like `HAVING SUM(PRICE)>0`.
