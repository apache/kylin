---
title: Precompute the join relations
language: en
sidebar_label: Precompute the join relations
pagination_label: Precompute the join relations
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - precompute the join relations
draft: false
last_update:
    date: 08/19/2022
---

Pre-computation of the join relations refers to the process of expanding the joined tables of a model into a flat table based on mappings, and then building indexes based on the flat table. Kylin will precompute each join relation and generate a flat table that contains dimensions, measures and columns referenced by [computed columns](computed_column.md) by default. This article will cover the principles and features of **Precompute** **Join Relationships**.

### Principles

This article takes *Fact* as the fact table and *Dim* as the dimension table to introduce how **Precompute Join Relationships** will affect the generation of a flat table. Suppose the table structures and data are as follows: 

- Table *Fact*

| col1 | col2 | col3 |
| ---- | ---- | ---- |
| 1    | a    | AAA  |
| 2    | b    | BBB  |
| 3    | c    | CCC  |

- Table *DIM*

| col1 | col2 | col3 |
| ---- | ---- | ---- |
| 1    | A1   | AAAA |
| 1    | A2   | BBBB |
| 2    | B1   | CCCC |
| 3    | C1   | DDDD |

If *Fact* inner joins *Dim* and **Precompute Join Relationships** is enabled, it will generate a flat table as below:

| Fact.col1 | Fact.col2 | Fact.col3 | Dim.col1 | Dim.col2 | Dim.col3 |
| --------- | --------- | --------- | -------- | -------- | -------- |
| 1         | a         | AAA       | 1        | A1       | AAAA     |
| 1         | a         | AAA       | 1        | A2       | BBBB     |
| 2         | b         | BBB       | 2        | B1       | CCCC     |
| 3         | c         | CCC       | 3        | C1       | DDDD     |

If *Fact* inner joins *Dim* and **Precompute Join Relationships** is disabled, the flat table generated will be: 

| Fact.col1 | Fact.col2 | Fact.col3 |
| --------- | --------- | --------- |
| 1         | a         | AAA       |
| 2         | b         | BBB       |
| 3         | c         | CCC       |

> [!NOTE]
>
> In this scenario, the generation of a flat table does not rely on the dimension table and it will be stored as a snapshot in Kylin during the building process.

### Feature comparison 

To strike the right balance between performance and cost, you can choose whether to enable **Precompute Join Relationships** based on your business needs and data characteristics when [designing a model](../manual_modeling.md). The following table compares the features of enabling and disabling **Precompute Join Relationships**. 

| **Precompute Join Relationships** | **Query performance** | **Building duration** | **Storage costs** | **Adaptability to new query scenarios** | **Impact**                                                   |
| -------------------------- | ------------ | ------------ | ------------ | -------------------- | ------------------------------------------------------------ |
| Enable                            | Good                  | Longer                | Higher            | Fair                                    | ● All columns in dimension tables can be set as dimensions, or defined as measures or computed columns.  <br />|
| Disable                           | Fair                  | Shorter               | Lower             | Good                                    | ● Columns in dimension tables cannot be set as dimensions, or defined as measures or computed columns, which means they cannot be referenced by indexes.<br />● Indexes and corresponding dimension snapshots will be hit by queries simultaneously, so users can get the query results through real-time join queries. <br />In snowflake models, if a foreign key corresponds to a dimension table, and the table is set as an excluded table or **Precompute Join Relationships** is disabled, the dimension table will not be referenced when generating indexes. |

### FAQ

- Question: If **Precompute Join Relationships** is enabled in a model, what will happen if I disable it?

  Answer: If **Precompute Join Relationships** is disabled, Kylin will automatically delete all related indexes, dimensions, measures, and computed columns. Please use caution when you perform this operation.  

- Question: If the table relationship is one-to-many or many-to-many, is there anything I should be aware of before enabling **Precompute Join Relationships**? 

  Answer: In such a scenario, derived dimension queries will be disabled. If columns of the joined tables are not set as dimensions, these columns will not be referenced when generating indexes, or aggregate indexes or table indexes to accelerate queries.     

- Question: If a table is excluded, will it affect precomputing the join relations?

  Answer: Even if **Precompute Join Relationships** is enabled, this table will not be used to generate a flat table or referenced when generating indexes.

- Question: What's the difference between excluding tables and disabling **Precompute Join Relationships**? 

  Answer: The table below summarizes the main differences. 

| Category                              | Effective level | Applicable scenario                                          |
| ------------------------------------- | --------------- | ------------------------------------------------------------ |
| Exclude tables                        | Project-level   | Often used when returning the latest data for queries is required. The corresponding foreign keys of the join relations, instead of the columns of the excluded tables, will be solidified into the indexes. |
| Disable Precompute Join Relationships | Model-level     | Often used to reduce storage costs and improve building effeciency, for example, in one-to-many or many-to-many relationships. |

> [!NOTE]
>
> When designing a model, please do not use the columns of the excluded tables as dimensions, or else the index building job may fail. 
