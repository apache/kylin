---
title: Basic Query Execution
language: en
sidebar_label: Basic Query Execution
pagination_label: Basic Query Execution
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - basic query execution
draft: false
last_update:
    date: 08/17/2022
---


kylin offers an Intelligent OLAP Platform for big data to simplify multidimensional analytics. kylin finishes data loading and precomputation with the index building jobs and delivers sub-second query latencydelivers sub-second query latency on massive dataset.

Unlike the execution process of regular queries, kylin will be using precalculated data to answer queries instead of doing the calculation online, which will improve query performance significantly.

### Query Execution Process with Precalculation

Different from regular query engines, kylin uses precalculated results to replace real time calculation, in order to improve the query performance and concurrency. A simplified version of query execution process can be described as below:

1. Parse SQL statement and extract all the `FROM` clauses. 

2. Find the **matching** and **minimum cost** model for each `FROM` clause.

   The **matching** here means:

   * The relationship of tables used in `FROM` clause must match the fact and dimension tables defined in models. Please note that the relationship of `LEFT JOIN` does not match `INNER JOIN`.
   * For aggregate queries, the columns in `GROUP BY` clause must be defined as dimensions in models. Meanwhile, the aggregate functions in `SELECT` clause must be defined as measures in models. 
   * For non-aggregate queries, table index must be defined in models and all columns appeared in query must be contained in the table index.

   The **minimum cost** here means that kylin will automatically select the minimum cost index if there are multiple matching indices. For example, table index can also serve aggregate queries, but its cost is high because of the real time calculation. Therefore, using table index to answer aggregate query is always the last option and only happens when all aggregate indices cannot match.

3. If all the `FROM` clauses match successfully, kylin will execute the query using index data.

   All the `FROM` clauses will be replaced by precalculated results, and the query will execute from there to get the final result. If you execute queries via Web UI, you can find the name(s) of the answering model(s) in the **Answered By** field after a query returns successfully. For more details, please refer to [Execute SQL Query in Web UI](insight.md).

4. If there is one or more `FROM` clause cannot find a matching index, then kylin cannot execute the query using index data.x

   The query will fail with an error message of `no model found` or `no realization found`. This means the data required for this query does not exist in the system.

   As a special case, if the pushdown engine is enabled, then kylin will not report error, and instead route this query to the pushdown engine. For more details, please refer to [Query Pushdown](../../query/pushdown/intro.md).

