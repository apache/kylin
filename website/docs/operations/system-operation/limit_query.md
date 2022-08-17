---
title: Limit query current capacity, protect query stability
language: en
sidebar_label: Limit query current capacity, protect query stability
pagination_label: Limit query current capacity, protect query stability
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - limit query current capacity
    - protect query stability
draft: false
last_update:
    date: 08/12/2022
---

Query resources are usually limited. During certain periods of time, the query volume suddenly increases, or when a small number of large queries occupy too many resources, query resource competition may occur, resulting in large fluctuations in overall query performance.

In order to avoid the above situation, we can adopt the query current capacity limiting strategy, by rejecting or limiting the performance of part of large queries, to ensure that most of the small queries are not affected, and to ensure the overall stability of the query.


### Query classification

Through long-term observation, we can roughly divide queries into two categories: large queries and small queries. They have different typical characteristics:

- **Big query**: The number is small and the resources are occupied. The fluctuation of the big query has a great impact on the overall stability of the query.
- **Small queries**: The number is large, each small query occupies less resources, protecting small queries can effectively ensure the overall stability of the query.

According to the characteristics of these two types of queries, we have designed different query current limiting strategies, which can be selected as needed. For details, see **two query current limiting strategies** below.

At the same time, for the judgment of large and small queries, we also provide parameters, allowing users to fine-tune according to the actual situation. See below for **Determination of Large Query**.

### Two query current capacity limiting strategies

**Strategy 1: Small query priority scheduling strategy**

After enabling the priority scheduling policy for small queries, small queries will be scheduled first, and large queries will be scheduled later.

Set `kylin.query.query-limit-enabled = true` in `kylin.properties`, the default value is **false**. Also configure Ops Plan to enable large query rejection policy.

**Strategy 2: Large query rejection strategy**

Different from the post-scheduling of large queries in strategy 1, when strategy 2 is used, large queries will be rejected directly after reaching the upper limit of Spark task load. Spark task load refers to the ratio of the number of tasks in the Pending state to the number of tasks in the Running state in the Spark cluster. This strategy requires the cooperation of the Ops Plan to collect the task load indicator, and when the indicator value reaches the upper limit, it triggers the rejection of large queries.

Set `kylin.query.share-state-switch-implement=jdbc` in `kylin.properties`, and configure Ops Plan to enable large query rejection policy. The default value is **close**.

Among them, the default value of Spark task load is 50. Generally, it is not recommended to modify it. 


### Judgment of large query

An important factor that affects the effect of the above query current limiting strategy is the determination of large queries. We provide both default values and allow flexible adjustments based on actual queries and system conditions.

**Main principle:**

The system mainly uses **data scan rows** as the basis for judging whether it is a large query. The sum of the number of rows scanned for a query data, when this value exceeds the threshold, it is determined as a large query, otherwise it is a small query. This value may be different from the "Number of records scanned by query" displayed on the page of the query result. This number of rows refers to the number of rows of the parquet file scanned after **pruning**.

**Judgment settings for large queries:**

The system provides the initial threshold setting for determining whether it is a large query and the number of data scan rows, and also provides a mechanism to automatically update this threshold. The configuration of related parameters will be described in detail below.

To adjust, adjust the following parameters in `kylin.properties`:
- `kylin.query.big-query-source-scan-rows-threshold`: Determines whether it is a big query, the initial threshold of the number of rows to be scanned. The default value is `-1` , which means that the user does not specify, and the system automatically calculates the initial threshold at startup. In addition, this threshold can be automatically updated to suit the cluster environment by collecting query information during system operation.
- `kylin.query.auto-adjust-big-query-rows-threshold-enabled`: Whether to automatically update the above thresholds. The default value is false, set to true to enable automatic update.
- `kylin.query.big-query-threshold-update-interval-second`: Interval to automatically update the above threshold. The default value is 10800, in seconds.
- `kylin.query.big-query-second`: The time limit that the big query needs to meet when the above threshold is automatically updated, the default value is 10, in seconds.

In addition, when the query contains limit, the following optimizations can also be enabled to make the automatic update threshold more accurate and avoid misjudgment of large queries.
- `kylin.query.apply-limit-info-to-source-scan-rows-enabled`: Whether to apply limit information to optimize scan row count estimation. The default value is false.
