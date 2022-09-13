---
title: Query History Field Table
language: en
sidebar_label: Query History Field Table
pagination_label: Query History Field Table
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - query history field table
draft: false
last_update:
    date: 09/13/2022
---

Starting with Kylin 5.0, the system uses RDBMS to store query history. And each metadata will store two tables related to query history:

- *query_history* : information for each query
- *query_history_realization* : each index that hit by query

### ***query_history*** table

| Field Name             | Description                                                         |
| ---------------------- | ------------------------------------------------------------ |
| id                     | auto increment primary key                                   |
| cache_hit              | whether the query hits the cache                                             |
| duration               | duration of query execution                                               |
| engine_type            | four query objects, the blue name indicates the query hits model (index group), *HIVE* indicates that the query is pushed down to Hive, *CONSTANTS* indicates the query constant, and the blank indicates that the query failed. |
| error_type             | error message when error occurs during query execution                                         |
| server               | if hostname exists, use hostname of query node, otherwise use IP address of query node                                 |
| index_hit              | whether the query hits index or not                                         |
| is_agg_index_used      | whether the query hits the aggregate index or not                                     |
| is_table_index_used    | whether the query hits table index or not                                      |
| is_table_snapshot_used | whether the query hits the dimension table                                           |
| month                  | the month in which the query is submitted, which is needed to count the history of the query by month.             |
| query_first_day_of_month  | the initial time of the month in which the query time is located. This field is required when statistically querying the history by month         |
| query_first_day_of_week   | the initial time of the week in which the query time is located. This field is required when statistically querying the history by month         |
| query_day                 | the initial time of the day in which the query time is located. This field is required when statistically querying the history by month         |
| query_id               | query identity                                                |
| query_status           | two query statuses, *SUCCEEDED* indicates that the query was successful, *FAILED* indicates that the query was abnormal |
| query_time             | query start time                                                 |
| realizations           | a string concatenated by the model ID number, Layout ID number, and index type，which format is *modelId#layoutId#indexType* |
| result_row_count       | number of query results                                                 |
| sql_text               | SQL statement that was submitted                                             |
| sql_pattern            | the sql pattern of SQL statement that was submitted . This field is needed when sql is accelerated.           |
| submitter              | Kyligence Enterprise user who submits the query                        |
| total_scan_bytes       | total scanned lines of the query                                          |
| total_scan_count       | total scanned bytes of the query                                             |
| project_name           | project name                                             |
| reserved_field_1       | reserved field, currently unused                                             |
| reserved_field_2       | reserved field, currently unused                                               |
| reserved_field_3       | Starting from 4.2, this field is used to record the reason why the index cannot be recommended                                        |
| reserved_field_4       | reserved field, currently unused                                               |

### ***query_history_realization*** table

| Field Name | Description                                                  |
| ---------- | ------------------------------------------------------------ |
| query_time       | unix timestamp of query start time (when query start to execute)                          |
| duration   | duration of query execution                          |
| index_type | three index types, *Table Index* for table index, *Agg Index* for aggregate index, and *Table Snapshot* for dimension table |
| layout_id  | ID of layout                                                 |
| model      | ID of model                                                 |
| query_id   | ID of query                                               |
| project_name           | Project name                                 |

