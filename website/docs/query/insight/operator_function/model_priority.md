---
title: Specify Model Priorities in SQL
language: en
sidebar_label: Specify Model Priorities in SQL
pagination_label: Specify Model Priorities in SQL
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - model priorities
draft: false
last_update:
    date: 08/17/2022
---


When there are more than on model that can answer a query, kylin will automatically choose a best model. But in some cases, a user would like to use some certain models for some certain queries. And the user can achieve that via a model priority hint in the SQL.


### Model Priority Hint
The syntax
```
SELECT /*+ MODEL_PRIORITY(model1, model2) */ col1, col2 from ....
```
The hint starts with `/*+` and ends with `*/`. And the hint must be placed right after the `SELECT` clause.

`MODEL_PRIORITY(model1, model2, ...)` specifies the model priorities, `MODEL_PRIORITY(...)` accept a list of model names with descending priority. The model that doesn't appear in the hint will be assigned with the lowest priority.
The model priority hint will be valid for the entire query. Only the first hint occured in the SQL will be valid.

During the model matching, if multiple model is capable to answer the query. kylin will use the model with the highest priority specified in the model priority hint

Supported starts from kylin 5.0


#### Example
```
SELECT /*+ MODEL_PRIORITY(model1, model2) */ col1, col2 from table;
```
If both model1 and model2 is capable to answer the query, model1 will be the chosen for this query.
