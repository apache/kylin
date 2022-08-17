---
title: Aggregate Functions
language: en
sidebar_label: Aggregate Functions
pagination_label: Aggregate Functions
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - aggregate functions
draft: false
last_update:
    date: 08/17/2022
---


| Syntax       | Description                                                  | Example                                                      | Normal Query | Pushdown Query | Defined as Computed Column | Suggested as Computed Column |
| ------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------ | -------------- | -------------------------- | ---------------------------- |
| AVG(numeric) | Returns the average (arithmetic mean) of *numeric* across all input values | `SELECT AVG(PRICE) FROM KYLIN_SALES` <br /> = 49.23855638491023 | ✔️            | ✔️              | ❌                          | ❌                            |
| SUM(numeric) | Returns the sum of *numeric* across all input values         | `SELECT SUM(PRICE) FROM KYLIN_SALES`<br /> = 244075.5240     | ✔️            | ✔️              | ❌                          | ❌                            |
| MAX(value)   | Returns the maximum value of *value* across all input values | `SELECT MAX(PRICE) FROM KYLIN_SALES`<br /> = 99.9865         | ✔️            | ✔️              | ❌                          | ❌                            |
| MIN(value)   | Returns the minimum value of *value* across all input values | `SELECT MIN(PRICE) FROM KYLIN_SALES`<br /> = 0.0008          | ✔️            | ✔️              | ❌                          | ❌                            |
| COUNT(value) | Returns the number of input rows for which *value* is not null (wholly not null if *value* is composite) | `SELECT count(PRICE) FROM KYLIN_SALES` <br /> = 4957         | ✔️            | ✔️              | ❌                          | ❌                            |
| COUNT(*)     | Returns the number of input rows                             | `SELECT COUNT(*) FROM KYLIN_COUNTRY`<br /> = 244             | ✔️            | ✔️              | ❌                          | ❌                            |
| CORR(value1, value2) | Returns the correlation of two columns                | `SELECT CORR(ITEM_COUNT, PRICE) FROM KYLIN_SALES`<br /> = 0.1278             | ✔️        | ✔️        | ❌                | ❌                |

