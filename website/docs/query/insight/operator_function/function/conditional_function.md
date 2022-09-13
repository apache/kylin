---
title: Conditional Functions
language: en
sidebar_label: Conditional Functions
pagination_label: Conditional Functions
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - conditional functions
draft: false
last_update:
    date: 08/17/2022
---

| Syntax                                                       | Description                                                  | Example                                                      | Normal Query | Pushdown Query | Defined as Computed Column | Suggested as Computed Column |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------ | -------------- | -------------------------- | ------------------------- |
| CASE value WHEN value1 THEN result1 WHEN valueN THEN resultN ELSE resultZ END | Simple case                                                  | `CASE OPS_REGION WHEN 'Beijing' THEN 'BJ' WHEN 'Shanghai' THEN 'SH'WHEN 'Hongkong' THEN 'HK' END FROM KYLIN_SALES` <br /> = HK SH BJ | ✔️            | ✔️              | ✔️                          | ✔️                         |
| CASE WHEN condition1 THEN result1 WHEN conditionN THEN resultN ELSE resultZ END | Searched case                                                | `CASE WHEN OPS_REGION='Beijing'THEN 'BJ' WHEN OPS_REGION='Shanghai' THEN 'SH' WHEN OPS_REGION='Hongkong' THEN 'HK' END FROM KYLIN_SALES`<br /> = HK SH BJ | ✔️            | ✔️              | ✔️                          | ✔️                         |
| NULLIF(value, value)                                         | Return NULL if the values are the same. Otherwise, return the first value. | `NULLIF(5,5)`<br /> = null                                   | ✔️            | ✔️              | ✔️                          |  ✔️                         |
| COALESCE(value, value [, value ]*)                           | Return the first not null value.                 | `COALESCE(NULL,NULL,5)`<br /> = 5                            | ✔️            | ✔️              | ✔️                          |  ✔️                          |
| IFNULL(value1, value2)                                       | Return value2 if value1 is NULL. Otherwise, return value1. | `IFNULL('kylin','apache')`<br /> = 'kylin'  | ✔️        | ✔️        | ✔️                | ✔️              |
| ISNULL(value)                                                | Return true if value is NULL. Otherwise, return false. | `ISNULL('kylin')` <br /> = false                   | ✔️        | ✔️        | ✔️                |  ✔️                |
| NVL(value1, value2)                                          | Return value2 if value1 is NULL. Otherwise, return value1. Value1, value2 must have same data type. | `NVL('kylin','apache')`<br /> = 'kylin'  | ✔️        | ✔️        | ✔️                | ✔️               |
