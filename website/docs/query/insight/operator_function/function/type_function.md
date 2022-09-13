---
title: Type Functions
language: en
sidebar_label: Type Functions
pagination_label: Type Functions
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - type functions
draft: false
last_update:
    date: 08/17/2022
---

| Syntax                | Description                                                        | Example                                                        | Normal Query                                            | Pushdown Query                                          | Defined as Computed Column                              | Suggested as Computed Column                            |
| ------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| CAST(value AS type) | Converts a value to a given type. | Example 1: Converts `time` to `string`<br />`CAST(CURRENT_DATE as varchar)`<br /> = 2018-10-10 | ✔️ | ✔️ | ✔️ | ✔️ |
| DATE&lt;String&gt; | Converts a string to DATE type,<br />equaling to CAST(string AS date) | Example 1:<br />`DATE'2018-10-10'`<br /> = 2018-10-10<br /><br />Example 2: Gets the corresponding month of the string (use with time function, MONTH() )<br />`MONTH(DATE'2018-10-10')`<br /> = 10 | ✔️ | ✔️ | ❌ | ❌ |
| TIMESTAMP&lt;String&gt; | Converts a string to TIMESTAMP type,<br />equaling to CAST(string AS timestamp) | Example 1:<br />`TIMESTAMP'2018-10-10 15:57:07`<br /> = 2018-10-10 15:57:07<br /><br />Example 2: Gets the corresponding second of the string (use with time function, SECOND() )<br />`SECOND(TIMESTAMP'2018-10-10 15:57:07')`<br /> = 7 | ✔️ | ✔️ | ❌ | ❌ |

> Note:
>
> 1. Only the following types of conversions are supported: `char`,` varchar`, `boolean`, `int`,` integer`, `tinyint`,` smallint`, `bigint`,` float`, `double`,` decimal`, `numeric`,` date`, `time`,` timestamp`
> 2. However, conversion from `bigint` to` timestamp` is not supported at this time
> 3. If converting from `char` to` int`, non-numeric values in `char` will return null
> 4. Length n does not take effect when other data types are converted to `char(n)`/`varchar(n)`
