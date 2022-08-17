---
title: Comparison Operators
language: en
sidebar_label: Comparison Operators
pagination_label: Comparison Operators
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - comparison operators
draft: false
last_update:
    date: 08/17/2022
---

| Operator             | Description                                                  | Syntax                                      | Example                                                      |
| -------------------- | ------------------------------------------------------------ | ------------------------------------------- | ------------------------------------------------------------ |
| <                    | Less than                                                    | A<B                                         | Profit < Cost                                                |
| <=                   | Less than or equal                                           | A<=B                                        | Profit <=Cost                                                |
| >                    | Greater than                                                 | A>=B                                        | Profit >Cost                                                 |
| >=                   | Greater than or equal                                        | A>=B                                        | Profit >=Cost                                                |
| `<>`                   | Not Equal                                                    | `A<>B`                                        | `Profit1<>Profit2`                                             |
| IS NULL              | Whether *value* is null                                      | value IS NULL                               | profit IS NULL                                               |
| IS NOT NULL          | Whether *value* is not null                                  | value IS NOT NULL                           | profit IS NOT NULL                                           |
| IS DISTINCT FROM     | Whether two values are not equal, treating null values as the same | value1 IS DISTINCT FROM value2              | profit1 IS DISTINCT FROM profit2                             |
| IS NOT DISTINCT FROM | Whether two values are equal, treating null values as the same | value1 IS NOT DISTINCT FROM value2          | profit1 IS NOT DISTINCT FROM profit2                         |
| BETWEEN              | Return true if the specified value is greater than or equal to value1 and less than or equal to value2 | A BETWEEN   value1 AND value2               | profit BETWEEN 1 AND 1000      Date BETWEEN '2016-01-01' AND '2016-12-30' |
| NOT BETWEEN          | Whether *value1* is less than *value2* or greater than *value3* | value1 NOT BETWEEN value2 AND value3        | profit NOT BETWEEN 1 AND 1000      Date NOT BETWEEN '2016-01-01' AND '2016-12-30' |
| LIKE                 | Whether *string1* matches pattern *string2*, *string1* and *string2* are string types | string1 LIKE string2                        | name LIKE '%frank%'                                          |
| NOT LIKE             | Whether *string1* does not match pattern *string2*, *string1* and *string2* are string types | string1 NOT LIKE string2 [ ESCAPE string3 ] | name NOT LIKE '%frank%'                                      |
| SIMILAR TO           | Whether *string1* matches *string2* in regular expression    | string1 SIMILAR TO string2                  | name SIMILAR TO 'frank'                                      |
| NOT SIMILAR TO       | Whether *string1* does not match *string2* in regular expression | string1 NOT SIMILAR TO string2              | name NOT SIMILAR TO 'frank'                                  |


Limitations
- The current SIMILAR TO ESCAPE syntax is limited to scenarios that support adding and hitting the model in SQL statements, and other scenarios such as adding computable columns.
- The string literals including specific symbols need to be escaped by default and the escape character is `\`. For example, to match `\kylin` , it should be using `\\kylin`. For `SIMILAR TO` and  `NOT SIMILAR TO` function, the functions use regex match and there is an escaped process. For example, for `\\\\kylin`, the result will be `true` when using  `SIMILAR TO` to compare with `\kylin` and `\\kylin`.
