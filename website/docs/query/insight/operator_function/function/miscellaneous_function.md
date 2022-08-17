---
title: Other Functions
language: en
sidebar_label: Other Functions
pagination_label: Other Functions
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - other functions
draft: false
last_update:
    date: 08/17/2022
---

| Syntax                        | Description                                                  | Example                                                | Normal Query | Pushdown Query | Defined as Computed Column | Suggested as Computed Column |
| ----------------------------- | ------------------------------------------------------------ | ------------------------------------------------------ | ------------ | -------------- | -------------------------- | ---------------------------- |
| UUID()                        | Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string | ` UUID()`<br /> = 46707d92-02f4-4817-8116-a4c3b23e6266 | ❌            | ✔️              | ❌                          | ❌                            |
| MONOTONICALLY_INCREASING_ID() | Returns monotonically increasing 64-bit integers. The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive. | ` MONOTONICALLY_INCREASING_ID()`<br /> = 1111111       | ✔️            | ✔️              | ❌                          | ❌                            |
| EXPLODE(array)                | Returns new rows for each element in the given array column.    | `EXPLODE(array[1, 2, 3])`<br /> = <br/> 1  <br/> 2 <br/> 3  |✔️         |❌       | ❌                | ❌                |
| SIZE(expr) | Expr must be of type array or map and return the number of elements contained in array or map. | `SIZE(array[1, 2, 3])`<br /> = <br/> 3 |✔️ |✔️ | ❌ | ❌ |

