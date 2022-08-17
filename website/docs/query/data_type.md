---
title: Data Types
language: en
sidebar_label: Data Types
pagination_label: Data Types
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - data types
draft: false
last_update:
    date: 08/17/2022
---

Supported data types:

| Data Types  | Description                                                  | Range of Numbers                                             |
| ----------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| tinyint     | Small interger numbers.                           | (-128，127)                                                  |
| smallint    | Small interger numbers.                           | (-32,768，32,767)                                            |
| int/integer | Integer numbers.                                  | (-2,147,483,648，2,147,483,647)                              |
| bigint      | Large integer numbers.                            | (-9,223,372,036,854,775,808，9,223,372,036,854,775,807)      |
| float       | Single-precision floating point numbers.          | (-3.402823466E+38，-1.175494351E-38)，0，(1.175494351E-38，3.402823466351E+38) |
| double      | Double-precision floating point numbers.          | (-1.7976931348623157E+308，-2.2250738585072014E-308)，0，(2.2250738585072014E-308，1.797693134 8623157E+308) |
| decimal     | An exact numeric data type defined by its *precision* (total number of digits) and *scale* (number of digits to the right of the decimal point). | ---                                                          |
| timestamp   | Values comprising values of fields year, month, day, hour, minute, and second, with the session local time-zone. The timestamp value represents an absolute point in time. | ---                                                          |
| date        | Values comprising values of fields year, month and day, without a time-zone | ---                                                          |
| varchar     | Variable length string                            | ---                                                          |
| char        | Fixed length string                               | ---                                                          |
| boolean     | Boolean values                                    | ---                                                          |

> **Note**: There is an inaccurate accuracy problem when calculating double type data.

