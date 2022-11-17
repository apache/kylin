---
title: Query Pushdown
language: en
sidebar_label: Query Pushdown
pagination_label: Query Pushdown
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - query pushdown
draft: false
last_update:
    date: 08/17/2022
---

Kylin integrates a Smart Pushdown engine which works SQL on Hadoop engine like SparkSQL. 

For queries which cannot be answered by Kylin, they can be routed into Pushdown Query Engine when necessary.

