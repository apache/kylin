---
title: Segment Pruning When Querying
language: en
sidebar_label: Segment Pruning When Querying
pagination_label: Segment Pruning When Querying
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - segment pruning
draft: false
last_update:
    date: 08/17/2022
---

Starting from Kylin 5.0, we support the calculation of the dimension value range (maximum and minimum) of all dimensions when building the Segment, so we can prune segment during queries, reducing the scanning range of the segment to optimize some query performance.


### Configuration

This optimization is enable by default. Under normal circumstances, you do not need to pay attention to this optimization. In some extreme cases, system-level or project-level shutdown is supported.

To disable it on the system level, configure the parameters in `$KYLIN_HOME/conf/kylin.properties` . To disable it on project level, add the configuration in **Setting -> Advanced Settings -> Custom Project Configuration**.

`kylin.storage.columnar.dimension-range-filter-enabled=false`

### Known Limitation

1. Currently, only the query filter conditions including `=, in, >, >=, <, <=, and, or`  support pruning segment. Filters including `not, is null` are not supported.

2. This optimization will slightly increase the building time, but it is basically negligible compared to the total building time.

3. The historical data that has been built will not use this optimization. If you want the historical data to apply this optimization, you need to refresh the Segment.

4. The multi-level partition models are not supported.
