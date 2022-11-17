---
title: Measures
language: en
sidebar_label: Measures
pagination_label: Measures
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - measures
draft: false
last_update:
    date: 08/19/2022
---

This section introduces how to design measures in project.

Kylin provides basic measures such as SUM, MAX, MIN, COUNT and also advanced measures including TopN, precise count distinct, approximate count distinct, and approximate Percentile.

In the model editing page, there are three ways to add measures:

> **Note**: It's highly recommended to finish the basic model design before add measures. You can click the **M** button on the right side in the model editing page to popup the measure list.



- Drag&Drop: drag the column that you want to define as a measure from the model to the measure list area, and then edit the measure in the pop-up window.

- Add Measure: click the first button **+ (Add)** on the measure list, and then edit the measure in the pop-up window.

- Add Measure in Batch: click the button **+ (Batch Add)** in the middle of the measure list, and then add multiple measures in the pop-up window.

  > **Note**: The Batch Add only includes SUM, MAX, MIN, and COUNT. If you need to add advanced measure, please choose the first two ways.

### Known Limitation

When the calculated column type of the SUM measure is `decimal(P,D)`, the precision is `P + 10`, the maximum precision is 38, and custom precision is not supported.
