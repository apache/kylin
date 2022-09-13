---
title: Skip Dictionary Encoding Optimization for Integer Type 
language: en
sidebar_label: Skip Dictionary Encoding Optimization for Integer Type 
pagination_label: Skip Dictionary Encoding Optimization for Integer Type 
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - skip dictionary encoding optimization for integer type
draft: false
last_update:
    date: 09/13/2022
---

Starting from Kylin 5, the system supports no dictionary encoding for integer types

With the following settings, optimization of precise deduplication queries can be started:

1. Build a model that contains precise deduplication metrics

2. Modify the configuration of the model and add custom settings to the model in the settings interface:
    `kylin.query.skip-encode-integer-enabled = true`

3. Build the model

### Configuration scope

This configuration is only available at the model level

### Precautions

1. This operation can improve the build performance, if the data hash is serious, it may cause the inflation rate to be too high
2. If the value of this parameter changes, you need to re-brush the entire model

