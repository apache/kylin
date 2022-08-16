---
title: Query Flame Graph
language: en
sidebar_label: Query Flame Graph
pagination_label: Query Flame Graph
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - query flame graph
draft: false
last_update:
    date: 08/12/2022
---

Kylin has built-in async-profiler. When flame graphs are needed to diagnose query performance, users can generate query flame graphs for Spark Driver and Executor by calling the API interface.

Since the flame graph is generated at the system level, it will affect all projects. Only the Admin user has the authority to use this function.

### Configs
| Config                                                      | Comment                                                         |
| ----------------------------------------------------------- | ------------------------------------------------------------ |
| kylin.query.async-profiler-enabled                          | enable the profiling feature (default to TRUE). After enables, you can trigger the generation and download of the flame graph by calling the API  |
| kylin.query.async-profiler-result-timeout                   | the timeout for the result collection (default to 60s) |
| kylin.query.async-profiler-profile-timeout                  | the timeout for the profiling (default to 5min)  |


### Start Profiling
invoke the below HTTP API to start generating flame graph

- GET `http://host:port/kylin/api/query/profile/start?params={params}`

- URL Parameters
  - `params`, Optional, String, specify async-profiler params to start profiling, default to `start,event=cpu` (profile the cpu only), ref to https://github.com/jvm-profiling-tools/async-profiler for more parameters

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

### Stop Profiling
invoke the below HTTP API to stop generating flame graph and download flame graph

- GET `http://host:port/kylin//api/query/profile/dump?params={params}`

- URL Parameters
  - `params`, Optional, String, specify async-profiler params to start profiling, default to `flamegraph` (dump the result as flamegraph), ref to https://github.com/jvm-profiling-tools/async-profiler for more parameters

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

### Limitations
1. Current async-profiler version comes with Kylin is Linux x64 (glibc): async-profiler-2.5-linux-x64.tar.gz, Other platforms are not supported.
2. The profiling may have some impact on the performance of Kylin, avoid doing a long time profiling in production.
3. The flame graph result can only be downloaded once.
