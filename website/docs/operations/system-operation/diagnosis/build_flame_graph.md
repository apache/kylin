---
title: Build Flame Graph
language: en
sidebar_label: Build Flame Graph
pagination_label: Build Flame Graph
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - build flame graph
draft: false
last_update:
    date: 08/12/2022
---

Kylin has built-in async-profiler. When flame graphs are needed to diagnose build tasks performance, users can generate query flame graphs for Spark Driver and Executor by calling the API interface.

Since the flame graph is generated at the system level, it will affect all projects. Only the Admin user has the authority to use this function.

### Configs
| Config                                      | Comment                                                                                                                                          |
|---------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| kylin.engine.async-profiler-enabled         | enable the profiling feature (default to FALSE). After enables, you can trigger the generation and download of the flame graph by calling the API |
| kylin.engine.async-profiler-result-timeout  | the timeout for the result collection (default to 60s)                                                                                           |
| kylin.engine.async-profiler-profile-timeout | the timeout for the profiling (default to 5min)                                                                                                  |


### Start Profiling
invoke the below HTTP API to start generating flame graph

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
    - `Accept-Language: en`
    - `Accept-Language: zh`
  - `Content-Type: application/json;charset=utf-8`

**There are two ways to generate**

- GET `http://host:port/kylin/api/jobs/profile/start_project?project={projectName}&step_id={jobStepId}&params={params}`
  - URL Parameters
    - `project`, Required, String, specifies the projectName where the current build task is located, no default value
    - `step_id`, Required, String, specifies the jobStepId of the current build task, which can be found in the YARN interface, copying the rest of its Name except `job_step_`.
    - `params`, Optional, String, specify async-profiler parameter, default is `start,event=cpu` (profile cpu usage)

- GET `http://host:port/kylin/api/jobs/profile/start_appid?app_id={yarnAppId}&params={params}`
  - URL Parameters
    - `app_id`, Required, String, specifies the Application ID of the current build task submitted to YARN
    - `params`, Optional, String, specify async-profiler parameter, default is `start,event=cpu` (profile cpu usage)

### Stop Profiling
invoke the below HTTP API to stop generating flame graph and download flame graph

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
    - `Accept-Language: en`
    - `Accept-Language: zh`
  - `Content-Type: application/json;charset=utf-8`

**There are two ways to get it**

- GET `http://host:port/kylin/api/jobs/profile/dump_project?project={projectName}&step_id={jobStepId}&params={params}`
  - URL Parameters
    - `project`, Required, String, specifies the projectName where the current build task is located, no default value
    - `step_id`, Required, String, specifies the jobStepId of the current build task, which can be found in the YARN interface, copying the rest of its Name except `job_step_`.
    - `params`, Optional, String, specify the async-profiler parameter, default is `flamegraph` (collects the results as a flame graph)

- GET `http://host:port/kylin/api/jobs/profile/dump_appid?app_id={yarnAppId}&params={params}`
  - URL Parameters
    - `app_id`, Required，String，specify the Application ID of the current build task submitted to YARN
    - `params`, Optional, String, specify the async-profiler parameter, the default is `flamegraph` (collects the results as a flame graph)

### Limitations
1. Current async-profiler version comes with Kylin is Linux x64 (glibc): async-profiler-2.5-linux-x64.tar.gz, Other platforms are not supported.
2. The profiling may have some impact on the performance of Kylin, avoid doing a long time profiling in production.
3. The parameters involved in building the flame chart are system level parameters, please do not set them at other levels as they may cause abnormal behavior.
4. The `params` support configuration parameters can be found at https://github.com/jvm-profiling-tools/async-profiler
5. On some machines, the required native libraries may fail to load when the `/tmp` disk has the noexec attribute, causing Spark initialization to fail and affecting normal build tasks, so this feature is disabled by default.
