---
title: Kylin Guardian Process
language: en
sidebar_label: Kylin Guardian Process
pagination_label: Kylin Guardian Process
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - guardian process
draft: false
last_update:
    date: 08/12/2022
---

Since Kylin 5.0, the system added a function of a daemon process for monitoring the health state of Kylin. This function is called **Kylin Guardian Process**. If the Kylin Guardian Process detects Kylin is in an unhealthy state, it will restart Kylin server or downgrade service.

### Usage

#### Turn On
Kylin Guardian Process is **disabled** by default. If you want to enable it, you need to add the configuration `kylin.guardian.enabled = true` in the global configuration file `$KYLIN_HOME/conf/kylin.properties`.
> **Note**: All the following configurations take effect if `kylin.guardian.enabled = true`

If Kylin Guardian Process is enabled, a daemon process will be automatically started after starting Kylin. This process is bound to environment variable `KYLIN_HOME`, which means each Kylin instance has only one Kylin Guardian Process corresponding to it.

Kylin Guardian Process description:
- The process ID is recorded in `$KYLIN_HOME/kgid`.

- The log of the process is output in `$KYLIN_HOME/logs/guardian.log`.

- Kylin Guardian Process will periodically check the health status of Kylin. The time delay of the first check is configured by the parameter `kylin.guardian.check-init-delay` (Unit: minutes), the default is 5 minutes, and the check interval is set by the parameter `kylin.guardian.check-interval` (Unit: minutes), the default is 1 minute.


#### Check Items
Kylin Guardian Process currently checks the following 4 aspects of Kylin instance's health.

- Kylin process status

  If the process number file `$KYLIN_HOME/pid` exists and the corresponding process does not exist, it means Kylin server is in an abnormal down state, and Kylin Guardian Process will restart it.

- Spark Context restart failure check
  
  If the number of Spark Context restart failure times is greater than or equals to the value of configuration `kylin.guardian.restart-spark-fail-threshold`, which is 3 times by default, Kylin Guardian Process will restart Kylin. This function is enabled by default. If you want to disable it, please add the configuration `kylin.guardian.restart-spark-fail-restart-enabled = false` in `$KYLIN_HOME/conf/kylin.properties`.

- **Bad Query** canceled failed check

  >**Note**: Some queries will be forcibly closed due to abnormal reasons. At this time, the query is **Bad Query**, and the common case is a timeout query.

  If Kylin Guardian Process detects the number of Bad Query cancellation times is greater than or equals to the value of configuration `kylin.guardian.kill-slow-query-fail-threshold`, which is 3 times by default, Kylin Guardian Process will restart Kylin. It is enabled by default. If you want to disable it, you can add the configuration `kylin.guardian.kill-slow-query-fail-restart-enabled = false` in `$KYLIN_HOME/conf/kylin.properties`.

- Full GC(Garbage Collection, Garbage collection mechanism in Java) duration check

  If the Full GC duration ratio in most recent period (default is value of `kylin.guardian.full-gc-check-factor` * value of `kylin.guardian.check-interval`) is greater than or equals to the value of configuration `kylin.guardian.full-gc-duration-ratio-threshold` which is 75% by default, Kylin Guardian Process will restart Kylin. It is enabled by default. If you want to disable it, you can add the configuration `kylin.guardian.full-gc-duration-ratio-restart-enabled = false` in `$KYLIN_HOME/conf/kylin.properties`.


#### Kylin Guardian Process High Availability
To ensure the high availability of Kylin Guardian Process, Kylin will also periodically check the status of Kylin Guardian Process. If Kylin detects the Kylin Guardian Process does not exist, it will automatically start it. The feature is enabled by default. If you want to disable it, you can add the configuration `kylin.guardian.ha-enabled=false` in `$$KYLIN_HOME/conf/kylin.properties`. The time delay of the first check is configured by the parameter `kylin.guardian.ha-check-init-delay` (Unit: minutes) which is 5 minutes by default, and the check interval is set by the parameter `kylin.guardian.ha-check-interval` (Unit: minutes) which is 1 minute by default.


#### Kylin OOM(Out of Memory) restarts automatically
Kylin Guardian Process supports restarting Kylin when the JVM of Kylin appears OOM.
