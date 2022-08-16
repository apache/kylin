---
title: Log Rotate Configuration
language: en
sidebar_label: Log Rotate Configuration
pagination_label: Log Rotate Configuration
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - log rotate configuration
draft: false
last_update:
    date: 08/16/2022
---

The three log files, `shell.stderr`, `shell.stdout`, and `kylin.out` under the log directory `$KYLIN_HOME/logs/` of Kylin, trigger log rolling checks regularly by default.

> **Caution:** Any change of configurations below requires a restart to take effect. 

| Properties                               | Descript                        | Default              | Options |
|------------------------------------------| --------------------------------|----------------------|---------|
| kylin.env.max-keep-log-file-number       | Maximum number of files to keep for log rotate | 10                   |         |
| kylin.env.max-keep-log-file-threshold-mb | Log files are rotated when they grow bigger than this  | 256，whose unit is MB |         |
| kylin.env.log-rotate-check-cron          | The `crontab` time configuration                         | 33 * * * *           |         |
| kylin.env.log-rotate-enabled             | Whether to enable `crontab` to check log rotate               | true                 | false   |

### Default Regularly Rotate strategy

To use the default regularly rotate strategy, you need to set the parameter `kylin.env.log-rotate-enabled=true` (default), and also need to ensure that users running Kylin can use the `logrotate` and `crontab` commands to add a scheduled task.

When using the rotate strategy, Kylin will add or update `crontab` tasks according to the `kylin.env.log-rotate-check-cron` parameter on startup or restart, and remove the added `crontab` tasks on exit.

### Known Limitations
- If the default regularly rotate policy conditions are not met, Kylin will only trigger the log rolling check at startup. Every time the `kylin.sh start` command is executed, according to the parameter `kylin.env.max-keep-log-file-number` and `kylin.env.max-keep-log-file-threshold-mb` for log rolling. If Kylin runs for a long time, the log file may be too large.
- When using `crontab` to control log rotation, the rolling operation is implemented by the `logrotate` command. If the log file is too large, the log may be lost during the rotation.
