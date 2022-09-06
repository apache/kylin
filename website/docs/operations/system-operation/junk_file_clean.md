---
title: Junk File Cleanup
language: en
sidebar_label: Junk File Cleanup
pagination_label: Junk File Cleanup
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - junk file cleanup
draft: false
last_update:
    date: 08/12/2022
---

After Kylin runs for a period of time, the system may generate a certain number of junk files, which may occupy a large amount of storage space. At this time, junk cleaning is required.

Junk file cleaning can improve the stability and performance of the Kylin system. Effective junk file cleaning can not only save storage space, but also ensure the ecological health of the cluster where Kylin is located.


### Default recommended regular junk file cleaning method

By default, the system will automatically clean up junk every day at 0:00 AM.

- To modify the time and frequency of regular junk file cleaning, adjust the parameters in `$KYLIN_HOME/conf/kylin.properties`. The default configuration is `kylin.metadata.ops-cron=0 0 0 * * *`, which refers to junk file cleaning at 0:00 a.m. every day. The parameters from left to right in the configuration items represent: seconds, minutes, hours, Day, month, day of the week. By modifying the cron configuration, users can customize the junk file cleaning time, for example, every Saturday at 11 pm, the corresponding configuration should be changed to `kylin.metadata.ops-cron=0 0 23 * * 6`.

- The default 4-hour timeout for automatic junk file cleaning, and it will automatically terminate after the timeout. The default configuration is `kylin.metadata.ops-cron-timeout=4h`.

- Before the system regularly cleans up junk files, the metadata will be automatically backed up to the HDFS path `{kylin.env.hdfs-working-dir}/{MetadataIdentitiy}/_backup/{yyyy-MM-dd-HH-mm-ss}_backup/ metadata.zip`.

- The system regularly cleans up junk files and will not enter system maintenance mode.

- For more details on cron configuration, please refer to [Introduction to CronTrigger](http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html).

### Junk file cleanup range

The scope of junk file cleanup includes:
- Invalid or expired metadata:
  - Query history.
    - The total number of query history for all projects. The query history that exceeds this threshold number `kylin.query.queryhistory.project-max-size=10000000` (default) will be cleared.
    - The query history of a single project exceeds this threshold `kylin.query.queryhistory.project-max-size=1000000` (default) The query history will be cleared.
    - The query history time of all projects. The query history that exceeds this threshold `kylin.query.queryhistory.survival-time-threshold=30d` (default 30 days) will be cleared. This configuration also supports units: milliseconds ms, microseconds us, minutes m or min, hours h.
  - Invalid optimization suggestion table data.
  - Expired capacity billing metadata. Capacity billing information that exceeds this threshold `kylin.garbage.storage.sourceusage-survival-time-threshold=90d` (default 90 days) will be cleaned up.
  - Invalid or out-of-date item-related metadata.
    - `kylin.garbage.storage.executable-survival-time-threshold=30d` (default 30 days) above this threshold and completed metadata tasks are cleaned up.
  - Audit log. Audit logs that exceed this threshold `kylin.metadata.audit-log.max-size=500000` (default) will be cleaned up.
- Invalid or expired HDFS data:
  - Asynchronous query result file. HDFS asynchronous query result files that exceed this threshold `kylin.query.async.result-retain-days=7d` (default 7 days) will be cleaned up.
  - Invalid or expired files on HDFS. Include invalid or expired indexes, snapshots, dictionaries, etc.
    - Invalid files on HDFS that exceed this threshold `kylin.garbage.storage.cuboid-layout-survival-time-threshold=7d` (default 7 days) are cleaned up.
  - Low Usage indexes on HDFS.
    - Low usage storage refers to indexes whose usage frequency is lower than a certain threshold and data built under them within a certain time interval. You can configure the definition of low usage storage under a project in the project's Settings > Basic Settings > Index Optimization > Low Usage Storage .
    - If recommendation is turned off, indexes with low cost performance will be cleaned according to the index optimization strategy during junk file cleaning. You can also manually clean up by clicking the **Clear** button under **Dashboard > Storage Quota > Low Usage Storage**.
    - If recommendation is turned on, the cleanup of Low Usage storage will no longer be triggered during junk file cleaning, and the corresponding inefficient index will be converted to **model optimization suggestions**, and the button to clean up junk file will not appear in the dashboard.

  > Note: The default timed junk file cleaning method starts from Kylin 5.0 and later, will clean up invalid or expired HDFS data.

### Compatible with historically supported junk cleaning tools

> Note: In order to be compatible with the command-line tool cleanup that has been provided in history, the behavior of the previously provided tools has not changed. Users who have used this method can gradually abandon this method according to the actual situation. Users who are not using this tool could not pay attention to this section.

Kylin provides a junk file cleaning command line tool for checking and cleaning HDFS data, so as to ensure that the system is in a good running state. Please execute the following command in the terminal:

````sh
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.tool.routine.RoutineTool
````

When executing this command without any parameters, it will only list the data in HDFS that can be cleaned, but will not perform the actual cleaning action.

This command supports standard short and long parameters. The parameter descriptions are as follows:
- `-m, --metadata`: Perform metadata junk file cleaning.
- `-c, --cleanup`: Perform data junk file cleanup. Without this parameter, the tool will not make any modification to the HDFS data.
- `-p [project...], --projects=[project...]`: Specifies the projects to clean. When specifying multiple items, separate them with commas. Without this parameter, the tool will clean up all items.
- `-h, --help`: Print help information.
- `-r number`: The number of requests per second when accessing cloud environment object storage. `-r 10` means 10 requests per second. You can use this parameter to limit the frequency of requests for object storage in the cloud environment by the junk file cleaning tool to avoid errors due to exceeding the request frequency limit.
- `-t number`: The number of request retries when accessing the cloud environment object storage fails. `-t 3` means to retry 3 times.

**Note**: Whether this command executes metadata junk file cleaning through -m or data junk file cleaning through -c, Kylin will enter maintenance mode. If junk file cleaning is forcibly interrupted, you need to manually exit maintenance mode. Refer to [ Maintenance Mode](maintenance_mode.md).

In addition, from Kylin 5, the new command line tool `FastRoutineTool`
````sh
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.tool.routine.FastRoutineTool
````
The only difference compared to `RoutineTool` is that when performing data junk file cleaning with the `-c` parameter, it does not enter maintenance mode. Maintenance mode is still entered when performing metadata junk file cleanup via -m.
