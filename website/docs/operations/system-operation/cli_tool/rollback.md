---
title: Rollback Tool
language: en
sidebar_label: Rollback Tool
pagination_label: Rollback Tool
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - rollback tool
draft: false
last_update:
    date: 08/12/2022
---

When a user error causes metadata or data loss, or when Kylin is unavailable due to an unknown issue, you can roll back to a specified moment with the rollback tool to ensure production stability.

> Note: The rollback tool is used in some emergency cases, please use this before read the materials carefully.

### Introduction {#tools_Introduction}

**How to Use** 

- Stop all Kylin services.

- Use tools to roll back

  ```bash
  $KYLIN_HOME/bin/rollback.sh  --project project_example --time '2020-09-01 15:20:19'
  ```

- Observe logs to identify differences of metadata such as project, model, user, Segment, task, etc.

- Complete the interaction and confirmation to ensure that you are aware of the impact of the rollback.

- After completed the rollback, start Kylin service.

##### Parameters：

- `-p，--project <arg> `: Project name [optional], `<arg>` is the project name.
- `-t, --time <arg>`: Historical time point to roll back [required]. `<arg>` is the specific time to roll back to, the format is ` yyyy-MM-dd HH: mm: ss`. The available value is the time point from the earliest backup version time to date.
- `--skip-check-data`: Skip checking whether the resource file is available [optional].

### Difference from Metadata Backup and Rollback Tool{#difference}

Kylin now offers metadata backup and restore tools that go some way to protecting metadata from loss. However, the tool has some limitations.

- There is no way to rewind to the specified moment, you can only rewind to the version that has been backed up in the past.
- The metadata backup tool is straightforward, and it is possible that metadata may not be available after being backed up. For example, some files are cleaned up as garbage because the metadata does not exist, and the corresponding backup metadata is not available.

### Configurations{#related_configurations}


The premise of using the rollback tool is that the resource data (cube data, dictionary data, snapshot data, etc.) must be guaranteed not to be deleted within the rollback time range. The retention period of the resource data involves two configurations

*  `kylin.storage.time-machine-enabled` After this configuration is enabled, the resources in the retention period will not be deleted in the Kylin service. After being enabled, the snapshot data retention time will be the same as the time configured in `kylin.storage.resource-survival-time-threshold`, the default value is False.
*  `kylin.storage.resource-survival-time-threshold` Resource data retention time, the default value is `7d`, unit description:` d` (day), `h` (hour),` m` (minute).

### Caution and Common Errors {#caution_and_common_errors}

The following are some errors and points of attention that may be encountered during the use of the tool

**Points to take attention**

- Using the rollback tool will roll back the state of the task execution to the state of the historical moment, and will restart the execution after the Kylin service is started.
- After the rollback tool configuration is turned on, more garbage files may be saved and more storage space may be token up. Using the garbage cleaning tool during the retention period cannot clean up the expired resource data during the retention period.
- During the execution of the tool, if it is run multiple times, each run will keep a backup of the current metadata in the `{working-dir}/_current_backup` directory, the file names are distinguished by time.
- The time specified by the user cannot be greater than the current time.
- All service nodes must be shut down before using the tool, otherwise it will cause data inconsistency.
- If the user manually deletes the dictionary data of the project and then regenerates the dictionary data again, using the rollback tool will cause the dictionary data and the index data to be inconsistent.
- After opening the `kylin.storage.time-machine-enabled` configuration after upgrading, users need to wait for a configured retention period before they can be rolled back.
- The user rolls back to the historical moment, and the snapshot data used is also the snapshot data of the historical moment instead of using the latest snapshot data.
- If the rollback time specified by the user is less than the minimum time of the metadata backup, the rollback cannot be performed.

**Possible error results**

- Using the rollback tool reverts the state of the task execution back to the historical moment, and the execution is triggered again when the Kylin is started.
- Turning on the time machine causes more junk files to be saved, taking up more storage space, and using the junk cleanup tool during the retention period does not clean up resource data that has expired during the retention period.
- During tool execution, if there are multiple runs, each run keeps a backup of the current metadata in the `{working-dir}/_current_backup` directory, distinguishing the file name by time.
- The time specified by the user cannot be greater than the current time.
- All service nodes must be turned off before using the tool, otherwise data inconsistencies will result.
- If a user manually deletes the dictionary data for an item and then regenerates the dictionary data, using the rollback tool can cause inconsistencies between the dictionary data and CUBE data.
- A user who has just upgraded a `kylin.storage.time-machine-enabled` configuration needs to wait until a configuration's retention period has passed before being guaranteed to roll back any time within the retention period.
- The user rolls back to the historical moment and the snapshot data used is also the snapshot data for the historical moment, not the latest snapshot data used.
- If the user-specified rollback time is less than the minimum time for metadata backup, it cannot be rolled back.
- `dectect port available failed` -> Failure to detect user ports requires shutting down the service nodes of the cluster.
- `check storage data available failed` -> Failed to detect resource file, user can use `--skip-check-data` parameter to force rollback。
- `restore current metadata failed, please restore the metadata database manually` -> The metadata rollback fails, and overwriting with the current backup also fails. Manual intervention is required to solve the problem. This situation must be handled carefully to avoid loss of metadata.
- The rollback scope of the rollback tool does not include historical recommendations and projects manually deleted by the user.

### Appendix {#appendix}

The following is a detailed process for the tool to perform rollback.

- Backup metadata
- Check if the cluster is stopped
- Find the snapshot file of metadata from the backup directory, and then replay the `auditlog` log to the time specified by the user
- Compare the metadata differences and remind the user
- Wait for confirmation
- Check if the resource referenced to by metadata is available
- Roll back the metadata. If the rollback fails, it will be overwritten with a backup of the current metadata
