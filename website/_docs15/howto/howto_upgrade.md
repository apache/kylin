---
layout: docs15
title:  How to Upgrade
categories: howto
permalink: /docs15/howto/howto_upgrade.html
since: v1.5
---

## Upgrade from v1.3 to v1.5

From v1.3 to v1.5, Kylin's cube data is backward compatible, but metadata has been refactored as new schema, to support new features on cubing and query enhancement. So if you want to deploy v1.5 on your v1.3 base, you need to upgrade the metadata as following steps:

#### 1. Backup metadata on v1.3
To avoid data loss during the upgrade, a backup at the very beginning is always suggested. In case of upgrade failure, you can roll back to original state with the backup.

```
$KYLIN_HOME/bin/metastore.sh backup
``` 
It will print the backup folder, take it down and make sure it will not be deleted before the upgrade finished. If there is no "metastore.sh", you can use HBase's snapshot command to do backup:

```
hbase shell
snapshot 'kylin_metadata', 'kylin_metadata_backup20160101'
```
Here 'kylin_metadata' is the default kylin metadata table name, replace it with the right table name of your Kylin metastore.

#### 2. Stop Kylin v1.3 instance
Before deploying Kylin v1.5 instance, you need to stop the old instance. Note that end users cannot access kylin service from this point.

```
$KYLIN_HOME/bin/kylin.sh stop
```
#### 3. Install Kylin v1.5 and copy back "conf"
Download the new Kylin v1.5 binary package from Kylin's download page; Extract it to a different folder other than current KYLIN_HOME; Before copy back the "conf" folder, do a compare and merge between the old and new kylin.properties to ensure newly introduced property will be kept.

#### (Optional) 4. Upgrading metadata will not bring new features of v1.5 to existing cube built with v1.3 engine. If you want to leverage those features, please refer to [Highlight]() part.

#### 5. Automaticly upgrade metadata
Kylin v1.5 package provides a script for metadata automaticly upgrade. In this upgrade, empty cubes will be updated to v1.5 version and all new features are enabled for them. But those non-empty cubes are not able to use those new features.

```
export KYLIN_HOME="<path_of_new_installation>"
$KYLIN_HOME/bin/upgrade_v2.sh
```
After this, the metadata in hbase table has been applied with new metadata schema.

#### 6. Start Kylin v1.5 instance
```
$KYLIN_HOME/bin/kylin.sh start
```
Check the log and open web UI to see if the upgrade succeeded.

## Rollback if the upgrade is failed
If the new version couldn't startup normally, you need to roll back to orignal v1.3 version. The steps are as followed:

#### 1. Stop Kylin v1.5 instance

```
$KYLIN_HOME/bin/kylin.sh stop
```
#### 2. Restore 1.3 metadata from backup folder

```
export KYLIN_HOME="<path_of_1.3_installation>"
$KYLIN_HOME/bin/metastore.sh restore <backup_folder>
``` 
#### 3. Deploy coprocessor of v1.3
Since coprocessor of used HTable are upgraded as v1.5, you need to manually downgrade them with this command.

```
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.job.tools.DeployCoprocessorCLI $KYLIN_HOME/lib/kylin-coprocessor*.jar -all
```

#### 4. Start Kylin v1.3 instance
 
```
$KYLIN_HOME/bin/kylin.sh start
```

## Highlights
Since old cubes built with v1.3 cannot leverage new features of v1.5. But if you must have them on your cubes, you can choose one of these solutions:
#### 1. Rebuilt cubes
If the cost of rebuilding is acceptable, if you purge the cube before Step 4(Running Upgrade Scripts). After upgrade done, you need to manually rebuilt those segments by yourself.
#### 2. Use hybrid model
If you don't want to rebuild any cube, but want to leverage new features for  new data. You can use hybrid model, which contains not only your old cube, but also an empty cube which has same model with the old one. For the empty cube, you can do incremental building with v2 features. For the old cube, you can refresh existing segments only.

Here is the command to create hybrid model:

```
export KYLIN_HOME="<path_of_v1.5_installation>"
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.storage.hbase.util.ExtendCubeToHybridCLI <project_name> <cube_name>
```