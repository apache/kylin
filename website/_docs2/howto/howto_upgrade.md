---
layout: docs2
title:  How to Upgrade
categories: howto
permalink: /docs2/howto/howto_upgrade.html
version: v2.0
since: v2.0
---

## Upgrade from v1.x to v2.0 

From v1.x to v2.0, Kylin's cube data is backward compatible, but metadata has been refactored as new schema, to support new features on cubing and query enhancement. So if you want to deploy v2.0 on your v1.x base, you need to upgrade the metadata as following steps:

#### 1. Backup metadata on v1.x
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

#### 2. Stop Kylin v1.x instance
Before deploying Kylin v2.0 instance, you need to stop the old instance. Note that end users cannot access kylin service from this point.

```
$KYLIN_HOME/bin/kylin.sh stop
```
#### 3. Install Kylin v2.0 and copy back "conf"
Download the new Kylin v2.0 binary package from Kylin's download page; Extract it to a different folder other than current KYLIN_HOME; Before copy back the "conf" folder, do a compare and merge between the old and new kylin.properties to ensure newly introduced property will be kept.

#### (Optional) 4. Upgrading metadata will not bring new features of v2.0 to existing cube built with v1.x engine. If you want to leverage those features, please refer to [Highlight]() part.

#### 5. Automaticly upgrade metadata
Kylin v2.0 package provides a script for metadata automaticly upgrade. In this upgrade, empty cubes will be updated to v2.0 version and all new features are enabled for them. But those non-empty cubes are not able to use those new features.

```
export KYLIN_HOME="<path_of_new_installation>"
$KYLIN_HOME/bin/upgrade_v2.sh
```
After this, the metadata in hbase table has been applied with new metadata schema.

#### 6. Start Kylin v2.0 instance
```
$KYLIN_HOME/bin/kylin.sh start
```
Check the log and open web UI to see if the upgrade succeeded.

## Rollback if the upgrade is failed
If the new version couldn't startup normally, you need to roll back to orignal v1.x version. The steps are as followed:

#### 1. Stop Kylin v2.0 instance

```
$KYLIN_HOME/bin/kylin.sh stop
```
#### 2. Rstore 1.x metadata from backup folder

```
export KYLIN_HOME="<path_of_1.x_installation>"
$KYLIN_HOME/bin/metastore.sh restore <backup_folder>
``` 
#### 3. Deploy coprocessor of v1.x
Since coprocessor of used HTable are upgraded as v2.0, you need to manually downgrade them with this command.

```
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.job.tools.DeployCoprocessorCLI $KYLIN_HOME/lib/kylin-coprocessor*.jar -all
```

#### 4. Start Kylin v1.x instance
 
```
$KYLIN_HOME/bin/kylin.sh start
```

## Highlights
Since old cubes built with v1.x cannot leverage new features of v2.0. But if you must have them on your cubes, you can choose one of these solutions:
#### 1. Rebuilt cubes
If the cost of rebuilding is acceptable, if you purge the cube before Step 4(Running Upgrade Scripts). After upgrade done, you need to manually rebuilt those segments by yourself.
#### 2. Use hybrid model
If you don't want to rebuild any cube, but want to leverage new features for  new data. You can use hybrid model, which contains not only your old cube, but also an empty cube which has same model with the old one. For the empty cube, you can do incremental building with v2 features. For the old cube, you can refresh existing segments only.

Here is the command to create hybrid model:

```
export KYLIN_HOME="<path_of_v2.0_installation>"
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.storage.hbase.util.ExtendCubeToHybridCLI <project_name> <cube_name>
```