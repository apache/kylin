---
layout: docs15
title:  Upgrade from old version
categories: howto
permalink: /docs15/howto/howto_upgrade.html
since: v1.5.1
---

## Upgrade from prior 1.5 to v1.5.1

Kylin 1.5.1 is not backward compatible in terms of metadata. (The built cubes are still functional after metadata upgrade) So if you want to deploy v1.5.x code on your prior 1.5 metadata store (in the following text we'll use v1.3.0 as example), you need to upgrade the metadata as following steps:

#### 1. Backup metadata on v1.3.0

To avoid data loss during the upgrade, a backup at the very beginning is always suggested. In case of upgrade failure, you can roll back to original state with the backup.

```
export KYLIN_HOME="<path_of_1_3_0_installation>" 
$KYLIN_HOME/bin/metastore.sh backup
``` 

It will print the backup folder, write it down and make sure it will not be deleted before the upgrade finished. We'll later reference this folder as BACKUP_FOLDER.

#### 2. Stop Kylin v1.3.0 instance

Before deploying Kylin v1.5.1 instance, you need to stop the old instance. Note that end users cannot access kylin service from this point.

```
$KYLIN_HOME/bin/kylin.sh stop
```

#### 3. Install Kylin v1.5.1 and copy back "conf"

Download the new Kylin v1.5.1 binary package from Kylin's download page; Extract it to a different folder other than current KYLIN_HOME; Before copy back the "conf" folder, do a compare and merge between the old and new kylin.properties to ensure newly introduced property will be kept.

#### 4. Automaticly upgrade metadata

Kylin v1.5.1 package provides a tool for metadata automaticly upgrade. In this upgrade, all cubes' metadata will be updated to v1.5.1 compatible format. The treatment of empty cubes and non-empty cubes are different though. For empty cubes, we'll upgrade the cube's storage engine and cubing engine to the latest, so that new features will be enabled for the new coming cubing jobs. But those non-empty cubes carries legacy cube segments, so we'll remain its old storage engine and cubing engine. In other word, the non-empty cubes will not enjoy the performance and storage wise gains released in 1.5.x versions. Check the last section to see how to deal with non-empty cubes.
To avoid corrupting the metadata store, metadata upgrade is performed against a copy of the local metadata backup, i.e. a copy of BACKUP_FOLDER.

```
export KYLIN_HOME="<path_of_1_5_0_installation>" 
$KYLIN_HOME/bin/kylin.sh  org.apache.kylin.cube.upgrade.entry.CubeMetadataUpgradeEntry_v_1_5_1 <path_of_BACKUP_FOLDER>
```

The above commands will first copy the BACKUP_FOLDER to ${BACKUP_FOLDER}_workspace, and perform the upgrade against the workspace folder at local disk. Check the output, if no error happened, then you have a 1.5.1 compatible metadata saved in the workspace folder now. Otherwise the upgrade process is not successful, please don't take further actions. 
The next thing to do is to override the metatdata store with the new metadata in workspace:

```
$KYLIN_HOME/bin/metastore.sh reset
$KYLIN_HOME/bin/metastore.sh restore <path_of_workspace>
```

The last thing to do is to upgrade all cubes' coprocessor:

```
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI $KYLIN_HOME/lib/kylin-coprocessor*.jar all
```

#### 6. Start Kylin v1.5.1 instance

```
$KYLIN_HOME/bin/kylin.sh start
```

Check the log and open web UI to see if the upgrade succeeded.

## Rollback if the upgrade is failed

If the new version couldn't startup normally, you need to roll back to orignal v1.3.0 version. The steps are as followed:

#### 1. Stop Kylin v1.5.1 instance

```
$KYLIN_HOME/bin/kylin.sh stop
```

#### 2. Restore 1.3.0 metadata from backup folder

```
export KYLIN_HOME="<path_of_1_3_0_installation>"
$KYLIN_HOME/bin/metastore.sh reset
$KYLIN_HOME/bin/metastore.sh restore <path_of_BACKUP_FOLDER>
``` 

#### 3. Deploy coprocessor of v1.3.0

Since coprocessor of used HTable are upgraded as v1.5.1, you need to manually downgrade them with this command.

```
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.job.tools.DeployCoprocessorCLI $KYLIN_HOME/lib/kylin-coprocessor*.jar all
```

#### 4. Start Kylin v1.3.0 instance

```
$KYLIN_HOME/bin/kylin.sh start
```

## For non-empty cubes

Since old cubes built with v1.3.0 cannot leverage new features of v1.5.1. But if you must have them on your cubes, you can choose one of these solutions:

#### 1. Rebuild cubes

This is the simplest way: If the cost of rebuilding is acceptable, if you purge the cube before Metadata Upgrade. After upgrade done, you need to manually rebuild those segments by yourself.

#### 2. Use hybrid model

If you can't rebuild any segments, but want to leverage new features for new segments. You can use hybrid model, which contains not only your old segments, but also an new empty cube which has same model with the old one. For the empty cube, you can do incremental building with new v1.5.x features. For the old cube, you can refresh existing segments only.

Here is the command to create hybrid model:

```
export KYLIN_HOME="<path_of_1_5_0_installation>"
$KYLIN_HOME/bin/kylin.sh org.apache.kylin.storage.hbase.util.ExtendCubeToHybridCLI <project_name> <cube_name>
```
