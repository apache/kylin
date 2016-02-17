---
layout: docs2
title:  How to Upgrade
categories: howto
permalink: /docs2/howto/howto_upgrade.html
version: v1.2
since: v0.7.1
---

## Upgrade among v0.7.x and v1.x 

From v0.7.1 to latest v1.2, Kylin's metadata is backward compatible, the upgrade can be finished in couple of minutes:

#### 1. Backup metadata
Backup the Kylin metadata peridically is a good practice, and is highly suggested before upgrade; 

```
cd $KYLIN_HOME
./bin/metastore.sh backup
``` 
It will print the backup folder, take it down and make sure it will not be deleted before the upgrade finished. If there is no "metastore.sh", use HBase's snapshot command to do backup:

```
hbase shell
snapshot 'kylin_metadata', 'kylin_metadata_backup20150610'
```
Here 'kylin_metadata' is the default kylin metadata table name, replace it with the right table name of your Kylin;

#### 2. Install new Kylin and copy back "conf"
Download the new Kylin binary package from Kylin's download page; Extract it to a different folder other than current KYLIN_HOME; Before copy back the "conf" folder, do a compare and merge between the old and new kylin.properties to ensure newly introduced property will be kept.

#### 3. Stop old and start new Kylin instance
```
cd $KYLIN_HOME
./bin/kylin.sh stop
export KYLIN_HOME="<path_of_new_installation>"
cd $KYLIN_HOME
./bin/kylin.sh start
```

#### 4. Back-port if the upgrade is failed
If the new version couldn't startup and need back-port, shutdown it and then switch to the old KYLIN_HOME to start. Idealy that would return to the origin state. If the metadata is broken, restore it from the backup folder.

```
./bin/metastore.sh restore <path_of_metadata_backup>
```

## Upgrade from v0.6.x to v0.7.x 

In v0.7, Kylin refactored the metadata structure, for the new features like inverted-index and streaming; If you have cube created with v0.6 and want to keep in v0.7, a migration is needed; (Please skip v0.7.1 as
it has several compatible issues and the fix will be included in v0.7.2) Below is the steps;

#### 1. Backup v0.6 metadata
To avoid data loss in the migration, a backup at the very beginning is always suggested; You can use HBase's backup or snapshot command to achieve this; Here is a sample with snapshot:

```
hbase shell
snapshot 'kylin_metadata', 'kylin_metadata_backup20150610'
```

'kylin_metadata' is the default kylin metadata table name, replace it with the right table name of your Kylin;

#### 2. Dump v0.6 metadata to local file
This is also a backup method; As the migration tool is only tested with local file system, this step is must; All metadata need be downloaded, including snapshot, dictionary, etc;

```
hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-job.jar  org.apache.kylin.common.persistence.ResourceTool  download  ./meta_dump
```

(./meta_dump is the local folder that the metadata will be downloaded, change to name you preferred)

#### 3. Run CubeMetadataUpgrade to migrate the metadata
This step is to run the migration tool to parse the v0.6 metadata and then convert to v0.7 format; A verification will be performed in the last, and report error if some cube couldn't be migrated;

```
hbase org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-job.jar org.apache.kylin.job.CubeMetadataUpgrade ./meta_dump
```

1. The tool will not overwrite v0.6 metadata; It will create a new folder with "_v2" suffix in the same folder, in this case the "./meta_dump_v2" will be created;
2. By default this tool will only migrate the job history in last 30 days; If you want to keep elder job history, please tweak upgradeJobInstance() method by your own;
3. If you see _No error or warning messages; The migration is success_ , that's good; Otherwise please check the error/warning messages carefully;
4. For some problem you may need manually update the JSON file, to check whether the problem is gone, you can run a verify against the new metadata:

```
hbase org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-job.jar org.apache.kylin.job.CubeMetadataUpgrade ./meta_dump2 verify
```

#### 4. Upload the new metadata to HBase
Now the new format of metadata will be upload to the HBase to replace the old format; Stop Kylin, and then:

```
hbase org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-job.jar  org.apache.kylin.common.persistence.ResourceTool  reset
hbase org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-job.jar  org.apache.kylin.common.persistence.ResourceTool  upload  ./meta_dump_v2
```

#### 5. Update HTables to use new coprocessor
Kylin uses HBase coprocessor to do server side aggregation; When Kylin instance upgrades to V0.7, the HTables that created in V0.6 should also be updated to use the new coprocessor:

```
hbase org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-job.jar  org.apache.kylin.job.tools.DeployCoprocessorCLI ${KYLIN_HOME}/lib/kylin-coprocessor-x.x.x.jar
```

Done; Update your v0.7 Kylin configure to point to the same metadata HBase table, then start Kylin server; Check whether all cubes and other information are kept;