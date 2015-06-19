In v0.7, Kylin refactored the metadata structure, for the new features like inverted-index and streaming; If you have cube created with v0.6 and want to keep in v0.7, a migration is needed; (Please skip v0.7.1 as
it has several compatible issues and the fix will be included in v0.7.2) Below is the steps;

# Backup v0.6 metadata

To avoid any data loss in the migration, a backup at the very beginning is always suggested; You can use HBase's backup or snapshot command to achieve this; Here is a sample with snapshot:

    hbase shell
    snapshot 'kylin_metadata', 'kylin_metadata_backup20150610'

'kylin_metadata' is the default kylin metadata table name, replace it with the right table name of your Kylin;

# Dump v0.6 metadata to local file

This is also a backup method; As the migration tool is only tested with local file system, this step is must; All metadata need be downloaded, including snapshot, dictionary, etc;

	hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar  org.apache.kylin.common.persistence.ResourceTool  download  ./meta_dump

(./meta_dump is the local folder that the metadata will be downloaded, change to name you preferred)

# Run CubeMetadataUpgrade to migrate the metadata

This step is to run the migration tool to parse the v0.6 metadata and then convert to v0.7 format; A verification will be performed in the last, and report error if some cube couldn't be migrated;

    hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar org.apache.kylin.job.CubeMetadataUpgrade ./meta_dump

1. The tool will not overwrite v0.6 metadata; It will create a new folder with "_v2" suffix in the same folder, in this case the "./meta_dump_v2" will be created;
2. By default this tool will only migrate the job history in last 30 days; If you want to keep elder job history, please tweak upgradeJobInstance() method by your own;
3. If you see _No error or warning messages; The migration is success_ , that's good; Otherwise please check the error/warning messages carefully;
4. For some problem you may need manually update the JSON file, to check whether the problem is gone, you can run a verify against the new metadata:

    hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar org.apache.kylin.job.CubeMetadataUpgrade ./meta_dump2 verify

# Upload the new metadata to HBase

Now the new format of metadata will be upload to the HBase to replace the old format; Stop Kylin, and then:

	hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar  org.apache.kylin.common.persistence.ResourceTool  reset
	hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar  org.apache.kylin.common.persistence.ResourceTool  upload  ./meta_dump_v2

Done; Update your v0.7 Kylin configure to point to the same metadata HBase table, then start Kylin server; Check whether all cubes and other information are kept;