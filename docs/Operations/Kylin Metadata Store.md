Kylin organizes all of its metadata (including cube descriptions and instances, projects, inverted index description and instances, jobs, tables and dictionaries) as a hierarchy of files, that are stored in HBase. You can backup and restore these metadata by download to local file system and upload again.

Check the `conf/kylin.properties`

	kylin.metadata.url=kylin_metadata@hbase

This indicates that the metadata will be saved as a HTable called `kylin_metadata`. You can scan the HTable in HBase shell.

# Backup Metadata Store

Sometimes you want to backup Kylin's metadata. Below command downloads all metadata to local directory `~/meta_dump`.

	hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar  org.apache.kylin.common.persistence.ResourceTool  download  ~/meta_dump

Add `-Dexclude=/dict,/job_output,/table_snapshot` flag to the command to exclude certain metadata sub-directories.

# Restore Metadata Store

To restore a backup, first clean up the metadata store.

	hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar  org.apache.kylin.common.persistence.ResourceTool  reset  

Then upload the backup metadata from local file system.

	hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar  org.apache.kylin.common.persistence.ResourceTool  upload  ~/meta_dump


