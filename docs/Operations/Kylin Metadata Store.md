Kylin organizes all of its metadata(including cube descriptions and instances, projects, inverted index description and instances, jobs, tables and dictionaries) as a hierarchy file system. However, Kylin uses hbase to store it, rather than normal file system. If you check your kylin configuration file(kylin.properties) you will find such a line:

`# The metadata store in hbase`
`kylin.metadata.url=kylin_metadata@hbase:sandbox.hortonworks.com:2181:/hbase-unsecure`

This indicates that the metadata will be saved as a htable called `kylin_metadata`. You can scan the htable in hbase shell to check it out.

# Backup Metadata Store

Sometimes you need to backup the Kylin's Metadata Store from hbase to your disk file system.
In such cases, assuming you're on the hadoop CLI(or sandbox) where you deployed Kylin, you can use:

	mkdir ~/meta_dump

	hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar org.apache.kylin.common.persistence.ResourceTool  copy ${KYLIN_HOME}/conf/kylin.properties ~/meta_dump 

to dump your metadata to your local folder ~/meta_dump.

# Restore Metadata Store

In case you find your metadata store messed up, and you want to restore to a previous backup:

first clean up the metadata store:

	hbase  org.apache.hadoop.util.RunJar ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar org.apache.kylin.common.persistence.ResourceTool  reset  

then upload the backup metadata in ~/meta_dump to Kylin's metadata store:

	hbase  org.apache.hadoop.util.RunJar  ${KYLIN_HOME}/lib/kylin-job-x.x.x-SNAPSHOT-job.jar org.apache.kylin.common.persistence.ResourceTool  copy ~/meta_dump ${KYLIN_HOME}/conf/kylin.properties  


