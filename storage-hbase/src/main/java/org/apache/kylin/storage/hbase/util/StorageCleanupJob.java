/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.storage.hbase.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageCleanupJob extends AbstractHadoopJob {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false).withDescription("Delete the unused storage").create("delete");

    protected static final Logger logger = LoggerFactory.getLogger(StorageCleanupJob.class);

    public static final long TIME_THREADSHOLD = 2 * 24 * 3600 * 1000l; // 2 days

    boolean delete = false;

    protected static ExecutableManager executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        logger.info("jobs args: " + Arrays.toString(args));
        try {
            options.addOption(OPTION_DELETE);
            parseOptions(options, args);

            logger.info("options: '" + getOptionsAsString() + "'");
            logger.info("delete option value: '" + getOptionValue(OPTION_DELETE) + "'");
            delete = Boolean.parseBoolean(getOptionValue(OPTION_DELETE));

            Configuration conf = HBaseConfiguration.create(getConf());

            cleanUnusedIntermediateHiveTable(conf);
            cleanUnusedHdfsFiles(conf);
            cleanUnusedHBaseTables(conf);

            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    private void cleanUnusedHBaseTables(Configuration conf) throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        IIManager iiManager = IIManager.getInstance(KylinConfig.getInstanceFromEnv());

        // get all kylin hbase tables
        Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
        Admin hbaseAdmin = conn.getAdmin();
        String tableNamePrefix = IRealizationConstants.SharedHbaseStorageLocationPrefix;
        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");
        List<String> allTablesNeedToBeDropped = new ArrayList<String>();
        for (HTableDescriptor desc : tableDescriptors) {
            String host = desc.getValue(IRealizationConstants.HTableTag);
            String creationTime = desc.getValue(IRealizationConstants.HTableCreationTime);
            if (KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix().equalsIgnoreCase(host)) {
                //only take care htables that belongs to self, and created more than 2 days
                if (StringUtils.isEmpty(creationTime) || (System.currentTimeMillis() - Long.valueOf(creationTime) > TIME_THREADSHOLD)) {
                    allTablesNeedToBeDropped.add(desc.getTableName().getNameAsString());
                } else {
                    logger.info("Exclude table " + desc.getTableName().getNameAsString() + " from drop list, as it is newly created");
                }
            }
        }

        // remove every segment htable from drop list
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String tablename = seg.getStorageLocationIdentifier();
                if (allTablesNeedToBeDropped.contains(tablename)) {
                    allTablesNeedToBeDropped.remove(tablename);
                    logger.info("Exclude table " + tablename + " from drop list, as the table belongs to cube " + cube.getName() + " with status " + cube.getStatus());
                }
            }
        }

        // remove every ii segment htable from drop list
        for (IIInstance ii : iiManager.listAllIIs()) {
            for (IISegment seg : ii.getSegments()) {
                String tablename = seg.getStorageLocationIdentifier();

                if (allTablesNeedToBeDropped.contains(tablename)) {
                    allTablesNeedToBeDropped.remove(tablename);
                    logger.info("Exclude table " + tablename + " from drop list, as the table belongs to ii " + ii.getName() + " with status " + ii.getStatus());
                }
            }
        }

        if (delete == true) {
            // drop tables
            for (String htableName : allTablesNeedToBeDropped) {
                logger.info("Deleting HBase table " + htableName);
                if (hbaseAdmin.tableExists(TableName.valueOf(htableName))) {
                    if (hbaseAdmin.isTableEnabled(TableName.valueOf(htableName))) {
                        hbaseAdmin.disableTable(TableName.valueOf(htableName));
                    }

                    hbaseAdmin.deleteTable(TableName.valueOf(htableName));
                    logger.info("Deleted HBase table " + htableName);
                } else {
                    logger.info("HBase table" + htableName + " does not exist");
                }
            }
        } else {
            System.out.println("--------------- Tables To Be Dropped ---------------");
            for (String htableName : allTablesNeedToBeDropped) {
                System.out.println(htableName);
            }
            System.out.println("----------------------------------------------------");
        }

        hbaseAdmin.close();
    }

    private void cleanUnusedHdfsFiles(Configuration conf) throws IOException {
        JobEngineConfig engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

        FileSystem fs = FileSystem.get(conf);
        List<String> allHdfsPathsNeedToBeDeleted = new ArrayList<String>();
        // GlobFilter filter = new
        // GlobFilter(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
        // + "/kylin-.*");
        FileStatus[] fStatus = fs.listStatus(new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()));
        for (FileStatus status : fStatus) {
            String path = status.getPath().getName();
            // System.out.println(path);
            if (path.startsWith(JobInstance.JOB_WORKING_DIR_PREFIX)) {
                String kylinJobPath = engineConfig.getHdfsWorkingDirectory() + path;
                allHdfsPathsNeedToBeDeleted.add(kylinJobPath);
            }
        }

        List<String> allJobs = executableManager.getAllJobIds();
        for (String jobId : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate files
            final ExecutableState state = executableManager.getOutput(jobId).getState();
            if (!state.isFinalState()) {
                String path = JobInstance.getJobWorkingDir(jobId, engineConfig.getHdfsWorkingDirectory());
                allHdfsPathsNeedToBeDeleted.remove(path);
                logger.info("Remove " + path + " from deletion list, as the path belongs to job " + jobId + " with status " + state);
            }
        }

        // remove every segment working dir from deletion list
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String path = JobInstance.getJobWorkingDir(jobUuid, engineConfig.getHdfsWorkingDirectory());
                    allHdfsPathsNeedToBeDeleted.remove(path);
                    logger.info("Remove " + path + " from deletion list, as the path belongs to segment " + seg + " of cube " + cube.getName());
                }
            }
        }

        if (delete == true) {
            // remove files
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                logger.info("Deleting hdfs path " + hdfsPath);
                Path p = new Path(hdfsPath);
                if (fs.exists(p) == true) {
                    fs.delete(p, true);
                    logger.info("Deleted hdfs path " + hdfsPath);
                } else {
                    logger.info("Hdfs path " + hdfsPath + "does not exist");
                }
            }
        } else {
            System.out.println("--------------- HDFS Path To Be Deleted ---------------");
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                System.out.println(hdfsPath);
            }
            System.out.println("-------------------------------------------------------");
        }

    }

    private void cleanUnusedIntermediateHiveTable(Configuration conf) throws IOException {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        final CliCommandExecutor cmdExec = config.getCliCommandExecutor();
        final int uuidLength = 36;
        
        final String useDatabaseHql = "USE " + config.getHiveDatabaseForIntermediateTable() + ";";
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(useDatabaseHql);
        hiveCmdBuilder.addStatement("show tables " + "\'kylin_intermediate_*\'" + "; ");

        Pair<Integer, String> result = cmdExec.execute(hiveCmdBuilder.build());

        String outputStr = result.getSecond();
        BufferedReader reader = new BufferedReader(new StringReader(outputStr));
        String line = null;
        List<String> allJobs = executableManager.getAllJobIds();
        List<String> allHiveTablesNeedToBeDeleted = new ArrayList<String>();
        List<String> workingJobList = new ArrayList<String>();

        for (String jobId : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate table
            final ExecutableState state = executableManager.getOutput(jobId).getState();

            if (!state.isFinalState()) {
                workingJobList.add(jobId);
                logger.info("Remove intermediate hive table with job id " + jobId + " with job status " + state);
            }
        }

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("kylin_intermediate_")) {
                boolean isNeedDel = false;
                String uuid = line.substring(line.length() - uuidLength, line.length());
                uuid = uuid.replace("_", "-");
                //Check whether it's a hive table in use
                if (allJobs.contains(uuid) && !workingJobList.contains(uuid)) {
                    isNeedDel = true;
                }

                if (isNeedDel) {
                    allHiveTablesNeedToBeDeleted.add(line);
                }
            }
        }

        if (delete == true) {
            hiveCmdBuilder.reset();
            hiveCmdBuilder.addStatement(useDatabaseHql);
            for (String delHive : allHiveTablesNeedToBeDeleted) {
                hiveCmdBuilder.addStatement("drop table if exists " + delHive + "; ");
                logger.info("Remove " + delHive + " from hive tables.");
            }

            try {
                cmdExec.execute(hiveCmdBuilder.build());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("------ Intermediate Hive Tables To Be Dropped ------");
            for (String hiveTable : allHiveTablesNeedToBeDeleted) {
                System.out.println(hiveTable);
            }
            System.out.println("----------------------------------------------------");
        }

        if (reader != null)
            reader.close();
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StorageCleanupJob(), args);
        System.exit(exitCode);
    }
}
