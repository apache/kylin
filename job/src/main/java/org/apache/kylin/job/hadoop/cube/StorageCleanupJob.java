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

package org.apache.kylin.job.hadoop.cube;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author ysong1
 */
public class StorageCleanupJob extends AbstractHadoopJob {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false).withDescription("Delete the unused storage").create("delete");

    protected static final Logger log = LoggerFactory.getLogger(StorageCleanupJob.class);

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

        log.info("----- jobs args: " + Arrays.toString(args));
        try {
            options.addOption(OPTION_DELETE);
            parseOptions(options, args);

            log.info("options: '" + getOptionsAsString() + "'");
            log.info("delete option value: '" + getOptionValue(OPTION_DELETE) + "'");
            delete = Boolean.parseBoolean(getOptionValue(OPTION_DELETE));

            Configuration conf = HBaseConfiguration.create(getConf());

            cleanUnusedHdfsFiles(conf);
            cleanUnusedHBaseTables(conf);
            cleanUnusedIntermediateHiveTable(conf);

            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            throw e;
        }
    }

    private void cleanUnusedHBaseTables(Configuration conf) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        IIManager iiManager = IIManager.getInstance(KylinConfig.getInstanceFromEnv());

        // get all kylin hbase tables
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
        String tableNamePrefix = IRealizationConstants.SharedHbaseStorageLocationPrefix;
        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");
        List<String> allTablesNeedToBeDropped = new ArrayList<String>();
        for (HTableDescriptor desc : tableDescriptors) {
            String host = desc.getValue(IRealizationConstants.HTableTag);
            if (KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix().equalsIgnoreCase(host)) {
                //only take care htables that belongs to self
                allTablesNeedToBeDropped.add(desc.getTableName().getNameAsString());
            }
        }

        // remove every segment htable from drop list
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String tablename = seg.getStorageLocationIdentifier();
                if (allTablesNeedToBeDropped.contains(tablename)) {
                    allTablesNeedToBeDropped.remove(tablename);
                    log.info("Exclude table " + tablename + " from drop list, as the table belongs to cube " + cube.getName() + " with status " + cube.getStatus());
                }
            }
        }

        // remove every ii segment htable from drop list
        for (IIInstance ii : iiManager.listAllIIs()) {
            for (IISegment seg : ii.getSegments()) {
                String tablename = seg.getStorageLocationIdentifier();

                if (allTablesNeedToBeDropped.contains(tablename)) {
                    allTablesNeedToBeDropped.remove(tablename);
                    log.info("Exclude table " + tablename + " from drop list, as the table belongs to ii " + ii.getName() + " with status " + ii.getStatus());
                }
            }
        }

        if (delete == true) {
            // drop tables
            for (String htableName : allTablesNeedToBeDropped) {
                log.info("Deleting HBase table " + htableName);
                if (hbaseAdmin.tableExists(htableName)) {
                    if (hbaseAdmin.isTableEnabled(htableName)) {
                        hbaseAdmin.disableTable(htableName);
                    }

                    hbaseAdmin.deleteTable(htableName);
                    log.info("Deleted HBase table " + htableName);
                } else {
                    log.info("HBase table" + htableName + " does not exist");
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
                String kylinJobPath = engineConfig.getHdfsWorkingDirectory() + "/" + path;
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
                log.info("Remove " + path + " from deletion list, as the path belongs to job " + jobId + " with status " + state);
            }
        }

        // remove every segment working dir from deletion list
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String path = JobInstance.getJobWorkingDir(jobUuid, engineConfig.getHdfsWorkingDirectory());
                    allHdfsPathsNeedToBeDeleted.remove(path);
                    log.info("Remove " + path + " from deletion list, as the path belongs to segment " + seg + " of cube " + cube.getName());
                }
            }
        }

        if (delete == true) {
            // remove files
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                log.info("Deleting hdfs path " + hdfsPath);
                Path p = new Path(hdfsPath);
                if (fs.exists(p) == true) {
                    fs.delete(p, true);
                    log.info("Deleted hdfs path " + hdfsPath);
                } else {
                    log.info("Hdfs path " + hdfsPath + "does not exist");
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

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StorageCleanupJob(), args);
        System.exit(exitCode);
    }
}
