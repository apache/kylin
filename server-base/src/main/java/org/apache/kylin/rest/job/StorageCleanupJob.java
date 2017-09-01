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

package org.apache.kylin.rest.job;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public class StorageCleanupJob extends AbstractApplication {

    @SuppressWarnings("static-access")
    protected static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false)
            .withDescription("Delete the unused storage").create("delete");
    protected static final Option OPTION_FORCE = OptionBuilder.withArgName("force").hasArg().isRequired(false)
            .withDescription("Warning: will delete all kylin intermediate hive tables").create("force");

    protected static final Logger logger = LoggerFactory.getLogger(StorageCleanupJob.class);
    public static final int deleteTimeout = 10; // Unit minute

    protected boolean delete = false;
    protected boolean force = false;
    protected static ExecutableManager executableManager = ExecutableManager
            .getInstance(KylinConfig.getInstanceFromEnv());

    protected void cleanUnusedHBaseTables() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if ("hbase".equals(config.getMetadataUrl().getScheme())) {
            try {
                // use reflection to isolate NoClassDef errors when HBase is not available
                Class hbaseCleanUpUtil = Class.forName("org.apache.kylin.rest.job.StorageCleanJobHbaseUtil");
                Method cleanUnusedHBaseTables = hbaseCleanUpUtil.getDeclaredMethod("cleanUnusedHBaseTables",
                        boolean.class, int.class);
                cleanUnusedHBaseTables.invoke(hbaseCleanUpUtil, delete, deleteTimeout);
            } catch (Throwable e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DELETE);
        options.addOption(OPTION_FORCE);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("options: '" + optionsHelper.getOptionsAsString() + "'");
        logger.info("delete option value: '" + optionsHelper.getOptionValue(OPTION_DELETE) + "'");
        logger.info("force option value: '" + optionsHelper.getOptionValue(OPTION_FORCE) + "'");
        delete = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_DELETE));
        force = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_FORCE));
        cleanUnusedIntermediateHiveTable();
        cleanUnusedHdfsFiles();
        cleanUnusedHBaseTables();
    }

    private void cleanUnusedHdfsFiles() throws IOException {
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        JobEngineConfig engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

        FileSystem fs = HadoopUtil.getWorkingFileSystem(conf);
        List<String> allHdfsPathsNeedToBeDeleted = new ArrayList<String>();
        // GlobFilter filter = new
        // GlobFilter(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
        // + "/kylin-.*");
        // TODO: when first use, /kylin/kylin_metadata does not exist.
        try {
            FileStatus[] fStatus = fs.listStatus(new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()));
            for (FileStatus status : fStatus) {
                String path = status.getPath().getName();
                // System.out.println(path);
                if (path.startsWith("kylin-")) {
                    String kylinJobPath = engineConfig.getHdfsWorkingDirectory() + path;
                    allHdfsPathsNeedToBeDeleted.add(kylinJobPath);
                }
            }
        } catch (FileNotFoundException e) {
            logger.info("Working Directory does not exist on HDFS. ");
        }

        List<String> allJobs = executableManager.getAllJobIds();
        for (String jobId : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate files
            final ExecutableState state = executableManager.getOutput(jobId).getState();
            if (!state.isFinalState()) {
                String path = JobBuilderSupport.getJobWorkingDir(engineConfig.getHdfsWorkingDirectory(), jobId);
                allHdfsPathsNeedToBeDeleted.remove(path);
                logger.info("Skip " + path + " from deletion list, as the path belongs to job " + jobId
                        + " with status " + state);
            }
        }

        // remove every segment working dir from deletion list
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String path = JobBuilderSupport.getJobWorkingDir(engineConfig.getHdfsWorkingDirectory(), jobUuid);
                    allHdfsPathsNeedToBeDeleted.remove(path);
                    logger.info("Skip " + path + " from deletion list, as the path belongs to segment " + seg
                            + " of cube " + cube.getName());
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

    private void cleanUnusedIntermediateHiveTable() throws Exception {
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        JobEngineConfig engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        final CliCommandExecutor cmdExec = config.getCliCommandExecutor();
        final int uuidLength = 36;
        final String preFix = "kylin_intermediate_";
        final String uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

        ISourceMetadataExplorer explr = SourceFactory.getDefaultSource().getSourceMetadataExplorer();
        List<String> hiveTableNames = explr.listTables(config.getHiveDatabaseForIntermediateTable());
        Iterable<String> kylinIntermediates = Iterables.filter(hiveTableNames, new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return input != null && input.startsWith("kylin_intermediate_");
            }
        });

        List<String> allJobs = executableManager.getAllJobIds();
        List<String> allHiveTablesNeedToBeDeleted = new ArrayList<String>();
        List<String> workingJobList = new ArrayList<String>();
        Map<String, String> segmentId2JobId = Maps.newHashMap();

        StringBuilder sb = new StringBuilder();
        for (String jobId : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate table
            final ExecutableState state = executableManager.getOutput(jobId).getState();
            if (!state.isFinalState()) {
                workingJobList.add(jobId);
                sb.append(jobId).append("(").append(state).append("), ");
            }

            try {
                String segmentId = getSegmentIdFromJobId(jobId);
                if (segmentId != null) {//some jobs are not cubing jobs 
                    segmentId2JobId.put(segmentId, jobId);
                }
            } catch (Exception ex) {
                logger.warn("Failed to find segment ID from job ID " + jobId + ", ignore it");
                // some older version job metadata may fail to read, ignore it
            }
        }
        logger.info("Working jobIDs: " + workingJobList);

        for (String line : kylinIntermediates) {
            logger.info("Checking table " + line);

            if (!line.startsWith(preFix))
                continue;

            if (force == true) {
                logger.warn("Warning: will delete all intermediate hive tables!!!!!!!!!!!!!!!!!!!!!!");
                allHiveTablesNeedToBeDeleted.add(line);
                continue;
            }

            boolean isNeedDel = true;

            if (line.length() < preFix.length() + uuidLength) {
                logger.info("Skip deleting because length is not qualified");
                continue;
            }

            String uuid = line.substring(line.length() - uuidLength, line.length());
            uuid = uuid.replace("_", "-");
            final Pattern UUID_PATTERN = Pattern.compile(uuidPattern);

            if (!UUID_PATTERN.matcher(uuid).matches()) {
                logger.info("Skip deleting because pattern doesn't match");
                continue;
            }

            //Some intermediate table ends with job's uuid
            if (allJobs.contains(uuid)) {
                isNeedDel = !workingJobList.contains(uuid);
            } else if (isTableInUse(uuid, workingJobList)) {
                logger.info("Skip deleting because the table is in use");
                isNeedDel = false;
            }

            if (isNeedDel) {
                allHiveTablesNeedToBeDeleted.add(line);
            }
        }

        if (delete == true) {

            try {
                final String useDatabaseHql = "USE " + config.getHiveDatabaseForIntermediateTable() + ";";
                final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
                hiveCmdBuilder.addStatement(useDatabaseHql);
                for (String delHive : allHiveTablesNeedToBeDeleted) {
                    hiveCmdBuilder.addStatement("drop table if exists " + delHive + "; ");
                    logger.info("Remove " + delHive + " from hive tables.");
                }
                cmdExec.execute(hiveCmdBuilder.build());

                //if kylin.source.hive.keep-flat-table, some intermediate table might be kept 
                //delete external path
                for (String tableToDelete : allHiveTablesNeedToBeDeleted) {
                    String uuid = tableToDelete.substring(tableToDelete.length() - uuidLength, tableToDelete.length());
                    String segmentId = uuid.replace("_", "-");

                    if (segmentId2JobId.containsKey(segmentId)) {
                        String path = JobBuilderSupport.getJobWorkingDir(engineConfig.getHdfsWorkingDirectory(),
                                segmentId2JobId.get(segmentId)) + "/" + tableToDelete;
                        Path externalDataPath = new Path(path);
                        FileSystem fs = HadoopUtil.getWorkingFileSystem();
                        if (fs.exists(externalDataPath)) {
                            fs.delete(externalDataPath, true);
                            logger.info("Hive table {}'s external path {} deleted", tableToDelete, path);
                        } else {
                            logger.info(
                                    "Hive table {}'s external path {} not exist. It's normal if kylin.source.hive.keep-flat-table set false (By default)",
                                    tableToDelete, path);
                        }
                    } else {
                        logger.warn("Hive table {}'s job ID not found, segmentId2JobId: {}", tableToDelete,
                                segmentId2JobId.toString());
                    }
                }
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
    }

    private String getSegmentIdFromJobId(String jobId) {
        AbstractExecutable abstractExecutable = executableManager.getJob(jobId);
        String segmentId = abstractExecutable.getParam("segmentId");
        return segmentId;
    }

    private boolean isTableInUse(String segUuid, List<String> workingJobList) {
        for (String jobId : workingJobList) {
            String segmentId = getSegmentIdFromJobId(jobId);

            if (segUuid.equals(segmentId))
                return true;
        }
        return false;
    }

}
