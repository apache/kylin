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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public class StorageCleanupJob extends AbstractApplication {

    @SuppressWarnings("static-access")
    protected static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false)
            .withDescription("Delete the unused storage").create("delete");
    @SuppressWarnings("static-access")
    protected static final Option OPTION_FORCE = OptionBuilder.withArgName("force").hasArg().isRequired(false)
            .withDescription("Warning: will delete all kylin intermediate hive tables").create("force");

    protected static final Logger logger = LoggerFactory.getLogger(StorageCleanupJob.class);
    
    // ============================================================================
    
    final protected KylinConfig config;
    final protected FileSystem hbaseFs;
    final protected FileSystem defaultFs;
    final protected ExecutableManager executableManager;
    
    protected boolean delete = false;
    protected boolean force = false;
    
    private List<String> hiveGarbageTables = Collections.emptyList();
    private List<String> hbaseGarbageTables = Collections.emptyList();
    private List<String> hdfsGarbageFiles = Collections.emptyList();
    private long hdfsGarbageFileBytes = 0;

    public StorageCleanupJob() throws IOException {
        this(KylinConfig.getInstanceFromEnv(), HadoopUtil.getWorkingFileSystem(),
                HadoopUtil.getWorkingFileSystem(HBaseConfiguration.create()));
    }
    
    protected StorageCleanupJob(KylinConfig config, FileSystem defaultFs, FileSystem hbaseFs) {
        this.config = config;
        this.defaultFs = defaultFs;
        this.hbaseFs = hbaseFs;
        this.executableManager = ExecutableManager.getInstance(config);
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }
    
    public void setForce(boolean force) {
        this.force = force;
    }
    
    public List<String> getHiveGarbageTables() {
        return hiveGarbageTables;
    }

    public List<String> getHbaseGarbageTables() {
        return hbaseGarbageTables;
    }

    public List<String> getHdfsGarbageFiles() {
        return hdfsGarbageFiles;
    }

    public long getHdfsFileGarbageBytes() {
        return hdfsGarbageFileBytes;
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
        
        cleanup();
    }
    
    // function entrance
    public void cleanup() throws Exception {
        
        cleanUnusedIntermediateHiveTable();
        cleanUnusedHBaseTables();
        cleanUnusedHdfsFiles();
    }

    protected void cleanUnusedHBaseTables() throws IOException {
        if ("hbase".equals(config.getStorageUrl().getScheme()) && !"".equals(config.getMetadataUrl().getScheme())) {
            final int deleteTimeoutMin = 10; // Unit minute
            try {
                // use reflection to isolate NoClassDef errors when HBase is not available
                Class hbaseCleanUpUtil = Class.forName("org.apache.kylin.rest.job.StorageCleanJobHbaseUtil");
                Method cleanUnusedHBaseTables = hbaseCleanUpUtil.getDeclaredMethod("cleanUnusedHBaseTables",
                        boolean.class, int.class);
                hbaseGarbageTables = (List<String>) cleanUnusedHBaseTables.invoke(hbaseCleanUpUtil, delete,
                        deleteTimeoutMin);
            } catch (Throwable e) {
                logger.error("Error during HBase clean up", e);
            }
        }
    }

    protected class UnusedHdfsFileCollector {
        LinkedHashSet<Pair<FileSystem, String>> list = new LinkedHashSet<>();
        
        public void add(FileSystem fs, String path) {
            list.add(Pair.newPair(fs, path));
        }
    }
    
    private void cleanUnusedHdfsFiles() throws IOException {
        
        UnusedHdfsFileCollector collector = new UnusedHdfsFileCollector();
        collectUnusedHdfsFiles(collector);
        
        if (collector.list.isEmpty()) {
            logger.info("No HDFS files to clean up");
            return;
        }
        
        long garbageBytes = 0;
        List<String> garbageList = new ArrayList<>();
        
        for (Pair<FileSystem, String> entry : collector.list) {
            FileSystem fs = entry.getKey();
            String path = entry.getValue();

            try {
                garbageList.add(path);
                ContentSummary sum = fs.getContentSummary(new Path(path));
                if (sum != null)
                    garbageBytes += sum.getLength();
                
                if (delete) {
                    logger.info("Deleting HDFS path " + path);
                    fs.delete(new Path(path), true);
                } else {
                    logger.info("Dry run, pending delete HDFS path " + path);
                }
            } catch (IOException e) {
                logger.error("Error dealing unused HDFS path " + path, e);
            }
        }
        
        hdfsGarbageFileBytes = garbageBytes;
        hdfsGarbageFiles = garbageList;
    }
    
    protected void collectUnusedHdfsFiles(UnusedHdfsFileCollector collector) throws IOException {
        if (StringUtils.isNotEmpty(config.getHBaseClusterFs())) {
            cleanUnusedHdfsFiles(hbaseFs, collector);
        }
        cleanUnusedHdfsFiles(defaultFs, collector);
    }
    
    private void cleanUnusedHdfsFiles(FileSystem fs, UnusedHdfsFileCollector collector) throws IOException {
        final JobEngineConfig engineConfig = new JobEngineConfig(config);
        final CubeManager cubeMgr = CubeManager.getInstance(config);
        
        List<String> allHdfsPathsNeedToBeDeleted = new ArrayList<String>();
        
        try {
            FileStatus[] fStatus = fs.listStatus(new Path(config.getHdfsWorkingDirectory()));
            if (fStatus != null) {
                for (FileStatus status : fStatus) {
                    String path = status.getPath().getName();
                    if (path.startsWith("kylin-")) {
                        String kylinJobPath = engineConfig.getHdfsWorkingDirectory() + path;
                        allHdfsPathsNeedToBeDeleted.add(kylinJobPath);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("Working Directory does not exist on HDFS.", e);
        }

        // only remove FINISHED and DISCARDED job intermediate files
        List<String> allJobs = executableManager.getAllJobIds();
        for (String jobId : allJobs) {
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
        
        // collect the garbage
        for (String path : allHdfsPathsNeedToBeDeleted)
            collector.add(fs, path);
    }

    private void cleanUnusedIntermediateHiveTable() throws Exception {
        try {
            cleanUnusedIntermediateHiveTableInternal();
        } catch (NoClassDefFoundError e) {
            if (e.getMessage().contains("HiveConf"))
                logger.info("Skip cleanup of tntermediate Hive table, seems no Hive on classpath");
            else
                throw e;
        }
    }
    
    private void cleanUnusedIntermediateHiveTableInternal() throws Exception {
        final int uuidLength = 36;
        final String prefix = MetadataConstants.KYLIN_INTERMEDIATE_PREFIX;
        final String uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
        
        List<String> hiveTableNames = getHiveTables();
        Iterable<String> kylinIntermediates = Iterables.filter(hiveTableNames, new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return input != null && input.startsWith(MetadataConstants.KYLIN_INTERMEDIATE_PREFIX);
            }
        });

        List<String> allJobs = executableManager.getAllJobIds();
        List<String> workingJobList = new ArrayList<String>();
        Map<String, String> segmentId2JobId = Maps.newHashMap();

        for (String jobId : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate table
            final ExecutableState state = executableManager.getOutput(jobId).getState();
            if (!state.isFinalState()) {
                workingJobList.add(jobId);
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
        logger.debug("Working jobIDs: " + workingJobList);

        // filter tables to delete
        List<String> allHiveTablesNeedToBeDeleted = new ArrayList<String>();
        for (String tableName : kylinIntermediates) {
            logger.debug("Checking if table is garbage -- " + tableName);

            if (!tableName.startsWith(prefix))
                continue;

            if (force) {
                logger.debug("Force include table " + tableName);
                allHiveTablesNeedToBeDeleted.add(tableName);
                continue;
            }

            boolean isNeedDel = true;

            if (tableName.length() < prefix.length() + uuidLength) {
                logger.debug("Skip table because length is not qualified, " + tableName);
                continue;
            }

            String uuid = tableName.substring(tableName.length() - uuidLength, tableName.length());
            uuid = uuid.replace("_", "-");
            final Pattern UUID_PATTERN = Pattern.compile(uuidPattern);

            if (!UUID_PATTERN.matcher(uuid).matches()) {
                logger.debug("Skip table because pattern doesn't match, " + tableName);
                continue;
            }

            //Some intermediate table ends with job's uuid
            if (allJobs.contains(uuid)) {
                isNeedDel = !workingJobList.contains(uuid);
            } else if (isTableInUse(uuid, workingJobList)) {
                logger.debug("Skip table because the table is in use, " + tableName);
                isNeedDel = false;
            }

            if (isNeedDel) {
                allHiveTablesNeedToBeDeleted.add(tableName);
            }
        }

        // conclude hive tables to delete
        hiveGarbageTables = allHiveTablesNeedToBeDeleted;
        if (allHiveTablesNeedToBeDeleted.isEmpty()) {
            logger.info("No Hive tables to clean up");
            return;
        }
        
        if (delete) {
            try {
                deleteHiveTables(allHiveTablesNeedToBeDeleted, segmentId2JobId);
            } catch (IOException e) {
                logger.error("Error during deleting Hive tables", e);
            }
        } else {
            for (String table : allHiveTablesNeedToBeDeleted) {
                logger.info("Dry run, pending delete Hive table " + table);
            }
        }
    }
    
    // override by test
    protected List<String> getHiveTables() throws Exception {
        ISourceMetadataExplorer explr = SourceManager.getDefaultSource().getSourceMetadataExplorer();
        return explr.listTables(config.getHiveDatabaseForIntermediateTable());
    }
    
    // override by test
    protected CliCommandExecutor getCliCommandExecutor() throws IOException {
        return config.getCliCommandExecutor();
    }

    private void deleteHiveTables(List<String> allHiveTablesNeedToBeDeleted, Map<String, String> segmentId2JobId)
            throws IOException {
        final JobEngineConfig engineConfig = new JobEngineConfig(config);
        final int uuidLength = 36;
        
        final String useDatabaseHql = "USE " + config.getHiveDatabaseForIntermediateTable() + ";";
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(useDatabaseHql);
        for (String delHive : allHiveTablesNeedToBeDeleted) {
            hiveCmdBuilder.addStatement("drop table if exists " + delHive + "; ");
            logger.info("Deleting Hive table " + delHive);
        }
        getCliCommandExecutor().execute(hiveCmdBuilder.build());
        
        // If kylin.source.hive.keep-flat-table, some intermediate table might be kept. 
        // Do delete external path.
        for (String tableToDelete : allHiveTablesNeedToBeDeleted) {
            String uuid = tableToDelete.substring(tableToDelete.length() - uuidLength, tableToDelete.length());
            String segmentId = uuid.replace("_", "-");

            if (segmentId2JobId.containsKey(segmentId)) {
                String path = JobBuilderSupport.getJobWorkingDir(engineConfig.getHdfsWorkingDirectory(),
                        segmentId2JobId.get(segmentId)) + "/" + tableToDelete;
                Path externalDataPath = new Path(path);
                if (defaultFs.exists(externalDataPath)) {
                    defaultFs.delete(externalDataPath, true);
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
