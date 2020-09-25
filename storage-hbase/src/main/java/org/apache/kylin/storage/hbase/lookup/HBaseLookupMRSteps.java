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

package org.apache.kylin.storage.hbase.lookup;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.SnapshotTableDesc;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfoManager;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.LookupMaterializeContext;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.lookup.LookupExecutableUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class HBaseLookupMRSteps {
    protected static final Logger logger = LoggerFactory.getLogger(HBaseLookupMRSteps.class);
    private CubeInstance cube;
    private JobEngineConfig config;

    public HBaseLookupMRSteps(CubeInstance cube) {
        this.cube = cube;
        this.config = new JobEngineConfig(cube.getConfig());
    }

    public void addMaterializeLookupTablesSteps(LookupMaterializeContext context) {
        CubeDesc cubeDesc = cube.getDescriptor();
        Set<String> allLookupTables = Sets.newHashSet();
        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            TableRef table = dim.getTableRef();
            if (cubeDesc.getModel().isLookupTable(table)) {
                allLookupTables.add(table.getTableIdentity());
            }
        }
        List<SnapshotTableDesc> snapshotTableDescs = cubeDesc.getSnapshotTableDescList();
        for (SnapshotTableDesc snapshotTableDesc : snapshotTableDescs) {
            if (ExtTableSnapshotInfo.STORAGE_TYPE_HBASE.equals(snapshotTableDesc.getStorageType())
                    && allLookupTables.contains(snapshotTableDesc.getTableName())) {
                addMaterializeLookupTableSteps(context, snapshotTableDesc.getTableName(), snapshotTableDesc);
            }
        }
    }

    public void addMaterializeLookupTableSteps(LookupMaterializeContext context, String tableName, SnapshotTableDesc snapshotTableDesc) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ExtTableSnapshotInfoManager extTableSnapshotInfoManager = ExtTableSnapshotInfoManager.getInstance(kylinConfig);
        TableDesc tableDesc = TableMetadataManager.getInstance(kylinConfig).getTableDesc(tableName, cube.getProject());
        IReadableTable sourceTable = SourceManager.createReadableTable(tableDesc, context.getJobFlow().getId());
        try {
            ExtTableSnapshotInfo latestSnapshot = extTableSnapshotInfoManager.getLatestSnapshot(
                    sourceTable.getSignature(), tableName);
            if (latestSnapshot != null) {
                logger.info("there is latest snapshot exist for table:{}, skip build snapshot step.", tableName);
                context.addLookupSnapshotPath(tableName, latestSnapshot.getResourcePath());
                return;
            }
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
        logger.info("add build snapshot steps for table:{}", tableName);
        String snapshotID = genLookupSnapshotID();
        context.addLookupSnapshotPath(tableName, ExtTableSnapshotInfo.getResourcePath(tableName, snapshotID));
        addLookupTableConvertToHFilesStep(context.getJobFlow(), tableName, snapshotID);
        addLookupTableHFilesBulkLoadStep(context.getJobFlow(), tableName, snapshotID);
        if (snapshotTableDesc !=null && snapshotTableDesc.isEnableLocalCache()) {
            addUpdateSnapshotQueryCacheStep(context.getJobFlow(), tableName, snapshotID);
        }
    }

    private String genLookupSnapshotID() {
        return RandomUtil.randomUUID().toString();
    }

    private void addLookupTableConvertToHFilesStep(DefaultChainedExecutable jobFlow, String tableName, String snapshotID) {
        MapReduceExecutable createHFilesStep = new MapReduceExecutable();
        createHFilesStep
                .setName(ExecutableConstants.STEP_NAME_MATERIALIZE_LOOKUP_TABLE_CONVERT_HFILE + ":" + tableName);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, cube.getName());
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT,
                getLookupTableHFilePath(tableName, jobFlow.getId()));
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_TABLE_NAME, tableName);
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobFlow.getId());
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_LOOKUP_SNAPSHOT_ID, snapshotID);
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_LookupTable_HFile_Generator_" + tableName + "_Step");

        createHFilesStep.setMapReduceParams(cmd.toString());
        createHFilesStep.setMapReduceJobClass(LookupTableToHFileJob.class);
        createHFilesStep.setCounterSaveAs(BatchConstants.LOOKUP_EXT_SNAPSHOT_SRC_RECORD_CNT_PFX + tableName);

        jobFlow.addTask(createHFilesStep);
    }

    private void addLookupTableHFilesBulkLoadStep(DefaultChainedExecutable jobFlow, String tableName, String snapshotID) {
        HadoopShellExecutable bulkLoadStep = new HadoopShellExecutable();
        bulkLoadStep.setName(ExecutableConstants.STEP_NAME_MATERIALIZE_LOOKUP_TABLE_BULK_LOAD + ":" + tableName);

        StringBuilder cmd = new StringBuilder();
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT,
                getLookupTableHFilePath(tableName, jobFlow.getId()));
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_TABLE_NAME, tableName);
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobFlow.getId());
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_LOOKUP_SNAPSHOT_ID, snapshotID);

        bulkLoadStep.setJobParams(cmd.toString());
        bulkLoadStep.setJobClass(LookupTableHFilesBulkLoadJob.class);
        jobFlow.addTask(bulkLoadStep);
    }

    private void addUpdateSnapshotQueryCacheStep(DefaultChainedExecutable jobFlow, String tableName, String snapshotID) {
        UpdateSnapshotCacheForQueryServersStep updateSnapshotCacheStep = new UpdateSnapshotCacheForQueryServersStep();
        updateSnapshotCacheStep.setName(ExecutableConstants.STEP_NAME_LOOKUP_SNAPSHOT_CACHE_UPDATE + ":" + tableName);

        LookupExecutableUtil.setProjectName(cube.getProject(), updateSnapshotCacheStep.getParams());
        LookupExecutableUtil.setLookupTableName(tableName, updateSnapshotCacheStep.getParams());
        LookupExecutableUtil.setLookupSnapshotID(snapshotID, updateSnapshotCacheStep.getParams());
        jobFlow.addTask(updateSnapshotCacheStep);
    }

    private String getLookupTableHFilePath(String tableName, String jobId) {
        return HBaseConnection.makeQualifiedPathInHBaseCluster(JobBuilderSupport.getJobWorkingDir(config, jobId) + "/"
                + tableName + "/hfile/");
    }

    public void appendMapReduceParameters(StringBuilder buf) {
        appendMapReduceParameters(buf, JobEngineConfig.DEFAULT_JOB_CONF_SUFFIX);
    }

    public void appendMapReduceParameters(StringBuilder buf, String jobType) {
        try {
            String jobConf = config.getHadoopJobConfFilePath(jobType);
            if (jobConf != null && jobConf.length() > 0) {
                buf.append(" -conf ").append(jobConf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
