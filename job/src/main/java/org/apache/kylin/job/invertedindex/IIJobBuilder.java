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

package org.apache.kylin.job.invertedindex;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIJoinedFlatTableDesc;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.hadoop.dict.CreateInvertedIndexDictionaryJob;
import org.apache.kylin.job.hadoop.invertedindex.IIBulkLoadJob;
import org.apache.kylin.job.hadoop.invertedindex.IICreateHFileJob;
import org.apache.kylin.job.hadoop.invertedindex.IICreateHTableJob;
import org.apache.kylin.job.hadoop.invertedindex.IIDistinctColumnsJob;
import org.apache.kylin.job.hadoop.invertedindex.InvertedIndexJob;
import org.apache.kylin.metadata.model.DataModelDesc.RealizationCapacity;
import org.apache.kylin.source.hive.HiveMRInput.BatchCubingInputSide;

import com.google.common.base.Preconditions;

/**
 */
public final class IIJobBuilder {

    final JobEngineConfig engineConfig;

    public IIJobBuilder(JobEngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public IIJob buildJob(IISegment seg, String submitter) {
        checkPreconditions(seg);

        IIJob result = initialJob(seg, "BUILD", submitter);
        final String jobId = result.getId();
        final IIJoinedFlatTableDesc intermediateTableDesc = new IIJoinedFlatTableDesc(seg.getIIDesc());
        final String intermediateTableIdentity = getIntermediateTableIdentity(intermediateTableDesc);
        final String factDistinctColumnsPath = getIIDistinctColumnsPath(seg, jobId);
        final String iiRootPath = getJobWorkingDir(jobId) + "/" + seg.getIIInstance().getName() + "/";
        final String iiPath = iiRootPath + "*";

        final AbstractExecutable intermediateHiveTableStep = createFlatHiveTableStep(intermediateTableDesc, jobId);
        result.addTask(intermediateHiveTableStep);

        result.addTask(createFactDistinctColumnsStep(seg, intermediateTableIdentity, jobId, factDistinctColumnsPath));

        result.addTask(createBuildDictionaryStep(seg, factDistinctColumnsPath));

        result.addTask(createInvertedIndexStep(seg, intermediateTableIdentity, iiRootPath));

        // create htable step
        result.addTask(createCreateHTableStep(seg));

        // generate hfiles step
        result.addTask(createConvertToHfileStep(seg, iiPath, jobId));

        // bulk load step
        result.addTask(createBulkLoadStep(seg, jobId));

        return result;
    }

    private AbstractExecutable createFlatHiveTableStep(IIJoinedFlatTableDesc intermediateTableDesc, String jobId) {
        return BatchCubingInputSide.createFlatHiveTableStep(engineConfig, intermediateTableDesc, jobId);
    }

    private IIJob initialJob(IISegment seg, String type, String submitter) {
        IIJob result = new IIJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(engineConfig.getTimeZone()));
        result.setIIName(seg.getIIInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setName(seg.getIIInstance().getName() + " - " + seg.getName() + " - " + type + " - " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);
        return result;
    }

    private void checkPreconditions(IISegment seg) {
        Preconditions.checkNotNull(seg, "segment cannot be null");
        Preconditions.checkNotNull(engineConfig, "jobEngineConfig cannot be null");
    }

    private void appendMapReduceParameters(StringBuilder builder, JobEngineConfig engineConfig) {
        try {
            String jobConf = engineConfig.getHadoopJobConfFilePath(RealizationCapacity.MEDIUM);
            if (jobConf != null && jobConf.length() > 0) {
                builder.append(" -conf ").append(jobConf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getIIDistinctColumnsPath(IISegment seg, String jobUuid) {
        return getJobWorkingDir(jobUuid) + "/" + seg.getIIInstance().getName() + "/ii_distinct_columns";
    }

    private String getHFilePath(IISegment seg, String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getIIInstance().getName() + "/hfile/";
    }

    private MapReduceExecutable createFactDistinctColumnsStep(IISegment seg, String factTableName, String jobId, String output) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(IIDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, engineConfig);
        appendExecCmdParameters(cmd, "tablename", factTableName);
        appendExecCmdParameters(cmd, "iiname", seg.getIIInstance().getName());
        appendExecCmdParameters(cmd, "output", output);
        appendExecCmdParameters(cmd, "jobname", "Kylin_Fact_Distinct_Columns_" + seg.getIIInstance().getName() + "_Step");

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    private HadoopShellExecutable createBuildDictionaryStep(IISegment seg, String factDistinctColumnsPath) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "iiname", seg.getIIInstance().getName());
        appendExecCmdParameters(cmd, "input", factDistinctColumnsPath);

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateInvertedIndexDictionaryJob.class);
        return buildDictionaryStep;
    }

    private MapReduceExecutable createInvertedIndexStep(IISegment seg, String intermediateHiveTable, String iiOutputTempPath) {
        // base cuboid job
        MapReduceExecutable buildIIStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, engineConfig);

        buildIIStep.setName(ExecutableConstants.STEP_NAME_BUILD_II);

        appendExecCmdParameters(cmd, "iiname", seg.getIIInstance().getName());
        appendExecCmdParameters(cmd, "tablename", intermediateHiveTable);
        appendExecCmdParameters(cmd, "output", iiOutputTempPath);
        appendExecCmdParameters(cmd, "jobname", ExecutableConstants.STEP_NAME_BUILD_II);

        buildIIStep.setMapReduceParams(cmd.toString());
        buildIIStep.setMapReduceJobClass(InvertedIndexJob.class);
        return buildIIStep;
    }

    private HadoopShellExecutable createCreateHTableStep(IISegment seg) {
        HadoopShellExecutable createHtableStep = new HadoopShellExecutable();
        createHtableStep.setName(ExecutableConstants.STEP_NAME_CREATE_HBASE_TABLE);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "iiname", seg.getIIInstance().getName());
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());

        createHtableStep.setJobParams(cmd.toString());
        createHtableStep.setJobClass(IICreateHTableJob.class);

        return createHtableStep;
    }

    private MapReduceExecutable createConvertToHfileStep(IISegment seg, String inputPath, String jobId) {
        MapReduceExecutable createHFilesStep = new MapReduceExecutable();
        createHFilesStep.setName(ExecutableConstants.STEP_NAME_CONVERT_II_TO_HFILE);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, engineConfig);
        appendExecCmdParameters(cmd, "iiname", seg.getIIInstance().getName());
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getHFilePath(seg, jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "jobname", "Kylin_HFile_Generator_" + seg.getIIInstance().getName() + "_Step");

        createHFilesStep.setMapReduceParams(cmd.toString());
        createHFilesStep.setMapReduceJobClass(IICreateHFileJob.class);

        return createHFilesStep;
    }

    private HadoopShellExecutable createBulkLoadStep(IISegment seg, String jobId) {
        HadoopShellExecutable bulkLoadStep = new HadoopShellExecutable();
        bulkLoadStep.setName(ExecutableConstants.STEP_NAME_BULK_LOAD_HFILE);

        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "input", getHFilePath(seg, jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "iiname", seg.getIIInstance().getName());

        bulkLoadStep.setJobParams(cmd.toString());
        bulkLoadStep.setJobClass(IIBulkLoadJob.class);

        return bulkLoadStep;

    }

    private StringBuilder appendExecCmdParameters(StringBuilder buf, String paraName, String paraValue) {
        return buf.append(" -").append(paraName).append(" ").append(paraValue);
    }

    private String getJobWorkingDir(String uuid) {
        return engineConfig.getHdfsWorkingDirectory() + "kylin-" + uuid;
    }

    private String getIntermediateTableIdentity(IIJoinedFlatTableDesc intermediateTableDesc) {
        return engineConfig.getConfig().getHiveDatabaseForIntermediateTable() + "." + intermediateTableDesc.getTableName();
    }
}
