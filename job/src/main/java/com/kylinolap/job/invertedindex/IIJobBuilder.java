/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.invertedindex;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.google.common.base.Preconditions;
import com.kylinolap.cube.model.CubeDesc.RealizationCapacity;
import com.kylinolap.invertedindex.IISegment;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.job.AbstractJobBuilder;
import com.kylinolap.job.common.HadoopShellExecutable;
import com.kylinolap.job.common.MapReduceExecutable;
import com.kylinolap.job.constant.ExecutableConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.dict.CreateInvertedIndexDictionaryJob;
import com.kylinolap.job.hadoop.hive.IIJoinedFlatTableDesc;
import com.kylinolap.job.hadoop.invertedindex.IIBulkLoadJob;
import com.kylinolap.job.hadoop.invertedindex.IICreateHFileJob;
import com.kylinolap.job.hadoop.invertedindex.IICreateHTableJob;
import com.kylinolap.job.hadoop.invertedindex.IIDistinctColumnsJob;
import com.kylinolap.job.hadoop.invertedindex.InvertedIndexJob;
import com.kylinolap.job.execution.AbstractExecutable;

/**
 * Created by shaoshi on 1/15/15.
 */
public final class IIJobBuilder extends AbstractJobBuilder {
    
    private IIJobBuilder() {
        
    }
    
    public static IIJobBuilder newBuilder() {
        return new IIJobBuilder();
    }

    public IIJob buildJob() {
        checkPreconditions();

        IIJob result = initialJob("BUILD");
        final String jobId = result.getId();
        final IIJoinedFlatTableDesc intermediateTableDesc = new IIJoinedFlatTableDesc(getIIDesc());
        final String intermediateHiveTableName = getIntermediateHiveTableName(intermediateTableDesc, jobId);
        final String factDistinctColumnsPath = getIIDistinctColumnsPath(jobId);
        final String iiRootPath = getJobWorkingDir(jobId) + "/" + getIIName() + "/";
        final String iiPath = iiRootPath + "*";

        final AbstractExecutable intermediateHiveTableStep = createIntermediateHiveTableStep(intermediateTableDesc, jobId);
        result.addTask(intermediateHiveTableStep);

        result.addTask(createFactDistinctColumnsStep(intermediateHiveTableName, jobId, factDistinctColumnsPath));

        result.addTask(createBuildDictionaryStep(factDistinctColumnsPath));

        result.addTask(createInvertedIndexStep(intermediateHiveTableName, iiRootPath));

        result.addTask(this.createCreateHTableStep());

        // create htable step
        result.addTask(createCreateHTableStep());
        
        // generate hfiles step
        result.addTask(createConvertToHfileStep(iiPath, jobId));
        // bulk load step
        result.addTask(createBulkLoadStep(jobId));


        return result;
    }
    
    protected IIDesc getIIDesc() {
        return ((IISegment)segment).getIIDesc();
    }

    private IIJob initialJob(String type) {
        IIJob result = new IIJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(jobEngineConfig.getTimeZone()));
        result.setIIName(getIIName());
        result.setSegmentId(segment.getUuid());
        result.setName(getIIName() + " - " + segment.getName() + " - " + type + " - " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(this.submitter);
        return result;
    }

    private void checkPreconditions() {
        Preconditions.checkNotNull(this.segment, "segment cannot be null");
        Preconditions.checkNotNull(this.jobEngineConfig, "jobEngineConfig cannot be null");
    }

    private String getIIName() {
        return ((IISegment)segment).getIIInstance().getName();
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
    

    private String getIIDistinctColumnsPath(String jobUuid) {
        return getJobWorkingDir(jobUuid) + "/" + getIIName() + "/ii_distinct_columns";
    }

    private String getHTableName() {
        return ((IISegment)segment).getStorageLocationIdentifier();
    }

    private String getHFilePath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + getIIName() + "/hfile/";
    }

    private MapReduceExecutable createFactDistinctColumnsStep(String factTableName, String jobId, String output) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(IIDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, jobEngineConfig);
        appendExecCmdParameters(cmd, "tablename", factTableName);
        appendExecCmdParameters(cmd, "iiname", getIIName());
        appendExecCmdParameters(cmd, "output", output);
        appendExecCmdParameters(cmd, "jobname", "Kylin_Fact_Distinct_Columns_" + getIIName() + "_Step");

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    private HadoopShellExecutable createBuildDictionaryStep(String factDistinctColumnsPath) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "iiname", getIIName());
        appendExecCmdParameters(cmd, "input", factDistinctColumnsPath);

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateInvertedIndexDictionaryJob.class);
        return buildDictionaryStep;
    }

    private MapReduceExecutable createInvertedIndexStep(String intermediateHiveTable, String iiOutputTempPath) {
        // base cuboid job
        MapReduceExecutable buildIIStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, jobEngineConfig);

        buildIIStep.setName(ExecutableConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, "iiname", getIIName());
        appendExecCmdParameters(cmd, "tablename", intermediateHiveTable);
        appendExecCmdParameters(cmd, "output", iiOutputTempPath);
        appendExecCmdParameters(cmd, "jobname", ExecutableConstants.STEP_NAME_BUILD_II);

        buildIIStep.setMapReduceParams(cmd.toString());
        buildIIStep.setMapReduceJobClass(InvertedIndexJob.class);
        return buildIIStep;
    }


    private HadoopShellExecutable createCreateHTableStep() {
        HadoopShellExecutable createHtableStep = new HadoopShellExecutable();
        createHtableStep.setName(ExecutableConstants.STEP_NAME_CREATE_HBASE_TABLE);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "iiname", getIIName());
        appendExecCmdParameters(cmd, "htablename", getHTableName());

        createHtableStep.setJobParams(cmd.toString());
        createHtableStep.setJobClass(IICreateHTableJob.class);

        return createHtableStep;
    }

    private MapReduceExecutable createConvertToHfileStep(String inputPath, String jobId) {
        MapReduceExecutable createHFilesStep = new MapReduceExecutable();
        createHFilesStep.setName(ExecutableConstants.STEP_NAME_CONVERT_II_TO_HFILE);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, jobEngineConfig);
        appendExecCmdParameters(cmd, "iiname", getIIName());
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", getHTableName());
        appendExecCmdParameters(cmd, "jobname", "Kylin_HFile_Generator_" + getIIName() + "_Step");

        createHFilesStep.setMapReduceParams(cmd.toString());
        createHFilesStep.setMapReduceJobClass(IICreateHFileJob.class);

        return createHFilesStep;
    }

    private HadoopShellExecutable createBulkLoadStep(String jobId) {
        HadoopShellExecutable bulkLoadStep = new HadoopShellExecutable();
        bulkLoadStep.setName(ExecutableConstants.STEP_NAME_BULK_LOAD_HFILE);

        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "input", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", getHTableName());
        appendExecCmdParameters(cmd, "iiname", getIIName());

        bulkLoadStep.setJobParams(cmd.toString());
        bulkLoadStep.setJobClass(IIBulkLoadJob.class);

        return bulkLoadStep;

    }

}
