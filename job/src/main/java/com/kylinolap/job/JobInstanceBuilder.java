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
package com.kylinolap.job;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.dict.lookup.HiveTable;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.constant.JobStepCmdTypeEnum;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.hive.JoinedFlatTableDesc;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author George Song (ysong1)
 */
public class JobInstanceBuilder {

    private static Logger log = LoggerFactory.getLogger(JobInstanceBuilder.class);

    private CubeInstance cube;
    private String htablename;
    private String cubeName;
    private String segmentName;
    private CubeSegment cubeSegment;
    private String jobUUID;
    private final JobEngineConfig engineConfig;

    private String jobWorkingDir;

    public JobInstanceBuilder(JobEngineConfig engineCfg) {
        this.engineConfig = engineCfg;
    }

    public List<JobStep> buildSteps(JobInstance jobInstance) throws IOException {
        init(jobInstance);
        switch (jobInstance.getType()) {
        case BUILD:
            return createBuildCubeSegmentSteps(jobInstance);
        case MERGE:
            return createMergeCubeSegmentsSteps(jobInstance);
        default:
            throw new IllegalArgumentException("job type:" + jobInstance.getType() + " not supported");
        }
    }

    private void init(JobInstance jobInstance) {
        cubeName = jobInstance.getRelatedCube();
        if (cubeName == null) {
            throw new IllegalArgumentException("Cube name is null or empty!");
        }
        cube = CubeManager.getInstance(this.engineConfig.getConfig()).getCube(cubeName);
        jobUUID = jobInstance.getUuid();
        if (jobUUID == null || jobUUID.equals("")) {
            throw new IllegalArgumentException("Job UUID is null or empty!");
        }

        segmentName = jobInstance.getRelatedSegment();
        if (segmentName == null || segmentName.equals("")) {
            throw new IllegalArgumentException("Cube segment name is null or empty!");
        }

        // only the segment which can be build
        cubeSegment = cube.getSegment(segmentName, CubeSegmentStatusEnum.NEW);
        htablename = cubeSegment.getStorageLocationIdentifier();

        this.jobWorkingDir = JobInstance.getJobWorkingDir(jobInstance, engineConfig);
    }

    private String appendMapReduceParameters(String cmd, JobInstance jobInstance) throws IOException {
        StringBuffer buf = new StringBuffer(cmd);
        String jobConf = engineConfig.getHadoopJobConfFilePath(cube.getDescriptor().getCapacity());
        if (StringUtils.isBlank(jobConf) == false) {
            buf.append(" -conf " + jobConf);
        }
        
        String extraArgs = engineConfig.getMapReduceCmdExtraArgs();
        if (StringUtils.isBlank(extraArgs) == false) {
            extraArgs = extraArgs.replace("${CUBE}", jobInstance.getRelatedCube());
            extraArgs = extraArgs.replace("${TYPE}", jobInstance.getType().toString());
            extraArgs = extraArgs.replace("${UUID}", jobInstance.getUuid());
            buf.append(" ").append(extraArgs);
        }

        return buf.toString();
    }

    private String appendExecCmdParameters(String cmd, String paraName, String paraValue) {
        StringBuffer buf = new StringBuffer(cmd);
        buf.append(" -" + paraName + " " + paraValue);
        return buf.toString();
    }

    private String getIntermediateHiveTablePath() {
        JoinedFlatTableDesc intermediateTableDesc = new JoinedFlatTableDesc(cube.getDescriptor(), this.cubeSegment);
        return JoinedFlatTable.getTableDir(intermediateTableDesc, jobWorkingDir, jobUUID);
    }

    private String[] getCuboidOutputPaths(String cubeName, int totalRowkeyColumnCount, int groupRowkeyColumnsCount) {
        String[] paths = new String[groupRowkeyColumnsCount + 1];
        for (int i = 0; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnCount - i;
            if (dimNum == totalRowkeyColumnCount) {
                paths[i] = jobWorkingDir + "/" + cubeName + "/cuboid/" + "base_cuboid";
            } else {
                paths[i] = jobWorkingDir + "/" + cubeName + "/cuboid/" + dimNum + "d_cuboid";
            }
        }
        return paths;
    }

    private String getFactDistinctColumnsPath() {
        return jobWorkingDir + "/" + cubeName + "/fact_distinct_columns";
    }

    private String getRowkeyDistributionOutputPath() {
        return jobWorkingDir + "/" + cubeName + "/rowkey_stats";
    }

    private List<JobStep> createMergeCubeSegmentsSteps(JobInstance jobInstance) throws IOException {

        if (cube.getMergingSegments() == null || cube.getMergingSegments().size() < 2) {
            throw new IllegalArgumentException("Merging segments count should be more than 2");
        }


        String[] cuboidPaths = new String[cube.getMergingSegments().size()];
        for (int i = 0; i < cube.getMergingSegments().size(); i++) {
            CubeSegment seg = cube.getMergingSegments().get(i);
            cuboidPaths[i] = JobInstance.getJobWorkingDir(seg.getLastBuildJobID(), engineConfig.getHdfsWorkingDirectory()) + "/" + jobInstance.getRelatedCube() + "/cuboid/*";
        }
        String formattedPath = formatPaths(cuboidPaths);

        // clear existing steps
//        jobInstance.clearSteps();
        int stepSeqNum = 0;
        List<JobStep> result = Lists.newArrayList();

        // merge cuboid data of ancestor segments
        result.add(createMergeCuboidDataStep(jobInstance, stepSeqNum++, formattedPath));

        // get output distribution step
        result.add(createRangeRowkeyDistributionStep(jobInstance, stepSeqNum++, jobWorkingDir + "/" + cubeName + "/merged_cuboid"));

        // create htable step
        result.add(createCreateHTableStep(jobInstance, stepSeqNum++));

        // generate hfiles step
        result.add(createConvertCuboidToHfileStep(jobInstance, stepSeqNum++, jobWorkingDir + "/" + cubeName + "/merged_cuboid"));

        // bulk load step
        result.add(createBulkLoadStep(jobInstance, stepSeqNum++));

        try {
            log.debug(JsonUtil.writeValueAsIndentString(jobInstance));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return result;
    }

    private List<JobStep> createBuildCubeSegmentSteps(JobInstance jobInstance) throws IOException {

        // clear existing steps
//        jobInstance.clearSteps();

        int groupRowkeyColumnsCount = cube.getDescriptor().getRowkey().getNCuboidBuildLevels();
        int totalRowkeyColumnsCount = cube.getDescriptor().getRowkey().getRowKeyColumns().length;

        String[] cuboidOutputTempPath = getCuboidOutputPaths(cubeName, totalRowkeyColumnsCount, groupRowkeyColumnsCount);

        int stepSeqNum = 0;
        List<JobStep> result = Lists.newArrayList();
        if (this.engineConfig.isFlatTableByHive()) {
            // by default in here

            // flat hive table step
            result.add(createIntermediateHiveTableStep(jobInstance, stepSeqNum++, cuboidOutputTempPath));
        }

        // fact distinct columns step
        result.add(createFactDistinctColumnsStep(jobInstance, stepSeqNum++, cuboidOutputTempPath));

        // build dictionary step
        result.add(createBuildDictionaryStep(jobInstance, stepSeqNum++));

        // base cuboid step
        result.add(createBaseCuboidStep(jobInstance, stepSeqNum++, cuboidOutputTempPath));

        // n dim cuboid steps
        for (int i = 1; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnsCount - i;
            result.add(createNDimensionCuboidStep(jobInstance, stepSeqNum++, cuboidOutputTempPath, dimNum, totalRowkeyColumnsCount));
        }

        // get output distribution step
        result.add(createRangeRowkeyDistributionStep(jobInstance, stepSeqNum++, jobWorkingDir + "/" + cubeName + "/cuboid/*"));

        // create htable step
        result.add(createCreateHTableStep(jobInstance, stepSeqNum++));
        // generate hfiles step
        result.add(createConvertCuboidToHfileStep(jobInstance, stepSeqNum++, jobWorkingDir + "/" + cubeName + "/cuboid/*"));
        // bulk load step
        result.add(createBulkLoadStep(jobInstance, stepSeqNum++));

        try {
            log.debug(JsonUtil.writeValueAsIndentString(jobInstance));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return result;
    }

    private String formatPaths(String[] paths) {
        return StringUtils.join(paths, ",");
    }

    private JobStep createBuildDictionaryStep(JobInstance jobInstance, int stepSeqNum) {
        // base cuboid job
        JobStep buildDictionaryStep = new JobStep();
        buildDictionaryStep.setName(JobConstants.STEP_NAME_BUILD_DICTIONARY);
        String cmd = "";
        cmd = appendExecCmdParameters(cmd, "cubename", cubeName);
        cmd = appendExecCmdParameters(cmd, "segmentname", segmentName);
        cmd = appendExecCmdParameters(cmd, "input", getFactDistinctColumnsPath());

        buildDictionaryStep.setExecCmd(cmd);
        buildDictionaryStep.setSequenceID(stepSeqNum);
        buildDictionaryStep.setStatus(JobStepStatusEnum.PENDING);
        buildDictionaryStep.setRunAsync(false);
        buildDictionaryStep.setCmdType(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NO_MR_DICTIONARY);
        return buildDictionaryStep;
    }

    private JobStep createIntermediateHiveTableStep(JobInstance jobInstance, int stepSeqNum, String[] cuboidOutputTempPath) throws IOException {
        JoinedFlatTableDesc intermediateTableDesc = new JoinedFlatTableDesc(cube.getDescriptor(), this.cubeSegment);
        String dropTableHql = JoinedFlatTable.generateDropTableStatement(intermediateTableDesc, jobUUID);
        String createTableHql = JoinedFlatTable.generateCreateTableStatement(intermediateTableDesc, jobWorkingDir, jobUUID);
        String insertDataHql = JoinedFlatTable.generateInsertDataStatement(intermediateTableDesc, jobUUID, this.engineConfig);

        JobStep intermediateHiveTableStep = new JobStep();
        intermediateHiveTableStep.setName(JobConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);

        StringBuffer buf = new StringBuffer();
        buf.append("hive -e \"");
        buf.append(dropTableHql + "\n");
        buf.append(createTableHql + "\n");
        buf.append(insertDataHql + "\n");
        buf.append("\"");

        intermediateHiveTableStep.setSequenceID(stepSeqNum);
        intermediateHiveTableStep.setExecCmd(buf.toString());
        intermediateHiveTableStep.setStatus(JobStepStatusEnum.PENDING);
        intermediateHiveTableStep.setRunAsync(false);
        intermediateHiveTableStep.setCmdType(JobStepCmdTypeEnum.SHELL_CMD_HADOOP);
        return intermediateHiveTableStep;
    }

    private JobStep createFactDistinctColumnsStep(JobInstance jobInstance, int stepSeqNum, String[] cuboidOutputTempPath) throws IOException {
        // base cuboid job
        JobStep factDistinctColumnsStep = new JobStep();

        String inputLocation;
        String cmd = "";

        inputLocation = getIntermediateHiveTablePath();
        cmd = appendMapReduceParameters(cmd, jobInstance);

        factDistinctColumnsStep.setName(JobConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);

        cmd = appendExecCmdParameters(cmd, "cubename", cubeName);
        cmd = appendExecCmdParameters(cmd, "input", inputLocation);
        cmd = appendExecCmdParameters(cmd, "output", getFactDistinctColumnsPath());
        cmd = appendExecCmdParameters(cmd, "jobname", "Kylin_Fact_Distinct_Columns_" + jobInstance.getRelatedCube() + "_Step_" + stepSeqNum);

        factDistinctColumnsStep.setExecCmd(cmd);
        factDistinctColumnsStep.setSequenceID(stepSeqNum);
        factDistinctColumnsStep.setStatus(JobStepStatusEnum.PENDING);
        factDistinctColumnsStep.setRunAsync(true);
        factDistinctColumnsStep.setCmdType(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_FACTDISTINCT);
        return factDistinctColumnsStep;
    }

    private JobStep createBaseCuboidStep(JobInstance jobInstance, int stepSeqNum, String[] cuboidOutputTempPath) throws IOException {
        // base cuboid job
        JobStep baseCuboidStep = new JobStep();

        String inputLocation;
        String cmd = "";

        if (this.engineConfig.isFlatTableByHive()) {
            inputLocation = getIntermediateHiveTablePath();
            cmd = appendMapReduceParameters(cmd, jobInstance);
        } else {
            HiveTable factTableInHive = new HiveTable(MetadataManager.getInstance(this.engineConfig.getConfig()), cube.getDescriptor().getFactTable());
            inputLocation = factTableInHive.getHDFSLocation(false);
            cmd = appendMapReduceParameters(cmd, jobInstance);
            cmd = appendExecCmdParameters(cmd, "inputformat", "TextInputFormat");
        }

        baseCuboidStep.setName(JobConstants.STEP_NAME_BUILD_BASE_CUBOID);

        cmd = appendExecCmdParameters(cmd, "cubename", cubeName);
        cmd = appendExecCmdParameters(cmd, "segmentname", segmentName);
        cmd = appendExecCmdParameters(cmd, "input", inputLocation);
        cmd = appendExecCmdParameters(cmd, "output", cuboidOutputTempPath[0]);
        cmd = appendExecCmdParameters(cmd, "jobname", "Kylin_Base_Cuboid_Builder_" + jobInstance.getRelatedCube() + "_Step_" + stepSeqNum);
        cmd = appendExecCmdParameters(cmd, "level", "0");

        baseCuboidStep.setExecCmd(cmd);
        baseCuboidStep.setSequenceID(stepSeqNum);
        baseCuboidStep.setStatus(JobStepStatusEnum.PENDING);
        baseCuboidStep.setRunAsync(true);
        baseCuboidStep.setCmdType(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_BASECUBOID);
        return baseCuboidStep;
    }

    private JobStep createNDimensionCuboidStep(JobInstance jobInstance, int stepSeqNum, String[] cuboidOutputTempPath, int dimNum, int totalRowkeyColumnCount) throws IOException {
        // ND cuboid job
        JobStep ndCuboidStep = new JobStep();

        ndCuboidStep.setName(JobConstants.STEP_NAME_BUILD_N_D_CUBOID + " : " + dimNum + "-Dimension");
        String cmd = "";

        cmd = appendMapReduceParameters(cmd, jobInstance);
        cmd = appendExecCmdParameters(cmd, "cubename", cubeName);
        cmd = appendExecCmdParameters(cmd, "segmentname", segmentName);
        cmd = appendExecCmdParameters(cmd, "input", cuboidOutputTempPath[totalRowkeyColumnCount - dimNum - 1]);
        cmd = appendExecCmdParameters(cmd, "output", cuboidOutputTempPath[totalRowkeyColumnCount - dimNum]);
        cmd = appendExecCmdParameters(cmd, "jobname", "Kylin_ND-Cuboid_Builder_" + jobInstance.getRelatedCube() + "_Step_" + stepSeqNum);
        cmd = appendExecCmdParameters(cmd, "level", "" + (totalRowkeyColumnCount - dimNum));

        ndCuboidStep.setExecCmd(cmd);
        ndCuboidStep.setSequenceID(stepSeqNum);
        ndCuboidStep.setStatus(JobStepStatusEnum.PENDING);
        ndCuboidStep.setRunAsync(true);
        ndCuboidStep.setCmdType(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NDCUBOID);
        return ndCuboidStep;
    }

    private JobStep createRangeRowkeyDistributionStep(JobInstance jobInstance, int stepSeqNum, String inputPath) throws IOException {
        JobStep rowkeyDistributionStep = new JobStep();
        rowkeyDistributionStep.setName(JobConstants.STEP_NAME_GET_CUBOID_KEY_DISTRIBUTION);
        String cmd = "";

        cmd = appendMapReduceParameters(cmd, jobInstance);
        cmd = appendExecCmdParameters(cmd, "input", inputPath);
        cmd = appendExecCmdParameters(cmd, "output", getRowkeyDistributionOutputPath());
        cmd = appendExecCmdParameters(cmd, "jobname", "Kylin_Region_Splits_Calculator_" + jobInstance.getRelatedCube() + "_Step_" + stepSeqNum);
        cmd = appendExecCmdParameters(cmd, "cubename", cubeName);

        rowkeyDistributionStep.setExecCmd(cmd);
        rowkeyDistributionStep.setSequenceID(stepSeqNum);
        rowkeyDistributionStep.setStatus(JobStepStatusEnum.PENDING);
        rowkeyDistributionStep.setRunAsync(true);
        rowkeyDistributionStep.setCmdType(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION);
        return rowkeyDistributionStep;
    }

    private JobStep createMergeCuboidDataStep(JobInstance jobInstance, int stepSeqNum, String inputPath) throws IOException {
        JobStep mergeCuboidDataStep = new JobStep();
        mergeCuboidDataStep.setName(JobConstants.STEP_NAME_MERGE_CUBOID);
        String cmd = "";

        cmd = appendMapReduceParameters(cmd, jobInstance);
        cmd = appendExecCmdParameters(cmd, "cubename", cubeName);
        cmd = appendExecCmdParameters(cmd, "segmentname", segmentName);
        cmd = appendExecCmdParameters(cmd, "input", inputPath);
        cmd = appendExecCmdParameters(cmd, "output", jobWorkingDir + "/" + cubeName + "/merged_cuboid");
        cmd = appendExecCmdParameters(cmd, "jobname", "Kylin_Merge_Cuboid_" + jobInstance.getRelatedCube() + "_Step_" + stepSeqNum);

        mergeCuboidDataStep.setExecCmd(cmd);
        mergeCuboidDataStep.setSequenceID(stepSeqNum);
        mergeCuboidDataStep.setStatus(JobStepStatusEnum.PENDING);
        mergeCuboidDataStep.setRunAsync(true);
        mergeCuboidDataStep.setCmdType(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_MERGECUBOID);
        return mergeCuboidDataStep;
    }

    private JobStep createCreateHTableStep(JobInstance jobInstance, int stepSeqNum) {
        JobStep createHtableStep = new JobStep();
        createHtableStep.setName(JobConstants.STEP_NAME_CREATE_HBASE_TABLE);
        String cmd = "";
        cmd = appendExecCmdParameters(cmd, "cubename", cubeName);
        cmd = appendExecCmdParameters(cmd, "input", getRowkeyDistributionOutputPath() + "/part-r-00000");
        cmd = appendExecCmdParameters(cmd, "htablename", htablename);

        createHtableStep.setExecCmd(cmd);
        createHtableStep.setSequenceID(stepSeqNum);
        createHtableStep.setStatus(JobStepStatusEnum.PENDING);
        createHtableStep.setRunAsync(false);
        createHtableStep.setCmdType(JobStepCmdTypeEnum.JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE);

        return createHtableStep;
    }

    private JobStep createConvertCuboidToHfileStep(JobInstance jobInstance, int stepSeqNum, String inputPath) throws IOException {
        JobStep createHFilesStep = new JobStep();
        createHFilesStep.setName(JobConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE);
        String cmd = "";

        cmd = appendMapReduceParameters(cmd, jobInstance);
        cmd = appendExecCmdParameters(cmd, "cubename", cubeName);
        cmd = appendExecCmdParameters(cmd, "input", inputPath);
        cmd = appendExecCmdParameters(cmd, "output", jobWorkingDir + "/" + cubeName + "/hfile");
        cmd = appendExecCmdParameters(cmd, "htablename", htablename);
        cmd = appendExecCmdParameters(cmd, "jobname", "Kylin_HFile_Generator_" + jobInstance.getRelatedCube() + "_Step_" + stepSeqNum);

        createHFilesStep.setExecCmd(cmd);
        createHFilesStep.setSequenceID(stepSeqNum);
        createHFilesStep.setStatus(JobStepStatusEnum.PENDING);
        createHFilesStep.setRunAsync(true);
        createHFilesStep.setCmdType(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_CONVERTHFILE);

        return createHFilesStep;

    }

    private JobStep createBulkLoadStep(JobInstance jobInstance, int stepSeqNum) {
        JobStep bulkLoadStep = new JobStep();
        bulkLoadStep.setName(JobConstants.STEP_NAME_BULK_LOAD_HFILE);

        String cmd = "";
        cmd = appendExecCmdParameters(cmd, "input", jobWorkingDir + "/" + cubeName + "/hfile/");
        cmd = appendExecCmdParameters(cmd, "htablename", htablename);
        cmd = appendExecCmdParameters(cmd, "cubename", cubeName);

        bulkLoadStep.setSequenceID(stepSeqNum);
        bulkLoadStep.setExecCmd(cmd);
        bulkLoadStep.setStatus(JobStepStatusEnum.PENDING);
        bulkLoadStep.setRunAsync(false);
        bulkLoadStep.setCmdType(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NO_MR_BULKLOAD);

        return bulkLoadStep;

    }
}
