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

package org.apache.kylin.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.job.common.HadoopShellExecutable;
import org.apache.kylin.job.common.MapReduceExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.hadoop.cube.BaseCuboidJob;
import org.apache.kylin.job.hadoop.cube.FactDistinctColumnsJob;
import org.apache.kylin.job.hadoop.cube.NDCuboidJob;
import org.apache.kylin.job.hadoop.cubev2.InMemCuboidJob;
import org.apache.kylin.job.hadoop.cubev2.SaveStatisticsStep;
import org.apache.kylin.job.hadoop.dict.CreateDictionaryJob;

public class BatchCubingJobBuilder extends JobBuilderSupport {
    
    public static interface IMRBuildInputParticipant {
        
    }
    
    public static interface IMRBuildOutputParticipant {
        
    }
    
    public BatchCubingJobBuilder(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
    }

    public CubingJob build() {
        final CubingJob result = CubingJob.createBuildJob(seg,  submitter,  config);
        final String jobId = result.getId();
        final String cuboidRootPath = getCuboidRootPath(jobId);
        final CubeJoinedFlatTableDesc flatHiveTableDesc = new CubeJoinedFlatTableDesc(seg.getCubeDesc(), seg);

        result.addTask(createFlatHiveTableStep(flatHiveTableDesc, jobId));
        
        result.addTask(createFactDistinctColumnsStep(flatHiveTableDesc, jobId));
        result.addTask(createBuildDictionaryStep(jobId));
        
        if (config.isInMemCubing()) {
            result.addTask(createSaveStatisticsStep(jobId));
            
            // create htable step
            result.addTask(createCreateHTableStep(jobId));
            result.addTask(createInMemCubingStep(flatHiveTableDesc, result.getId()));
            // bulk load step
            result.addTask(createBulkLoadStep(jobId));
        } else {
            final int groupRowkeyColumnsCount = seg.getCubeDesc().getRowkey().getNCuboidBuildLevels();
            final int totalRowkeyColumnsCount = seg.getCubeDesc().getRowkey().getRowKeyColumns().length;
            final String[] cuboidOutputTempPath = getCuboidOutputPaths(cuboidRootPath, totalRowkeyColumnsCount, groupRowkeyColumnsCount);
            
            // base cuboid step
            result.addTask(createBaseCuboidStep(flatHiveTableDesc, cuboidOutputTempPath, jobId));

            // n dim cuboid steps
            for (int i = 1; i <= groupRowkeyColumnsCount; i++) {
                int dimNum = totalRowkeyColumnsCount - i;
                result.addTask(createNDimensionCuboidStep(cuboidOutputTempPath, dimNum, totalRowkeyColumnsCount));
            }
            result.addTask(createRangeRowkeyDistributionStep(cuboidRootPath + "*", jobId));
            // create htable step
            result.addTask(createCreateHTableStep(jobId));
            // generate hfiles step
            result.addTask(createConvertCuboidToHfileStep(cuboidRootPath + "*", jobId));
            // bulk load step
            result.addTask(createBulkLoadStep(jobId));
        }

        result.addTask(createUpdateCubeInfoAfterBuildStep(jobId));

        result.addTask(createGarbageCollectionStep(null, flatHiveTableDesc.getTableName(jobId)));

        return result;
    }

    private MapReduceExecutable createFactDistinctColumnsStep(CubeJoinedFlatTableDesc flatHiveTableDesc, String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(FactDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "output", getFactDistinctColumnsPath(jobId));
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "statisticsenabled", String.valueOf(config.isInMemCubing()));
        appendExecCmdParameters(cmd, "statisticsoutput", getStatisticsPath(jobId));
        appendExecCmdParameters(cmd, "statisticssamplingpercent", String.valueOf(config.getConfig().getCubingInMemSamplingPercent()));
        appendExecCmdParameters(cmd, "jobname", "Kylin_Fact_Distinct_Columns_" + seg.getCubeInstance().getName() + "_Step");
        appendExecCmdParameters(cmd, "tablename", flatHiveTableDesc.getTableName(jobId));

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    private HadoopShellExecutable createBuildDictionaryStep(String jobId) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", getFactDistinctColumnsPath(jobId));

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateDictionaryJob.class);
        return buildDictionaryStep;
    }

    private MapReduceExecutable createBaseCuboidStep(CubeJoinedFlatTableDesc flatHiveTableDesc, String[] cuboidOutputTempPath, String jobId) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, seg);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", getFlatHiveTableLocation(flatHiveTableDesc, jobId));
        appendExecCmdParameters(cmd, "output", cuboidOutputTempPath[0]);
        appendExecCmdParameters(cmd, "jobname", "Kylin_Base_Cuboid_Builder_" + seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "level", "0");

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(BaseCuboidJob.class);
        baseCuboidStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return baseCuboidStep;
    }

    private MapReduceExecutable createNDimensionCuboidStep(String[] cuboidOutputTempPath, int dimNum, int totalRowkeyColumnCount) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : " + dimNum + "-Dimension");
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", cuboidOutputTempPath[totalRowkeyColumnCount - dimNum - 1]);
        appendExecCmdParameters(cmd, "output", cuboidOutputTempPath[totalRowkeyColumnCount - dimNum]);
        appendExecCmdParameters(cmd, "jobname", "Kylin_ND-Cuboid_Builder_" + seg.getCubeInstance().getName() + "_Step");
        appendExecCmdParameters(cmd, "level", "" + (totalRowkeyColumnCount - dimNum));

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(NDCuboidJob.class);
        return ndCuboidStep;
    }

    private SaveStatisticsStep createSaveStatisticsStep(String jobId) {
        SaveStatisticsStep result = new SaveStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_SAVE_STATISTICS);
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setStatisticsPath(getStatisticsPath(jobId));
        return result;
    }

    private MapReduceExecutable createInMemCubingStep(CubeJoinedFlatTableDesc flatHiveTableDesc, String jobId) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, seg);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", getFlatHiveTableLocation(flatHiveTableDesc, jobId));
        appendExecCmdParameters(cmd, "statisticsoutput", getStatisticsPath(jobId));
        appendExecCmdParameters(cmd, "output", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "jobname", "Kylin_Cube_Builder_" + seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "level", "0");
        appendExecCmdParameters(cmd, "tablename", flatHiveTableDesc.getTableName(jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(InMemCuboidJob.class);
        baseCuboidStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES + "," + CubingJob.CUBE_SIZE_BYTES);
        return baseCuboidStep;
    }

    private UpdateCubeInfoAfterBuildStep createUpdateCubeInfoAfterBuildStep(String jobId) {
        final UpdateCubeInfoAfterBuildStep updateCubeInfoStep = new UpdateCubeInfoAfterBuildStep();
        updateCubeInfoStep.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        updateCubeInfoStep.setCubeName(seg.getCubeInstance().getName());
        updateCubeInfoStep.setSegmentId(seg.getUuid());
        updateCubeInfoStep.setCubingJobId(jobId);
        return updateCubeInfoStep;
    }
    
    private String getFlatHiveTableLocation(CubeJoinedFlatTableDesc flatTableDesc, String jobId) {
        return getJobWorkingDir(jobId) + "/" + flatTableDesc.getTableName(jobId);
    }
    
    private String[] getCuboidOutputPaths(String cuboidRootPath, int totalRowkeyColumnCount, int groupRowkeyColumnsCount) {
        String[] paths = new String[groupRowkeyColumnsCount + 1];
        for (int i = 0; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnCount - i;
            if (dimNum == totalRowkeyColumnCount) {
                paths[i] = cuboidRootPath + "base_cuboid";
            } else {
                paths[i] = cuboidRootPath + dimNum + "d_cuboid";
            }
        }
        return paths;
    }

}
