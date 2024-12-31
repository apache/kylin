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

package org.apache.kylin.engine.spark.job;

import static org.apache.kylin.engine.spark.job.StageEnum.BUILD_FLAT_TABLE_STATS;
import static org.apache.kylin.engine.spark.job.StageEnum.BUILD_GLOBAL_DICT;
import static org.apache.kylin.engine.spark.job.StageEnum.BUILD_LAYER;
import static org.apache.kylin.engine.spark.job.StageEnum.COST_BASED_PLANNER;
import static org.apache.kylin.engine.spark.job.StageEnum.MATERIALIZE_FACT_VIEW;
import static org.apache.kylin.engine.spark.job.StageEnum.MATERIALIZE_FLAT_TABLE;
import static org.apache.kylin.engine.spark.job.StageEnum.REFRESH_COLUMN_BYTES;
import static org.apache.kylin.engine.spark.job.StageEnum.REFRESH_SNAPSHOT;
import static org.apache.kylin.engine.spark.job.StageEnum.WAIT_FOR_RESOURCE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.builder.SnapshotBuilder;
import org.apache.kylin.engine.spark.job.step.ParamPropagation;
import org.apache.kylin.engine.spark.job.step.StageExec;
import org.apache.kylin.engine.spark.job.step.build.BuildStepExec;
import org.apache.kylin.fileseg.FileSegments;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.tracker.AppStatusContext;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentBuildJob extends SegmentJob {

    private boolean usePlanner = false;

    public static void main(String[] args) {
        SegmentBuildJob segmentBuildJob = new SegmentBuildJob();
        segmentBuildJob.execute(args);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }

    @Override
    protected void waitForResourceSuccess() throws Exception {
        if (config.isBuildCheckPartitionColEnabled()) {
            checkDateFormatIfExist(project, dataflowId);
        }
        val waitForResource = WAIT_FOR_RESOURCE.createExec(this);
        waitForResource.onStageFinished(ExecutableState.SUCCEED);
        infos.recordStageId("");
    }

    @Override
    protected final void extraInit() {
        super.extraInit();
        String enablePlanner = StringUtils.trim(getParam(NBatchConstants.P_JOB_ENABLE_PLANNER));
        if (StringUtils.equalsIgnoreCase(enablePlanner, KylinConfigBase.TRUE)) {
            usePlanner = true;
        }
    }

    @Override
    protected final void doExecute() throws Exception {

        REFRESH_SNAPSHOT.createThenExecute(this);

        appStatusContext = new AppStatusContext(getSparkSession().sparkContext(), config);
        appStatusContext.appStatusTracker().startMonitorBuildResourceState();

        Stream<NDataSegment> segmentStream = //
                config.isSegmentParallelBuildEnabled() //
                        ? readOnlySegments.parallelStream() //
                        : readOnlySegments.stream();
        segmentStream.forEach(segment -> {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                infos.clearCuboidsNumPerLayer(segment.getId());

                val stepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
                val step = new BuildStepExec(stepId);

                final ParamPropagation params = new ParamPropagation();

                step.addStage(MATERIALIZE_FACT_VIEW.createExec(this, segment, params));
                step.addStage(BUILD_GLOBAL_DICT.createExec(this, segment, params));
                step.addStage(MATERIALIZE_FLAT_TABLE.createExec(this, segment, params));

                // enable cost based planner according to the parameter
                if (usePlanner) {
                    step.addStage(COST_BASED_PLANNER.createExec(this, segment, params));
                }

                step.addStage(BUILD_FLAT_TABLE_STATS.createExec(this, segment, params));
                step.addStage(BUILD_LAYER.createExec(this, segment, params));
                step.addStage(REFRESH_COLUMN_BYTES.createExec(this, segment, params));

                if (StringUtils.equals(KylinBuildEnv.get().buildJobInfos().getSegmentId(), segment.getId())) {
                    log.info("Segment[{}] build failed, rebuild with new parms", segment.getId());
                    segment.setExtraBuildOptions((KylinBuildEnv.get().buildJobInfos() //
                            .getJobRetryInfosForSegmentParam()));
                }

                step.doExecute();

            } catch (IOException e) {
                Throwables.propagate(e);
            } finally {
                FileSegments.clearFileSegFilterLocally();
            }
        });

        updateSegmentSourceBytesSize();
    }

    @Override // Copied from DFBuildJob
    protected final String calculateRequiredCores() throws Exception {
        if (config.isSparkEngineTaskImpactInstanceEnabled()) {
            String maxLeafTasksNums = maxLeafTasksNums();
            int factor = config.getSparkEngineTaskCoreFactor();
            int requiredCore = (int) Double.parseDouble(maxLeafTasksNums) / factor;
            log.info("The maximum number of tasks required to run the job is {}, require cores: {}", maxLeafTasksNums,
                    requiredCore);
            return String.valueOf(requiredCore);
        } else {
            return JobConstants.DEFAULT_REQUIRED_CORES;
        }
    }

    // Copied from DFBuildJob
    private String maxLeafTasksNums() throws IOException {
        if (Objects.isNull(rdSharedPath)) {
            rdSharedPath = config.getJobTmpShareDir(project, jobId);
        }
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FileStatus[] fileStatuses = fs.listStatus(rdSharedPath,
                path -> path.toString().endsWith(ResourceDetectUtils.cubingDetectItemFileSuffix()));
        return ResourceDetectUtils.selectMaxValueInFiles(fileStatuses);
    }

    // Copied from DFBuildJob
    public void refreshSnapshot(StageExec stageExec) throws Exception {
        SnapshotBuilder snapshotBuilder = new SnapshotBuilder(getJobId());
        if (config.isSnapshotManualManagementEnabled()) {
            log.info("Skip snapshot build in snapshot manual mode, dataflow: {}, only calculate total rows",
                    dataflowId);
            snapshotBuilder.calculateTotalRows(getSparkSession(), getDataflow(dataflowId).getModel(),
                    getIgnoredSnapshotTables());
            stageExec.onStageSkipped();
            return;
        } else if (!needBuildSnapshots()) {
            log.info("Skip snapshot build, dataflow {}, only calculate total rows", dataflowId);
            snapshotBuilder.calculateTotalRows(getSparkSession(), getDataflow(dataflowId).getModel(),
                    getIgnoredSnapshotTables());
            stageExec.onStageSkipped();
            return;
        }
        log.info("Refresh snapshot start.");
        //snapshot building
        snapshotBuilder.buildSnapshot(getSparkSession(), getDataflow(dataflowId).getModel(), //
                getIgnoredSnapshotTables());
        if (config.isSnapshotSpecifiedSparkConf()) {
            // exchange sparkSession for maintained sparkConf
            log.info("exchange sparkSession using maintained sparkConf");
            exchangeSparkSession();
        }
        log.info("Refresh snapshot complete.");
    }

    // Copied from DFBuildJob
    private void updateSegmentSourceBytesSize() {
        Map<String, Object> segmentSourceSize = ResourceDetectUtils.getSegmentSourceSize(rdSharedPath);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
            NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
            val update = new NDataflowUpdate(dataflow.getUuid());
            List<NDataSegment> nDataSegments = Lists.newArrayList();
            for (Map.Entry<String, Object> entry : segmentSourceSize.entrySet()) {
                NDataSegment segment = dataflow.getSegment(entry.getKey()).copy();
                segment.setSourceBytesSize((Long) entry.getValue());
                nDataSegments.add(segment);
            }
            update.setToUpdateSegs(nDataSegments.toArray(new NDataSegment[0]));
            dataflowManager.updateDataflow(update);

            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
            indexPlanManager.updateIndexPlan(dataflowId, copyForWrite -> copyForWrite //
                    .setLayoutBucketNumMapping(indexPlanManager.getIndexPlan(dataflowId).getLayoutBucketNumMapping()));
            return null;
        }, project);
    }

    @Override
    protected void addConcreteJobSparkConf(SparkConf sparkConf) throws JsonProcessingException {
        appendSparkConfOfIndexPlanner(sparkConf);
    }
}
