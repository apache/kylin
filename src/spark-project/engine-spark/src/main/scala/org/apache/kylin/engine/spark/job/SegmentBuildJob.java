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

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.builder.SnapshotBuilder;
import org.apache.kylin.engine.spark.job.exec.BuildExec;
import org.apache.kylin.engine.spark.job.stage.BuildParam;
import org.apache.kylin.engine.spark.job.stage.StageExec;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.tracker.BuildContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.kylin.engine.spark.job.StageType.BUILD_DICT;
import static org.apache.kylin.engine.spark.job.StageType.BUILD_LAYER;
import static org.apache.kylin.engine.spark.job.StageType.COST_BASED_PLANNER;
import static org.apache.kylin.engine.spark.job.StageType.GATHER_FLAT_TABLE_STATS;
import static org.apache.kylin.engine.spark.job.StageType.GENERATE_FLAT_TABLE;
import static org.apache.kylin.engine.spark.job.StageType.MATERIALIZED_FACT_TABLE;
import static org.apache.kylin.engine.spark.job.StageType.REFRESH_COLUMN_BYTES;
import static org.apache.kylin.engine.spark.job.StageType.REFRESH_SNAPSHOTS;
import static org.apache.kylin.engine.spark.job.StageType.WAITE_FOR_RESOURCE;

@Slf4j
public class SegmentBuildJob extends SegmentJob {

    private boolean usePlanner = false;

    public static void main(String[] args) {
        SegmentBuildJob segmentBuildJob = new SegmentBuildJob();
        segmentBuildJob.execute(args);
    }

    @Override
    protected final void extraInit() {
        super.extraInit();
        String enablePlanner = getParam(NBatchConstants.P_JOB_ENABLE_PLANNER);
        if (enablePlanner != null && Boolean.valueOf(enablePlanner)) {
            usePlanner = true;
        }
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }

    @Override
    protected void waiteForResourceSuccess() throws Exception {
        if (config.isBuildCheckPartitionColEnabled()) {
            checkDateFormatIfExist(project, dataflowId);
        }
        val waiteForResource = WAITE_FOR_RESOURCE.create(this, null, null);
        waiteForResource.onStageFinished(ExecutableState.SUCCEED);
        infos.recordStageId("");
    }

    @Override
    protected final void doExecute() throws Exception {

        log.info("Start sub stage {}" + REFRESH_SNAPSHOTS.name());
        REFRESH_SNAPSHOTS.create(this, null, null).toWork();
        log.info("End sub stage {}" + REFRESH_SNAPSHOTS.name());

        buildContext = new BuildContext(getSparkSession().sparkContext(), config);
        buildContext.appStatusTracker().startMonitorBuildResourceState();

        build();

        updateSegmentSourceBytesSize();
    }

    @Override // Copied from DFBuildJob
    protected final String calculateRequiredCores() throws Exception {
        if (config.getSparkEngineTaskImpactInstanceEnabled()) {
            String maxLeafTasksNums = maxLeafTasksNums();
            int factor = config.getSparkEngineTaskCoreFactor();
            int requiredCore = (int) Double.parseDouble(maxLeafTasksNums) / factor;
            log.info("The maximum number of tasks required to run the job is {}, require cores: {}", maxLeafTasksNums,
                    requiredCore);
            return String.valueOf(requiredCore);
        } else {
            return SparkJobConstants.DEFAULT_REQUIRED_CORES;
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

    protected void build() throws IOException {
        Stream<NDataSegment> segmentStream = config.isSegmentParallelBuildEnabled() ? //
                readOnlySegments.parallelStream() : readOnlySegments.stream();
        segmentStream.forEach(seg -> {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                infos.clearCuboidsNumPerLayer(seg.getId());

                val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
                val exec = new BuildExec(jobStepId);

                val buildParam = new BuildParam();
                MATERIALIZED_FACT_TABLE.createStage(this, seg, buildParam, exec);
                BUILD_DICT.createStage(this, seg, buildParam, exec);
                GENERATE_FLAT_TABLE.createStage(this, seg, buildParam, exec);
                // enable cost based planner according to the parameter
                if (usePlanner) {
                    COST_BASED_PLANNER.createStage(this, seg, buildParam, exec);
                }

                GATHER_FLAT_TABLE_STATS.createStage(this, seg, buildParam, exec);
                BUILD_LAYER.createStage(this, seg, buildParam, exec);
                REFRESH_COLUMN_BYTES.createStage(this, seg, buildParam, exec);

                buildSegment(seg, exec);

            } catch (IOException e) {
                Throwables.propagate(e);
            }
        });
    }

    private void buildSegment(NDataSegment dataSegment, BuildExec exec) throws IOException {
        log.info("Encoding segment {}", dataSegment.getId());
        exec.buildSegment();
    }

    // Copied from DFBuildJob
    public void tryRefreshSnapshots(StageExec stageExec) throws Exception {
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
        log.info("Refresh SNAPSHOT.");
        //snapshot building
        snapshotBuilder.buildSnapshot(getSparkSession(), getDataflow(dataflowId).getModel(), //
                getIgnoredSnapshotTables());
        if (config.isSnapshotSpecifiedSparkConf()) {
            // exchange sparkSession for maintained sparkConf
            log.info("exchange sparkSession using maintained sparkConf");
            exchangeSparkSession();
        }
        log.info("Finished SNAPSHOT.");
    }

    // Copied from DFBuildJob
    private void updateSegmentSourceBytesSize() {
        Map<String, Object> segmentSourceSize = ResourceDetectUtils.getSegmentSourceSize(rdSharedPath);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
            NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
            NDataflow newDF = dataflow.copy();
            val update = new NDataflowUpdate(dataflow.getUuid());
            List<NDataSegment> nDataSegments = Lists.newArrayList();
            for (Map.Entry<String, Object> entry : segmentSourceSize.entrySet()) {
                NDataSegment segment = newDF.getSegment(entry.getKey());
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
}
