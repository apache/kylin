/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.engine.spark.job;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.DFLayoutMergeAssist;
import io.kyligence.kap.engine.spark.utils.BuildUtils;
import io.kyligence.kap.engine.spark.utils.JobMetrics;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.engine.spark.utils.Metrics;
import io.kyligence.kap.engine.spark.utils.QueryExecutionCache;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;

public class DFMergeJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(DFMergeJob.class);
    private BuildLayoutWithUpdate buildLayoutWithUpdate;

    @Override
    protected void doExecute() throws Exception {
        buildLayoutWithUpdate = new BuildLayoutWithUpdate();
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        String newSegmentId = getParam(NBatchConstants.P_SEGMENT_IDS);
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        mergeSnapshot(dataflowId, newSegmentId);

        //merge and save segments
        mergeSegments(dataflowId, newSegmentId, layoutIds);
    }

    private void mergeSnapshot(String dataflowId, String segmentId) {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowId);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = dataflow.getMergingSegments(mergedSeg);

        Collections.sort(mergingSegments);
        infos.clearMergingSegments();
        infos.recordMergingSegments(mergingSegments);

        NDataflow flowCopy = dataflow.copy();
        NDataSegment segCopy = flowCopy.getSegment(segmentId);

        makeSnapshotForNewSegment(segCopy, mergingSegments);

        NDataflowUpdate update = new NDataflowUpdate(dataflowId);
        update.setToUpdateSegs(segCopy);
        mgr.updateDataflow(update);

    }

    private void makeSnapshotForNewSegment(NDataSegment newSeg, List<NDataSegment> mergingSegments) {
        NDataSegment lastSeg = mergingSegments.get(mergingSegments.size() - 1);
        for (Map.Entry<String, String> entry : lastSeg.getSnapshots().entrySet()) {
            newSeg.putSnapshotResPath(entry.getKey(), entry.getValue());
        }
    }

    private void mergeSegments(String dataflowId, String segmentId, Set<Long> specifiedCuboids) throws IOException {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowId);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = dataflow.getMergingSegments(mergedSeg);

        Map<Long, DFLayoutMergeAssist> mergeCuboidsAssist = generateMergeAssist(mergingSegments, ss, mergedSeg);
        for (DFLayoutMergeAssist assist : mergeCuboidsAssist.values()) {

            Dataset<Row> afterMerge = assist.merge();
            LayoutEntity layout = assist.getLayout();
            Dataset<Row> afterSort;
            if (layout.getIndex().getId() > IndexEntity.TABLE_INDEX_START_ID) {
                afterSort = afterMerge.sortWithinPartitions(NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet()));
            } else {
                Column[] dimsCols = NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet());
                Dataset<Row> afterAgg = CuboidAggregator.agg(ss, afterMerge, layout.getOrderedDimensions().keySet(),
                        layout.getOrderedMeasures(), mergedSeg, null);
                afterSort = afterAgg.sortWithinPartitions(dimsCols);
            }
            buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {
                @Override
                public String getName() {
                    return "merge-layout-" + layout.getId();
                }

                @Override
                public List<NDataLayout> build() throws IOException {
                    return Lists.newArrayList(saveAndUpdateCuboid(afterSort, mergedSeg, layout, assist));
                }
            }, config);

            buildLayoutWithUpdate.updateLayout(mergedSeg, config, project);
        }
    }

    public static Map<Long, DFLayoutMergeAssist> generateMergeAssist(List<NDataSegment> mergingSegments,
            SparkSession ss, NDataSegment mergedSeg) {
        // collect layouts need to merge
        Map<Long, DFLayoutMergeAssist> mergeCuboidsAssist = Maps.newConcurrentMap();
        for (NDataSegment seg : mergingSegments) {
            for (NDataLayout cuboid : seg.getSegDetails().getLayouts()) {
                long layoutId = cuboid.getLayoutId();

                DFLayoutMergeAssist assist = mergeCuboidsAssist.get(layoutId);
                if (assist == null) {
                    assist = new DFLayoutMergeAssist();
                    assist.addCuboid(cuboid);
                    assist.setSs(ss);
                    assist.setNewSegment(mergedSeg);
                    assist.setLayout(cuboid.getLayout());
                    assist.setToMergeSegments(mergingSegments);
                    mergeCuboidsAssist.put(layoutId, assist);
                } else
                    assist.addCuboid(cuboid);
            }
        }
        return mergeCuboidsAssist;
    }

    private NDataLayout saveAndUpdateCuboid(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout,
            DFLayoutMergeAssist assist) throws IOException {
        long layoutId = layout.getId();
        long sourceCount = 0L;

        for (NDataLayout cuboid : assist.getCuboids()) {
            sourceCount += cuboid.getSourceRows();
        }

        NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layoutId);

        // for spark metrics
        String queryExecutionId = UUID.randomUUID().toString();
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), queryExecutionId);
        ss.sparkContext().setJobDescription("merge layout " + layoutId);
        NSparkCubingEngine.NSparkCubingStorage storage = StorageFactory.createEngineAdapter(layout,
                NSparkCubingEngine.NSparkCubingStorage.class);
        String path = NSparkCubingUtil.getStoragePath(dataCuboid);
        String tempPath = path + DFBuildJob.TEMP_DIR_SUFFIX;
        // save to temp path
        storage.saveTo(tempPath, dataset, ss);

        JobMetrics metrics = JobMetricsUtils.collectMetrics(queryExecutionId);
        long rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT());
        if (rowCount == -1) {
            infos.recordAbnormalLayouts(dataCuboid.getLayoutId(),
                    "'Job metrics seems null, use count() to collect cuboid rows.'");
            logger.warn("Can not get cuboid row cnt.");
        }
        dataCuboid.setRows(rowCount);
        dataCuboid.setSourceRows(sourceCount);
        dataCuboid.setBuildJobId(jobId);

        int partitionNum = BuildUtils.repartitionIfNeed(layout, dataCuboid, storage, path, tempPath,
                KapConfig.wrap(config), ss);
        dataCuboid.setPartitionNum(partitionNum);
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        ss.sparkContext().setJobDescription(null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);

        BuildUtils.fillCuboidInfo(dataCuboid);

        return dataCuboid;
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfMergeJobInfo();
    }

    public static void main(String[] args) {
        DFMergeJob nDataflowBuildJob = new DFMergeJob();
        nDataflowBuildJob.execute(args);
    }

}
