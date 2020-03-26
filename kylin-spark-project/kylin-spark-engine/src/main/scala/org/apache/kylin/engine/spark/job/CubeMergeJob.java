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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.cube.ManagerHub;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.metadata.cube.model.ForestSpanningTree;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.DFLayoutMergeAssist;
import org.apache.kylin.engine.spark.utils.BuildUtils;
import org.apache.kylin.engine.spark.utils.JobMetrics;
import org.apache.kylin.engine.spark.utils.JobMetricsUtils;
import org.apache.kylin.engine.spark.utils.Metrics;
import org.apache.kylin.engine.spark.utils.QueryExecutionCache;
import scala.collection.JavaConversions;

public class CubeMergeJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(CubeMergeJob.class);
    private BuildLayoutWithUpdate buildLayoutWithUpdate;
    private List<CubeSegment> mergingSegments = Lists.newArrayList();
    private List<SegmentInfo> mergingSegInfos = Lists.newArrayList();

    @Override
    protected void doExecute() throws Exception {
        buildLayoutWithUpdate = new BuildLayoutWithUpdate();
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        String newSegmentId = getParam(MetadataConstants.P_SEGMENT_IDS);
        final CubeManager cubeManager = CubeManager.getInstance(config);
        final CubeInstance cube = cubeManager.getCubeByUuid(cubeId);
        final CubeSegment mergedSeg = cube.getSegmentById(newSegmentId);
        mergingSegments = cube.getMergingSegments(mergedSeg);
        for (CubeSegment segment : mergingSegments) {
            SegmentInfo segInfo = ManagerHub.getSegmentInfo(config, getParam(MetadataConstants.P_CUBE_ID), segment.getUuid());
            mergingSegInfos.add(segInfo);
        }

        //mergeSnapshot(cubeId, newSegmentId);

        //merge and save segments
        mergeSegments(cubeId, newSegmentId);
    }

    private void mergeSnapshot(String cubeId, String segmentId) throws IOException {
        final CubeManager cubeManager = CubeManager.getInstance(config);
        final CubeInstance cube = cubeManager.getCubeByUuid(cubeId);

        Collections.sort(mergingSegments);
        infos.clearMergingSegments();
        infos.recordMergingSegments(mergingSegInfos);

        CubeInstance cubeCopy = cube.latestCopyForWrite();
        CubeSegment segCopy = cubeCopy.getSegmentById(segmentId);
        makeSnapshotForNewSegment(segCopy, mergingSegments);
        CubeUpdate cubeUpdate = new CubeUpdate(cubeCopy);
        cubeUpdate.setToUpdateSegs(segCopy);
        cubeManager.updateCube(cubeUpdate);
    }

    private void makeSnapshotForNewSegment(CubeSegment newSeg, List<CubeSegment> mergingSegments) {
        CubeSegment lastSeg = mergingSegments.get(mergingSegments.size() - 1);
        for (Map.Entry<String, String> entry : lastSeg.getSnapshots().entrySet()) {
            newSeg.putSnapshotResPath(entry.getKey(), entry.getValue());
        }
    }

    private void mergeSegments(String cubeId, String segmentId) throws IOException {
        CubeManager mgr = CubeManager.getInstance(config);
        CubeInstance cube = mgr.getCubeByUuid(cubeId);
        CubeSegment mergedSeg = cube.getSegmentById(segmentId);
        SegmentInfo mergedSegInfo = ManagerHub.getSegmentInfo(config, getParam(MetadataConstants.P_CUBE_ID), mergedSeg.getUuid());

        Map<Long, DFLayoutMergeAssist> mergeCuboidsAssist = generateMergeAssist(mergingSegInfos, ss);
        for (DFLayoutMergeAssist assist : mergeCuboidsAssist.values()) {
            SpanningTree spanningTree = new ForestSpanningTree(JavaConversions.asJavaCollection(mergedSegInfo.toBuildLayouts()));
            Dataset<Row> afterMerge = assist.merge(config, cube.getName());
            LayoutEntity layout = assist.getLayout();

            Dataset<Row> afterSort;
            if (layout.isTableIndex()) {
                afterSort = afterMerge.sortWithinPartitions(NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet()));
            } else {
                Column[] dimsCols = NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet());
                Dataset<Row> afterAgg = CuboidAggregator.agg(ss, afterMerge, layout.getOrderedDimensions().keySet(),
                        layout.getOrderedMeasures(), spanningTree, false);
                afterSort = afterAgg.sortWithinPartitions(dimsCols);
            }
            buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {
                @Override
                public String getName() {
                    return "merge-layout-" + layout.getId();
                }

                @Override
                public LayoutEntity build() throws IOException {
                    return saveAndUpdateCuboid(afterSort, mergedSegInfo, layout, assist);
                }
            }, config);

            buildLayoutWithUpdate.updateLayout(mergedSegInfo, config);
        }
    }

    public static Map<Long, DFLayoutMergeAssist> generateMergeAssist(List<SegmentInfo> mergingSegments,
            SparkSession ss) {
        // collect layouts need to merge
        Map<Long, DFLayoutMergeAssist> mergeCuboidsAssist = Maps.newConcurrentMap();
        for (SegmentInfo seg : mergingSegments) {
            scala.collection.immutable.List<LayoutEntity> cuboids = seg.layouts();
            for (int i = 0; i < cuboids.size(); i++) {
                LayoutEntity cuboid = cuboids.apply(i);
                long layoutId = cuboid.getId();

                DFLayoutMergeAssist assist = mergeCuboidsAssist.get(layoutId);
                if (assist == null) {
                    assist = new DFLayoutMergeAssist();
                    assist.addCuboid(cuboid);
                    assist.setSs(ss);
                    assist.setLayout(cuboid);
                    assist.setNewSegment(seg);
                    assist.setToMergeSegments(mergingSegments);
                    mergeCuboidsAssist.put(layoutId, assist);
                } else {
                    assist.addCuboid(cuboid);
                }
            }
        }
        return mergeCuboidsAssist;
    }

    private LayoutEntity saveAndUpdateCuboid(Dataset<Row> dataset, SegmentInfo seg, LayoutEntity layout,
            DFLayoutMergeAssist assist) throws IOException {
        long layoutId = layout.getId();
        long sourceCount = 0L;

        for (LayoutEntity cuboid : assist.getCuboids()) {
            sourceCount += cuboid.getSourceRows();
        }

        // for spark metrics
        String queryExecutionId = UUID.randomUUID().toString();
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), queryExecutionId);
        ss.sparkContext().setJobDescription("merge layout " + layoutId);
        NSparkCubingEngine.NSparkCubingStorage storage = StorageFactory.createEngineAdapter(layout,
                NSparkCubingEngine.NSparkCubingStorage.class);
        String path = PathManager.getParquetStoragePath(config, getParam(MetadataConstants.P_CUBE_NAME), seg.name(), String.valueOf(layoutId));
        String tempPath = path + CubeBuildJob.TEMP_DIR_SUFFIX;
        // save to temp path
        storage.saveTo(tempPath, dataset, ss);

        JobMetrics metrics = JobMetricsUtils.collectMetrics(queryExecutionId);
        long rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT());
        if (rowCount == -1) {
            infos.recordAbnormalLayouts(layout.getId(),
                    "'Job metrics seems null, use count() to collect cuboid rows.'");
            logger.warn("Can not get cuboid row cnt.");
        }

        layout.setRows(rowCount);
        layout.setSourceRows(sourceCount);

        int partitionNum = BuildUtils.repartitionIfNeed(layout, storage, path, tempPath, config, ss);
        layout.setShardNum(partitionNum);
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        ss.sparkContext().setJobDescription(null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);

        BuildUtils.fillCuboidInfo(layout, path);

        return layout;
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfMergeJobInfo();
    }

    public static void main(String[] args) {
        CubeMergeJob nDataflowBuildJob = new CubeMergeJob();
        nDataflowBuildJob.execute(args);
    }

}
