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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.spark.builder.NBuildSourceInfo;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.cube.ManagerHub;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.metadata.cube.model.ForestSpanningTree;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.CubeMergeAssist;
import org.apache.kylin.engine.spark.utils.BuildUtils;
import org.apache.kylin.engine.spark.utils.JobMetrics;
import org.apache.kylin.engine.spark.utils.JobMetricsUtils;
import org.apache.kylin.engine.spark.utils.Metrics;
import org.apache.kylin.engine.spark.utils.QueryExecutionCache;
import scala.collection.JavaConversions;

public class CubeMergeJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(CubeMergeJob.class);

    private BuildLayoutWithUpdate buildLayoutWithUpdate;
    private Map<Long, CubeMergeAssist> mergeCuboidsAssist;
    private List<CubeSegment> mergingSegments = Lists.newArrayList();
    private List<SegmentInfo> mergingSegInfos = Lists.newArrayList();
    private Map<Long, Short> cuboidShardNum = Maps.newConcurrentMap();

    @Override
    protected void doExecute() throws Exception {
        buildLayoutWithUpdate = new BuildLayoutWithUpdate(config);
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
        // merge segments
        mergeSegments(cubeId, newSegmentId);
        // update segment
        updateSegmentInfo(cubeId, newSegmentId);
    }

    private void mergeSegments(String cubeId, String segmentId) throws IOException {
        CubeManager mgr = CubeManager.getInstance(config);
        CubeInstance cube = mgr.getCubeByUuid(cubeId);
        CubeSegment mergedSeg = cube.getSegmentById(segmentId);
        SegmentInfo mergedSegInfo = ManagerHub.getSegmentInfo(config, getParam(MetadataConstants.P_CUBE_ID), mergedSeg.getUuid());

        mergeCuboidsAssist = generateMergeAssist(mergingSegInfos, ss);
        for (CubeMergeAssist assist : mergeCuboidsAssist.values()) {
            SpanningTree spanningTree = new ForestSpanningTree(JavaConversions.asJavaCollection(mergedSegInfo.toBuildLayouts()));
            Dataset<Row> afterMerge = assist.merge(config, cube.getName());
            LayoutEntity layout = assist.getLayout();

            Dataset<Row> afterSort;
            if (layout.isTableIndex()) {
                afterSort =
                        afterMerge.sortWithinPartitions(NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet()));
            } else {
                Set<Integer> dimColumns = layout.getOrderedDimensions().keySet();
                Dataset<Row> afterAgg = CuboidAggregator.agg(ss, afterMerge, dimColumns,
                        layout.getOrderedMeasures(), spanningTree, false);
                afterSort = afterAgg.sortWithinPartitions(
                        NSparkCubingUtil.getColumns(dimColumns));
            }
            buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {
                @Override
                public String getName() {
                    return "merge-cuboid-" + layout.getId();
                }

                @Override
                public LayoutEntity build() throws IOException {
                    return saveAndUpdateCuboid(afterSort, mergedSegInfo, layout, assist);
                }

                @Override
                public NBuildSourceInfo getBuildSourceInfo() {
                    return null;
                }
            }, config);

            buildLayoutWithUpdate.updateLayout(mergedSegInfo, config);
        }
    }

    public static Map<Long, CubeMergeAssist> generateMergeAssist(List<SegmentInfo> mergingSegments,
                                                                 SparkSession ss) {
        // collect layouts need to merge
        Map<Long, CubeMergeAssist> mergeCuboidsAssist = Maps.newConcurrentMap();
        for (SegmentInfo seg : mergingSegments) {
            scala.collection.immutable.List<LayoutEntity> cuboids = seg.layouts();
            for (int i = 0; i < cuboids.size(); i++) {
                LayoutEntity cuboid = cuboids.apply(i);
                long layoutId = cuboid.getId();

                CubeMergeAssist assist = mergeCuboidsAssist.get(layoutId);
                if (assist == null) {
                    assist = new CubeMergeAssist();
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
                                             CubeMergeAssist assist) throws IOException {
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
        String path = PathManager.getParquetStoragePath(config, getParam(MetadataConstants.P_CUBE_NAME), seg.name(), seg.identifier(), String.valueOf(layoutId));
        String tempPath = path + CubeBuildJob.TEMP_DIR_SUFFIX;
        // save to temp path
        storage.saveTo(tempPath, dataset, ss);

        JobMetrics metrics = JobMetricsUtils.collectMetrics(queryExecutionId);
        long rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT());
        if (rowCount == -1) {
            infos.recordAbnormalLayouts(layout.getId(),
                    "'Job metrics seems null, use count() to collect cuboid rows.'");
            logger.warn("Can not get cuboid row cnt, use count() to collect cuboid rows.");
            layout.setRows(dataset.count());
        } else {
            layout.setRows(rowCount);
        }
        layout.setSourceRows(sourceCount);

        int partitionNum = BuildUtils.repartitionIfNeed(layout, storage, path, tempPath, config, ss);
        layout.setShardNum(partitionNum);
        cuboidShardNum.put(layoutId, (short)partitionNum);
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        ss.sparkContext().setJobDescription(null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);

        BuildUtils.fillCuboidInfo(layout, path);

        return layout;
    }

    private void updateSegmentInfo(String cubeId, String segmentId) throws IOException {
        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cubeCopy = cubeManager.getCubeByUuid(cubeId).latestCopyForWrite();
        CubeUpdate update = new CubeUpdate(cubeCopy);

        List<CubeSegment> cubeSegments = Lists.newArrayList();
        CubeSegment segment = cubeCopy.getSegmentById(segmentId);
        long totalSourceSize = 0l;
        long totalInputRecords = 0l;
        long totalInputRecordsSize = 0l;
        for (CubeMergeAssist assist : mergeCuboidsAssist.values()) {
            totalSourceSize += assist.getLayout().getByteSize();
        }
        for (CubeSegment toRemoveSeg : mergingSegments) {
            totalInputRecords += toRemoveSeg.getInputRecords();
            totalInputRecordsSize += toRemoveSeg.getInputRecordsSize();
        }
        // Unit: KB
        segment.setSizeKB(totalSourceSize / 1024);
        segment.setInputRecords(totalInputRecords);
        segment.setInputRecordsSize(totalInputRecordsSize);
        segment.setLastBuildJobID(getParam(MetadataConstants.P_JOB_ID));
        segment.setCuboidShardNums(cuboidShardNum);
        Map<String, String> additionalInfo = segment.getAdditionalInfo();
        additionalInfo.put("storageType", "" + IStorageAware.ID_PARQUET);
        segment.setAdditionalInfo(additionalInfo);
        cubeSegments.add(segment);
        update.setToUpdateSegs(cubeSegments.toArray(new CubeSegment[0]));
        cubeManager.updateCube(update, true);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfMergeJobInfo();
    }

    public static void main(String[] args) {
        CubeMergeJob cubeMergeJob = new CubeMergeJob();
        cubeMergeJob.execute(args);
    }

}
