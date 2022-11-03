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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.DFLayoutMergeAssist;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.datasource.storage.StorageStore;
import org.apache.spark.sql.datasource.storage.StorageStoreFactory;
import org.apache.spark.sql.datasource.storage.WriteTaskStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;

/**
 * After KE 4.3, we use {@link io.kyligence.kap.engine.spark.job.SegmentMergeJob} to merge segment
 */
@Deprecated
public class DFMergeJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(DFMergeJob.class);
    protected BuildLayoutWithUpdate buildLayoutWithUpdate;

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

    public static void main(String[] args) {
        DFMergeJob nDataflowBuildJob = new DFMergeJob();
        nDataflowBuildJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        buildLayoutWithUpdate = new BuildLayoutWithUpdate();
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        String newSegmentId = getParam(NBatchConstants.P_SEGMENT_IDS);
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        mergeColumnSize(dataflowId, newSegmentId);

        //merge flat table
        try {
            mergeFlatTable(dataflowId, newSegmentId);
        } catch (Exception e) {
            logger.warn("Merge flat table failed.", e);
        }
        //merge and save segments
        mergeSegments(dataflowId, newSegmentId, layoutIds);
    }

    private void mergeColumnSize(String dataflowId, String segmentId) {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowId);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = dataflow.getMergingSegments(mergedSeg);

        Collections.sort(mergingSegments);
        infos.clearMergingSegments();
        infos.recordMergingSegments(mergingSegments);

        NDataflow flowCopy = dataflow.copy();
        NDataSegment segCopy = flowCopy.getSegment(segmentId);

        mergeColumnSizeForNewSegment(segCopy, mergingSegments);
        NDataflowUpdate update = new NDataflowUpdate(dataflowId);
        update.setToUpdateSegs(segCopy);
        mgr.updateDataflow(update);

    }

    private void mergeColumnSizeForNewSegment(NDataSegment segCopy, List<NDataSegment> mergingSegments) {
        SourceUsageManager usageManager = SourceUsageManager.getInstance(config);
        Map<String, Long> result = Maps.newHashMap();
        for (val seg : mergingSegments) {
            Map<String, Long> newByteSizeMap = MapUtils.isEmpty(seg.getColumnSourceBytes())
                    ? usageManager.calcAvgColumnSourceBytes(seg)
                    : seg.getColumnSourceBytes();
            mergeByteSizeMap(result, newByteSizeMap);
        }
        segCopy.setColumnSourceBytes(result);
    }

    private void mergeByteSizeMap(Map<String, Long> result, Map<String, Long> newByteSizeMap) {
        for (Map.Entry<String, Long> entry : newByteSizeMap.entrySet()) {
            val oriSize = result.getOrDefault(entry.getKey(), 0L);
            result.put(entry.getKey(), oriSize + entry.getValue());
        }
    }

    protected List<NDataSegment> getMergingSegments(NDataflow dataflow, NDataSegment mergedSeg) {
        return dataflow.getMergingSegments(mergedSeg);
    }

    protected void mergeSegments(String dataflowId, String segmentId, Set<Long> specifiedCuboids) throws IOException {
        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowId);
        final NDataSegment mergedSeg = dataflow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = getMergingSegments(dataflow, mergedSeg);

        Map<Long, DFLayoutMergeAssist> mergeCuboidsAssist = generateMergeAssist(mergingSegments, ss, mergedSeg);
        for (DFLayoutMergeAssist assist : mergeCuboidsAssist.values()) {

            Dataset<Row> afterMerge = assist.merge();
            LayoutEntity layout = assist.getLayout();
            Dataset<Row> afterSort;
            if (IndexEntity.isTableIndex(layout.getIndex().getId())) {
                afterSort = afterMerge
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet()));
            } else {
                Column[] dimsCols = NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet());
                Dataset<Row> afterAgg = CuboidAggregator.agg(afterMerge, layout.getOrderedDimensions().keySet(),
                        layout.getOrderedMeasures(), mergedSeg, null);
                afterSort = afterAgg.sortWithinPartitions(dimsCols);
            }
            buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {

                @Override
                public long getIndexId() {
                    return layout.getIndexId();
                }

                @Override
                public String getName() {
                    return "merge-layout-" + layout.getId();
                }

                @Override
                public List<NDataLayout> build() throws IOException {
                    return Lists.newArrayList(saveAndUpdateCuboid(afterSort, mergedSeg, layout, assist));
                }
            }, config);
        }

        buildLayoutWithUpdate.updateLayout(mergedSeg, config, project);
    }

    protected NDataLayout saveAndUpdateCuboid(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout,
            DFLayoutMergeAssist assist) throws IOException {
        ss.sparkContext().setLocalProperty("spark.scheduler.pool", "merge");
        long layoutId = layout.getId();
        long sourceCount = 0L;

        for (NDataLayout cuboid : assist.getCuboids()) {
            sourceCount += cuboid.getSourceRows();
        }
        NDataLayout dataLayout = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layoutId);
        val path = NSparkCubingUtil.getStoragePath(seg, layoutId);
        int storageType = layout.getModel().getStorageType();
        StorageStore storage = StorageStoreFactory.create(storageType);
        ss.sparkContext().setJobDescription("Merge layout " + layoutId);
        WriteTaskStats taskStats = storage.save(layout, new Path(path), KapConfig.wrap(config), dataset);
        ss.sparkContext().setJobDescription(null);
        dataLayout.setBuildJobId(jobId);
        long rowCount = taskStats.numRows();
        if (rowCount == -1) {
            KylinBuildEnv.get().buildJobInfos().recordAbnormalLayouts(layout.getId(),
                    "Job metrics seems null, use count() to collect cuboid rows.");
            logger.info("Can not get cuboid row cnt.");
        }
        dataLayout.setRows(rowCount);
        dataLayout.setSourceRows(sourceCount);
        dataLayout.setPartitionNum(taskStats.numBucket());
        dataLayout.setPartitionValues(taskStats.partitionValues());
        dataLayout.setFileCount(taskStats.numFiles());
        dataLayout.setByteSize(taskStats.numBytes());
        return dataLayout;
    }

    private List<String> predicatedSegments(Predicate<NDataSegment> predicate, final List<NDataSegment> sources) {
        return sources.stream().filter(predicate).map(NDataSegment::getId).collect(Collectors.toList());
    }

    private List<Path> getSegmentFlatTables(String dataFlowId, List<NDataSegment> segments) {
        // check flat table ready
        List<String> notReadies = predicatedSegments((NDataSegment segment) -> !segment.isFlatTableReady(), segments);
        if (CollectionUtils.isNotEmpty(notReadies)) {
            final String logStr = String.join(",", notReadies);
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] Plan to merge segments' flat table, "
                    + "but found that some's flat table were not ready like [{}]", logStr);
            return Lists.newArrayList();
        }

        // check flat table exists
        final FileSystem fs = HadoopUtil.getWorkingFileSystem();
        List<String> notExists = predicatedSegments((NDataSegment segment) -> {
            try {
                Path p = config.getFlatTableDir(project, dataFlowId, segment.getId());
                return !fs.exists(p);
            } catch (IOException ioe) {
                logger.warn("[UNEXPECTED_THINGS_HAPPENED] When checking segment's flat table exists, segment id: {}",
                        segment.getId(), ioe);
                return true;
            }
        }, segments);
        if (CollectionUtils.isNotEmpty(notExists)) {
            final String logStr = String.join(",", notExists);
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] Plan to merge segments' flat table, "
                    + "but found that some's flat table were not exists like [{}]", logStr);
            return Lists.newArrayList();
        }

        return segments.stream().map(segment -> config.getFlatTableDir(project, dataFlowId, segment.getId()))
                .collect(Collectors.toList());
    }

    private void mergeFlatTable(String dataFlowId, String segmentId) {
        if (!config.isPersistFlatTableEnabled()) {
            logger.info("project {} flat table persisting is not enabled.", project);
            return;
        }
        final NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataFlow = dfMgr.getDataflow(dataFlowId);
        final NDataSegment mergedSeg = dataFlow.getSegment(segmentId);
        final List<NDataSegment> mergingSegments = getMergingSegments(dataFlow, mergedSeg);
        if (mergingSegments.size() < 1) {
            return;
        }

        Collections.sort(mergingSegments);

        // check flat table paths
        List<Path> flatTables = getSegmentFlatTables(dataFlowId, mergingSegments);
        if (CollectionUtils.isEmpty(flatTables)) {
            return;
        }

        Dataset<Row> flatTableDs = null;
        String[] names = ss.read().parquet(flatTables.get(0).toString()).schema().fieldNames();
        Arrays.sort(names);
        for (Path p : flatTables) {
            Dataset<Row> newDs = ss.read().parquet(p.toString());
            String[] fieldNames = newDs.schema().fieldNames();
            Arrays.sort(fieldNames);
            if (Arrays.equals(names, fieldNames)) {
                flatTableDs = Objects.isNull(flatTableDs) ? newDs : flatTableDs.union(newDs);
            } else {
                logger.info("Schema: {} in path: {} is conflict with others: {}. Skip merge flat table.", fieldNames, p,
                        names);
                return;
            }
        }

        if (Objects.isNull(flatTableDs)) {
            return;
        }

        // persist storage
        Path newPath = config.getFlatTableDir(project, dataFlowId, segmentId);
        ss.sparkContext().setLocalProperty("spark.scheduler.pool", "merge");
        ss.sparkContext().setJobDescription("Persist flat table.");
        flatTableDs.write().mode(SaveMode.Overwrite).parquet(newPath.toString());

        logger.info("Persist merged flat tables to path {} with schema [{}], " + "new segment id: {}, dataFlowId: {}",
                newPath, names, segmentId, dataFlowId);

        NDataflow dfCopied = dataFlow.copy();
        NDataSegment segmentCopied = dfCopied.getSegment(segmentId);
        segmentCopied.setFlatTableReady(true);

        NDataflowUpdate update = new NDataflowUpdate(dataFlowId);
        update.setToUpdateSegs(segmentCopied);
        dfMgr.updateDataflow(update);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfMergeJobInfo();
    }

}
