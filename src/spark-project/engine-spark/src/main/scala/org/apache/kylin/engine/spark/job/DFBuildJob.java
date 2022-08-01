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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.NBuildSourceInfo;
import org.apache.kylin.engine.spark.builder.SnapshotBuilder;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTree;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTreeFactory;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.datasource.storage.StorageStore;
import org.apache.spark.sql.datasource.storage.StorageStoreFactory;
import org.apache.spark.sql.datasource.storage.StorageStoreUtils;
import org.apache.spark.sql.datasource.storage.WriteTaskStats;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;
import scala.collection.JavaConversions;

/**
 * After KE 4.3, we use {@link org.apache.kylin.engine.spark.job.SegmentBuildJob} to build segment
 */
@Deprecated
public class DFBuildJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(DFBuildJob.class);
    protected static String TEMP_DIR_SUFFIX = "_temp";
    public final HashMap<String, Long> seg2Count = new HashMap<>();
    protected NDataflowManager dfMgr;
    protected BuildLayoutWithUpdate buildLayoutWithUpdate;

    public static void main(String[] args) {
        DFBuildJob nDataflowBuildJob = new DFBuildJob();
        nDataflowBuildJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        buildLayoutWithUpdate = new BuildLayoutWithUpdate();
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS), ","));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        dfMgr = NDataflowManager.getInstance(config, project);
        List<String> persistedFlatTable = new ArrayList<>();
        List<String> persistedViewFactTable = new ArrayList<>();
        Path shareDir = config.getJobTmpShareDir(project, jobId);

        if (config.isBuildCheckPartitionColEnabled()) {
            checkDateFormatIfExist(project, dataflowId);
        }

        IndexPlan indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan();
        Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(indexPlan, layoutIds).stream().filter(Objects::nonNull)
                .collect(Collectors.toSet());

        buildSnapshot();

        //TODO: what if a segment is deleted during building?
        for (String segId : segmentIds) {
            NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId);
            NDataSegment seg = getSegment(segId);
            if (needSkipSegment(segId)) {
                continue;
            }
            // choose source
            DFChooser datasetChooser = new DFChooser(nSpanningTree, seg, jobId, ss, config, true);
            datasetChooser.decideSources();
            NBuildSourceInfo buildFromFlatTable = datasetChooser.flatTableSource();
            Map<Long, NBuildSourceInfo> buildFromLayouts = datasetChooser.reuseSources();
            infos.clearCuboidsNumPerLayer(segId);
            // build cuboids from reused layouts
            if (!buildFromLayouts.isEmpty()) {
                NBuildSourceInfo min = Collections.min(buildFromLayouts.values(),
                        (o1, o2) -> Math.toIntExact(o1.getCount() - o2.getCount()));
                long count = SanityChecker.getCount(min.getParentDS(), indexPlan.getLayoutEntity(min.getLayoutId()));
                seg2Count.put(segId, count);
                build(buildFromLayouts.values(), segId, nSpanningTree);
            }

            // build cuboids from flat table
            if (buildFromFlatTable != null) {
                val path = datasetChooser.persistFlatTableIfNecessary();
                if (!path.isEmpty()) {
                    logger.info("FlatTable persisted, compute column size");
                    persistedFlatTable.add(path);
                    computeColumnBytes(datasetChooser, seg, dataflowId, path);
                } else {
                    logger.info("FlatTable not persisted, only compute row count");
                    long rowCount = buildFromFlatTable.getFlattableDS().count();
                    updateColumnBytesInseg(dataflowId, new HashMap<>(), seg.getId(), rowCount);
                }
                if (!StringUtils.isBlank(buildFromFlatTable.getViewFactTablePath())) {
                    persistedViewFactTable.add(buildFromFlatTable.getViewFactTablePath());
                }
                if (!seg2Count.containsKey(segId)) {
                    seg2Count.put(segId, buildFromFlatTable.getParentDS().count());
                }
                build(Collections.singletonList(buildFromFlatTable), segId, nSpanningTree);
            }
            infos.recordSpanningTree(segId, nSpanningTree);
        }
        Map<String, Object> segmentSourceSize = ResourceDetectUtils.getSegmentSourceSize(shareDir);
        updateSegmentSourceBytesSize(dataflowId, segmentSourceSize);

        tailingCleanups(segmentIds, persistedFlatTable, persistedViewFactTable);
        buildLayoutWithUpdate.shutDown();
    }

    protected void buildSnapshot() throws IOException {
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        SnapshotBuilder snapshotBuilder = new SnapshotBuilder();
        if (!config.isSnapshotManualManagementEnabled()) {
            //snapshot building
            snapshotBuilder.buildSnapshot(ss, dfMgr.getDataflow(dataflowId).getModel(), getIgnoredSnapshotTables());
        } else {
            //calculate total rows
            logger.info("Skip snapshot build in snapshot manual mode, dataflow: {}, only calculate total rows",
                    dataflowId);
            snapshotBuilder.calculateTotalRows(ss, dfMgr.getDataflow(dataflowId).getModel(),
                    getIgnoredSnapshotTables());
        }
    }

    private void computeColumnBytes(DFChooser datasetChooser, NDataSegment seg, String dataflowId, String path) {
        ss.sparkContext().setJobDescription("Compute column bytes");
        val df = ss.read().parquet(path);
        val columnBytes = (Map<String, Object>) JavaConversions.mapAsJavaMap(datasetChooser.computeColumnBytes(df));
        updateColumnBytesInseg(dataflowId, columnBytes, seg.getId(), df.count());
        ss.sparkContext().setJobDescription(null);
    }

    private boolean needSkipSegment(String segId) {
        NDataSegment seg = getSegment(segId);
        if (seg == null || seg.getSegRange() == null || seg.getModel() == null || seg.getIndexPlan() == null) { // vivo
            logger.info("Skip segment {}", segId);
            if (seg != null)
                logger.info("Args is {} {} {}", seg.getSegRange(), seg.getModel(), seg.getIndexPlan());
            return true;
        }
        return false;
    }

    public void updateColumnBytesInseg(String dataflowId, Map<String, Object> columnBytes, String id, long rowCount) {
        HashMap<String, Long> map = Maps.newHashMap();
        val rows = config.getCapacitySampleRows();
        double multiple = 0D;
        if (rowCount < rows)
            multiple = 1D;
        else
            multiple = (double) rowCount / rows;
        for (Map.Entry<String, Object> entry : columnBytes.entrySet()) {
            map.put(entry.getKey(), (long) (Long.parseLong(entry.getValue().toString()) * multiple));
        }
        NDataflow dataflow = dfMgr.getDataflow(dataflowId);
        NDataflow newDF = dataflow.copy();
        val update = new NDataflowUpdate(dataflow.getUuid());
        List<NDataSegment> nDataSegments = Lists.newArrayList();
        NDataSegment segment = newDF.getSegment(id);
        segment.setSourceCount(rowCount);
        segment.getColumnSourceBytes().putAll(map);
        nDataSegments.add(segment);

        update.setToUpdateSegs(nDataSegments.toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

    }

    public void tailingCleanups(Set<String> segmentIds, List<String> flatTables, List<String> factViews)
            throws IOException {
        val fs = HadoopUtil.getWorkingFileSystem();
        for (String viewPath : factViews) {
            fs.delete(new Path(viewPath), true);
            logger.debug("Delete persisted view fact table: {}.", viewPath);
        }

        if (!config.isPersistFlatTableEnabled()) {
            for (String path : flatTables) {
                fs.delete(new Path(path), true);
                logger.debug("Delete persisted flat table: {}.", path);
            }
        }
        resetSegmentMemOnly(segmentIds, !config.isPersistFlatTableEnabled());
    }

    protected void updateSegmentSourceBytesSize(String dataflowId, Map<String, Object> toUpdateSegmentSourceSize) {
        NDataflow dataflow = dfMgr.getDataflow(dataflowId);
        NDataflow newDF = dataflow.copy();
        val update = new NDataflowUpdate(dataflow.getUuid());
        List<NDataSegment> dataSegments = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : toUpdateSegmentSourceSize.entrySet()) {
            NDataSegment segment = newDF.getSegment(entry.getKey());
            if (Objects.isNull(segment)) {
                logger.info("Skip empty segment {} when updating segment source", entry.getKey());
                continue;
            }
            segment.setSourceBytesSize((Long) entry.getValue());
            segment.setLastBuildTime(System.currentTimeMillis());
            dataSegments.add(segment);
        }
        update.setToUpdateSegs(dataSegments.toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
        indexPlanManager.updateIndexPlan(dataflowId, copyForWrite -> {
            copyForWrite
                    .setLayoutBucketNumMapping(indexPlanManager.getIndexPlan(dataflowId).getLayoutBucketNumMapping());
        });
    }

    @Override
    protected String calculateRequiredCores() throws Exception {
        if (config.getSparkEngineTaskImpactInstanceEnabled()) {
            Path shareDir = config.getJobTmpShareDir(project, jobId);
            String maxLeafTasksNums = maxLeafTasksNums(shareDir);
            val config = KylinConfig.getInstanceFromEnv();
            val factor = config.getSparkEngineTaskCoreFactor();
            int requiredCore = (int) Double.parseDouble(maxLeafTasksNums) / factor;
            logger.info("The maximum number of tasks required to run the job is {}, require cores: {}",
                    maxLeafTasksNums, requiredCore);
            return String.valueOf(requiredCore);
        } else {
            return SparkJobConstants.DEFAULT_REQUIRED_CORES;
        }
    }

    private String maxLeafTasksNums(Path shareDir) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FileStatus[] fileStatuses = fs.listStatus(shareDir,
                path -> path.toString().endsWith(ResourceDetectUtils.cubingDetectItemFileSuffix()));
        return ResourceDetectUtils.selectMaxValueInFiles(fileStatuses);
    }

    public NDataSegment getSegment(String segId) {
        // ensure the seg is the latest.
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        return dfMgr.getDataflow(dataflowId).getSegment(segId);
    }

    protected void build(Collection<NBuildSourceInfo> buildSourceInfos, String segId, NSpanningTree st)
            throws IOException {

        val theFirstLevelBuildInfos = buildLayer(buildSourceInfos, segId, st);
        val queue = new LinkedList<List<NBuildSourceInfo>>();

        if (!theFirstLevelBuildInfos.isEmpty()) {
            queue.offer(theFirstLevelBuildInfos);
        }

        while (!queue.isEmpty()) {
            val buildInfos = queue.poll();
            val theNextLayer = buildLayer(buildInfos, segId, st);
            if (!theNextLayer.isEmpty()) {
                queue.offer(theNextLayer);
            }
        }

    }

    // build current layer and return the next layer to be built.
    private List<NBuildSourceInfo> buildLayer(Collection<NBuildSourceInfo> buildSourceInfos, String segId,
            NSpanningTree st) throws IOException {
        val seg = getSegment(segId);
        int cuboidsNumInLayer = 0;

        // build current layer
        List<IndexEntity> allIndexesInCurrentLayer = new ArrayList<>();
        for (NBuildSourceInfo info : buildSourceInfos) {
            Collection<IndexEntity> toBuildCuboids = info.getToBuildCuboids();
            HashSet children = toBuildCuboids.stream().map(IndexEntity::getId)
                    .collect(Collectors.toCollection(Sets::newHashSet));
            infos.recordParent2Children(seg.getLayout(info.getLayoutId()), children);
            cuboidsNumInLayer += toBuildCuboids.size();
            Preconditions.checkState(!toBuildCuboids.isEmpty(), "To be built cuboids is empty.");
            Dataset<Row> parentDS = info.getParentDS();

            for (IndexEntity index : toBuildCuboids) {
                Preconditions.checkNotNull(parentDS, "Parent dataset is null when building.");
                buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {
                    @Override
                    public long getIndexId() {
                        return index.getId();
                    }

                    @Override
                    public String getName() {
                        return "build-index-" + index.getId();
                    }

                    @Override
                    public List<NDataLayout> build() throws IOException {
                        return buildIndex(seg, index, parentDS, st, info.getLayoutId());
                    }
                }, config);
                allIndexesInCurrentLayer.add(index);
            }
        }

        infos.recordCuboidsNumPerLayer(segId, cuboidsNumInLayer);
        buildLayoutWithUpdate.updateLayout(seg, config, project);

        // decided the next layer by current layer's all indexes.
        st.decideTheNextLayer(allIndexesInCurrentLayer, getSegment(segId));
        return constructTheNextLayerBuildInfos(st, seg, allIndexesInCurrentLayer);
    }

    // decided and construct the next layer.
    protected List<NBuildSourceInfo> constructTheNextLayerBuildInfos( //
            NSpanningTree st, //
            NDataSegment seg, //
            Collection<IndexEntity> allIndexesInCurrentLayer) { //

        val childrenBuildSourceInfos = new ArrayList<NBuildSourceInfo>();
        for (IndexEntity index : allIndexesInCurrentLayer) {
            val children = st.getChildrenByIndexPlan(index);

            if (!children.isEmpty()) {
                val theRootLevelBuildInfos = new NBuildSourceInfo();
                theRootLevelBuildInfos.setSparkSession(ss);
                LayoutEntity layout = new ArrayList<>(st.getLayouts(index)).get(0);
                theRootLevelBuildInfos.setLayoutId(layout.getId());
                theRootLevelBuildInfos.setParentStorageDF(StorageStoreUtils.toDF(seg, layout, ss));
                theRootLevelBuildInfos.setToBuildCuboids(children);
                childrenBuildSourceInfos.add(theRootLevelBuildInfos);
            }
        }
        // return the next to be built layer.
        return childrenBuildSourceInfos;
    }

    private List<NDataLayout> buildIndex(NDataSegment seg, IndexEntity cuboid, Dataset<Row> parent,
            NSpanningTree nSpanningTree, long parentId) throws IOException {
        String parentName = String.valueOf(parentId);
        if (parentId == DFChooser.FLAT_TABLE_FLAG()) {
            parentName = "flat table";
        }
        logger.info("Build index:{}, in segment:{}", cuboid.getId(), seg.getId());
        LinkedList<NDataLayout> layouts = Lists.newLinkedList();
        Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
        Dataset<Row> afterPrj;
        Function<LayoutEntity, Column[]> toOrder;
        if (IndexEntity.isTableIndex(cuboid.getId())) {
            Preconditions.checkArgument(cuboid.getMeasures().isEmpty());
            afterPrj = parent.select(NSparkCubingUtil.getColumns(dimIndexes));
            toOrder = layout -> NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet());
        } else {
            afterPrj = CuboidAggregator.agg(parent, dimIndexes, cuboid.getEffectiveMeasures(), seg, nSpanningTree);
            toOrder = layout -> NSparkCubingUtil.getColumns(layout.getOrderedDimensions().keySet(),
                    layout.getOrderedMeasures().keySet());
        }
        for (LayoutEntity layout : nSpanningTree.getLayouts(cuboid)) {
            if (seg.isAlreadyBuilt(layout.getId())) {
                logger.info("Skip already built layout:{}, in index:{}", layout.getId(), cuboid.getId());
                continue;
            }
            logger.info("Build layout:{}, in index:{}", layout.getId(), cuboid.getId());
            ss.sparkContext().setJobDescription("build " + layout.getId() + " from parent " + parentName);
            Set<Integer> rowKeys = layout.getOrderedDimensions().keySet();

            Dataset<Row> afterSort = afterPrj.select(toOrder.apply(layout))
                    .sortWithinPartitions(NSparkCubingUtil.getColumns(rowKeys));
            layouts.add(saveAndUpdateLayout(afterSort, seg, layout));

            onLayoutFinished(layout.getId());
        }
        ss.sparkContext().setJobDescription(null);
        logger.info("Finished Build index :{}, in segment:{}", cuboid.getId(), seg.getId());
        return layouts;
    }

    protected NDataLayout saveAndUpdateLayout(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout)
            throws IOException {
        ss.sparkContext().setLocalProperty("spark.scheduler.pool", "build");
        long layoutId = layout.getId();
        NDataLayout dataLayout = getDataLayout(seg, layoutId);
        val path = NSparkCubingUtil.getStoragePath(seg, layoutId);
        int storageType = layout.getModel().getStorageType();
        StorageStore storage = StorageStoreFactory.create(storageType);
        storage.setStorageListener(new SanityChecker(seg2Count.getOrDefault(seg.getId(), SanityChecker.SKIP_FLAG())));
        WriteTaskStats taskStats = storage.save(layout, new Path(path), KapConfig.wrap(config), dataset);
        dataLayout.setBuildJobId(jobId);
        long rowCount = taskStats.numRows();
        if (rowCount == -1) {
            KylinBuildEnv.get().buildJobInfos().recordAbnormalLayouts(layout.getId(),
                    "Job metrics seems null, use count() to collect cuboid rows.");
            logger.warn("Can not get cuboid={} row cnt.", layout.getId());
        }
        dataLayout.setRows(rowCount);
        dataLayout.setSourceRows(taskStats.sourceRows());
        dataLayout.setPartitionNum(taskStats.numBucket());
        dataLayout.setPartitionValues(taskStats.partitionValues());
        dataLayout.setFileCount(taskStats.numFiles());
        dataLayout.setByteSize(taskStats.numBytes());
        dataLayout.setReady(true);
        return dataLayout;
    }

    protected NDataLayout getDataLayout(NDataSegment seg, long layoutId) {
        return NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layoutId);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }

    private void resetSegmentMemOnly(Set<String> segmentIds, final boolean resetFlatTable) {
        // reset flags in mem meta-store before being dumped to meta_out
        // segment resumable flags shouldn't cross building jobs
        Optional.ofNullable(segmentIds).orElseGet(Collections::emptySet).forEach(segId -> {
            NDataSegment readOnlySeg = getSegment(segId);
            NDataflow readOnlyDf = readOnlySeg.getDataflow();
            NDataflow dfCopy = readOnlyDf.copy();
            NDataSegment segCopy = dfCopy.getSegment(readOnlySeg.getId());

            // reset
            segCopy.setDictReady(false);
            if (resetFlatTable) {
                segCopy.setFlatTableReady(false);
            }
            segCopy.setFactViewReady(false);

            NDataflowUpdate dfUpdate = new NDataflowUpdate(readOnlyDf.getId());
            dfUpdate.setToUpdateSegs(segCopy);

            NDataflowManager.getInstance(config, project).updateDataflow(dfUpdate);
        });
    }

}
