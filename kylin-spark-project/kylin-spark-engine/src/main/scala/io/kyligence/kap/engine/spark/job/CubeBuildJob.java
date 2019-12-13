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

package io.kyligence.kap.engine.spark.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.metadata.cube.ManagerHub;
import org.apache.kylin.engine.spark.metadata.cube.model.Cube;
import org.apache.kylin.engine.spark.metadata.cube.model.CubeUpdate2;
import org.apache.kylin.engine.spark.metadata.cube.model.DataLayout;
import org.apache.kylin.engine.spark.metadata.cube.model.DataSegment;
import org.apache.kylin.engine.spark.metadata.cube.model.IndexEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTreeFactory;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.NBuildSourceInfo;
import io.kyligence.kap.engine.spark.utils.BuildUtils;
import io.kyligence.kap.engine.spark.utils.JobMetrics;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.engine.spark.utils.Metrics;
import io.kyligence.kap.engine.spark.utils.QueryExecutionCache;

public class CubeBuildJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger( CubeBuildJob.class);
    protected static String TEMP_DIR_SUFFIX = "_temp";

//    private NDataflowManager dfMgr;
    private BuildLayoutWithUpdate buildLayoutWithUpdate;

    @Override
    protected void doExecute() throws Exception {
        long start = System.currentTimeMillis();
        logger.info("Start building cube job...");
        buildLayoutWithUpdate = new BuildLayoutWithUpdate();
//        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(MetadataConstants.P_SEGMENT_IDS)));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(MetadataConstants.P_LAYOUT_IDS));
//        dfMgr = NDataflowManager.getInstance(config, project);
        List<String> persistedFlatTable = new ArrayList<>();
        List<String> persistedViewFactTable = new ArrayList<>();
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        try {
//            IndexPlan indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan();
            Cube cube = ManagerHub.getCube(config, getParam(MetadataConstants.P_CUBE_ID));
            Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(cube, layoutIds).stream() //
                    .filter(Objects::nonNull) //
                    .collect(Collectors.toSet()); //

            //TODO: what if a segment is deleted during building?
            for (String segId : segmentIds) {
                SpanningTree SpanningTree = SpanningTreeFactory.fromLayouts(cuboids, cube.getUuid());
                DataSegment seg = getSegment(segId);

                // choose source
                ParentSourceChooser sourceChooser = new ParentSourceChooser(SpanningTree, seg, jobId, ss, config, true);
                sourceChooser.decideSources();
                NBuildSourceInfo buildFromFlatTable = sourceChooser.flatTableSource();
                Map<Long, NBuildSourceInfo> buildFromLayouts = sourceChooser.reuseSources();

                infos.clearCuboidsNumPerLayer(segId);
                
                // build cuboids from flat table
                if (buildFromFlatTable != null) {
                    collectPersistedTablePath(persistedFlatTable, persistedViewFactTable, sourceChooser, buildFromFlatTable);
                    build(Collections.singletonList(buildFromFlatTable), segId, SpanningTree);
                }

                // build cuboids from reused layouts
                if (!buildFromLayouts.isEmpty()) {
                    build(buildFromLayouts.values(), segId, SpanningTree);
                }
                infos.recordSpanningTree(segId, SpanningTree);
            }
            updateSegmentSourceBytesSize(cube, ResourceDetectUtils.getSegmentSourceSize(shareDir));
        } finally {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            for (String viewPath : persistedViewFactTable) {
                fs.delete(new Path(viewPath), true);
                logger.info("Delete persisted view fact table: {}.", viewPath);
            }
            for (String path : persistedFlatTable) {
                fs.delete(new Path(path), true);
                logger.info("Delete persisted flat table: {}.", path);
            }
            logger.info("Building job takes {} ms", (System.currentTimeMillis() - start));
        }
    }
    
    private void collectPersistedTablePath //
            (List<String> persistedFlatTable, //
            List<String> persistedViewFactTable, // 
            ParentSourceChooser sourceChooser, // 
            NBuildSourceInfo buildFromFlatTable) { //
        String flatTablePath = sourceChooser.persistFlatTableIfNecessary();
        if (!flatTablePath.isEmpty()) {
            persistedFlatTable.add(flatTablePath);
        }
        if (!buildFromFlatTable.getViewFactTablePath().isEmpty()) {
            persistedViewFactTable.add(buildFromFlatTable.getViewFactTablePath());
        }
    }

    private void updateSegmentSourceBytesSize(Cube cube, Map<String, Object> toUpdateSegmentSourceSize)
            throws Exception {
        //TODO[xyxy]: is this CopyOnWrite supposed to be preserved?
//        NDataflow dataflow = dfMgr.getDataflow(dataflowId);
//        NDataflow newDF = dataflow.copy();
        CubeUpdate2 update = new CubeUpdate2(cube.getUuid());
        List<DataSegment> DataSegments = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : toUpdateSegmentSourceSize.entrySet()) {
            DataSegment segment = cube.getSegment(entry.getKey());
            segment.setSourceBytesSize((Long) entry.getValue());
            segment.setLastBuildTime(System.currentTimeMillis());
            DataSegments.add(segment);
        }
        update.setToUpdateSegs(DataSegments.toArray(new DataSegment[0]));
        //TODO[xyxy]: There exists a class CubeUpdate2 for opensource kylin
//        dfMgr.updateDataflow(update);
        ManagerHub.updateCube(config, update);
    }

    private void build(Collection<NBuildSourceInfo> buildSourceInfos, String segId, SpanningTree st) {

        List<NBuildSourceInfo> theFirstLevelBuildInfos = buildLayer(buildSourceInfos, segId, st);
        LinkedList<List<NBuildSourceInfo>> queue = new LinkedList<>();

        if (!theFirstLevelBuildInfos.isEmpty()) {
            queue.offer(theFirstLevelBuildInfos);
        }

        while (!queue.isEmpty()) {
            List<NBuildSourceInfo> buildInfos = queue.poll();
            List<NBuildSourceInfo> theNextLayer = buildLayer(buildInfos, segId, st);
            if (!theNextLayer.isEmpty()) {
                queue.offer(theNextLayer);
            }
        }

    }

    // build current layer and return the next layer to be built.
    private List<NBuildSourceInfo> buildLayer(Collection<NBuildSourceInfo> buildSourceInfos, String segId, SpanningTree st) {
        DataSegment seg = getSegment(segId);
        int cuboidsNumInLayer = 0;

        // build current layer
        List<IndexEntity> allIndexesInCurrentLayer = new ArrayList<>();
        for (NBuildSourceInfo info : buildSourceInfos) {
            Collection<IndexEntity> toBuildCuboids = info.getToBuildCuboids();
            infos.recordParent2Children(seg.getLayout(info.getLayoutId()),
                    toBuildCuboids.stream().map(IndexEntity::getId).collect(Collectors.toList()));
            cuboidsNumInLayer += toBuildCuboids.size();
            Preconditions.checkState(!toBuildCuboids.isEmpty(), "To be built cuboids is empty.");
            Dataset<Row> parentDS = info.getParentDS();

            for (IndexEntity index : toBuildCuboids) {
                Preconditions.checkNotNull(parentDS, "Parent dataset is null when building.");
                buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {
                    @Override
                    public String getName() {
                        return "build-index-" + index.getId();
                    }

                    @Override
                    public List<DataLayout> build() throws IOException {
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
    private List<NBuildSourceInfo> constructTheNextLayerBuildInfos( //
            SpanningTree st, //
            DataSegment seg, //
            Collection<IndexEntity> allIndexesInCurrentLayer) { //

        List<NBuildSourceInfo> childrenBuildSourceInfos = new ArrayList<>();
        for (IndexEntity index : allIndexesInCurrentLayer) {
            Collection<IndexEntity> children = st.getChildrenByIndexPlan(index);

            if (!children.isEmpty()) {
                NBuildSourceInfo theRootLevelBuildInfos = new NBuildSourceInfo();
                theRootLevelBuildInfos.setSparkSession(ss);
                LayoutEntity layout = new ArrayList<>(st.getLayouts(index)).get(0);
                String path = NSparkCubingUtil.getStoragePath(getDataCuboid(seg, layout.getId()));
                theRootLevelBuildInfos.setLayoutId(layout.getId());
                theRootLevelBuildInfos.setParentStoragePath(path);
                theRootLevelBuildInfos.setToBuildCuboids(children);
                childrenBuildSourceInfos.add(theRootLevelBuildInfos);
            }
        }
        // return the next to be built layer.
        return childrenBuildSourceInfos;
    }

    @Override
    protected String calculateRequiredCores() throws Exception {
        if (config.getSparkEngineTaskImpactInstanceEnabled()) {
            Path shareDir = config.getJobTmpShareDir(project, jobId);
            String maxLeafTasksNums = maxLeafTasksNums(shareDir);
            logger.info("The maximum number of tasks required to run the job is {}", maxLeafTasksNums);
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            int factor = config.getSparkEngineTaskCoreFactor();
            int i = Double.valueOf(maxLeafTasksNums).intValue() / factor;
            logger.info("require cores: " + i);
            return String.valueOf(i);
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

    private DataSegment getSegment(String segId) {
        // ensure the seg is the latest.
//        return dfMgr.getDataflow(dataflowId).getSegment(segId);
        return ManagerHub.getCube(config, getParam(MetadataConstants.P_CUBE_ID))
                .getSegment(segId);
    }

    private List<DataLayout> buildIndex(DataSegment seg, IndexEntity cuboid, Dataset<Row> parent,
                                        SpanningTree spanningTree, long parentId) throws IOException {
        String parentName = String.valueOf(parentId);
        if (parentId == ParentSourceChooser.FLAT_TABLE_FLAG()) {
            parentName = "flat table";
        }
        logger.info("Build index:{}, in segment:{}", cuboid.getId(), seg.getId());
        LinkedList<DataLayout> layouts = Lists.newLinkedList();
        Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
        if (cuboid.getId() >= IndexEntity.TABLE_INDEX_START_ID) {
            Preconditions.checkArgument(cuboid.getMeasures().isEmpty());
            Dataset<Row> afterPrj = parent.select(NSparkCubingUtil.getColumns(dimIndexes));
            // TODO: shard number should respect the shard column defined in cuboid
            for (LayoutEntity layout : spanningTree.getLayouts(cuboid)) {
                logger.info("Build layout:{}, in index:{}", layout.getId(), cuboid.getId());
                ss.sparkContext().setJobDescription("build " + layout.getId() + " from parent " + parentName);
                Set<Integer> orderedDims = layout.getOrderedDimensions().keySet();
                Dataset<Row> afterSort = afterPrj.select(NSparkCubingUtil.getColumns(orderedDims))
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(orderedDims));
                layouts.add(saveAndUpdateLayout(afterSort, seg, layout));
            }
        } else {
            Dataset<Row> afterAgg = CuboidAggregator.agg(ss, parent, dimIndexes, cuboid.getEffectiveMeasures(), seg,
                    spanningTree);
            for (LayoutEntity layout : spanningTree.getLayouts(cuboid)) {
                logger.info("Build layout:{}, in index:{}", layout.getId(), cuboid.getId());
                ss.sparkContext().setJobDescription("build " + layout.getId() + " from parent " + parentName);
                Set<Integer> rowKeys = layout.getOrderedDimensions().keySet();

                Dataset<Row> afterSort = afterAgg
                        .select(NSparkCubingUtil.getColumns(rowKeys, layout.getOrderedMeasures().keySet()))
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(rowKeys));

                layouts.add(saveAndUpdateLayout(afterSort, seg, layout));
            }
        }
        ss.sparkContext().setJobDescription(null);
        logger.info("Finished Build index :{}, in segment:{}", cuboid.getId(), seg.getId());
        return layouts;
    }

    private DataLayout saveAndUpdateLayout(Dataset<Row> dataset, DataSegment seg, LayoutEntity layout)
            throws IOException {
        long layoutId = layout.getId();

        DataLayout dataCuboid = getDataCuboid(seg, layoutId);

        // for spark metrics
        String queryExecutionId = UUID.randomUUID().toString();
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), queryExecutionId);

        NSparkCubingEngine.NSparkCubingStorage storage = StorageFactory.createEngineAdapter(layout,
                NSparkCubingEngine.NSparkCubingStorage.class);
        String path = NSparkCubingUtil.getStoragePath(dataCuboid);
        String tempPath = path + TEMP_DIR_SUFFIX;
        // save to temp path
        storage.saveTo(tempPath, dataset, ss);

        JobMetrics metrics = JobMetricsUtils.collectMetrics(queryExecutionId);
        dataCuboid.setBuildJobId(jobId);
        long rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT());
        if (rowCount == -1) {
            infos.recordAbnormalLayouts(dataCuboid.getLayoutId(),
                    "'Job metrics seems null, use count() to collect cuboid rows.'");
            logger.warn("Can not get cuboid row cnt.");
        }
        dataCuboid.setRows(rowCount);
        dataCuboid.setSourceRows(metrics.getMetrics(Metrics.SOURCE_ROWS_CNT()));
        int partitionNum = BuildUtils.repartitionIfNeed(layout, dataCuboid, storage, path, tempPath,
                config, ss);
        dataCuboid.setPartitionNum(partitionNum);
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);
        BuildUtils.fillCuboidInfo(dataCuboid);
        return dataCuboid;
    }

    private DataLayout getDataCuboid(DataSegment seg, long layoutId) {
        return DataLayout.newDataLayout(seg.getCube(), seg.getId(), layoutId);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }

    public static void main(String[] args) {
        CubeBuildJob nDataflowBuildJob = new CubeBuildJob();
        nDataflowBuildJob.execute(args);
    }
}
