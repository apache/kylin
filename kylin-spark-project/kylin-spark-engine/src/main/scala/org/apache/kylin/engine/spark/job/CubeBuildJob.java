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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.NBuildSourceInfo;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.cube.ManagerHub;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.metadata.cube.model.ForestSpanningTree;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree;
import org.apache.kylin.engine.spark.utils.BuildUtils;
import org.apache.kylin.engine.spark.utils.JobMetrics;
import org.apache.kylin.engine.spark.utils.JobMetricsUtils;
import org.apache.kylin.engine.spark.utils.Metrics;
import org.apache.kylin.engine.spark.utils.QueryExecutionCache;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import scala.collection.JavaConversions;

public class CubeBuildJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(CubeBuildJob.class);
    protected static String TEMP_DIR_SUFFIX = "_temp";

    private CubeManager cubeManager;
    private CubeInstance cubeInstance;
    private BuildLayoutWithUpdate buildLayoutWithUpdate;
    private Map<Long, Short> cuboidShardNum = Maps.newConcurrentMap();
    private Map<Long, Long> cuboidsRowCount = Maps.newConcurrentMap();
    public static void main(String[] args) {
        CubeBuildJob cubeBuildJob = new CubeBuildJob();
        cubeBuildJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {

        long start = System.currentTimeMillis();
        logger.info("Start building cube job...");
        buildLayoutWithUpdate = new BuildLayoutWithUpdate();
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(MetadataConstants.P_SEGMENT_IDS)));
        cubeManager = CubeManager.getInstance(config);
        cubeInstance = cubeManager.getCubeByUuid(getParam(MetadataConstants.P_CUBE_ID));
        List<String> persistedFlatTable = new ArrayList<>();
        List<String> persistedViewFactTable = new ArrayList<>();
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        try {
            //TODO: what if a segment is deleted during building?
            for (String segId : segmentIds) {
                SegmentInfo seg = ManagerHub.getSegmentInfo(config, getParam(MetadataConstants.P_CUBE_ID), segId);
                SpanningTree spanningTree = new ForestSpanningTree(
                        JavaConversions.asJavaCollection(seg.toBuildLayouts()));
                // choose source
                ParentSourceChooser sourceChooser = new ParentSourceChooser(spanningTree, seg, jobId, ss, config, true);
                sourceChooser.decideSources();
                NBuildSourceInfo buildFromFlatTable = sourceChooser.flatTableSource();
                Map<Long, NBuildSourceInfo> buildFromLayouts = sourceChooser.reuseSources();

                infos.clearCuboidsNumPerLayer(segId);

                // build cuboids from flat table
                if (buildFromFlatTable != null) {
                    collectPersistedTablePath(persistedFlatTable, sourceChooser);
                    build(Collections.singletonList(buildFromFlatTable), seg, spanningTree);
                }

                // build cuboids from reused layouts
                if (!buildFromLayouts.isEmpty()) {
                    build(buildFromLayouts.values(), seg, spanningTree);
                }
                infos.recordSpanningTree(segId, spanningTree);

                logger.info("Updating segment info");
                updateSegmentInfo(getParam(MetadataConstants.P_CUBE_ID), seg, buildFromFlatTable.getFlattableDS().count());
            }
            updateSegmentSourceBytesSize(getParam(MetadataConstants.P_CUBE_ID),
                    ResourceDetectUtils.getSegmentSourceSize(shareDir));
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

    private void updateSegmentInfo(String cubeId, SegmentInfo segmentInfo, long sourceRowCount) throws IOException {
        CubeInstance cubeInstance = cubeManager.getCubeByUuid(cubeId);
        CubeInstance cubeCopy = cubeInstance.latestCopyForWrite();
        CubeUpdate update = new CubeUpdate(cubeCopy);

        List<CubeSegment> cubeSegments = Lists.newArrayList();
        CubeSegment segment = cubeCopy.getSegmentById(segmentInfo.id());
        segment.setSizeKB(segmentInfo.getAllLayoutSize() / 1024);
        segment.setLastBuildTime(System.currentTimeMillis());
        segment.setLastBuildJobID(getParam(MetadataConstants.P_JOB_ID));
        segment.setInputRecords(sourceRowCount);
        segment.setSnapshots(new ConcurrentHashMap<>(segmentInfo.getSnapShot2JavaMap()));
        segment.setCuboidShardNums(cuboidShardNum);
        Map<String, String> additionalInfo = segment.getAdditionalInfo();
        additionalInfo.put("storageType", "" + IStorageAware.ID_PARQUET);
        segment.setAdditionalInfo(additionalInfo);
        cubeSegments.add(segment);
        update.setToUpdateSegs(cubeSegments.toArray(new CubeSegment[0]));
        cubeManager.updateCube(update);
    }

    private void collectPersistedTablePath(List<String> persistedFlatTable, ParentSourceChooser sourceChooser) {
        String flatTablePath = sourceChooser.persistFlatTableIfNecessary();
        if (!flatTablePath.isEmpty()) {
            persistedFlatTable.add(flatTablePath);
        }
    }

    private void updateSegmentSourceBytesSize(String cubeId, Map<String, Object> toUpdateSegmentSourceSize)
            throws IOException {
        CubeInstance cubeInstance = cubeManager.getCubeByUuid(cubeId);
        CubeInstance cubeCopy = cubeInstance.latestCopyForWrite();
        CubeUpdate update = new CubeUpdate(cubeCopy);
        List<CubeSegment> cubeSegments = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : toUpdateSegmentSourceSize.entrySet()) {
            CubeSegment segment = cubeCopy.getSegmentById(entry.getKey());
            segment.setInputRecordsSize((Long) entry.getValue());
            segment.setLastBuildTime(System.currentTimeMillis());
            cubeSegments.add(segment);
        }
        update.setToUpdateSegs(cubeSegments.toArray(new CubeSegment[0]));
        cubeManager.updateCube(update);
    }

    private void build(Collection<NBuildSourceInfo> buildSourceInfos, SegmentInfo seg, SpanningTree st) {

        List<NBuildSourceInfo> theFirstLevelBuildInfos = buildLayer(buildSourceInfos, seg, st);
        LinkedList<List<NBuildSourceInfo>> queue = new LinkedList<>();

        if (!theFirstLevelBuildInfos.isEmpty()) {
            queue.offer(theFirstLevelBuildInfos);
        }

        while (!queue.isEmpty()) {
            List<NBuildSourceInfo> buildInfos = queue.poll();
            List<NBuildSourceInfo> theNextLayer = buildLayer(buildInfos, seg, st);
            if (!theNextLayer.isEmpty()) {
                queue.offer(theNextLayer);
            }
        }

    }

    // build current layer and return the next layer to be built.
    private List<NBuildSourceInfo> buildLayer(Collection<NBuildSourceInfo> buildSourceInfos, SegmentInfo seg,
                                              SpanningTree st) {
        int cuboidsNumInLayer = 0;

        // build current layer
        List<LayoutEntity> allIndexesInCurrentLayer = new ArrayList<>();
        for (NBuildSourceInfo info : buildSourceInfos) {
            Collection<LayoutEntity> toBuildCuboids = info.getToBuildCuboids();
            infos.recordParent2Children(info.getLayout(),
                    toBuildCuboids.stream().map(LayoutEntity::getId).collect(Collectors.toList()));
            cuboidsNumInLayer += toBuildCuboids.size();
            Preconditions.checkState(!toBuildCuboids.isEmpty(), "To be built cuboids is empty.");
            Dataset<Row> parentDS = info.getParentDS();
            // record the source count of flat table
            if (info.getLayoutId() == ParentSourceChooser.FLAT_TABLE_FLAG()) {
                cuboidsRowCount.putIfAbsent(info.getLayoutId(), parentDS.count());
            }

            for (LayoutEntity index : toBuildCuboids) {
                Preconditions.checkNotNull(parentDS, "Parent dataset is null when building.");
                buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {
                    @Override
                    public String getName() {
                        return "build-cuboid-" + index.getId();
                    }

                    @Override
                    public LayoutEntity build() throws IOException {
                        return buildCuboid(seg, index, parentDS, st, info.getLayoutId());
                    }
                }, config);
                allIndexesInCurrentLayer.add(index);
            }
        }

        infos.recordCuboidsNumPerLayer(seg.id(), cuboidsNumInLayer);
        buildLayoutWithUpdate.updateLayout(seg, config);

        // decided the next layer by current layer's all indexes.
        st.decideTheNextLayer(allIndexesInCurrentLayer, seg);
        return constructTheNextLayerBuildInfos(st, seg, allIndexesInCurrentLayer);
    }

    // decided and construct the next layer.
    private List<NBuildSourceInfo> constructTheNextLayerBuildInfos(SpanningTree st, SegmentInfo seg,
                                                                   Collection<LayoutEntity> allIndexesInCurrentLayer) {

        List<NBuildSourceInfo> childrenBuildSourceInfos = new ArrayList<>();
        for (LayoutEntity index : allIndexesInCurrentLayer) {
            Collection<LayoutEntity> children = st.getChildrenByIndexPlan(index);

            if (!children.isEmpty()) {
                NBuildSourceInfo theRootLevelBuildInfos = new NBuildSourceInfo();
                theRootLevelBuildInfos.setSparkSession(ss);
                String path = PathManager.getParquetStoragePath(config, getParam(MetadataConstants.P_CUBE_NAME), seg.name(), seg.identifier(),
                        String.valueOf(index.getId()));
                theRootLevelBuildInfos.setLayoutId(index.getId());
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

    private LayoutEntity buildCuboid(SegmentInfo seg, LayoutEntity cuboid, Dataset<Row> parent,
                                    SpanningTree spanningTree, long parentId) throws IOException {
        String parentName = String.valueOf(parentId);
        if (parentId == ParentSourceChooser.FLAT_TABLE_FLAG()) {
            parentName = "flat table";
        }
        logger.info("Build index:{}, in segment:{}", cuboid.getId(), seg.id());
        LayoutEntity layoutEntity = cuboid;
        Set<Integer> dimIndexes = cuboid.getOrderedDimensions().keySet();
        if (cuboid.isTableIndex()) {
            Dataset<Row> afterPrj = parent.select(NSparkCubingUtil.getColumns(dimIndexes));
            // TODO: shard number should respect the shard column defined in cuboid
            logger.info("Build layout:{}, in index:{}", layoutEntity.getId(), cuboid.getId());
            ss.sparkContext().setJobDescription("build " + layoutEntity.getId() + " from parent " + parentName);
            Set<Integer> orderedDims = layoutEntity.getOrderedDimensions().keySet();
            Dataset<Row> afterSort = afterPrj.select(NSparkCubingUtil.getColumns(orderedDims))
                    .sortWithinPartitions(NSparkCubingUtil.getFirstColumn(orderedDims));
            saveAndUpdateLayout(afterSort, seg, layoutEntity, parentId);
        } else {
            Dataset<Row> afterAgg = CuboidAggregator.agg(ss, parent, dimIndexes, cuboid.getOrderedMeasures(),
                    spanningTree, false);
            logger.info("Build layout:{}, in index:{}", layoutEntity.getId(), cuboid.getId());
            ss.sparkContext().setJobDescription("build " + layoutEntity.getId() + " from parent " + parentName);
            Set<Integer> rowKeys = layoutEntity.getOrderedDimensions().keySet();

            Dataset<Row> afterSort = afterAgg
                    .select(NSparkCubingUtil.getColumns(rowKeys, layoutEntity.getOrderedMeasures().keySet()))
                    .sortWithinPartitions(NSparkCubingUtil.getFirstColumn(rowKeys));

            saveAndUpdateLayout(afterSort, seg, layoutEntity, parentId);
        }
        ss.sparkContext().setJobDescription(null);
        logger.info("Finished Build index :{}, in segment:{}", cuboid.getId(), seg.id());
        return layoutEntity;
    }

    private void saveAndUpdateLayout(Dataset<Row> dataset, SegmentInfo seg, LayoutEntity layout,
                                     long parentId) throws IOException {
        long layoutId = layout.getId();

        // for spark metrics
        String queryExecutionId = UUID.randomUUID().toString();
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), queryExecutionId);

        NSparkCubingEngine.NSparkCubingStorage storage = StorageFactory.createEngineAdapter(layout,
                NSparkCubingEngine.NSparkCubingStorage.class);
        String path = PathManager.getParquetStoragePath(config, getParam(MetadataConstants.P_CUBE_NAME), seg.name(), seg.identifier(),
                String.valueOf(layoutId));
        String tempPath = path + TEMP_DIR_SUFFIX;
        // save to temp path
        logger.info("Cuboids are saved to temp path : " + tempPath);
        storage.saveTo(tempPath, dataset, ss);

        JobMetrics metrics = JobMetricsUtils.collectMetrics(queryExecutionId);
        long rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT());
        if (rowCount == -1) {
            infos.recordAbnormalLayouts(layoutId, "'Job metrics seems null, use count() to collect cuboid rows.'");
            logger.debug("Can not get cuboid row cnt, use count() to collect cuboid rows.");
            long cuboidRowCnt = dataset.count();
            layout.setRows(cuboidRowCnt);
            // record the row count of cuboid
            cuboidsRowCount.putIfAbsent(layoutId, cuboidRowCnt);
            layout.setSourceRows(cuboidsRowCount.get(parentId));
        } else {
            layout.setRows(rowCount);
            layout.setSourceRows(metrics.getMetrics(Metrics.SOURCE_ROWS_CNT()));
        }
        int shardNum = BuildUtils.repartitionIfNeed(layout, storage, path, tempPath, cubeInstance.getConfig(), ss);
        layout.setShardNum(shardNum);
        cuboidShardNum.put(layoutId, (short)shardNum);
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);
        BuildUtils.fillCuboidInfo(layout, path);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }
}
