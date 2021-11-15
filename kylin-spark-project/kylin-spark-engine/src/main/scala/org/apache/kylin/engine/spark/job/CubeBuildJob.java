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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.engine.mr.common.StatisticsDecisionUtil;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.shaded.com.google.common.base.Joiner;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

import scala.Tuple2;
import scala.collection.JavaConversions;

public class CubeBuildJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(CubeBuildJob.class);
    protected static String TEMP_DIR_SUFFIX = "_temp";

    private CubeManager cubeManager;
    private CubeInstance cubeInstance;
    private BuildLayoutWithUpdate buildLayoutWithUpdate;
    private Map<Long, Short> cuboidShardNum = Maps.newConcurrentMap();
    private Map<Long, Long> cuboidsRowCount = Maps.newConcurrentMap();
    private Map<Long, Long> recommendCuboidMap = new HashMap<>();

    public static void main(String[] args) {
        CubeBuildJob cubeBuildJob = new CubeBuildJob();
        cubeBuildJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {

        long start = System.currentTimeMillis();
        logger.info("Start building cube job for {} ...", getParam(MetadataConstants.P_SEGMENT_IDS));
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(MetadataConstants.P_SEGMENT_IDS)));

        // For now, Kylin should only build one segment in one time, cube planner has this restriction (maybe we can remove this limitation later)
        Preconditions.checkArgument(segmentIds.size() == 1, "Build one segment in one time.");

        String firstSegmentId = segmentIds.iterator().next();
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        cubeManager = CubeManager.getInstance(config);
        cubeInstance = cubeManager.getCubeByUuid(cubeId);
        CubeSegment newSegment = cubeInstance.getSegmentById(firstSegmentId);
        SpanningTree spanningTree;
        ParentSourceChooser sourceChooser;

        // Cuboid Statistics is served for Cube Planner Phase One at the moment
        boolean needStatistics = StatisticsDecisionUtil.isAbleToOptimizeCubingPlan(newSegment)
                || config.isSegmentStatisticsEnabled();

        if (needStatistics) {
            // 1.1 Call CuboidStatistics#statistics
            SegmentInfo statisticsSeg = ManagerHub.getSegmentInfo(config, cubeId, firstSegmentId, CuboidModeEnum.CURRENT_WITH_BASE);
            long startMills = System.currentTimeMillis();
            spanningTree = new ForestSpanningTree(JavaConversions.asJavaCollection(statisticsSeg.toBuildLayouts()));
            sourceChooser = new ParentSourceChooser(spanningTree, statisticsSeg, newSegment, jobId, ss, config, false);
            sourceChooser.setNeedStatistics();
            sourceChooser.decideFlatTableSource(null);
            Map<Long, HLLCounter> hllMap = new HashMap<>();
            for (Tuple2<Object, AggInfo> cuboidData : sourceChooser.aggInfo()) {
                hllMap.put((Long) cuboidData._1, cuboidData._2.cuboid().counter());
            }
            logger.info("Cuboid statistics return {} records and cost {} ms.", hllMap.size(), (System.currentTimeMillis() - startMills));

            // 1.2 Save cuboid statistics
            String jobTmpDir = config.getJobTmpDir(project) + "/" + jobId;
            Path statisticsDir = new Path(jobTmpDir + "/" + ResourceStore.CUBE_STATISTICS_ROOT + "/" + cubeId + "/" + firstSegmentId + "/");
            Optional<HLLCounter> hll = hllMap.values().stream().max(Comparator.comparingLong(HLLCounter::getCountEstimate));
            long rc = hll.map(HLLCounter::getCountEstimate).orElse(1L);
            CubeStatsWriter.writeCuboidStatistics(HadoopUtil.getCurrentConfiguration(), statisticsDir, hllMap, 1, rc);

            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            ResourceStore rs = ResourceStore.getStore(config);
            String metaKey = newSegment.getStatisticsResourcePath();
            Path statisticsFile = new Path(statisticsDir, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);
            FSDataInputStream is = fs.open(statisticsFile);
            rs.putResource(metaKey, is, System.currentTimeMillis()); // write to Job-Local metastore
            logger.info("{}'s stats saved to resource key({}) with path({})", newSegment, metaKey, statisticsFile);

            // 1.3 Trigger cube planner phase one and save optimized cuboid set into CubeInstance
            recommendCuboidMap = StatisticsDecisionUtil.optimizeCubingPlan(newSegment);
            if (!recommendCuboidMap.isEmpty()) {
                logger.info("Triggered cube planner phase one. The number of recommend cuboids is {}. " +
                        "If there are too many cuboids, " +
                        "you can reduce the number of recommend cuboids by reducing the value of " +
                        "'kylin.cube.cubeplanner.expansion-threshold'", recommendCuboidMap.size());
            }
        }

        buildLayoutWithUpdate = new BuildLayoutWithUpdate(config);
        List<String> persistedFlatTable = new ArrayList<>();
        List<String> persistedViewFactTable = new ArrayList<>();
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        try {
            //TODO: what if a segment is deleted during building?
            for (String segId : segmentIds) {
                SegmentInfo seg = ManagerHub.getSegmentInfo(config, cubeId, segId);
                spanningTree = new ForestSpanningTree(
                        JavaConversions.asJavaCollection(seg.toBuildLayouts()));
                logger.info("There are {} cuboids to be built in segment {}.",
                        seg.toBuildLayouts().size(), seg.name());
                for (LayoutEntity cuboid : JavaConversions.asJavaCollection(seg.toBuildLayouts())) {
                    logger.debug("Cuboid {} has row keys: {}", cuboid.getId(),
                            Joiner.on(", ").join(cuboid.getOrderedDimensions().keySet()));
                }

                // choose source
                sourceChooser = new ParentSourceChooser(spanningTree, seg, newSegment, jobId, ss, config, true);
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
                assert buildFromFlatTable != null;
                updateSegmentInfo(getParam(MetadataConstants.P_CUBE_ID), seg, buildFromFlatTable.getFlatTableDS().count());
            }
            updateCubeAndSegmentMeta(getParam(MetadataConstants.P_CUBE_ID),
                    ResourceDetectUtils.getSegmentSourceSize(shareDir), recommendCuboidMap);
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
        List<String> cuboidStatics = new LinkedList<>();

        String template = "{\"cuboid\":%d, \"rows\": %d, \"size\": %d \"deviation\": %7f}";
        for (LayoutEntity layoutEntity : segmentInfo.getAllLayoutJava()) {
            double deviation = 0.0d;
            if (layoutEntity.getRows() > 0 && recommendCuboidMap != null && !recommendCuboidMap.isEmpty()) {
                long diff = (layoutEntity.getRows() - recommendCuboidMap.get(layoutEntity.getId()));
                deviation = diff / (layoutEntity.getRows() + 0.0d);
            }
            cuboidStatics.add(String.format(Locale.getDefault(), template, layoutEntity.getId(),
                    layoutEntity.getRows(), layoutEntity.getByteSize(), deviation));
        }

        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());
            JavaRDD<String> cuboidStatRdd = jsc.parallelize(cuboidStatics, 1);
            for (String cuboid : cuboidStatics) {
                logger.info("Statistics \t: {}", cuboid);
            }
            String pathDir = config.getHdfsWorkingDirectory() + segment.getPreciseStatisticsResourcePath();
            logger.info("Saving {} {} .", pathDir, segmentInfo);
            Path path = new Path(pathDir);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
            cuboidStatRdd.saveAsTextFile(pathDir);
        } catch (Exception e) {
            logger.error("Write metrics failed.", e);
        }

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
        cubeManager.updateCube(update, true);
    }

    private void collectPersistedTablePath(List<String> persistedFlatTable, ParentSourceChooser sourceChooser) {
        String flatTablePath = sourceChooser.persistFlatTableIfNecessary();
        if (!flatTablePath.isEmpty()) {
            persistedFlatTable.add(flatTablePath);
        }
    }

    private void updateCubeAndSegmentMeta(String cubeId, Map<String, Object> toUpdateSegmentSourceSize,
                                          Map<Long, Long> recommendCuboidMap) throws IOException {
        CubeInstance cubeInstance = cubeManager.getCubeByUuid(cubeId);
        CubeInstance cubeCopy = cubeInstance.latestCopyForWrite();
        CubeUpdate update = new CubeUpdate(cubeCopy);

        if (recommendCuboidMap != null && !recommendCuboidMap.isEmpty())
            update.setCuboids(recommendCuboidMap);

        List<CubeSegment> cubeSegments = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : toUpdateSegmentSourceSize.entrySet()) {
            CubeSegment segment = cubeCopy.getSegmentById(entry.getKey());
            if (segment.getInputRecords() > 0L) {
                segment.setInputRecordsSize((Long) entry.getValue());
                segment.setLastBuildTime(System.currentTimeMillis());
                cubeSegments.add(segment);
            }
        }
        if (!cubeSegments.isEmpty()) {
            update.setToUpdateSegs(cubeSegments.toArray(new CubeSegment[0]));
            cubeManager.updateCube(update, true);
        }
    }

    private void build(Collection<NBuildSourceInfo> buildSourceInfos, SegmentInfo seg, SpanningTree st) throws InterruptedException{

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
                                              SpanningTree st) throws InterruptedException{
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

            buildLayoutWithUpdate.cacheAndRegister(info.getLayoutId(), parentDS);

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

                    @Override
                    public NBuildSourceInfo getBuildSourceInfo() {
                        return info;
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
            int factor = config.getSparkEngineTaskCoreFactor();
            int i = Double.valueOf(maxLeafTasksNums).intValue() / factor;
            logger.info("require cores: " + i);
            return String.valueOf(i);
        } else {
            return config.getSparkEngineRequiredTotalCores();
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
                    .sortWithinPartitions(NSparkCubingUtil.getColumns(orderedDims));
            saveAndUpdateLayout(afterSort, seg, layoutEntity, parentId);
        } else {
            Dataset<Row> afterAgg = CuboidAggregator.agg(ss, parent, dimIndexes, cuboid.getOrderedMeasures(),
                    spanningTree, false);
            logger.info("Build layout:{}, in index:{}", layoutEntity.getId(), cuboid.getId());
            ss.sparkContext().setJobDescription("build " + layoutEntity.getId() + " from parent " + parentName);
            Set<Integer> rowKeys = layoutEntity.getOrderedDimensions().keySet();

            Dataset<Row> afterSort = afterAgg
                    .select(NSparkCubingUtil.getColumns(rowKeys, layoutEntity.getOrderedMeasures().keySet()))
                    .sortWithinPartitions(NSparkCubingUtil.getColumns(rowKeys));

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
        cuboidShardNum.put(layoutId, (short) shardNum);
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);
        BuildUtils.fillCuboidInfo(layout, path);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }
}
