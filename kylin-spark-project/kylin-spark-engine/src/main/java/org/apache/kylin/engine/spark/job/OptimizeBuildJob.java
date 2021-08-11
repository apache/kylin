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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.NBuildSourceInfo;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.cube.ManagerHub;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.metadata.cube.model.ForestSpanningTree;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree;
import org.apache.kylin.engine.spark.utils.JobMetrics;
import org.apache.kylin.engine.spark.utils.JobMetricsUtils;
import org.apache.kylin.engine.spark.utils.Metrics;
import org.apache.kylin.engine.spark.utils.QueryExecutionCache;
import org.apache.kylin.engine.spark.utils.BuildUtils;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.shaded.com.google.common.base.Joiner;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.UUID;

import java.util.stream.Collectors;

public class OptimizeBuildJob extends SparkApplication {
    private static final Logger logger = LoggerFactory.getLogger(OptimizeBuildJob.class);

    private Map<Long, HLLCounter> cuboidHLLMap = Maps.newHashMap();
    protected static String TEMP_DIR_SUFFIX = "_temp";

    private BuildLayoutWithUpdate buildLayoutWithUpdate;
    private Map<Long, Short> cuboidShardNum = Maps.newConcurrentMap();
    private Map<Long, Long> cuboidsRowCount = Maps.newConcurrentMap();

    private Configuration conf = HadoopUtil.getCurrentConfiguration();
    private CubeManager cubeManager;
    private CubeInstance cubeInstance;
    private SegmentInfo optSegInfo;
    private SegmentInfo originalSegInfo;
    private CubeSegment optSeg;
    private CubeSegment originalSeg;
    private long baseCuboidId;

    public static void main(String[] args) {
        OptimizeBuildJob optimizeBuildJob = new OptimizeBuildJob();
        optimizeBuildJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        String segmentId = getParam(CubingExecutableUtil.SEGMENT_ID);
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);

        cubeManager = CubeManager.getInstance(config);
        cubeInstance = cubeManager.getCubeByUuid(cubeId);
        optSeg = cubeInstance.getSegmentById(segmentId);
        originalSeg = cubeInstance.getOriginalSegmentToOptimize(optSeg);
        originalSegInfo = ManagerHub.getSegmentInfo(config, cubeId, originalSeg.getUuid());

        calculateCuboidFromBaseCuboid();
        buildCuboidFromParent(cubeId);
    }

    private void calculateCuboidFromBaseCuboid() throws IOException {
        logger.info("Start to calculate cuboid statistics for optimized segment");
        long start = System.currentTimeMillis();

        baseCuboidId = cubeInstance.getCuboidScheduler().getBaseCuboidId();
        LayoutEntity baseCuboid = originalSegInfo.getAllLayoutJava().stream()
                .filter(layoutEntity -> layoutEntity.getId() == baseCuboidId).iterator().next();
        Dataset<Row> baseCuboidDS = StorageFactory
                .createEngineAdapter(baseCuboid, NSparkCubingEngine.NSparkCubingStorage.class)
                .getFrom(PathManager.getParquetStoragePath(config, cubeInstance.getName(), optSeg.getName(),
                        optSeg.getStorageLocationIdentifier(), String.valueOf(baseCuboid.getId())), ss);

        Map<Long, HLLCounter> hllMap = new HashMap<>();

        for (Tuple2<Object, AggInfo> cuboidData : CuboidStatisticsJob.statistics(baseCuboidDS,
                originalSegInfo, getNewCuboidIds())) {
            hllMap.put((Long) cuboidData._1, cuboidData._2.cuboid().counter());
        }

        String jobTmpDir = config.getJobTmpDir(project) + "/" + jobId;
        Path statisticsDir = new Path(jobTmpDir + "/" + ResourceStore.CUBE_STATISTICS_ROOT + "/"
                + cubeInstance.getUuid() + "/" + optSeg.getUuid() + "/");

        CubeStatsWriter.writeCuboidStatistics(conf, statisticsDir, hllMap, 1, -1);

        logger.info("Calculate cuboid statistics from base cuboid job takes {} ms",
                (System.currentTimeMillis() - start));
    }

    private void buildCuboidFromParent(String cubeId) throws IOException {
        logger.info("Start to build recommend cuboid for optimized segment");
        long start = System.currentTimeMillis();
        optSegInfo = ManagerHub.getSegmentInfo(config, cubeId, optSeg.getUuid(), CuboidModeEnum.RECOMMEND);
        buildLayoutWithUpdate = new BuildLayoutWithUpdate(config);

        infos.clearAddCuboids();

        SpanningTree spanningTree;
        ParentSourceChooser sourceChooser;
        try {
            spanningTree = new ForestSpanningTree(JavaConversions.asJavaCollection(optSegInfo.toBuildLayouts()));
            logger.info("There are {} cuboids to be built in segment {}.", optSegInfo.toBuildLayouts().size(),
                    optSegInfo.name());
            for (LayoutEntity cuboid : JavaConversions.asJavaCollection(optSegInfo.toBuildLayouts())) {
                logger.debug("Cuboid {} has row keys: {}", cuboid.getId(),
                        Joiner.on(", ").join(cuboid.getOrderedDimensions().keySet()));
            }

            // choose source
            optSegInfo.removeLayout(baseCuboidId);
            sourceChooser = new ParentSourceChooser(spanningTree, optSegInfo, optSeg, jobId, ss, config, false);
            sourceChooser.decideSources();
            Map<Long, NBuildSourceInfo> buildFromLayouts = sourceChooser.reuseSources();

            infos.clearCuboidsNumPerLayer(optSegInfo.id());

            // build cuboids from reused layouts
            if (!buildFromLayouts.isEmpty()) {
                build(buildFromLayouts.values(), optSegInfo, spanningTree);
            }
            infos.recordSpanningTree(optSegInfo.id(), spanningTree);

            logger.info("Updating segment info");
            updateOptimizeSegmentInfo();
        } finally {
            logger.info("Building job takes {} ms", (System.currentTimeMillis() - start));
        }
    }

    private long[] getNewCuboidIds() {
        Set<Long> recommendCuboidsSet = cubeInstance.getCuboidsByMode(CuboidModeEnum.RECOMMEND_MISSING);
        Preconditions.checkNotNull(recommendCuboidsSet, "The recommend cuboid map could not be null");
        long[] recommendCuboid = new long[recommendCuboidsSet.size()];
        int i = 0;
        for (long cuboidId : recommendCuboidsSet) {
            recommendCuboid[i++] = cuboidId;
        }
        return recommendCuboid;
    }

    protected void updateOptimizeSegmentInfo() throws IOException {
        CubeInstance cubeCopy = optSeg.getCubeInstance().latestCopyForWrite();
        List<CubeSegment> cubeSegments = Lists.newArrayList();
        CubeUpdate update = new CubeUpdate(cubeCopy);

        optSeg.setSizeKB(optSegInfo.getAllLayoutSize() / 1024);
        optSeg.setLastBuildTime(System.currentTimeMillis());
        optSeg.setLastBuildJobID(jobId);
        optSeg.setInputRecords(originalSeg.getInputRecords());
        Map<Long, Short> existingShardNums = originalSeg.getCuboidShardNums();
        for (Long cuboidId : cubeCopy.getCuboidsByMode(CuboidModeEnum.RECOMMEND_EXISTING)) {
            cuboidShardNum.putIfAbsent(cuboidId, existingShardNums.get(cuboidId));
        }
        optSeg.setCuboidShardNums(cuboidShardNum);
        optSeg.setInputRecordsSize(originalSeg.getInputRecordsSize());
        Map<String, String> additionalInfo = optSeg.getAdditionalInfo();
        additionalInfo.put("storageType", "" + IStorageAware.ID_PARQUET);
        optSeg.setAdditionalInfo(additionalInfo);
        cubeSegments.add(optSeg);
        update.setToUpdateSegs(cubeSegments.toArray(new CubeSegment[0]));
        cubeManager.updateCube(update, true);
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
                if (!cubeInstance.getCuboidsByMode(CuboidModeEnum.RECOMMEND_EXISTING).contains(index.getId())) {
                    infos.recordAddCuboids(index.getId());
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
                            return null;
                        }
                    }, config);
                } else {
                    try {
                        updateExistingLayout(index, info.getLayoutId());
                    } catch (IOException e) {
                        logger.error("Failed to update existing cuboid info: {}", index.getId());
                    }
                }
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
        cuboidShardNum.put(layoutId, (short) shardNum);
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);
        BuildUtils.fillCuboidInfo(layout, path);
    }

    private void updateExistingLayout(LayoutEntity layout, long parentId) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        long layoutId = layout.getId();
        String path = PathManager.getParquetStoragePath(config, cubeInstance.getName(), optSegInfo.name(), optSegInfo.identifier(),
                String.valueOf(layoutId));
        Dataset<Row> dataset = StorageFactory
                .createEngineAdapter(layout, NSparkCubingEngine.NSparkCubingStorage.class)
                .getFrom(path, ss);
        logger.debug("Existing cuboid, use count() to collect cuboid rows.");
        long cuboidRowCnt = dataset.count();
        ContentSummary cs = HadoopUtil.getContentSummary(fs, new Path(path));
        layout.setRows(cuboidRowCnt);
        layout.setFileCount(cs.getFileCount());
        layout.setByteSize(cs.getLength());
        // record the row count of cuboid
        cuboidsRowCount.putIfAbsent(layoutId, cuboidRowCnt);
        layout.setSourceRows(cuboidsRowCount.get(parentId));
        int shardNum = originalSeg.getCuboidShardNums().get(layoutId);
        layout.setShardNum(shardNum);
        optSegInfo.updateLayout(layout);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfOptimizeJobInfo();
    }
}
