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
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
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
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;

public class DFBuildJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(DFBuildJob.class);
    protected static String TEMP_DIR_SUFFIX = "_temp";

    private NDataflowManager dfMgr;
    private BuildLayoutWithUpdate buildLayoutWithUpdate;

    @Override
    protected void doExecute() throws Exception {
        long start = System.currentTimeMillis();
        logger.info("Start Build");
        buildLayoutWithUpdate = new BuildLayoutWithUpdate();
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS)));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        dfMgr = NDataflowManager.getInstance(config, project);
        List<String> persistedFlatTable = new ArrayList<>();
        List<String> persistedViewFactTable = new ArrayList<>();
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        try {
            IndexPlan indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan();
            Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(indexPlan, layoutIds).stream()
                    .filter(Objects::nonNull).collect(Collectors.toSet());

            //TODO: what if a segment is deleted during building?
            for (String segId : segmentIds) {
                NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId);
                NDataSegment seg = getSegment(segId);

                // choose source
                DFChooser datasetChooser = new DFChooser(nSpanningTree, seg, jobId, ss, config, true);
                datasetChooser.decideSources();
                NBuildSourceInfo buildFromFlatTable = datasetChooser.flatTableSource();
                Map<Long, NBuildSourceInfo> buildFromLayouts = datasetChooser.reuseSources();

                infos.clearCuboidsNumPerLayer(segId);
                // build cuboids from flat table
                if (buildFromFlatTable != null) {
                    val path = datasetChooser.persistFlatTableIfNecessary();
                    if (!path.isEmpty()) {
                        persistedFlatTable.add(path);
                    }
                    if (!org.apache.commons.lang3.StringUtils.isBlank(buildFromFlatTable.getViewFactTablePath())) {
                        persistedViewFactTable.add(buildFromFlatTable.getViewFactTablePath());
                    }
                    build(Collections.singletonList(buildFromFlatTable), segId, nSpanningTree);
                }

                // build cuboids from reused layouts
                if (!buildFromLayouts.isEmpty()) {
                    build(buildFromLayouts.values(), segId, nSpanningTree);
                }
                infos.recordSpanningTree(segId, nSpanningTree);
            }
            Map<String, Object> segmentSourceSize = ResourceDetectUtils.getSegmentSourceSize(shareDir);
            updateSegmentSourceBytesSize(dataflowId, segmentSourceSize);
        } finally {
            val fs = HadoopUtil.getWorkingFileSystem();
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

    private void updateSegmentSourceBytesSize(String dataflowId, Map<String, Object> toUpdateSegmentSourceSize) {
        NDataflow dataflow = dfMgr.getDataflow(dataflowId);
        NDataflow newDF = dataflow.copy();
        val update = new NDataflowUpdate(dataflow.getUuid());
        List<NDataSegment> nDataSegments = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : toUpdateSegmentSourceSize.entrySet()) {
            NDataSegment segment = newDF.getSegment(entry.getKey());
            segment.setSourceBytesSize((Long) entry.getValue());
            segment.setLastBuildTime(System.currentTimeMillis());
            nDataSegments.add(segment);
        }
        update.setToUpdateSegs(nDataSegments.toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);
    }

    @Override
    protected String calculateRequiredCores() throws Exception {
        if (config.getSparkEngineTaskImpactInstanceEnabled()) {
            Path shareDir = config.getJobTmpShareDir(project, jobId);
            String maxLeafTasksNums = maxLeafTasksNums(shareDir);
            logger.info("The maximum number of tasks required to run the job is {}", maxLeafTasksNums);
            val config = KylinConfig.getInstanceFromEnv();
            val factor = config.getSparkEngineTaskCoreFactor();
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

    private NDataSegment getSegment(String segId) {
        // ensure the seg is the latest.
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        return dfMgr.getDataflow(dataflowId).getSegment(segId);
    }

    private void build(Collection<NBuildSourceInfo> buildSourceInfos, String segId, NSpanningTree st)
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
    private List<NBuildSourceInfo> constructTheNextLayerBuildInfos( //
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

    private List<NDataLayout> buildIndex(NDataSegment seg, IndexEntity cuboid, Dataset<Row> parent,
            NSpanningTree nSpanningTree, long parentId) throws IOException {
        String parentName = String.valueOf(parentId);
        if (parentId == DFChooser.FLAT_TABLE_FLAG()) {
            parentName = "flat table";
        }
        logger.info("Build index:{}, in segment:{}", cuboid.getId(), seg.getId());
        LinkedList<NDataLayout> layouts = Lists.newLinkedList();
        Set<Integer> dimIndexes = cuboid.getEffectiveDimCols().keySet();
        if (cuboid.getId() >= IndexEntity.TABLE_INDEX_START_ID) {
            Preconditions.checkArgument(cuboid.getMeasures().isEmpty());
            Dataset<Row> afterPrj = parent.select(NSparkCubingUtil.getColumns(dimIndexes));
            // TODO: shard number should respect the shard column defined in cuboid
            for (LayoutEntity layout : nSpanningTree.getLayouts(cuboid)) {
                logger.info("Build layout:{}, in index:{}", layout.getId(), cuboid.getId());
                ss.sparkContext().setJobDescription("build " + layout.getId() + " from parent " + parentName);
                Set<Integer> orderedDims = layout.getOrderedDimensions().keySet();
                Dataset<Row> afterSort = afterPrj.select(NSparkCubingUtil.getColumns(orderedDims))
                        .sortWithinPartitions(NSparkCubingUtil.getColumns(orderedDims));
                layouts.add(saveAndUpdateLayout(afterSort, seg, layout));
            }
        } else {
            Dataset<Row> afterAgg = CuboidAggregator.agg(ss, parent, dimIndexes, cuboid.getEffectiveMeasures(), seg,
                    nSpanningTree);
            for (LayoutEntity layout : nSpanningTree.getLayouts(cuboid)) {
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

    private NDataLayout saveAndUpdateLayout(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout)
            throws IOException {
        long layoutId = layout.getId();

        NDataLayout dataCuboid = getDataCuboid(seg, layoutId);

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
        val partitionNum = BuildUtils.repartitionIfNeed(layout, dataCuboid, storage, path, tempPath,
                KapConfig.wrap(config), ss);
        dataCuboid.setPartitionNum(partitionNum);
        ss.sparkContext().setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY(), null);
        QueryExecutionCache.removeQueryExecution(queryExecutionId);
        BuildUtils.fillCuboidInfo(dataCuboid);
        return dataCuboid;
    }

    private NDataLayout getDataCuboid(NDataSegment seg, long layoutId) {
        return NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layoutId);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfBuildJobInfo();
    }

    public static void main(String[] args) {
        DFBuildJob nDataflowBuildJob = new DFBuildJob();
        nDataflowBuildJob.execute(args);
    }
}
