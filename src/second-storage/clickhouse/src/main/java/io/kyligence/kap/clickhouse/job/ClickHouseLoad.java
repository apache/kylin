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
package io.kyligence.kap.clickhouse.job;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_EXPORT_TO_SECOND_STORAGE;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.kyligence.kap.secondstorage.SecondStorageLockUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions;

import com.google.common.base.Preconditions;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.SegmentFileStatus;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * The mechanism we used is unique for ClickHouse That‘s why we name it {@link ClickHouseLoad} instead of NJDBCLoad.
 */
@Slf4j
public class ClickHouseLoad extends AbstractExecutable {
    public static final String SOURCE_URL = "source_url";
    public static final String ROOT_PATH = "root_path";

    private static String indexPath(String workingDir, String dataflowID, String segmentID, long layoutID) {
        return String.format(Locale.ROOT, "%s%s/%s/%d", workingDir, dataflowID, segmentID, layoutID);
    }

    private List<List<LoadInfo>> loadInfos = null;
    private LoadContext loadContext;

    public List<List<LoadInfo>> getLoadInfos() {
        return loadInfos;
    }

    public ClickHouseLoad() {
        this.setName(STEP_EXPORT_TO_SECOND_STORAGE);
    }

    public ClickHouseLoad(Object notSetId) {
        super(notSetId);
    }

    public ClickHouseLoad(String name) {
        this.setName(name);
    }

    private String getUtParam(String key) {
        return System.getProperty(key, null);
    }

    private Engine createTableEngine() {
        val sourceType = getTableSourceType();
        return new Engine(sourceType.getTableEngineType(), getUtParam(ROOT_PATH), getUtParam(SOURCE_URL), sourceType);
    }

    private boolean isAzurePlatform() {
        return KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory().startsWith("wasb");
    }

    private boolean isAwsPlatform() {
        return KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory().startsWith("s3");
    }

    private boolean isUt() {
        return KylinConfig.getInstanceFromEnv().isUTEnv();
    }

    private TableSourceType getTableSourceType() {
        TableSourceType sourceType = TableSourceType.HDFS;
        if (isAwsPlatform()) {
            sourceType = TableSourceType.S3;
        } else if (isAzurePlatform()) {
            sourceType = TableSourceType.BLOB;
        } else if (isUt()) {
            sourceType = TableSourceType.UT;
        }
        return sourceType;
    }

    protected String[] selectInstances(String[] nodeNames, int shardNumber) {
        if (nodeNames.length == shardNumber)
            return nodeNames;
        throw new UnsupportedOperationException();
    }

    protected FileProvider getFileProvider(NDataflow df, String segmentId, long layoutId) {
        final String workingDir = KapConfig.wrap(df.getConfig()).getReadParquetStoragePath(df.getProject());
        String segmentLayoutRoot = indexPath(workingDir, df.getId(), segmentId, layoutId);
        return new SegmentFileProvider(segmentLayoutRoot);
    }

    private List<LoadInfo> distributeLoad(NDataflow dataFlow,
                                          IndexPlan indexPlan,
                                          TablePlan tablePlan,
                                          String[] nodeNames,
                                          TableFlow tableFlow) {
        return getLayoutIds().stream()
                .map(indexPlan::getLayoutEntity)
                .filter(SecondStorageUtil::isBaseTableIndex)
                .flatMap(layoutEntity ->
                        getSegmentIds().stream()
                                .filter(segmentId -> dataFlow.getSegment(segmentId).getLayoutsMap().containsKey(layoutEntity.getId()))
                                .map(segmentId ->
                                        genLoadInfoBySegmentId(segmentId, nodeNames, layoutEntity, tablePlan, dataFlow, tableFlow)
                                )
                ).collect(Collectors.toList());
    }

    private LoadInfo genLoadInfoBySegmentId(String segmentId, String[] nodeNames, LayoutEntity currentLayoutEntity,
                                            TablePlan tablePlan, NDataflow dataFlow, TableFlow tableFlow) {
        TableEntity tableEntity = tablePlan.getEntity(currentLayoutEntity).orElse(null);
        Preconditions.checkArgument(tableEntity != null);
        int shardNumber = Math.min(nodeNames.length, tableEntity.getShardNumbers());
        return LoadInfo.distribute(selectInstances(nodeNames, shardNumber), dataFlow.getModel(),
                dataFlow.getSegment(segmentId), getFileProvider(dataFlow, segmentId, currentLayoutEntity.getId()),
                currentLayoutEntity, tableFlow);
    }

    public static class MethodContext {
        private final String project;
        private final String dataflowId;
        private final KylinConfig config;
        private final NDataflow df;
        private final String database;
        private final Function<LayoutEntity, String> prefixTableName;

        MethodContext(ClickHouseLoad load) {
            this.project = load.getProject();
            this.dataflowId = load.getParam(NBatchConstants.P_DATAFLOW_ID);
            this.config = KylinConfig.getInstanceFromEnv();
            final NDataflowManager dfMgr = NDataflowManager.getInstance(config, load.getProject());
            this.df = dfMgr.getDataflow(dataflowId);
            this.database = NameUtil.getDatabase(df);
            this.prefixTableName = s -> NameUtil.getTable(df, s.getId());
        }

        IndexPlan indexPlan() {
            return df.getIndexPlan();
        }

        TablePlan tablePlan() {
            return SecondStorage.tablePlanManager(config, project).get(dataflowId)
                    .orElseThrow(() -> new IllegalStateException(" no table plan found"));
        }

        TableFlow tableFlow() {
            return SecondStorage.tableFlowManager(config, project).get(dataflowId)
                    .orElseThrow(() -> new IllegalStateException(" no table flow found"));
        }

        boolean isIncremental(Set<String> segIds) {
            return this.df.getSegments().stream().filter(segment -> segIds.contains(segment.getId()))
                    .noneMatch(segment -> segment.getSegRange().isInfinite());
        }

        public String getProject() {
            return project;
        }

        public String getDataflowId() {
            return dataflowId;
        }

        public KylinConfig getConfig() {
            return config;
        }

        public NDataflow getDf() {
            return df;
        }

        public String getDatabase() {
            return database;
        }

        public Function<LayoutEntity, String> getPrefixTableName() {
            return prefixTableName;
        }
    }

    /**
     * update load info before use it。 For example, refresh job will set old segment id to load info.
     * @param infoList
     * @return new loadInfo list
     */
    protected List<LoadInfo> preprocessLoadInfo(List<LoadInfo> infoList) {
        return infoList;
    }

    /**
     * extend point for subclass
     */
    protected void init() {
        this.loadContext = new LoadContext(this);
        this.loadState();
    }

    protected void preCheck() {
        SecondStorageUtil.isModelEnable(getProject(), getTargetModelId());
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        SegmentRange<Long> range = new SegmentRange.TimePartitionedSegmentRange(getDataRangeStart(), getDataRangeEnd());
        SecondStorageLockUtils.acquireLock(getTargetModelId(), range).lock();
        try {
            init();
            preCheck();
            final MethodContext mc = new MethodContext(this);
            return wrapWithExecuteException(() -> {
                Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(mc.config, mc.project);
                List<NodeGroup> allGroup = nodeGroupManager.listAll();
                for (NodeGroup nodeGroup : allGroup) {
                    if (LockTypeEnum.locked(Arrays.asList(LockTypeEnum.LOAD.name()), nodeGroup.getLockTypes())) {
                        logger.info("project={} has been locked, skip the step", mc.getProject());
                        return ExecuteResult.createSkip();
                    }
                }
                String[][] nodeGroups = new String[allGroup.size()][];
                log.info("project={} replica, nodeGroup {}", project, nodeGroups);
                ListIterator<NodeGroup> it = allGroup.listIterator();
                while (it.hasNext()) {
                    nodeGroups[it.nextIndex()] = it.next().getNodeNames().toArray(new String[0]);
                }
                ShardOptions options = new ShardOptions(ShardOptions.buildReplicaSharding(nodeGroups));
                boolean isIncremental = mc.isIncremental(getSegmentIds());
                DataLoader dataLoader = new DataLoader(getId(), mc.getDatabase(), mc.getPrefixTableName(), createTableEngine(), isIncremental);
                val replicaShards = options.replicaShards();
                val replicaNum = replicaShards.length;
                List<List<LoadInfo>> tempLoadInfos = new ArrayList<>();
                val tableFlowManager = SecondStorage.tableFlowManager(mc.config, mc.project);
                val partitions = tableFlowManager.listAll().stream()
                        .flatMap(tableFlow -> tableFlow.getTableDataList().stream())
                        .flatMap(tableData -> tableData.getPartitions().stream()).collect(Collectors.toList());
                Map<String, Long> nodeSizeMap = new HashMap<>();
                partitions.forEach(partition -> partition.getNodeFileMap().forEach((node, files) -> {
                    Long size = nodeSizeMap.computeIfAbsent(node, n -> 0L);
                    size = size + files.stream().map(SegmentFileStatus::getLen).reduce(Long::sum).orElse(0L);
                    nodeSizeMap.put(node, size);
                }));

                int[] indexInGroup = getIndexInGroup(replicaShards[0], nodeSizeMap);
                log.info("project={} indexInGroup={}", project, indexInGroup);

                for (val shards : replicaShards) {
                    List<LoadInfo> infoList = distributeLoad(mc.df, mc.indexPlan(), mc.tablePlan(),
                            orderGroupByIndex(shards, indexInGroup), mc.tableFlow());
                    infoList = preprocessLoadInfo(infoList);
                    tempLoadInfos.add(infoList);
                }
                this.loadInfos = IntStream.range(0, tempLoadInfos.get(0).size()).mapToObj(idx -> {
                    List<LoadInfo> loadInfoBatch = new ArrayList<>(replicaNum);
                    for (val item : tempLoadInfos) {
                        loadInfoBatch.add(item.get(idx));
                    }
                    return loadInfoBatch;
                }).sorted(Comparator.comparing(infoBatch -> infoBatch.get(0).getSegmentId())).collect(Collectors.toList());

                loadData(dataLoader);
                updateMeta();
                return ExecuteResult.createSucceed();
            });
        } finally {
            SecondStorageLockUtils.unlock(getTargetModelId(), range);
        }
    }

    private void loadData(DataLoader dataLoader) throws InterruptedException, ExecutionException, SQLException, JobStoppedException {
        for (List<LoadInfo> infoBatch : loadInfos) {
            // if paused, stop load
            if (dataLoader.load(infoBatch, this.loadContext)) break;
        }
        if (isPaused()) {
            saveState(false);
        }
        if (isPaused() || isDiscarded()) {
            throw new JobStoppedException("job stop manually.");
        }
    }

    protected void updateMeta() {
        Preconditions.checkArgument(this.getLoadInfos() != null, "no load info found");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            final MethodContext mc = new MethodContext(this);
            val isIncrementalBuild = mc.isIncremental(getSegmentIds());
            return mc.tableFlow()
                    .update(copied -> this.getLoadInfos().stream().flatMap(Collection::stream)
                            .forEach(loadInfo -> loadInfo.upsertTableData(copied, mc.database,
                                    mc.prefixTableName.apply(loadInfo.getLayout()),
                                    isIncrementalBuild ? PartitionType.INCREMENTAL : PartitionType.FULL)));
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public boolean isDiscarded() {
        return ExecutableState.DISCARDED == SecondStorageUtil.getJobStatus(project, getParentId());
    }

    public boolean isPaused() {
        return ExecutableState.PAUSED == SecondStorageUtil.getJobStatus(project, getParentId());
    }

    public void saveState(boolean saveEmpty) {
        Preconditions.checkNotNull(loadContext, "load context can't be null");
        Map<String, String> info = new HashMap<>();
        // if save empty，will clean the
        info.put(LoadContext.CLICKHOUSE_LOAD_CONTEXT, saveEmpty ? LoadContext.emptyState() : loadContext.serializeToString());
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val manager = this.getManager();
            manager.updateJobOutput(getParentId(), null, info, null, null);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }


    public void loadState() {
        Preconditions.checkNotNull(loadContext, "load context can't be null");
        val state = getManager().getOutputFromHDFSByJobId(getParentId()).getExtra().get(LoadContext.CLICKHOUSE_LOAD_CONTEXT);
        if (!StringUtils.isEmpty(state)) {
            loadContext.deserializeToString(state);
        }
        // clean history, when job failed or KE process down, will run job without history.
        saveState(true);
    }

    /**
     * index of group order by size
     * @param group group
     * @param nodeSizeMap node size
     * @return index of group order by size
     */
    private int[] getIndexInGroup(String[] group, Map<String, Long> nodeSizeMap){
        return IntStream.range(0, group.length)
                .mapToObj(i -> new Pair<>(group[i], i))
                .sorted(Comparator.comparing(c -> nodeSizeMap.getOrDefault(c.getFirst(), 0L)))
                .map(Pair::getSecond).mapToInt(i -> i).toArray();
    }

    /**
     * order group by index
     * @param group group
     * @param index index
     * @return ordered group by index
     */
    private String[] orderGroupByIndex(String[] group, int[] index){
        return IntStream.range(0, group.length)
                .mapToObj(i -> group[index[i]])
                .collect(Collectors.toList()).toArray(new String[]{});
    }
}
