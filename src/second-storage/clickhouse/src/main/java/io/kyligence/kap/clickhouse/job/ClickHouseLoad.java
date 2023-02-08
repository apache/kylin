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
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SegmentOnlineMode;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.JobSchedulerModeEnum;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageConcurrentTestUtil;
import io.kyligence.kap.secondstorage.SecondStorageLockUtils;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.SegmentFileStatus;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
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
        return new Engine(sourceType, getUtParam(ROOT_PATH), getUtParam(SOURCE_URL));
    }

    public boolean isAzurePlatform() {
        String workingDirectory = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        return workingDirectory.startsWith("wasb") || workingDirectory.startsWith("abfs");
    }

    private boolean isAwsPlatform() {
        return KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory().startsWith("s3");
    }

    private boolean isUt() {
        return KylinConfig.getInstanceFromEnv().isUTEnv();
    }

    private AbstractTableSource getTableSourceType() {
        AbstractTableSource tableSource;
        if (isAwsPlatform()) {
            tableSource = new S3TableSource();
        } else if (isAzurePlatform()) {
            tableSource = new BlobTableSource();
        } else if (isUt()) {
            tableSource = new UtTableSource();
        } else {
            tableSource = new HdfsTableSource();
        }
        return tableSource;
    }

    protected String[] selectInstances(String[] nodeNames, int shardNumber) {
        if (nodeNames.length == shardNumber)
            return nodeNames;
        throw new UnsupportedOperationException();
    }
    
    protected FileProvider getFileProvider(NDataflow df, String segmentId, long layoutId) {
        String segmentLayoutRoot;
        if (JobSchedulerModeEnum.CHAIN.toString().equalsIgnoreCase(df.getConfig().getJobSchedulerMode())) {
            final String workingDir = KapConfig.wrap(df.getConfig()).getReadParquetStoragePath(df.getProject());
            segmentLayoutRoot = indexPath(workingDir, df.getId(), segmentId, layoutId);
            logger.info("Tiered storage load segment {} layout {} from index path {}", segmentId, layoutId,
                    segmentLayoutRoot);
        } else {
            segmentLayoutRoot = getConfig().getFlatTableDir(getProject(), df.getId(), segmentId).toString();
            logger.info("Tiered storage load segment {} layout {} from flat table path {}", segmentId, layoutId,
                    segmentLayoutRoot);
        }
        return new SegmentFileProvider(segmentLayoutRoot);
    }

    private List<LoadInfo> distributeLoad(MethodContext mc, String[] nodeNames) {
        return mc.layoutIds.stream()
                .map(mc.indexPlan()::getLayoutEntity)
                .filter(SecondStorageUtil::isBaseTableIndex)
                .flatMap(layoutEntity -> mc.segmentIds.stream()
                        .filter(segmentId -> filterLayoutBySegmentId(mc, segmentId, layoutEntity.getId()))
                        .map(segmentId -> getLoadInfo(segmentId, layoutEntity, mc, nodeNames))
                ).collect(Collectors.toList());
    }

    private boolean filterLayoutBySegmentId(MethodContext mc, String segmentId, long layoutId) {
        return isDAGJobScheduler() || mc.df.getSegment(segmentId).getLayoutsMap().containsKey(layoutId);
    }

    private LoadInfo getLoadInfo(String segmentId, LayoutEntity layoutEntity, MethodContext mc, String[] nodeNames) {
        LoadInfo loadInfo = genLoadInfoBySegmentId(segmentId, nodeNames, layoutEntity, mc.tablePlan(), mc.df, mc.tableFlow());
        loadInfo.setTargetDatabase(mc.database);
        loadInfo.setTargetTable(mc.prefixTableName.apply(loadInfo.getLayout()));
        return loadInfo;
    }

    private LoadInfo genLoadInfoBySegmentId(String segmentId, String[] nodeNames, LayoutEntity currentLayoutEntity,
                                            TablePlan tablePlan, NDataflow dataFlow, TableFlow tableFlow) {
        TableEntity tableEntity = tablePlan.getEntity(currentLayoutEntity).orElse(null);
        Preconditions.checkArgument(tableEntity != null);
        int shardNumber = Math.min(nodeNames.length, tableEntity.getShardNumbers());
        return LoadInfo.distribute(selectInstances(nodeNames, shardNumber), dataFlow.getModel(),
                dataFlow.getSegment(segmentId), getFileProvider(dataFlow, segmentId, currentLayoutEntity.getId()),
                currentLayoutEntity, tableFlow, tableEntity);
    }

    public static class MethodContext {
        private final String project;
        private final String dataflowId;
        private final KylinConfig config;
        private final NDataflow df;
        private final String database;
        private final Function<LayoutEntity, String> prefixTableName;
        private final Set<Long> layoutIds;
        private final Set<String> segmentIds;
        private final boolean isIncremental;

        MethodContext(ClickHouseLoad load) {
            this.project = load.getProject();
            this.dataflowId = load.getParam(NBatchConstants.P_DATAFLOW_ID);
            this.config = KylinConfig.getInstanceFromEnv();
            final NDataflowManager dfMgr = NDataflowManager.getInstance(config, load.getProject());
            this.df = dfMgr.getDataflow(dataflowId);
            this.database = NameUtil.getDatabase(df);
            this.prefixTableName = layoutEntity -> NameUtil.getTable(df, layoutEntity.getId());
            this.layoutIds = load.getLayoutIds();
            this.segmentIds = load.getSegmentIds();
            this.isIncremental = df.getSegments().stream().filter(segment -> segmentIds.contains(segment.getId()))
                    .noneMatch(segment -> segment.getSegRange().isInfinite());
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

        public String getProject() {
            return project;
        }

        public String getDataflowId() {
            return dataflowId;
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
     *
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
    public ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        if (!SecondStorageUtil.isModelEnable(getProject(), getParam(NBatchConstants.P_DATAFLOW_ID))) {
            return ExecuteResult.createSkip();
        }
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

                waitFlatTableStepEnd();

                String[][] nodeGroups = new String[allGroup.size()][];
                logger.info("project={} replica, nodeGroup {}", project, nodeGroups);
                ListIterator<NodeGroup> it = allGroup.listIterator();
                while (it.hasNext()) {
                    nodeGroups[it.nextIndex()] = it.next().getNodeNames().toArray(new String[0]);
                }
                ShardOptions options = new ShardOptions(ShardOptions.buildReplicaSharding(nodeGroups));

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
                logger.info("project={} indexInGroup={}", project, indexInGroup);

                for (val shards : replicaShards) {
                    List<LoadInfo> infoList = distributeLoad(mc, orderGroupByIndex(shards, indexInGroup));
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

                loadData(mc);
                updateMeta();
                return ExecuteResult.createSucceed();
            });
        } finally {
            SecondStorageLockUtils.unlock(getTargetModelId(), range);
        }
    }

    private void loadData(MethodContext mc)
            throws InterruptedException, ExecutionException, SQLException, JobStoppedException {
        List<DataLoader> dataLoaders = loadInfos.stream()
                .map(loadInfo -> new DataLoader(getId(), mc.getDatabase(), createTableEngine(), mc.isIncremental,
                        loadInfo, this.loadContext))
                .filter(dataLoader -> !loadContext.getHistorySegments(dataLoader.getSegmentKey()).contains(dataLoader.getSegmentId()))
                .collect(Collectors.toList());
        List<ShardLoader> shardLoaders = dataLoaders.stream()
                .flatMap(dataLoader -> dataLoader.getShardLoaders().stream()).collect(Collectors.toList());

        try {
            checkResumeFileCorrect(dataLoaders, loadContext);
            for (ShardLoader shardLoader : shardLoaders) {
                shardLoader.setup(loadContext.isNewJob());
            }
            loadToTemp(dataLoaders, mc.dataflowId);
            waitDFSEnd(getParentId());
            SecondStorageConcurrentTestUtil.wait(SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT);
            if (!isStop()) {
                // if load in commit, can't stop job
                beforeDataCommit();
                commitLoad(dataLoaders, shardLoaders, mc.dataflowId);
            }
        } finally {
            // if paused skip clean insert temp table
            boolean isPaused = isPauseOrError();
            shardLoaders.forEach(shardLoader -> shardLoader.cleanUpQuietly(isPaused));
            jobStopHandle(false);
        }
    }

    public void checkResumeFileCorrect(List<DataLoader> dataLoaders, LoadContext loadContext) {
        if (loadContext.isNewJob()) {
            return;
        }
        Set<String> allFiles = new HashSet<>();
        dataLoaders.forEach(dataLoader -> dataLoader.getSingleFileLoaderPerNode().values()
                .forEach(fileLoaders -> fileLoaders.forEach(fileLoader -> allFiles.add(fileLoader.getParquetFile()))));
        if (allFiles.isEmpty()) {
            return;
        }
        for (List<String> loadedFiles : loadContext.getHistory().values()) {
            if (!allFiles.containsAll(loadedFiles)) {
                throw new IllegalStateException("The file status changed after pausing, please try to resume");
            }
        }
    }

    private void executeTasks(ExecutorService executorService, List<Runnable> tasks, AtomicBoolean stopFlag,
            CountDownLatch latch, AtomicInteger currentProcessCnt, BooleanSupplier isStop)
            throws InterruptedException, ExecutionException {
        List<Future<?>> futureList = Lists.newArrayList();

        for (Runnable task : tasks) {
            futureList.add(executorService.submit(task));
        }

        do {
            boolean stopStatus = isStop.getAsBoolean();
            if (stopStatus) {
                stopFlag.set(true);
            }
            logger.info("Tiered storage process is {}", currentProcessCnt.get());
        } while (!latch.await(5, TimeUnit.SECONDS));

        for (Future<?> future : futureList) {
            future.get();
        }
    }

    public void loadToTemp(List<DataLoader> dataLoaders, String modelId)
            throws ExecutionException, InterruptedException {
        List<SecondStorageNode> nodes = SecondStorageUtil.listProjectNodes(getProject());
        final NDataflowManager dfMgr = NDataflowManager.getInstance(getConfig(), getProject());
        int processCnt = dfMgr.getDataflow(modelId).getConfig().getSecondStorageLoadThreadsPerJob();
        logger.info("Tiered storage load file start. Load file concurrency is {}", processCnt);
        Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> partitionsNode = getFileLoaders(dataLoaders);
        int pollSize = nodes.size() * processCnt;
        final ExecutorService executorService = new ThreadPoolExecutor(pollSize, pollSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new NamedThreadFactory("ClickhouseLoadFileWorker"));
        try {
            doLoadDataAction(executorService, partitionsNode, processCnt, this::isStop);
        } finally {
            if (!executorService.isShutdown()) {
                executorService.shutdown();
            }
        }
    }

    public void commitLoad(List<DataLoader> dataLoaders, List<ShardLoader> shardLoaders, String modelId)
            throws SQLException, ExecutionException, InterruptedException {
        for (ShardLoader shardLoader : shardLoaders) {
            shardLoader.createDestTableIgnoreExist();
        }
        List<SecondStorageNode> nodes = SecondStorageUtil.listProjectNodes(getProject());
        final NDataflowManager dfMgr = NDataflowManager.getInstance(getConfig(), getProject());
        int processCnt = dfMgr.getDataflow(modelId).getConfig().getSecondStorageCommitThreadsPerJob();
        logger.info("Tiered storage commit file start. Commit file concurrency is {}", processCnt);
        int pollSize = nodes.size() * processCnt;
        final ExecutorService executorService = new ThreadPoolExecutor(pollSize, pollSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new NamedThreadFactory("ClickhouseCommitLoadWorker"));
        try {
            val dropPartitionsNode = getBeforeCommitDropPartitions(dataLoaders);
            if (!dropPartitionsNode.isEmpty()) {
                doLoadDataAction(executorService, dropPartitionsNode, processCnt, () -> false);
            }
            val movePartitionsNode = getCommitMovePartitions(dataLoaders);
            doLoadDataAction(executorService, movePartitionsNode, processCnt, () -> false);
            dataLoaders.forEach(
                    dataLoader -> loadContext.finishSegment(dataLoader.getSegmentId(), dataLoader.getSegmentKey()));
        } catch (InterruptedException e) {
            val exceptionPartitionsNode = getExceptionCommitDropPartitions(dataLoaders);
            doLoadDataAction(executorService, exceptionPartitionsNode, processCnt, () -> false);
            throw e;
        } catch (Exception e) {
            val exceptionPartitionsNode = getExceptionCommitDropPartitions(dataLoaders);
            doLoadDataAction(executorService, exceptionPartitionsNode, processCnt, () -> false);
            ExceptionUtils.rethrow(e);
        } finally {
            if (!executorService.isShutdown()) {
                executorService.shutdown();
            }
            SecondStorageConcurrentTestUtil.wait(SecondStorageConcurrentTestUtil.WAIT_AFTER_COMMIT);
        }
    }

    private void doLoadDataAction(ExecutorService executorService,
            Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> partitionsNode, int processCnt,
            BooleanSupplier isStop) throws ExecutionException, InterruptedException {
        if (partitionsNode.isEmpty()) {
            return;
        }

        int pollSize = partitionsNode.size() * processCnt;
        CountDownLatch latch = new CountDownLatch(pollSize);
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        AtomicInteger currentProcessCnt = new AtomicInteger(0);
        AtomicBoolean isException = new AtomicBoolean(false);
        List<Runnable> tasks = Lists.newArrayList();
        for (Map.Entry<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> entry : partitionsNode.entrySet()) {
            String nodeName = entry.getKey();
            IntStream.range(0, processCnt).forEach(i -> tasks.add(() -> {
                try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(nodeName))) {
                    ConcurrentLinkedQueue<ClickhouseLoadActionUnit> actions = partitionsNode.get(nodeName);
                    while (!actions.isEmpty() && !isException.get() && !stopFlag.get()) {
                        ClickhouseLoadActionUnit action = actions.poll();
                        if (action == null) {
                            continue;
                        }
                        currentProcessCnt.incrementAndGet();
                        action.doAction(clickHouse);
                        currentProcessCnt.decrementAndGet();
                    }
                } catch (Exception e) {
                    isException.set(true);
                    currentProcessCnt.decrementAndGet();
                    ExceptionUtils.rethrow(e);
                } finally {
                    latch.countDown();
                }
            }));
        }
        // can't stop commit step
        executeTasks(executorService, tasks, stopFlag, latch, currentProcessCnt, isStop);
    }

    protected void updateMeta() {
        Preconditions.checkArgument(this.getLoadInfos() != null, "no load info found");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            final MethodContext mc = new MethodContext(this);
            val isIncrementalBuild = mc.isIncremental;
            mc.tableFlow()
                    .update(copied -> this.getLoadInfos().stream().flatMap(Collection::stream)
                            .forEach(loadInfo -> loadInfo.upsertTableData(copied, mc.database,
                                    mc.prefixTableName.apply(loadInfo.getLayout()),
                                    isIncrementalBuild ? PartitionType.INCREMENTAL : PartitionType.FULL)));

            updateDFSSegmentIfNeeded(mc);
            return null;
        }, project, 1, getEpochId());
    }

    protected void updateDFSSegmentIfNeeded(MethodContext mc) {
        if (!SegmentOnlineMode.ANY.toString().equalsIgnoreCase(getProjectConfig().getKylinEngineSegmentOnlineMode())){
            return;
        }

        if (!isDAGJobScheduler()){
            return;
        }

        final NDataflowManager dfMgr = NDataflowManager.getInstance(getConfig(), getProject());
        val dataflow = dfMgr.getDataflow(mc.getDataflowId()).copy();

        NDataflowUpdate update = new NDataflowUpdate(mc.getDf().getId());
        List<NDataSegment> toUpdateSegments = mc.segmentIds.stream()
                .map(dataflow::getSegment)
                .filter(Objects::nonNull)
                .filter(segment -> segment.getStatus() == SegmentStatusEnum.NEW)
                .peek(segment -> segment.setStatus(SegmentStatusEnum.READY)).collect(Collectors.toList());

        if (!toUpdateSegments.isEmpty()){
            update.setToUpdateSegs(toUpdateSegments.toArray(new NDataSegment[0]));
            dfMgr.updateDataflowWithoutIndex(update);
        }

        markDFStatus(mc.dataflowId);
    }

    protected void markDFStatus(String modelId) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataflow df = dfManager.getDataflow(modelId);
        boolean isOffline = dfManager.isOfflineModel(df);
        RealizationStatusEnum status = df.getStatus();
        if (RealizationStatusEnum.OFFLINE == status && !isOffline) {
            dfManager.updateDataflowStatus(df.getId(), RealizationStatusEnum.ONLINE);
        }
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
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, getEpochId(), getTempLockName());
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
     *
     * @param group       group
     * @param nodeSizeMap node size
     * @return index of group order by size
     */
    private int[] getIndexInGroup(String[] group, Map<String, Long> nodeSizeMap) {
        return IntStream.range(0, group.length)
                .mapToObj(i -> new Pair<>(group[i], i))
                .sorted(Comparator.comparing(c -> nodeSizeMap.getOrDefault(c.getFirst(), 0L)))
                .map(Pair::getSecond).mapToInt(i -> i).toArray();
    }

    /**
     * order group by index
     *
     * @param group group
     * @param index index
     * @return ordered group by index
     */
    private String[] orderGroupByIndex(String[] group, int[] index) {
        return IntStream.range(0, group.length)
                .mapToObj(i -> group[index[i]])
                .collect(Collectors.toList()).toArray(new String[]{});
    }


    private void waitFlatTableStepEnd() throws JobStoppedException, InterruptedException {
        if (!isDAGJobScheduler()) return;

        long waitSecond = getConfig().getSecondStorageWaitIndexBuildSecond();
        String jobId = getParentId();
        AbstractExecutable executable = getManager().getJob(jobId);
        boolean isAllFlatTableSuccess;
        do {
            if (isStop()) {
                break;
            }

            isAllFlatTableSuccess = isFlatTableSuccess(executable);

            if (!isAllFlatTableSuccess) {
                logger.info("Tiered storage is waiting flat table end");
                TimeUnit.SECONDS.sleep(waitSecond);
            }
        } while (!isAllFlatTableSuccess);
        jobStopHandle(true);

        logger.info("Tiered storage load beginning");
    }

    protected boolean isFlatTableSuccess(AbstractExecutable executable) {
        return SecondStorageUtil.checkBuildFlatTableIsSuccess(executable);
    }

    private void waitDFSEnd(String jobId) throws InterruptedException {
        if (!needWaitDFSEnd()) {
            return;
        }

        long waitSecond = getConfig().getSecondStorageWaitIndexBuildSecond();
        AbstractExecutable executable = getManager().getJob(jobId);
        boolean isDfsSuccess;

        do {
            if (isStop()) {
                break;
            }

            isDfsSuccess = isDfsSuccess(executable);

            if (!isDfsSuccess) {
                logger.info("Tiered storage is waiting dfs end");
                TimeUnit.SECONDS.sleep(waitSecond);
            }
        } while (!isDfsSuccess);

        logger.info("Tiered storage continue after DFS end");
    }

    protected boolean isDfsSuccess(AbstractExecutable executable) {
        return SecondStorageUtil.checkBuildDfsIsSuccess(executable);
    }

    protected boolean isDAGJobScheduler() {
        return JobSchedulerModeEnum.DAG == getParent().getJobSchedulerMode();
    }

    protected boolean needWaitDFSEnd() {
        if (!isDAGJobScheduler()) {
            return false;
        }

        return !SegmentOnlineMode.ANY.toString().equalsIgnoreCase(getProjectConfig().getKylinEngineSegmentOnlineMode());
    }

    protected void beforeDataCommit() {
        // Do nothing because function only used by refresh data.
    }

    protected void jobStopHandle(boolean isSaveEmpty) throws JobStoppedException {
        if (!isStop()) {
            return;
        }

        if (isPauseOrError()) {
            saveState(isSaveEmpty);
        }

        if (isPaused() || isDiscarded()) {
            throw new JobStoppedException("job stop manually.");
        }

        if (isStop()) {
            throw new JobStoppedException("job stop by main task.");
        }
    }

    public boolean isDiscarded() {
        return ExecutableState.DISCARDED == SecondStorageUtil.getJobStatus(project, getParentId());
    }

    public boolean isPaused() {
        // main task is error,and Tiered storage status change to paused
        return ExecutableState.PAUSED == SecondStorageUtil.getJobStatus(project, getParentId());
    }

    public boolean isError() {
        return ExecutableState.ERROR == SecondStorageUtil.getJobStatus(project, getParentId());
    }

    public boolean isSuicidal() {
        return ExecutableState.SUICIDAL == SecondStorageUtil.getJobStatus(project, getParentId());
    }

    public boolean isStop() {
        return isDiscarded() || isPaused() || isError() || isSuicidal();
    }

    public boolean isPauseOrError() {
        return isPaused() || isError();
    }

    public KylinConfig getProjectConfig() {
        return NProjectManager.getInstance(getConfig()).getProject(getProject()).getConfig();
    }

    private Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> getFileLoaders(List<DataLoader> dataLoaders) {
        Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> fileLoaders = new HashMap<>(dataLoaders.size());
        for (DataLoader dataLoader : dataLoaders) {
            dataLoader.getSingleFileLoaderPerNode().forEach((nodeName, files) -> fileLoaders
                    .computeIfAbsent(nodeName, v -> new ConcurrentLinkedQueue<>()).addAll(files));
        }
        return fileLoaders;
    }

    private Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> getBeforeCommitDropPartitions(
            List<DataLoader> dataLoaders) {
        Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> dropPartitions = new HashMap<>(dataLoaders.size());
        for (DataLoader dataLoader : dataLoaders) {
            dataLoader.getLoadCommitDropPartitions().forEach((nodeName, partition) -> dropPartitions
                    .computeIfAbsent(nodeName, v -> new ConcurrentLinkedQueue<>()).addAll(partition));
        }
        return dropPartitions;
    }

    private Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> getCommitMovePartitions(
            List<DataLoader> dataLoaders) throws SQLException {
        Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> movePartitions = new HashMap<>(dataLoaders.size());
        for (DataLoader dataLoader : dataLoaders) {
            dataLoader.getLoadCommitMovePartitions().forEach((nodeName, partition) -> movePartitions
                    .computeIfAbsent(nodeName, v -> new ConcurrentLinkedQueue<>()).addAll(partition));
        }
        return movePartitions;
    }

    private Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> getExceptionCommitDropPartitions(
            List<DataLoader> dataLoaders) throws SQLException {
        Map<String, ConcurrentLinkedQueue<ClickhouseLoadActionUnit>> exceptionPartitions = new HashMap<>(
                dataLoaders.size());
        for (DataLoader dataLoader : dataLoaders) {
            dataLoader.getLoadCommitExceptionPartitions().forEach((nodeName, partition) -> exceptionPartitions
                    .computeIfAbsent(nodeName, v -> new ConcurrentLinkedQueue<>()).addAll(partition));
        }
        return exceptionPartitions;
    }
}
