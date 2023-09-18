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

package io.kyligence.kap.secondstorage;

import static org.apache.kylin.common.exception.ServerErrorCode.BASE_TABLE_INDEX_NOT_AVAILABLE;
import static org.apache.kylin.common.exception.ServerErrorCode.PARTITION_COLUMN_NOT_AVAILABLE;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_DATA_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_STATUS_ERROR;
import static org.apache.kylin.job.constant.ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE;
import static org.apache.kylin.job.constant.ExecutableConstants.STEP_NAME_CLEANUP;
import static org.apache.kylin.job.constant.ExecutableConstants.STEP_NAME_MERGER_SPARK_SEGMENT;
import static org.apache.kylin.job.constant.ExecutableConstants.STEP_UPDATE_METADATA;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobSchedulerModeEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.StageBase;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.query.NativeQueryRealization;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.response.SecondStorageInfo;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
// CALL FROM CORE
public class SecondStorageUtil {
    public static final Set<ExecutableState> RUNNING_STATE = ImmutableSet.of(ExecutableState.RUNNING,
            ExecutableState.READY, ExecutableState.PAUSED);
    public static final Set<JobTypeEnum> RELATED_JOBS = ImmutableSet.of(JobTypeEnum.INDEX_BUILD,
            JobTypeEnum.INDEX_REFRESH, JobTypeEnum.INC_BUILD, JobTypeEnum.INDEX_MERGE,
            JobTypeEnum.EXPORT_TO_SECOND_STORAGE, JobTypeEnum.SECOND_STORAGE_REFRESH_SECONDARY_INDEXES);
    public static final Set<JobTypeEnum> BUILD_JOBS = ImmutableSet.of(JobTypeEnum.INDEX_BUILD,
            JobTypeEnum.INDEX_REFRESH, JobTypeEnum.INC_BUILD, JobTypeEnum.INDEX_MERGE);
    public static final Set<String> EXPORT_STEPS = ImmutableSet.of(SecondStorageConstants.STEP_EXPORT_TO_SECOND_STORAGE,
            SecondStorageConstants.STEP_REFRESH_SECOND_STORAGE, SecondStorageConstants.STEP_MERGE_SECOND_STORAGE);

    public static final Pattern PUSHED_AGGREGATES = Pattern.compile("PushedAggregates: \\[[^\\]]++\\]");
    public static final Pattern PUSHED_GROUP_BY = Pattern.compile("PushedGroupByExpressions: \\[[^\\]]++\\]");
    public static final Pattern PUSHED_FILTERS = Pattern.compile("PushedFilters: \\[[^\\]]++\\]");
    public static final Pattern PUSHED_LIMIT = Pattern.compile("PushedLimit: LIMIT [0-9]+");
    public static final Pattern PUSHED_OFFSET = Pattern.compile("PushedOffset: OFFSET [0-9]+");
    public static final Pattern PUSHED_TOP_N = Pattern.compile("PushedTopN: ORDER BY \\[.+?\\] LIMIT [0-9]+");

    private SecondStorageUtil() {
    }

    public static void initModelMetaData(String project, String model) {
        checkEnableModel(project, model);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            final KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<TablePlan>> tablePlanManager = tablePlanManager(config, project);
            Optional<Manager<TableFlow>> tableFlowManager = tableFlowManager(config, project);
            Preconditions.checkState(tableFlowManager.isPresent() && tablePlanManager.isPresent());
            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
            TablePlan tablePlan = tablePlanManager.get().makeSureRootEntity(model);
            tableFlowManager.get().makeSureRootEntity(model);
            Map<Long, List<LayoutEntity>> layouts = indexPlanManager.getIndexPlan(model).getAllLayoutsMap().values()
                    .stream().filter(SecondStorageUtil::isBaseTableIndex)
                    .collect(Collectors.groupingBy(LayoutEntity::getIndexId));
            for (Map.Entry<Long, List<LayoutEntity>> entry : layouts.entrySet()) {
                LayoutEntity layoutEntity = entry.getValue().stream().filter(SecondStorageUtil::isBaseTableIndex)
                        .findFirst().orElse(null);
                Preconditions.checkNotNull(layoutEntity);
                tablePlan = tablePlan.createTableEntityIfNotExists(layoutEntity, true);
            }
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static void checkSecondStorageData(String project) {
        if (!SecondStorageUtil.isProjectEnable(project)) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, "'%s' not enable second storage.", project));
        }
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Optional<Manager<TableFlow>> optionalTableFlowManager = SecondStorageUtil.tableFlowManager(config, project);
        if (!optionalTableFlowManager.isPresent()) {
            throw new KylinException(SECOND_STORAGE_DATA_NOT_EXIST,
                    String.format(Locale.ROOT, "'%s' second storage data not exist.", project));
        }
    }

    private static void checkEnableModel(String project, String model) {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        final IndexPlan indexPlan = indexPlanManager.getIndexPlan(model);
        if (!indexPlan.containBaseTableLayout()) {
            throw new KylinException(BASE_TABLE_INDEX_NOT_AVAILABLE,
                    MsgPicker.getMsg().getBaseTableIndexNotAvailable());
        }
        if (indexPlan.getModel().isIncrementBuildOnExpertMode()) {
            boolean containPartitionCol = indexPlan.getBaseTableLayout().getColumns().stream().anyMatch(col -> col
                    .getTableDotName().equals(indexPlan.getModel().getPartitionDesc().getPartitionDateColumn()));
            if (!containPartitionCol) {
                throw new KylinException(PARTITION_COLUMN_NOT_AVAILABLE,
                        MsgPicker.getMsg().getPartitionColumnNotAvailable());
            }
        }
    }

    public static boolean isBaseTableIndex(LayoutEntity index) {
        return index != null && IndexEntity.isTableIndex(index.getId()) && index.isBaseIndex();
    }

    public static LayoutEntity getBaseIndex(NDataflow df) {
        return df.getIndexPlan().getAllLayouts().stream().filter(SecondStorageUtil::isBaseTableIndex)
                .collect(Collectors.toList()).get(0);
    }

    public static List<String> getProjectLocks(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Optional<Manager<NodeGroup>> nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, project);
        if (!nodeGroupManager.isPresent()) {
            return new ArrayList<>();
        }
        List<NodeGroup> nodeGroups = nodeGroupManager.get().listAll();
        if (CollectionUtils.isNotEmpty(nodeGroups)) {
            Preconditions.checkState(nodeGroups.stream().map(NodeGroup::getLockTypes).distinct().count() == 1,
                    "Logical Error, this is a bug! Cluster has different lock type.");
            return nodeGroups.get(0).getLockTypes();
        }
        return new ArrayList<>();
    }

    public static List<AbstractExecutable> findSecondStorageRelatedJobByProject(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        return executableManager.getJobs().stream().map(executableManager::getJob)
                .filter(job -> RELATED_JOBS.contains(job.getJobType())).collect(Collectors.toList());
    }

    public static void validateProjectLock(String project, List<String> requestLocks) {
        LockTypeEnum.checkLocks(requestLocks, SecondStorageUtil.getProjectLocks(project));
    }

    public static void validateDisableModel(String project, String modelId) {
        validateProjectLock(project, Arrays.asList(LockTypeEnum.LOAD.name()));
        List<AbstractExecutable> jobs = SecondStorageUtil.findSecondStorageRelatedJobByProject(project);
        if (jobs.stream().filter(job -> RUNNING_STATE.contains(job.getStatus()))
                .anyMatch(job -> job.getTargetSubject().equals(modelId))) {
            String name = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getDataModelDesc(modelId).getAlias();
            throw new KylinException(JobErrorCode.SECOND_STORAGE_JOB_EXISTS,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageJobExists(), name));
        }
    }

    public static boolean isGlobalEnable() {
        return SecondStorage.enabled();
    }

    public static boolean isProjectEnable(String project) {
        if (isGlobalEnable()) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<NodeGroup>> nodeGroupManager = nodeGroupManager(config, project);
            return nodeGroupManager.isPresent() && !nodeGroupManager.get().listAll().isEmpty();
        }
        return false;
    }

    public static List<SecondStorageNode> listProjectNodes(String project) {
        if (!isProjectEnable(project)) {
            return Collections.emptyList();
        }
        Optional<Manager<NodeGroup>> groupManager = nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
        Preconditions.checkNotNull(groupManager);
        return groupManager.map(nodeGroupNManager -> nodeGroupNManager.listAll().stream()
                .flatMap(nodeGroup -> nodeGroup.getNodeNames().stream()).distinct().map(name -> {
                    Node node = SecondStorageNodeHelper.getNode(name);
                    return new SecondStorageNode(node);
                }).collect(Collectors.toList())).orElse(Collections.emptyList());
    }

    public static boolean isModelEnable(String project, String model) {
//        if (isProjectEnable(project)) {
//            KylinConfig config = KylinConfig.getInstanceFromEnv();
//            Optional<Manager<TableFlow>> tableFlowManager = tableFlowManager(config, project);
//            return tableFlowManager.isPresent() && tableFlowManager.get().get(model).isPresent();
//        }
        return false;
    }

    public static List<SecondStorageInfo> setSecondStorageSizeInfo(List<NDataModel> models) {
        if (models == null || models.isEmpty()) {
            return Collections.emptyList();
        }
        Optional<Manager<TableFlow>> tableFlowManager = SecondStorageUtil
                .tableFlowManager(KylinConfig.getInstanceFromEnv(), models.get(0).getProject());
        Preconditions.checkState(tableFlowManager.isPresent());
        return setSecondStorageSizeInfo(models, tableFlowManager.get());
    }

    protected static List<SecondStorageInfo> setSecondStorageSizeInfo(List<NDataModel> models,
            Manager<TableFlow> tableFlowManager) {
        List<Set<String>> shards = SecondStorageNodeHelper.groupsToShards(
                SecondStorageUtil.listNodeGroup(KylinConfig.getInstanceFromEnv(), models.get(0).getProject()));

        return models.stream().map(model -> {
            SecondStorageInfo secondStorageInfo = new SecondStorageInfo();
            secondStorageInfo.setSecondStorageEnabled(isModelEnable(model.getProject(), model.getId()));
            TableFlow tableFlow = tableFlowManager.get(model.getId()).orElse(null);
            if (isTableFlowEmpty(tableFlow)) {
                secondStorageInfo.setSecondStorageNodes(Collections.emptyMap());
                secondStorageInfo.setSecondStorageSize(0);
            } else {
                List<TablePartition> tablePartitions = tableFlow.getTableDataList().stream()
                        .flatMap(tableData -> tableData.getPartitions().stream()).collect(Collectors.toList());
                Set<String> nodesStr = Sets.newHashSet();

                for (TablePartition partition : tablePartitions) {
                    nodesStr.addAll(partition.getShardNodes());
                }

                secondStorageInfo.setSecondStorageNodes(convertNodesToPairs(new ArrayList<>(nodesStr)));
                secondStorageInfo.setSecondStorageSize(calculateSecondStorageSize(shards, tablePartitions));
            }
            return secondStorageInfo;
        }).collect(Collectors.toList());
    }

    public static long calculateSecondStorageSize(List<Set<String>> shards, List<TablePartition> partitions) {
        // key: node_name / value: node_size
        Map<String, Long> nodesSize = partitions.stream()
                .flatMap(partition -> partition.getSizeInNode().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));

        return shards.stream().mapToLong(shard -> shard.stream().mapToLong(replica -> nodesSize.getOrDefault(replica, 0L)).max().orElse(0)).sum();
    }

    public static SecondStorageNode transformNode(String name) {
        Node node = SecondStorageNodeHelper.getNode(name);
        return new SecondStorageNode(node);
    }

    public static Map<String, List<SecondStorageNode>> convertNodesToPairs(List<String> nodes) {
        Map<String, List<SecondStorageNode>> result = Maps.newHashMap();
        nodes.stream().sorted()
                .forEach(node -> result
                        .computeIfAbsent(SecondStorageNodeHelper.getPairByNode(node), k -> new ArrayList<>())
                        .add(new SecondStorageNode(SecondStorageNodeHelper.getNode(node))));
        return result;
    }

    public static boolean isTableFlowEmpty(TableFlow tableFlow) {
        return tableFlow == null || tableFlow.getTableDataList() == null || tableFlow.getTableDataList().isEmpty()
                || tableFlow.getTableDataList().get(0).getPartitions() == null
                || tableFlow.getTableDataList().get(0).getPartitions().isEmpty();
    }

    public static void disableProject(String project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<NodeGroup>> nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, project);
            Optional<Manager<TableFlow>> tableFlowManager = SecondStorageUtil.tableFlowManager(config, project);
            Optional<Manager<TablePlan>> tablePlanManager = SecondStorageUtil.tablePlanManager(config, project);
            nodeGroupManager.ifPresent(manager -> manager.listAll().forEach(manager::delete));
            tableFlowManager.ifPresent(manager -> manager.listAll().forEach(manager::delete));
            tablePlanManager.ifPresent(manager -> manager.listAll().forEach(manager::delete));
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static void disableModel(String project, String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<TableFlow>> tableFlowManager = SecondStorageUtil.tableFlowManager(config, project);
            Optional<Manager<TablePlan>> tablePlanManager = SecondStorageUtil.tablePlanManager(config, project);
            tablePlanManager.ifPresent(manager -> manager.listAll().stream()
                    .filter(tablePlan -> tablePlan.getId().equals(modelId)).forEach(manager::delete));
            tableFlowManager.ifPresent(manager -> manager.listAll().stream()
                    .filter(tableFlow -> tableFlow.getId().equals(modelId)).forEach(manager::delete));
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static void cleanSegments(String project, String model, Set<String> segments) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<TableFlow>> tableFlowManager = SecondStorageUtil.tableFlowManager(config, project);
            tableFlowManager.ifPresent(manager -> manager.listAll().stream()
                    .filter(tableFlow -> tableFlow.getId().equals(model)).forEach(tableFlow ->
                            tableFlow.update(copy ->
                                    copy.getTableDataList().stream().filter(
                                            tableData -> tableData.getDatabase().equals(NameUtil.getDatabase(config, project))
                                                    && tableData.getTable().startsWith(NameUtil.tablePrefix(model)))
                                            .forEach(tableData -> tableData.removePartitions(
                                                    tablePartition -> segments.contains(tablePartition.getSegmentId())))
                            )
                    ));
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static void cleanSegments(String project, String modelId, Set<String> segments, Set<Long> layoutIds) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<TableFlow>> tableFlowManager = SecondStorageUtil.tableFlowManager(config, project);

            tableFlowManager.ifPresent(
                    manager -> manager.listAll().stream().filter(tableFlow -> tableFlow.getId().equals(modelId))
                            .forEach(tableFlow -> tableFlow.update(copy -> copy.getTableDataList().stream()
                                    .filter(tableData -> layoutIds.contains(tableData.getLayoutID()))
                                    .forEach(tableData -> tableData.removePartitions(
                                            tablePartition -> segments.contains(tablePartition.getSegmentId()))))));

            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static Optional<Manager<TableFlow>> tableFlowManager(KylinConfig config, String project) {
        return isGlobalEnable() ? Optional.of(SecondStorage.tableFlowManager(config, project)) : Optional.empty();
    }

    public static List<TableFlow> listTableFlow(KylinConfig config, String project) {
        Optional<Manager<TableFlow>> optional = tableFlowManager(config, project);
        if (optional.isPresent()) {
            Manager<TableFlow> tableFlowManager = optional.get();
            return tableFlowManager.listAll();
        }
        return Collections.emptyList();
    }

    public static TableFlow getTableFlow(String project, String modelId) {
        Optional<Manager<TableFlow>> tableFlowManager = tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        Preconditions.checkState(tableFlowManager.isPresent());
        Optional<TableFlow> tableFlow = tableFlowManager.get().get(modelId);
        Preconditions.checkState(tableFlow.isPresent());
        return tableFlow.get();
    }

    public static Optional<Manager<TableFlow>> tableFlowManager(NDataflow dataflow) {
        return isGlobalEnable() ? tableFlowManager(dataflow.getConfig(), dataflow.getProject()) : Optional.empty();
    }

    public static Optional<Manager<TablePlan>> tablePlanManager(KylinConfig config, String project) {
        return isGlobalEnable() ? Optional.of(SecondStorage.tablePlanManager(config, project)) : Optional.empty();
    }

    public static TablePlan getTablePlan(String project, String modelId) {
        Optional<Manager<TablePlan>> tablePlanManager = tablePlanManager(KylinConfig.getInstanceFromEnv(), project);
        Preconditions.checkState(tablePlanManager.isPresent());
        Optional<TablePlan> tablePlan = tablePlanManager.get().get(modelId);
        Preconditions.checkState(tablePlan.isPresent());
        return tablePlan.get();
    }

    public static Optional<Manager<NodeGroup>> nodeGroupManager(KylinConfig config, String project) {
        return isGlobalEnable() ? Optional.of(SecondStorage.nodeGroupManager(config, project)) : Optional.empty();
    }

    public static List<NodeGroup> listNodeGroup(KylinConfig config, String project) {
        Optional<Manager<NodeGroup>> optional = nodeGroupManager(config, project);
        if (optional.isPresent()) {
            Manager<NodeGroup> nodeGroupManager = optional.get();
            return nodeGroupManager.listAll();
        }
        return Collections.emptyList();
    }

    public static ExecutableState getJobStatus(String project, String jobId) {
        NExecutableManager manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        return manager.getJob(jobId).getStatus();
    }

    public static void checkJobRestart(String project, String jobId) {
        if (!isProjectEnable(project))
            return;
        checkJobRestart(NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getJob(jobId));
    }

    public static boolean checkStorageEmpty(String project, String modelId, long layoutId) {
        List<TableData> tableDataList = getTableFlow(project, modelId).getTableData(layoutId);
        return CollectionUtils.isEmpty(tableDataList)
                || tableDataList.stream().allMatch(tableData -> CollectionUtils.isEmpty(tableData.getPartitions()));
    }

    @VisibleForTesting
    public static void checkJobRestart(AbstractExecutable executable) {
        boolean canRestart = executable.getJobType() != JobTypeEnum.EXPORT_TO_SECOND_STORAGE
                && executable.getJobType() != JobTypeEnum.SECOND_STORAGE_REFRESH_SECONDARY_INDEXES;
        if (BUILD_JOBS.contains(executable.getJobType()) && hasSecondStorageLoadJob(executable)) {
            if (executable.getJobSchedulerMode() == JobSchedulerModeEnum.DAG) {
                Optional<ChainedStageExecutable> detectResourceTask = getChainedStageExecutableByName(
                        ExecutableConstants.STEP_NAME_DETECT_RESOURCE, executable);
                canRestart = detectResourceTask.filter(task -> task.getStatus() != ExecutableState.SUCCEED).isPresent();
            } else {
                long finishedCount = ((DefaultExecutable) executable).getTasks().stream()
                        .filter(task -> ExecutableState.SUCCEED == task.getStatus()).count();
                canRestart = finishedCount < getExportStepPosition(executable) - 1;
            }
        }
        if (!canRestart) {
            throw new KylinException(ServerErrorCode.JOB_RESTART_FAILED, MsgPicker.getMsg().getJobRestartFailed());
        }
    }

    public static void checkSegmentRemove(String project, String modelId, String[] ids) {
        if (!isModelEnable(project, modelId))
            return;
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        for (NDataSegment segment : dataflow.getSegments(Sets.newHashSet(ids))) {
            startTime = Math.min(startTime, Long.parseLong(segment.getSegRange().getStart().toString()));
            endTime = endTime > Long.parseLong(segment.getSegRange().getStart().toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().getEnd().toString());
        }
        if (SecondStorageLockUtils.containsKey(modelId,
                new SegmentRange.TimePartitionedSegmentRange(startTime, endTime))) {
            throw new KylinException(ServerErrorCode.SEGMENT_DROP_FAILED, MsgPicker.getMsg().getSegmentDropFailed());
        }
    }

    public static void checkJobResume(String project, String jobId) {
        if (!isProjectEnable(project))
            return;
        checkJobResume(NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getJob(jobId));
    }

    @VisibleForTesting
    public static void checkJobResume(AbstractExecutable executable) {
        long runningJobsCount = ((DefaultExecutable) executable).getTasks().stream()
                .filter(task -> EXPORT_STEPS.contains(task.getName()))
                .filter(task -> ExecutableState.RUNNING == task.getStatus()).count();
        if (runningJobsCount != 0) {
            throw new KylinException(ServerErrorCode.JOB_RESUME_FAILED, MsgPicker.getMsg().getJobResumeFailed());
        }
    }

    public static void checkJobPause(String project, String jobId) {
        if (!isProjectEnable(project)) {
            return;
        }
        checkJobPause(NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getJob(jobId));
    }

    private static void checkJobPause(AbstractExecutable executable) {
        if (executable.getJobType() == JobTypeEnum.SECOND_STORAGE_REFRESH_SECONDARY_INDEXES) {
            throw new KylinException(ServerErrorCode.JOB_PAUSE_FAILED, MsgPicker.getMsg().getJobPauseFailed());
        }
    }

    private static boolean hasSecondStorageLoadJob(AbstractExecutable executable) {
        return ((DefaultExecutable) executable).getTasks().stream()
                .anyMatch(task -> EXPORT_STEPS.contains(task.getName()));
    }

    private static int getExportStepPosition(AbstractExecutable executable) {
        int exportStepPosition = 0;
        for (AbstractExecutable task : ((DefaultExecutable) executable).getTasks()) {
            exportStepPosition++;
            if (EXPORT_STEPS.contains(task.getName())) {
                break;
            }
        }
        return exportStepPosition;
    }

    public static boolean checkBuildFlatTableIsSuccess(AbstractExecutable executable) {
        if (executable.getJobSchedulerMode() != JobSchedulerModeEnum.DAG) {
            return true;
        }
        Optional<ChainedStageExecutable> buildSparkCubeTask = getChainedStageExecutableByName(STEP_NAME_BUILD_SPARK_CUBE, executable);
        if (!buildSparkCubeTask.isPresent()) {
            return true;
        }
        Map<String, List<StageBase>> stages = buildSparkCubeTask.get().getStagesMap();
        return stages.entrySet().stream()
                .map(segmentStages -> {
                    String segmentId = segmentStages.getKey();
                    return segmentStages.getValue().stream()
                            .filter(segmentStage -> ExecutableConstants.STAGE_NAME_GENERATE_FLAT_TABLE.equals(segmentStage.getName()))
                            .map(segmentStage -> segmentStage.getOutput(segmentId).getState() == ExecutableState.SUCCEED)
                            .findFirst()
                            .orElse(true);
                }).reduce((b1, b2) -> b1 && b2)
                .orElse(true);
    }

    private static Optional<ChainedStageExecutable> getChainedStageExecutableByName(String name, AbstractExecutable executable) {
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) executable).getTasks();
        return tasks.stream()
                .filter(task -> name.equals(task.getName()))
                .map(ChainedStageExecutable.class::cast)
                .findFirst();
    }

    public static boolean checkMergeFlatTableIsSuccess(AbstractExecutable executable) {
        if (executable.getJobSchedulerMode() != JobSchedulerModeEnum.DAG) {
            return true;
        }
        Optional<ChainedStageExecutable> mergeSparkCubeTask = getChainedStageExecutableByName(STEP_NAME_MERGER_SPARK_SEGMENT, executable);
        if (!mergeSparkCubeTask.isPresent()) {
            return true;
        }
        Map<String, List<StageBase>> stages = mergeSparkCubeTask.get().getStagesMap();
        return stages.entrySet().stream()
                .map(segmentStages -> {
                    String segmentId = segmentStages.getKey();
                    return segmentStages.getValue().stream()
                            .filter(segmentStage -> ExecutableConstants.STAGE_NAME_MERGE_FLAT_TABLE.equals(segmentStage.getName()))
                            .map(segmentStage -> segmentStage.getOutput(segmentId).getState() == ExecutableState.SUCCEED)
                            .findFirst()
                            .orElse(true);
                }).reduce((b1, b2) -> b1 && b2)
                .orElse(true);
    }

    public static boolean checkBuildDfsIsSuccess(AbstractExecutable executable) {
        if (executable.getJobSchedulerMode() != JobSchedulerModeEnum.DAG) {
            return true;
        }
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) executable).getTasks();

        return tasks.stream()
                .filter(task -> STEP_UPDATE_METADATA.equals(task.getName()))
                .map(task -> task.getStatus() == ExecutableState.SUCCEED)
                .findFirst()
                .orElse(true);
    }

    public static boolean checkMergeDfsIsSuccess(AbstractExecutable executable) {
        if (executable.getJobSchedulerMode() != JobSchedulerModeEnum.DAG) {
            return true;
        }

        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) executable).getTasks();

        return tasks.stream()
                .filter(task -> STEP_NAME_CLEANUP.equals(task.getName()))
                .map(task -> task.getStatus() == ExecutableState.SUCCEED)
                .findFirst()
                .orElse(true);
    }

    public static Set<Long> listEnableLayoutBySegment(String project, String modelId, String segmentId) {
        if (!isModelEnable(project, modelId)) {
            return ImmutableSet.of();
        }

        Optional<Manager<TableFlow>> tableFlowManager = tableFlowManager(KylinConfig.getInstanceFromEnv(), project);

        if (!tableFlowManager.isPresent()) {
            return ImmutableSet.of();
        }

        Optional<TableFlow> tableFlow = tableFlowManager.get().get(modelId);

        if (!tableFlow.isPresent()) {
            return ImmutableSet.of();
        }
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        return tableFlow.get().getLayoutBySegment(segmentId).stream().filter(
                layout -> dataflow.getIndexPlan() != null && dataflow.getIndexPlan().getLayoutEntity(layout) != null)
                .collect(Collectors.toSet());
    }

    public static String collectExecutedPlan(String executedPlan) {
        StringBuilder pushedExecutedPlan = new StringBuilder();
        List<Pattern> pushedPattern = Lists.newArrayList(PUSHED_AGGREGATES, PUSHED_FILTERS, PUSHED_GROUP_BY, PUSHED_LIMIT, PUSHED_OFFSET, PUSHED_TOP_N);
        for (Pattern pattern : pushedPattern) {
            Matcher match = pattern.matcher(executedPlan);
            if (match.find()) {
                pushedExecutedPlan.append(",").append(match.group());
            }
        }
        return pushedExecutedPlan.toString();
    }

    public static String convertExecutedPlan(String executedPlan, String project, List<NativeQueryRealization> realizations) {
        Pattern columnName = Pattern.compile("c[0-9]+");
        List<NativeQueryRealization> secondStorageLayout = realizations.stream().filter(NativeQueryRealization::isSecondStorage)
                .collect(Collectors.toList());
        if (secondStorageLayout.isEmpty() || project == null)
            return null;

        NativeQueryRealization realization = secondStorageLayout.get(0);
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getIndexPlan(realization.getModelId());
        LayoutEntity layout = indexPlan.getLayoutEntity(realization.getLayoutId());

        Matcher match = columnName.matcher(executedPlan);
        while (match.find()) {
            String name = match.group(0);
            Integer colIndex = convertColumnName(name);
            String alias = layout.getOrderedDimensions().get(colIndex).getAliasDotName();
            executedPlan = executedPlan.replace(match.group(), alias);
            match = columnName.matcher(executedPlan);
        }
        return executedPlan;
    }

    private static Integer convertColumnName(String name) {
        Pattern columnName = Pattern.compile("[0-9]+");
        Matcher match = columnName.matcher(name);
        if (match.find()) {
            return Integer.parseInt(match.group());
        }
        return null;
    }
}
