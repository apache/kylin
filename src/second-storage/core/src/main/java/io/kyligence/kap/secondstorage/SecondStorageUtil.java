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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.response.SecondStorageInfo;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;

public class SecondStorageUtil {
    public static final Set<ExecutableState> RUNNING_STATE = Sets
            .newHashSet(Arrays.asList(ExecutableState.RUNNING, ExecutableState.READY, ExecutableState.PAUSED));
    public static final Set<JobTypeEnum> RELATED_JOBS = Sets
            .newHashSet(Arrays.asList(JobTypeEnum.INDEX_BUILD, JobTypeEnum.INDEX_REFRESH, JobTypeEnum.INC_BUILD,
                    JobTypeEnum.INDEX_MERGE, JobTypeEnum.EXPORT_TO_SECOND_STORAGE));
    public static final Set<JobTypeEnum> BUILD_JOBS = Sets.newHashSet(JobTypeEnum.INDEX_BUILD,
            JobTypeEnum.INDEX_REFRESH, JobTypeEnum.INC_BUILD, JobTypeEnum.INDEX_MERGE);

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
            boolean containPartitionCol = indexPlan.getBaseTableLayout().getColumns().stream().anyMatch(col -> {
                return col.getTableDotName().equals(indexPlan.getModel().getPartitionDesc().getPartitionDateColumn());
            });
            if (!containPartitionCol) {
                throw new KylinException(PARTITION_COLUMN_NOT_AVAILABLE,
                        MsgPicker.getMsg().getPartitionColumnNotAvailable());
            }
        }
    }

    public static boolean isBaseTableIndex(LayoutEntity index) {
        return index != null && IndexEntity.isTableIndex(index.getId()) && index.isBaseIndex();
    }

    public static Optional<LayoutEntity> getBaseIndex(NDataflow df) {
        return df.getIndexPlan().getAllLayouts().stream().filter(SecondStorageUtil::isBaseTableIndex).findFirst();
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
        if (isProjectEnable(project)) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            Optional<Manager<TableFlow>> tableFlowManager = tableFlowManager(config, project);
            return tableFlowManager.isPresent() && tableFlowManager.get().get(model).isPresent();
        }
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
                    .filter(tableFlow -> tableFlow.getId().equals(model)).forEach(tableFlow -> {
                        tableFlow.update(copy -> {
                            copy.getTableDataList().stream().filter(
                                    tableData -> tableData.getDatabase().equals(NameUtil.getDatabase(config, project))
                                            && tableData.getTable().startsWith(NameUtil.tablePrefix(model)))
                                    .forEach(tableData -> tableData.removePartitions(
                                            tablePartition -> segments.contains(tablePartition.getSegmentId())));
                        });
                    }));
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

    public static Optional<Manager<TableFlow>> tableFlowManager(NDataflow dataflow) {
        return isGlobalEnable() ? tableFlowManager(dataflow.getConfig(), dataflow.getProject()) : Optional.empty();
    }

    public static Optional<Manager<TablePlan>> tablePlanManager(KylinConfig config, String project) {
        return isGlobalEnable() ? Optional.of(SecondStorage.tablePlanManager(config, project)) : Optional.empty();
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
        NExecutableManager executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        AbstractExecutable abstractExecutable = executableManager.getJob(jobId);
        JobTypeEnum type = abstractExecutable.getJobType();
        boolean canRestart = type != JobTypeEnum.EXPORT_TO_SECOND_STORAGE;

        ExecutablePO jobDetail = executableManager.getAllJobs().stream().filter(job -> job.getId().equals(jobId))
                .findFirst().orElseThrow(() -> new IllegalStateException("Job not found"));
        if (BUILD_JOBS.contains(jobDetail.getJobType())) {
            long finishedCount = jobDetail.getTasks().stream()
                    .filter(task -> "SUCCEED".equals(task.getOutput().getStatus())).count();
            // build job can't restart when second storage step is running
            canRestart = finishedCount < 3;
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
        NExecutableManager executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        AbstractExecutable abstractExecutable = executableManager.getJob(jobId);
        JobTypeEnum type = abstractExecutable.getJobType();
        boolean secondStorageLoading = type == JobTypeEnum.EXPORT_TO_SECOND_STORAGE;
        ExecutablePO jobDetail = executableManager.getAllJobs().stream().filter(job -> job.getId().equals(jobId))
                .findFirst().orElseThrow(() -> new IllegalStateException("Job not found"));
        if (BUILD_JOBS.contains(jobDetail.getJobType())) {
            secondStorageLoading = true;
            long finishedCount = jobDetail.getTasks().stream()
                    .filter(task -> "SUCCEED".equals(task.getOutput().getStatus())).count();
            // build job can't restart when second storage step is running
            if (finishedCount < 3) {
                secondStorageLoading = false;
            }
        }
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        long runningJobsCount = scheduler.getContext().getRunningJobs().keySet().stream()
                .filter(id -> id.startsWith(jobId)).count();
        if (secondStorageLoading && runningJobsCount > 0) {
            throw new KylinException(ServerErrorCode.JOB_RESUME_FAILED, MsgPicker.getMsg().getJobResumeFailed());
        }
    }
}
