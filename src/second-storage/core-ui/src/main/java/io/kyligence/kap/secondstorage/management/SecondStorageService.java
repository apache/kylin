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

package io.kyligence.kap.secondstorage.management;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_NODE_NOT_AVAILABLE;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_LOCK_FAIL;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_STATUS_ERROR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.secondstorage.database.QueryOperator;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SecondStorageConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.SecondStorageIndexCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageModelCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageProjectCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentCleanJobHandler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.guava20.shaded.common.collect.ImmutableList;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.JobFilter;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ModelService;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageQueryRouteUtil;
import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.config.DefaultSecondStorageProperties;
import io.kyligence.kap.secondstorage.config.SecondStorageModelSegment;
import io.kyligence.kap.secondstorage.config.SecondStorageProjectModelSegment;
import io.kyligence.kap.secondstorage.config.SecondStorageSegment;
import io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.factory.SecondStorageFactoryUtils;
import io.kyligence.kap.secondstorage.management.request.ProjectLoadResponse;
import io.kyligence.kap.secondstorage.management.request.ProjectRecoveryResponse;
import io.kyligence.kap.secondstorage.management.request.ProjectTableSyncResponse;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.MetadataOperator;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.response.TableSyncResponse;
import io.kyligence.kap.secondstorage.util.SecondStorageJobUtil;
import lombok.val;

public class SecondStorageService extends BasicService implements SecondStorageUpdater {
    private static final Logger logger = LoggerFactory.getLogger(SecondStorageService.class);

    private JobService jobService;

    private AclEvaluate aclEvaluate;

    @Autowired
    public SecondStorageService setAclEvaluate(final AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
        return this;
    }

    @Autowired
    public SecondStorageService setJobService(final JobService jobService) {
        this.jobService = jobService;
        return this;
    }

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;


    @Override
    @Transaction(project = 0)
    public String updateIndex(final String project, final String modelId) {
        if (!SecondStorageUtil.isModelEnable(project, modelId)) {
            return null;
        }

        val tableFlowManager = SecondStorageUtil.tableFlowManager(getConfig(), project);
        val tablePlanManager = SecondStorageUtil.tablePlanManager(getConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        Preconditions.checkState(tablePlanManager.isPresent());
        Preconditions.checkState(tableFlowManager.isPresent());
        val tableFlow = tableFlowManager.get().get(modelId);
        Preconditions.checkState(tableFlow.isPresent());

        if (indexPlan.getBaseTableLayout() == null) {
            return null;
        }
        // get all layout entity contains locked index
        Set<Long> allBaseLayout = indexPlan.getAllLayouts().stream().filter(LayoutEntity::isBaseIndex).map(LayoutEntity::getId).collect(Collectors.toSet());
        Set<Long> needDeleteLayoutIds = new HashSet<>(allBaseLayout.size());

        tableFlowManager.get().get(modelId).map(tf -> {
            // clean unused table_data, maybe index is deleted
            List<Long> deleteLayouts = tf.getTableDataList().stream()
                    .map(TableData::getLayoutID)
                    .filter(id -> !allBaseLayout.contains(id))
                    .collect(Collectors.toList());
            needDeleteLayoutIds.addAll(deleteLayouts);
            return tf;
        });

        String jobId = null;
        if (!needDeleteLayoutIds.isEmpty()) {
            jobId = triggerIndexClean(project, modelId, needDeleteLayoutIds);
        }

        tablePlanManager.get().get(modelId).map(tp -> {
            // clean unused table_entity, maybe index is deleted
            val deleteLayoutIds = tp.getTableMetas().stream()
                    .filter(tableEntity -> !allBaseLayout.contains(tableEntity.getLayoutID()))
                    .map(TableEntity::getLayoutID).collect(Collectors.toSet());
            tp = tp.update(t -> t.cleanTable(deleteLayoutIds));

            // add new base_layout if not exists
            tp.createTableEntityIfNotExists(indexPlan.getBaseTableLayout(), true);
            return tp;
        });

        tableFlowManager.get().get(modelId).map(tf -> {
            // clean unused table_data, maybe index is deleted
            List<Long> deleteLayouts = tf.getTableDataList().stream()
                    .map(TableData::getLayoutID)
                    .filter(id -> !allBaseLayout.contains(id))
                    .collect(Collectors.toList());

            tf = tf.update(t -> t.cleanTableData(tableData -> deleteLayouts.contains(tableData.getLayoutID())));
            return tf;
        });

        return jobId;
    }

    @Override
    public String cleanModel(final String project, final String modelId) {
        if (!SecondStorageUtil.isModelEnable(project, modelId)) {
            return null;
        }

        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {

            val tableFlowManager = getTableFlowManager(project);
            val tablePlanManager = SecondStorageUtil.tablePlanManager(getConfig(), project);
            val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), project);
            val indexPlan = indexPlanManager.getIndexPlan(modelId);
            Preconditions.checkState(tablePlanManager.isPresent());
            val tableFlow = tableFlowManager.get(modelId);
            Preconditions.checkState(tableFlow.isPresent());

            String jobIb = null;
            if (indexPlan.getBaseTableLayout() != null) {
                jobIb = triggerModelClean(project, modelId);
                tableFlowManager.get(modelId).map(tf -> {
                    tf.update(TableFlow::cleanTableData);
                    return tf;
                });
                tablePlanManager.get().get(modelId).map(tp -> {
                    tp = tp.update(TablePlan::cleanTable);
                    tp.createTableEntityIfNotExists(indexPlan.getBaseTableLayout(), true);
                    return tp;
                });
            }
            return jobIb;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    @Override
    public String disableModel(final String project, final String modelId) {
        if (!SecondStorageUtil.isModelEnable(project, modelId)) {
            return null;
        }

        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            String jobId = triggerModelClean(project, modelId);
            SecondStorageUtil.disableModel(project, modelId);
            return jobId;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    @Override
    public Map<String, Object> getQueryMetric(String project, String queryId) {
        QueryOperator queryOperator = SecondStorageFactoryUtils.createQueryMetricOperator(project);
        return queryOperator.getQueryMetric(queryId);
    }

    public SecondStorageService setModelService(final ModelService modelService) {
        this.modelService = modelService;
        return this;
    }


    public boolean isEnabled(String project, String modelId) {
        return SecondStorageUtil.isModelEnable(project, modelId);
    }

    public Manager<TableFlow> getTableFlowManager(String project) {
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        Preconditions.checkState(tableFlowManager.isPresent());
        return tableFlowManager.get();
    }

    public Optional<TableFlow> getTableFlow(String project, String modelId) {
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        Preconditions.checkState(tableFlowManager.isPresent());
        return tableFlowManager.get().listAll().stream().filter(tableFlow -> tableFlow.getId().equals(modelId)).findFirst();
    }

    public ProjectLoadResponse projectLoadData(List<String> projects) {
        projects.forEach(project -> {
            aclEvaluate.checkProjectWritePermission(project);
            SecondStorageUtil.validateProjectLock(project, Arrays.asList(LockTypeEnum.LOAD.name()));
        });
        ProjectLoadResponse projectLoadResponse = new ProjectLoadResponse();
        for (String project : projects) {
            ProjectRecoveryResponse projectRecoveryResponse = new ProjectRecoveryResponse();
            val config = KylinConfig.getInstanceFromEnv();
            val modelManager = NDataModelManager.getInstance(config, project);
            val dataflowManager = NDataflowManager.getInstance(config, project);
            val allModelAlias = modelManager.listAllModelAlias();
            val execManager = NExecutableManager.getInstance(config, project);
            List<String> failedModels = new ArrayList<>();
            List<String> submittedModels = new ArrayList<>();
            List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
            projectRecoveryResponse.setProject(project);
            projectRecoveryResponse.setSubmittedModels(submittedModels);
            projectRecoveryResponse.setFailedModels(failedModels);
            projectRecoveryResponse.setJobs(jobInfos);
            projectLoadResponse.getLoads().add(projectRecoveryResponse);
            val validModels = allModelAlias.stream()
                    .map(modelName -> modelManager.getDataModelDescByAlias(modelName).getUuid())
                    .filter(modelId -> SecondStorageUtil.isModelEnable(project, modelId))
                    .filter(modelId -> {
                        val jobs = execManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning);
                        if (!jobs.isEmpty()) {
                            failedModels.add(modelManager.getDataModelDesc(modelId).getAlias());
                        }
                        val dataflow = dataflowManager.getDataflow(modelId);
                        return jobs.isEmpty() && !dataflow.getSegments().isEmpty();
                    })
                    .map(modelId -> modelManager.getDataModelDesc(modelId).getAlias())
                    .collect(Collectors.toList());
            for (val modelName : validModels) {
                try {
                    List<JobInfoResponse.JobInfo> jobs = this.importSingleModel(project, modelName);
                    jobs.stream().map(item->jobInfos.add(item)).collect(Collectors.toList());
                    submittedModels.add(modelName);
                } catch (Exception e) {
                    failedModels.add(modelName);
                    logger.error("model {} recover failed", modelName, e);
                }
            }
        }
        return projectLoadResponse;
    }

    public List<JobInfoResponse.JobInfo> importSingleModel(String project, String modelName) {
        SecondStorageUtil.validateProjectLock(project, Collections.singletonList(LockTypeEnum.LOAD.name()));
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDescByAlias(modelName).getUuid();
        SecondStorageJobUtil.validateModel(project, model);
        Preconditions.checkState(SecondStorageUtil.isModelEnable(project, model),
                "model %s doesn't enable tiered storage.", model);

        val dataflowManager = NDataflowManager.getInstance(config, project);
        val segIds = dataflowManager.getDataflow(model).getQueryableSegments().stream()
                .map(NDataSegment::getId).collect(Collectors.toList());
        return modelService.exportSegmentToSecondStorage(project, model, segIds.toArray(new String[]{}));
    }

    @Transaction(project = 0)
    public Optional<JobInfoResponse.JobInfo> changeModelSecondStorageState(String project, String modelId, boolean enabled) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv())
            aclEvaluate.checkProjectAdminPermission(project);
        JobInfoResponse.JobInfo jobInfo = null;
        if (enabled) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                enableModelSecondStorage(project, modelId);
                return null;
            }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
        } else {
            SecondStorageUtil.validateDisableModel(project, modelId);
            val jobId = disableModel(project, modelId);
            jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN.name(), jobId);
        }
        return Optional.ofNullable(jobInfo);
    }

    @Transaction(project = 0)
    public Optional<JobInfoResponse.JobInfo> changeProjectSecondStorageState(String project, List<String> pairs, boolean enable) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv())
            aclEvaluate.checkProjectAdminPermission(project);
        JobInfoResponse.JobInfo jobInfo = null;
        if (enable) {
            if (!new HashSet<>(listAvailablePairs()).containsAll(pairs)) {
                throw new KylinException(SECOND_STORAGE_NODE_NOT_AVAILABLE, MsgPicker.getMsg().getSecondStorageNodeNotAvailable());
            }
            if (!SecondStorageUtil.isProjectEnable(project)) {
                enableProjectSecondStorage(project, pairs);
            }
            addNodeToProject(project, pairs);
        } else {
            String jobId = disableProjectSecondStorage(project);
            jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN.name(), jobId);
        }
        return Optional.ofNullable(jobInfo);
    }

    @Transaction(project = 0)
    public List<String> deleteProjectSecondStorageNode(String project, List<String> shardNames, boolean force) {
        aclEvaluate.checkProjectAdminPermission(project);

        if (!SecondStorageUtil.isProjectEnable(project)) {
            return Collections.emptyList();
        }

        Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(getConfig(), project);
        List<NodeGroup> nodeGroups = nodeGroupManager.listAll();

        Map<String, List<NodeData>> shards = convertNodeGroupToPairs(nodeGroups);

        if (shards.size() == shardNames.size()) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT, "Second storage shard names contains all %s", shardNames));
        }

        boolean isLocked = LockTypeEnum.locked(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(project));

        if (!isLocked) {
            lockOperate(project, Collections.singletonList(LockTypeEnum.LOAD.name()), LockOperateTypeEnum.LOCK.name());
        }

        try {
            return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                List<String> requiredDeleteNodeNames = shardNames.stream()
                        .flatMap(shardName -> SecondStorageNodeHelper.getPair(shardName).stream()).collect(Collectors.toList());

                val nodeManager = SecondStorageUtil.nodeGroupManager(getConfig(), project);
                Preconditions.checkState(nodeManager.isPresent());

                for (val nodeGroup : nodeManager.get().listAll()) {
                    nodeGroup.update(copied -> {
                        val nodeBuffer = Lists.newArrayList(copied.getNodeNames());
                        nodeBuffer.removeAll(requiredDeleteNodeNames);
                        copied.setNodeNames(nodeBuffer);
                    });
                }

                if (force) {
                    getTableFlowManager(project).listAll()
                            .forEach(tableFlow -> tableFlow.update(TableFlow::cleanTableData));
                    return ImmutableList.of(triggerProjectClean(project));
                } else {
                    List<TableFlow> tableFlowList = SecondStorageUtil.listTableFlow(getConfig(), project);
                    tableFlowList.forEach(tableFlow -> tableFlow.update(t -> t.removeNodes(requiredDeleteNodeNames)));

                    return Collections.emptyList();
                }
            }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
        } finally {
            if (!isLocked) {
                lockOperate(project, Collections.singletonList(LockTypeEnum.LOAD.name()), LockOperateTypeEnum.UNLOCK.name());
            }
        }
    }

    public void enableProjectSecondStorage(String project, List<String> pairs) {
        Preconditions.checkArgument(new HashSet<>(listAvailablePairs()).containsAll(pairs));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val nodeGroupManager = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
            Preconditions.checkState(nodeGroupManager.isPresent());
            int replicaNum = SecondStorageConfig.getInstanceFromEnv().getReplicaNum();
            for (int i = 0; i < replicaNum; i++) {
                nodeGroupManager.get().makeSureRootEntity("");
            }
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public void addNodeToProject(String project, List<String> pairs) {
        if (CollectionUtils.isEmpty(pairs)) {
            return;
        }
        SecondStorageUtil.validateProjectLock(project, Arrays.asList(LockTypeEnum.LOAD.name()));
        int replicaNum = SecondStorageConfig.getInstanceFromEnv().getReplicaNum();
        Map<Integer, List<String>> replicaNodes = SecondStorageNodeHelper
                .separateReplicaGroup(replicaNum, pairs.toArray(new String[0]));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val internalManager = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
            Preconditions.checkState(internalManager.isPresent());
            val allGroups = internalManager.get().listAll();
            for (Integer idx : replicaNodes.keySet()) {
                allGroups.get(idx).update(copied -> {
                    val nodeBuffer = Lists.newArrayList(copied.getNodeNames());
                    nodeBuffer.addAll(replicaNodes.get(idx));
                    copied.setNodeNames(nodeBuffer);
                });
            }
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public Map<String, Map<String, String>> projectClean(List<String> projects) {
        projects.forEach(project -> {
            aclEvaluate.checkProjectWritePermission(project);
            projectValidate(project);
        });
        Map<String, Map<String, String>> resultMap = new HashMap<>();
        for (String project : projects) {
            resultMap.put(project, triggerProjectSegmentClean(project));
        }
        return resultMap;
    }

    private Map<String, String> triggerProjectSegmentClean(String project) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);

        Map<String, String> resultMap = new HashMap<>();
        for (String model : modelManager.listAllModelIds()) {
            resultMap.put(model, triggerModelSegmentClean(project, model));
        }
        return resultMap;
    }

    private String triggerModelSegmentClean(String project, String model) {
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val segments = dataflowManager.getDataflow(model).getSegments().stream().map(NDataSegment::getId)
                .collect(Collectors.toSet());
        if (!SecondStorageUtil.isModelEnable(project, model) || segments.size() <= 0) {
          return null;
        }
        return triggerSegmentsClean(project, model, segments);
    }

    private void projectValidate(String project) {
        SecondStorageUtil.validateProjectLock(project, Arrays.asList(LockTypeEnum.LOAD.name()));
        // check related jobs
        val models = this.validateProjectDisable(project);
        if (!models.isEmpty()) {
            throw new KylinException(JobErrorCode.SECOND_STORAGE_PROJECT_JOB_EXISTS,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageProjectJobExists(), project));
        }
    }

    public String disableProjectSecondStorage(String project) {
        projectValidate(project);
        val jobId = triggerProjectClean(project);
        SecondStorageUtil.disableProject(project);
        return jobId;
    }

    private String triggerProjectClean(String project) {
        val jobHandler = new SecondStorageProjectCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.projectCleanParam(project, getUsername());
        return getManager(JobManager.class, project).addJob(param, jobHandler);
    }

    private String triggerModelClean(String project, String model) {
        SecondStorageUtil.validateProjectLock(project, Collections.singletonList(LockTypeEnum.LOAD.name()));
        val jobHandler = new SecondStorageModelCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.modelCleanParam(project, model, getUsername());
        return getManager(JobManager.class, project).addJob(param, jobHandler);
    }

    @Transaction(project = 0)
    public String triggerSegmentsClean(String project, String model, Set<String> segIds) {
        SecondStorageUtil.validateProjectLock(project, Arrays.asList(LockTypeEnum.LOAD.name()));
        SecondStorageJobUtil.validateSegment(project, model, Lists.newArrayList(segIds));
        Preconditions.checkState(SecondStorageUtil.isModelEnable(project, model));
        SecondStorageUtil.cleanSegments(project, model, segIds);
        val jobHandler = new SecondStorageSegmentCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.segmentCleanParam(project, model, getUsername(), segIds);
        return getManager(JobManager.class, project).addJob(param, jobHandler);
    }

    @Transaction(project = 0)
    public String triggerIndexClean(String project, String modelId, Set<Long> needDeleteLayoutIds) {
        SecondStorageUtil.validateProjectLock(project, Collections.singletonList(LockTypeEnum.LOAD.name()));
        Preconditions.checkState(SecondStorageUtil.isModelEnable(project, modelId));

        val jobHandler = new SecondStorageIndexCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.layoutCleanParam(project, modelId, getUsername(), needDeleteLayoutIds, Collections.emptySet());
        return getManager(JobManager.class, project).addJob(param, jobHandler);
    }

    public List<ProjectLock> lockList(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> projectInstances = NProjectManager.getInstance(config)
                .listAllProjects().stream()
                .filter(projectInstance -> {
                    if (project == null || projectInstance.getName().equals(project)) return true;
                    return false;
                })
                .collect(Collectors.toList());
        return projectInstances.stream()
                .filter(projectInstance -> {
                    Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(config, projectInstance.getName());
                    if (CollectionUtils.isEmpty(nodeGroupManager.listAll())) return false;
                    return true;
                }).map(projectInstance -> {
                    Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(config, projectInstance.getName());
                    List<String> lockTypes = nodeGroupManager.listAll().get(0).getLockTypes();
                    return new ProjectLock(projectInstance.getName(), lockTypes);
                }).collect(Collectors.toList());
    }

    @Transaction(project = 0)
    public void lockOperate(String project, List<String> lockTypes, String operateType) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) aclEvaluate.checkProjectAdminPermission(project);
        if (!SecondStorageUtil.isProjectEnable(project)) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR, String.format(Locale.ROOT, "'%s' not enable second storage.", project));
        }
        LockTypeEnum.check(lockTypes);
        LockOperateTypeEnum.check(operateType);
        if (LockOperateTypeEnum.LOCK.name().equals(operateType) && !KylinConfig.getInstanceFromEnv().isUTEnv()) {
            JobFilter jobFilter = new JobFilter(Arrays.asList(JobStatusEnum.RUNNING.name()),
                    null, 0, null, null, project, "last_modified", true);
            List<ExecutableResponse> executableResponses = jobService.listJobs(jobFilter);
            executableResponses.stream().forEach(job -> {
                List<ExecutableStepResponse> executableStepResponses = jobService.getJobDetail(project, job.getId());
                executableStepResponses.stream().forEach(step -> {
                    if ((SecondStorageConstants.SKIP_STEP_RUNNING.contains(step.getName()) && step.getStatus() == JobStatusEnum.RUNNING)
                            || SecondStorageConstants.SKIP_JOB_RUNNING.contains(step.getName())) {
                        throw new KylinException(SECOND_STORAGE_PROJECT_LOCK_FAIL,
                                String.format(Locale.ROOT, "project='%s' has job=%s that contains step operating clickhouse, so can not be locked",
                                        project, job.getId()));
                    }
                });
            });
        }

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Optional<Manager<NodeGroup>> optionalNodeGroupManager = SecondStorageUtil.nodeGroupManager(config, project);
        if (!optionalNodeGroupManager.isPresent()) {
            throw new KylinException(SECOND_STORAGE_NODE_NOT_AVAILABLE, String.format(Locale.ROOT, "'%s' second storage node not available.", project));
        }
        Manager<NodeGroup> nodeGroupManager = optionalNodeGroupManager.get();
        List<NodeGroup> nodeGroups = nodeGroupManager.listAll();

        if (LockOperateTypeEnum.LOCK.name().equals(operateType)) {
            for (NodeGroup nodeGroup : nodeGroups) {
                LockTypeEnum.checkLocks(lockTypes, nodeGroup.getLockTypes());
            }
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            nodeGroups.stream().forEach(x -> {
                x.update(y -> {
                    if (LockOperateTypeEnum.LOCK.name().equals(operateType)) {
                        y.setLockTypes(LockTypeEnum.merge(y.getLockTypes(), lockTypes));
                    } else {
                        y.setLockTypes(LockTypeEnum.subtract(y.getLockTypes(), lockTypes));
                    }
                });
            });
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
        // refresh size in clickhouse node
        sizeInNode(project);
    }

    @Transaction(project = 0)
    public ProjectTableSyncResponse tableSync(String project) {
        Properties properties = new Properties();
        properties.put(SecondStorageConstants.PROJECT, project);
        DefaultSecondStorageProperties defaultSecondStorageProperties = new DefaultSecondStorageProperties(properties);

        MetadataOperator metadataOperator = SecondStorageFactoryUtils.createMetadataOperator(defaultSecondStorageProperties);
        TableSyncResponse response = metadataOperator.tableSync();

        sizeInNode(project);

        return new ProjectTableSyncResponse(project, response.getNodes(), response.getDatabase(), response.getTables());
    }

    @Transaction(project = 0)
    public void sizeInNode(String project) {
        SecondStorageUtil.checkSecondStorageData(project);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<TableFlow> tableFlows = SecondStorageUtil.listTableFlow(config, project);
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        NDataflowManager dataflowManager = getManager(NDataflowManager.class, project);
        SecondStorageProjectModelSegment projectModelSegment = new SecondStorageProjectModelSegment();
        Map<String, SecondStorageModelSegment> modelSegmentMap = new HashMap<>();
        for(TableFlow tableFlow : tableFlows) {
            String uuid = tableFlow.getUuid();
            Map<String, SecondStorageSegment> segmentRangeMap = new HashMap<>();
            NDataflow dataflow = dataflowManager.getDataflow(tableFlow.getUuid());
            for (TableData tableData : tableFlow.getTableDataList()) {
                for (TablePartition tablePartition : tableData.getPartitions()) {
                    SegmentRange<Long> segmentRange = dataflow.getSegment(tablePartition.getSegmentId()).getSegRange();
                    segmentRangeMap.put(tablePartition.getSegmentId(),
                            new SecondStorageSegment(tablePartition.getSegmentId(), segmentRange));
                }
            }
            val model = modelManager.getDataModelDesc(tableFlow.getUuid());
            String dateFormat = null;
            if (model.isIncrementBuildOnExpertMode()) {
                dateFormat = model.getPartitionDesc().getPartitionDateFormat();
            }
            SecondStorageModelSegment modelSegment = new SecondStorageModelSegment(tableFlow.getUuid(), dateFormat, segmentRangeMap);
            modelSegmentMap.put(uuid, modelSegment);
        }
        projectModelSegment.setProject(project);
        projectModelSegment.setModelSegmentMap(modelSegmentMap);

        Properties properties = new Properties();
        properties.put(SecondStorageConstants.PROJECT_MODEL_SEGMENT_PARAM, projectModelSegment);

        DefaultSecondStorageProperties defaultSecondStorageProperties = new DefaultSecondStorageProperties(properties);
        MetadataOperator metadataOperator = SecondStorageFactoryUtils.createMetadataOperator(defaultSecondStorageProperties);
        metadataOperator.sizeInNode();
    }

    private Map<String, List<NodeData>> convertNodeGroupToPairs(List<NodeGroup> nodeGroups) {
        return convertNodesToPairs(nodeGroups.stream()
                .flatMap(group -> group.getNodeNames().stream())
                .collect(Collectors.toList()));
    }

    private Map<String, List<NodeData>> convertNodesToPairs(List<String> nodes) {
        Map<String, List<NodeData>> result = Maps.newHashMap();
        nodes.stream().sorted().forEach(node ->
                result.computeIfAbsent(SecondStorageNodeHelper.getPairByNode(node), k -> new ArrayList<>())
                        .add(new NodeData(SecondStorageNodeHelper.getNode(node))));
        return result;
    }

    public List<ProjectNode> projectNodes(String project) {
        List<String> allNodes = SecondStorageNodeHelper.getAllNames();
        List<ProjectNode> projectNodes;
        val config = KylinConfig.getInstanceFromEnv();
        if (StringUtils.isNotBlank(project)) {
            projectNodes = new ArrayList<>();
            Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(config, project);
            List<NodeGroup> nodeGroups = nodeGroupManager.listAll();
            if (CollectionUtils.isEmpty(nodeGroups)) {
                return projectNodes;
            }
            projectNodes.add(new ProjectNode(project, true, convertNodeGroupToPairs(nodeGroups)));
        } else {
            Set<String> projectNodeSet = new HashSet<>();
            List<ProjectInstance> projectInstances = NProjectManager.getInstance(config).listAllProjects().stream()
                    .collect(Collectors.toList());
            projectNodes = projectInstances.stream().map(projectInstance -> {
                Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(config, projectInstance.getName());
                List<NodeGroup> nodeGroups = nodeGroupManager.listAll();
                if (CollectionUtils.isEmpty(nodeGroups)) {
                    return new ProjectNode(projectInstance.getName(), false, Collections.emptyMap());
                }
                nodeGroups.stream().map(NodeGroup::getNodeNames).forEach(projectNodeSet::addAll);
                return new ProjectNode(projectInstance.getName(), true, convertNodeGroupToPairs(nodeGroups));
            }).collect(Collectors.toList());

            List<String> dataList = allNodes.stream()
                    .filter(node -> !projectNodeSet.contains(node))
                    .collect(Collectors.toList());

            projectNodes.add(new ProjectNode(null, false, convertNodesToPairs(dataList)));
        }
        return projectNodes;
    }

    public Map<String, List<NodeData>> listAvailableNodes() {
        val config = KylinConfig.getInstanceFromEnv();
        val usedNodes = NProjectManager.getInstance(config).listAllProjects().stream().flatMap(projectInstance -> {
            Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(config, projectInstance.getName());
            return nodeGroupManager.listAll().stream().flatMap(nodeGroup -> nodeGroup.getNodeNames().stream());
        }).collect(Collectors.toSet());
        List<String> allNodes = SecondStorageNodeHelper.getAllNames().stream()
                .filter(node -> !usedNodes.contains(node)).collect(Collectors.toList());
        return convertNodesToPairs(allNodes);
    }

    public List<String> listAvailablePairs() {
        val config = KylinConfig.getInstanceFromEnv();
        val usedNodes = NProjectManager.getInstance(config).listAllProjects().stream().flatMap(projectInstance -> {
            Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(config, projectInstance.getName());
            return nodeGroupManager.listAll().stream().flatMap(nodeGroup -> nodeGroup.getNodeNames().stream());
        }).collect(Collectors.toSet());
        List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
        return allPairs.stream()
                .filter(pair -> SecondStorageNodeHelper.getPair(pair).stream().noneMatch(node -> usedNodes.contains(node)))
                .collect(Collectors.toList());
    }

    public void enableModelSecondStorage(String project, String modelId) {
        if (isEnabled(project, modelId)) {
            return;
        }
        val indexPlanManager = getManager(NIndexPlanManager.class, project);
        final IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        if (!indexPlan.containBaseTableLayout() && !indexPlan.getModel().getEffectiveDimensions().isEmpty()) {
            indexPlanManager.updateIndexPlan(modelId, copied -> {
                copied.createAndAddBaseIndex(Collections.singletonList(copied.createBaseTableIndex(copied.getModel())));
            });
        }
        SecondStorageUtil.initModelMetaData(project, modelId);
    }

    public List<String> getAllSecondStorageModel(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        return modelManager.listAllModels().stream().filter(model -> SecondStorageUtil.isModelEnable(project, model.getId()))
                .map(NDataModel::getAlias).collect(Collectors.toList());
    }

    public List<String> validateProjectDisable(String project) {
        SecondStorageUtil.validateProjectLock(project, Arrays.asList(LockTypeEnum.LOAD.name()));
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        executableManager.getAllExecutables();
        val allJobs = executableManager.getJobs().stream()
                .map(executableManager::getJob)
                .filter(job -> SecondStorageUtil.RUNNING_STATE.contains(job.getStatus()))
                .filter(job -> SecondStorageUtil.RELATED_JOBS.contains(job.getJobType()))
                .collect(Collectors.toList());
        if (allJobs.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> models = new HashSet<>();
        allJobs.forEach(job -> {
            if (SecondStorageUtil.isModelEnable(job.getProject(), job.getTargetSubject())) {
                models.add(job.getTargetSubject());
            }
        });
        return Lists.newArrayList(models);
    }

    public List<String> getAllSecondStoragrJobs() {
        List<ProjectInstance> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects();
        for (ProjectInstance project : projects) {
            if (SecondStorageUtil.isProjectEnable(project.getName())) {
                List<String> allJobs = getProjectSecondStorageJobs(project.getName());
                if (!allJobs.isEmpty()) {
                    return allJobs;
                }
            }
        }
        return Collections.emptyList();
    }

    public List<String> getProjectSecondStorageJobs(String project) {
        if (!SecondStorageUtil.isProjectEnable(project)) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageProjectEnabled(), project));
        }
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        return executableManager.getJobs().stream()
                .map(executableManager::getJob)
                .filter(job -> SecondStorageUtil.RELATED_JOBS.contains(job.getJobType()))
                .filter(job -> SecondStorageUtil.RUNNING_STATE.contains(job.getStatus()))
                .map(job -> job.getId())
                .collect(Collectors.toList());
    }

    public void isProjectAdmin(String project) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            aclEvaluate.checkProjectAdminPermission(project);
        }
    }

    public void isGlobalAdmin() {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            aclEvaluate.checkIsGlobalAdmin();
        }
    }

    public void resetStorage() {
        isGlobalAdmin();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        val projects = projectManager.listAllProjects();
        projects.forEach(project -> SecondStorageUtil.disableProject(project.getName()));
    }

    public void refreshConf() {
        aclEvaluate.checkIsGlobalAdmin();
        SecondStorage.init(true);
    }

    public void updateNodeStatus(Map<String, Map<String, Boolean>> nodeStatusMap) {
        nodeStatusMap.forEach((pair, nodeStatus) -> {
            nodeStatus.forEach(SecondStorageQueryRouteUtil::setNodeStatus);
        });
    }
}
