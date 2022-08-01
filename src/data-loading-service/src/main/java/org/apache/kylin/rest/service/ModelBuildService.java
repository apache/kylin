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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARTITION_COLUMN;
import static org.apache.kylin.common.exception.ServerErrorCode.PARTITION_VALUE_NOT_SUPPORT;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CONCURRENT_SUBMIT_LIMIT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_DUPLICATE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.SegmentPartition;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.util.MultiPartitionUtil;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.PartitionsRefreshRequest;
import org.apache.kylin.rest.request.SegmentTimeRequest;
import org.apache.kylin.rest.response.BuildIndexResponse;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.response.JobInfoResponseWithFailure;
import org.apache.kylin.rest.response.RefreshAffectedSegmentsResponse;
import org.apache.kylin.rest.service.params.BasicSegmentParams;
import org.apache.kylin.rest.service.params.FullBuildSegmentParams;
import org.apache.kylin.rest.service.params.IncrementBuildSegmentParams;
import org.apache.kylin.rest.service.params.MergeSegmentParams;
import org.apache.kylin.rest.service.params.RefreshSegmentParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;

@Component("modelBuildService")
public class ModelBuildService extends BasicService implements ModelBuildSupporter {

    @Autowired
    private ModelService modelService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private SegmentHelper segmentHelper;

    //only fo test
    public JobInfoResponse buildSegmentsManually(String project, String modelId, String start, String end)
            throws Exception {
        return buildSegmentsManually(project, modelId, start, end, true, Sets.newHashSet(), null);
    }

    public JobInfoResponse buildSegmentsManually(String project, String modelId, String start, String end,
            boolean needBuild, Set<String> ignoredSnapshotTables, List<String[]> multiPartitionValues)
            throws Exception {
        return buildSegmentsManually(project, modelId, start, end, needBuild, ignoredSnapshotTables,
                multiPartitionValues, ExecutablePO.DEFAULT_PRIORITY, false);
    }

    public JobInfoResponse buildSegmentsManually(String project, String modelId, String start, String end,
            boolean needBuild, Set<String> ignoredSnapshotTables, List<String[]> multiPartitionValues, int priority,
            boolean buildAllSubPartitions) throws Exception {
        return buildSegmentsManually(project, modelId, start, end, needBuild, ignoredSnapshotTables,
                multiPartitionValues, priority, buildAllSubPartitions, null, false, null, null);
    }

    public JobInfoResponse buildSegmentsManually(String project, String modelId, String start, String end,
            boolean needBuild, Set<String> ignoredSnapshotTables, List<String[]> multiPartitionValues, int priority,
            boolean buildAllSubPartitions, List<Long> batchIndexIds, boolean partialBuild, String yarnQueue, Object tag)
            throws Exception {
        NDataModel modelDesc = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (!modelDesc.isMultiPartitionModel() && !CollectionUtils.isEmpty(multiPartitionValues)) {
            throw new KylinException(PARTITION_VALUE_NOT_SUPPORT, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getPartitionValueNotSupport(), modelDesc.getAlias()));
        }
        if (PartitionDesc.isEmptyPartitionDesc(modelDesc.getPartitionDesc())) {
            return fullBuildSegmentsManually(new FullBuildSegmentParams(project, modelId, needBuild)
                    .withIgnoredSnapshotTables(ignoredSnapshotTables).withPriority(priority)
                    .withPartialBuild(partialBuild).withBatchIndexIds(batchIndexIds).withYarnQueue(yarnQueue)
                    .withTag(tag));
        } else {
            return incrementBuildSegmentsManually(
                    new IncrementBuildSegmentParams(project, modelId, start, end, modelDesc.getPartitionDesc(),
                            modelDesc.getMultiPartitionDesc(), Lists.newArrayList(), needBuild, multiPartitionValues)
                                    .withIgnoredSnapshotTables(ignoredSnapshotTables).withPriority(priority)
                                    .withBuildAllSubPartitions(buildAllSubPartitions).withPartialBuild(partialBuild)
                                    .withBatchIndexIds(batchIndexIds).withYarnQueue(yarnQueue).withTag(tag));
        }
    }

    public JobInfoResponse fullBuildSegmentsManually(FullBuildSegmentParams params) {
        aclEvaluate.checkProjectOperationPermission(params.getProject());
        modelService.checkModelPermission(params.getProject(), params.getModelId());
        List<JobInfoResponse.JobInfo> jobIds = EnhancedUnitOfWork
                .doInTransactionWithCheckAndRetry(() -> constructFullBuild(params), params.getProject());
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(jobIds);
        return jobInfoResponse;
    }

    private List<JobInfoResponse.JobInfo> constructFullBuild(FullBuildSegmentParams params) {
        modelService.checkModelAndIndexManually(params);
        String project = params.getProject();
        String modelId = params.getModelId();
        boolean needBuild = params.isNeedBuild();

        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (model.getPartitionDesc() != null
                && !StringUtils.isEmpty(model.getPartitionDesc().getPartitionDateColumn())) {
            //increment build model
            throw new IllegalArgumentException(MsgPicker.getMsg().getCanNotBuildSegment());

        }
        val dataflowManager = getManager(NDataflowManager.class, project);
        val df = dataflowManager.getDataflow(modelId);
        val seg = df.getFirstSegment();
        if (Objects.isNull(seg)) {
            NDataSegment newSegment = dataflowManager.appendSegment(df,
                    SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                    needBuild ? SegmentStatusEnum.NEW : SegmentStatusEnum.READY);
            if (!needBuild) {
                return new LinkedList<>();
            }
            JobParam jobParam = new JobParam(newSegment, modelId, getUsername())
                    .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables()).withPriority(params.getPriority())
                    .withYarnQueue(params.getYarnQueue()).withTag(params.getTag());
            addJobParamExtParams(jobParam, params);
            return Lists.newArrayList(new JobInfoResponse.JobInfo(JobTypeEnum.INC_BUILD.toString(),
                    getManager(SourceUsageManager.class).licenseCheckWrap(project,
                            () -> getManager(JobManager.class, project).addSegmentJob(jobParam))));
        }
        if (!needBuild) {
            return new LinkedList<>();
        }
        List<JobInfoResponse.JobInfo> res = Lists.newArrayListWithCapacity(2);

        RefreshSegmentParams refreshSegmentParams = new RefreshSegmentParams(project, modelId,
                Lists.newArrayList(
                        getManager(NDataflowManager.class, project).getDataflow(modelId).getSegments().get(0).getId())
                        .toArray(new String[0]),
                true).withIgnoredSnapshotTables(params.getIgnoredSnapshotTables()) //
                        .withPriority(params.getPriority()) //
                        .withPartialBuild(params.isPartialBuild()) //
                        .withBatchIndexIds(params.getBatchIndexIds()).withYarnQueue(params.getYarnQueue())
                        .withTag(params.getTag());
        res.addAll(refreshSegmentById(refreshSegmentParams));
        return res;
    }

    private void addJobParamExtParams(JobParam jobParam, BasicSegmentParams params) {
        if (params.isPartialBuild()) {
            jobParam.addExtParams(NBatchConstants.P_PARTIAL_BUILD, String.valueOf(params.isPartialBuild()));
        }
        if (CollectionUtils.isNotEmpty(params.getBatchIndexIds())) {
            jobParam.setTargetLayouts(Sets.newHashSet(params.getBatchIndexIds()));
        }
    }

    @Transaction(project = 0)
    public List<JobInfoResponse.JobInfo> refreshSegmentById(RefreshSegmentParams params) {

        aclEvaluate.checkProjectOperationPermission(params.getProject());
        modelService.checkSegmentsExistById(params.getModelId(), params.getProject(), params.getSegmentIds());
        modelService.checkSegmentsStatus(params.getModelId(), params.getProject(), params.getSegmentIds(),
                SegmentStatusEnumToDisplay.LOADING, SegmentStatusEnumToDisplay.REFRESHING,
                SegmentStatusEnumToDisplay.MERGING, SegmentStatusEnumToDisplay.LOCKED);

        List<JobInfoResponse.JobInfo> jobIds = new ArrayList<>();
        NDataflowManager dfMgr = getManager(NDataflowManager.class, params.getProject());
        val jobManager = getManager(JobManager.class, params.getProject());
        IndexPlan indexPlan = modelService.getIndexPlan(params.getModelId(), params.getProject());
        NDataflow df = dfMgr.getDataflow(indexPlan.getUuid());

        for (String id : params.getSegmentIds()) {
            NDataSegment segment = df.getSegment(id);
            if (segment == null) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, MsgPicker.getMsg().getSegNotFound(), id, df.getModelAlias()));
            }

            NDataSegment newSeg = dfMgr.refreshSegment(df, segment.getSegRange());

            JobParam jobParam = new JobParam(newSeg, params.getModelId(), getUsername())
                    .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables()) //
                    .withPriority(params.getPriority()).withYarnQueue(params.getYarnQueue()).withTag(params.getTag());
            addJobParamExtParams(jobParam, params);
            String jobId = getManager(SourceUsageManager.class).licenseCheckWrap(params.getProject(),
                    () -> jobManager.refreshSegmentJob(jobParam, params.isRefreshAllLayouts()));

            jobIds.add(new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_REFRESH.toString(), jobId));
        }
        return jobIds;
    }

    //only for test
    public JobInfoResponse incrementBuildSegmentsManually(String project, String modelId, String start, String end,
            PartitionDesc partitionDesc, List<SegmentTimeRequest> segmentHoles) throws Exception {
        return incrementBuildSegmentsManually(new IncrementBuildSegmentParams(project, modelId, start, end,
                partitionDesc, null, segmentHoles, true, null));
    }

    @Override
    public JobInfoResponse incrementBuildSegmentsManually(IncrementBuildSegmentParams params) throws Exception {
        String project = params.getProject();
        aclEvaluate.checkProjectOperationPermission(project);
        modelService.checkModelPermission(project, params.getModelId());
        val modelManager = getManager(NDataModelManager.class, project);
        if (PartitionDesc.isEmptyPartitionDesc(params.getPartitionDesc())) {
            throw new KylinException(EMPTY_PARTITION_COLUMN, "Partition column is null.'");
        }

        String startFormat = DateFormat
                .getFormatTimeStamp(params.getStart(), params.getPartitionDesc().getPartitionDateFormat()).toString();
        String endFormat = DateFormat
                .getFormatTimeStamp(params.getEnd(), params.getPartitionDesc().getPartitionDateFormat()).toString();

        NDataModel copyModel = modelManager.copyForWrite(modelManager.getDataModelDesc(params.getModelId()));
        copyModel.setPartitionDesc(params.getPartitionDesc());

        if (params.getPartitionDesc() != null
                && !KylinConfig.getInstanceFromEnv().isUseBigIntAsTimestampForPartitionColumn()) {
            PartitionDesc partitionDesc = params.getPartitionDesc();
            partitionDesc.init(copyModel);
            if (!partitionDesc.checkIntTypeDateFormat()) {
                throw new KylinException(JobErrorCode.JOB_INT_DATE_FORMAT_NOT_MATCH_ERROR,
                        "int/bigint data type only support yyyymm/yyyymmdd format");
            }
        }

        copyModel.init(modelManager.getConfig(), project, getCCRelatedModels(project));
        String format = modelService.probeDateFormatIfNotExist(project, copyModel);

        List<JobInfoResponse.JobInfo> jobIds = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            IncrementBuildSegmentParams buildSegmentParams = new IncrementBuildSegmentParams(project,
                    params.getModelId(), startFormat, endFormat, params.getPartitionDesc(),
                    params.getMultiPartitionDesc(), format, params.getSegmentHoles(), params.isNeedBuild(),
                    params.getMultiPartitionValues()) //
                            .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables())
                            .withPriority(params.getPriority())
                            .withBuildAllSubPartitions(params.isBuildAllSubPartitions()) //
                            .withPartialBuild(params.isPartialBuild()) //
                            .withBatchIndexIds(params.getBatchIndexIds()).withYarnQueue(params.getYarnQueue())
                            .withTag(params.getTag());
            return innerIncrementBuild(buildSegmentParams);
        }, project);
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(jobIds);
        return jobInfoResponse;
    }

    private List<JobInfoResponse.JobInfo> innerIncrementBuild(IncrementBuildSegmentParams params) throws IOException {
        modelService.checkModelAndIndexManually(params);
        if (CollectionUtils.isEmpty(params.getSegmentHoles())) {
            params.setSegmentHoles(Lists.newArrayList());
        }
        NDataModel modelDesc = getManager(NDataModelManager.class, params.getProject())
                .getDataModelDesc(params.getModelId());
        if (PartitionDesc.isEmptyPartitionDesc(modelDesc.getPartitionDesc())
                || !modelDesc.getPartitionDesc().equals(params.getPartitionDesc()) || !ModelSemanticHelper
                        .isMultiPartitionDescSame(modelDesc.getMultiPartitionDesc(), params.getMultiPartitionDesc())) {
            aclEvaluate.checkProjectWritePermission(params.getProject());
            val request = modelService.convertToRequest(modelDesc);
            request.setPartitionDesc(params.getPartitionDesc());
            request.setProject(params.getProject());
            request.setMultiPartitionDesc(params.getMultiPartitionDesc());
            modelService.updateDataModelSemantic(params.getProject(), request);
            modelService.updateSecondStorageModel(params.getProject(), request.getId());
            params.getSegmentHoles().clear();
        }
        List<JobInfoResponse.JobInfo> res = Lists.newArrayListWithCapacity(params.getSegmentHoles().size() + 2);
        List<String[]> allPartitions = null;
        if (modelDesc.isMultiPartitionModel()) {
            allPartitions = modelDesc.getMultiPartitionDesc().getPartitions().stream()
                    .map(MultiPartitionDesc.PartitionInfo::getValues).collect(Collectors.toList());
        }
        for (SegmentTimeRequest hole : params.getSegmentHoles()) {
            res.add(constructIncrementBuild(new IncrementBuildSegmentParams(params.getProject(), params.getModelId(),
                    hole.getStart(), hole.getEnd(), params.getPartitionColFormat(), true, allPartitions)
                            .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables())
                            .withPriority(params.getPriority())
                            .withBuildAllSubPartitions(params.isBuildAllSubPartitions()) //
                            .withPartialBuild(params.isPartialBuild()) //
                            .withBatchIndexIds(params.getBatchIndexIds()).withYarnQueue(params.getYarnQueue())
                            .withTag(params.getTag())));
        }
        res.add(constructIncrementBuild(new IncrementBuildSegmentParams(params.getProject(), params.getModelId(),
                params.getStart(), params.getEnd(), params.getPartitionColFormat(), params.isNeedBuild(),
                params.getMultiPartitionValues()) //
                        .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables()) //
                        .withPriority(params.getPriority()) //
                        .withBuildAllSubPartitions(params.isBuildAllSubPartitions()) //
                        .withPartialBuild(params.isPartialBuild()) //
                        .withBatchIndexIds(params.getBatchIndexIds()).withYarnQueue(params.getYarnQueue())
                        .withTag(params.getTag())));
        return res;
    }

    @Override
    public JobInfoResponse.JobInfo constructIncrementBuild(IncrementBuildSegmentParams params) {
        String project = params.getProject();
        String modelId = params.getModelId();

        NDataModel modelDescInTransaction = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        JobManager jobManager = getManager(JobManager.class, project);
        TableDesc table = getManager(NTableMetadataManager.class, project)
                .getTableDesc(modelDescInTransaction.getRootFactTableName());
        val df = getManager(NDataflowManager.class, project).getDataflow(modelId);
        if (modelDescInTransaction.getPartitionDesc() == null
                || StringUtils.isEmpty(modelDescInTransaction.getPartitionDesc().getPartitionDateColumn())) {
            throw new IllegalArgumentException("Can not add a new segment on full build model.");
        }
        Preconditions.checkArgument(!PushDownUtil.needPushdown(params.getStart(), params.getEnd()),
                "Load data must set start and end date");
        val segmentRangeToBuild = SourceFactory.getSource(table).getSegmentRange(params.getStart(), params.getEnd());
        modelService.checkSegmentToBuildOverlapsBuilt(project, modelId, segmentRangeToBuild);
        modelService.saveDateFormatIfNotExist(project, modelId, params.getPartitionColFormat());
        checkMultiPartitionBuildParam(modelDescInTransaction, params);
        NDataSegment newSegment = getManager(NDataflowManager.class, project).appendSegment(df, segmentRangeToBuild,
                params.isNeedBuild() ? SegmentStatusEnum.NEW : SegmentStatusEnum.READY,
                params.getMultiPartitionValues());
        if (!params.isNeedBuild()) {
            return null;
        }
        // TODO
        JobParam jobParam = new JobParam(newSegment, modelId, getUsername())
                .withIgnoredSnapshotTables(params.getIgnoredSnapshotTables()).withPriority(params.getPriority())
                .withYarnQueue(params.getYarnQueue()).withTag(params.getTag());
        addJobParamExtParams(jobParam, params);
        if (modelDescInTransaction.isMultiPartitionModel()) {
            val model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
            jobParam.setTargetPartitions(
                    model.getMultiPartitionDesc().getPartitionIdsByValues(params.getMultiPartitionValues()));
        }
        return new JobInfoResponse.JobInfo(JobTypeEnum.INC_BUILD.toString(), getManager(SourceUsageManager.class)
                .licenseCheckWrap(project, () -> jobManager.addSegmentJob(jobParam)));
    }

    public void checkMultiPartitionBuildParam(NDataModel model, IncrementBuildSegmentParams params) {
        if (!model.isMultiPartitionModel()) {
            return;
        }
        if (params.isNeedBuild() && CollectionUtils.isEmpty(params.getMultiPartitionValues())) {
            throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY);
        }
        if (!params.isNeedBuild() && !CollectionUtils.isEmpty(params.getMultiPartitionValues())) {
            throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON);
        }
        for (String[] values : params.getMultiPartitionValues()) {
            if (values.length != model.getMultiPartitionDesc().getColumns().size()) {
                throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON);
            }
        }
    }

    @Transaction(project = 1)
    public BuildIndexResponse buildIndicesManually(String modelId, String project, int priority, String yarnQueue,
            Object tag) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModel modelDesc = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (ManagementType.MODEL_BASED != modelDesc.getManagementType()) {
            throw new KylinException(PERMISSION_DENIED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getCanNotBuildIndicesManually(), modelDesc.getAlias()));
        }

        NDataflow df = getManager(NDataflowManager.class, project).getDataflow(modelId);
        val segments = df.getSegments();
        if (segments.isEmpty()) {
            return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_SEGMENT);
        }

        String jobId = getManager(SourceUsageManager.class).licenseCheckWrap(project,
                () -> getManager(JobManager.class, project).addIndexJob(new JobParam(modelId, getUsername())
                        .withPriority(priority).withYarnQueue(yarnQueue).withTag(tag)));

        return new BuildIndexResponse(StringUtils.isBlank(jobId) ? BuildIndexResponse.BuildIndexType.NO_LAYOUT
                : BuildIndexResponse.BuildIndexType.NORM_BUILD, jobId);
    }

    @Transaction(project = 0)
    public JobInfoResponse buildSegmentPartitionByValue(String project, String modelId, String segmentId,
            List<String[]> partitionValues, boolean parallelBuild, boolean buildAllPartitions, int priority,
            String yarnQueue, Object tag) {
        aclEvaluate.checkProjectOperationPermission(project);
        modelService.checkModelPermission(project, modelId);
        modelService.checkSegmentsExistById(modelId, project, new String[] { segmentId });
        modelService.checkModelIsMLP(modelId, project);
        val dfm = getManager(NDataflowManager.class, project);
        val df = dfm.getDataflow(modelId);
        val segment = df.getSegment(segmentId);
        val duplicatePartitions = segment.findDuplicatePartitions(partitionValues);
        if (!duplicatePartitions.isEmpty()) {
            throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_DUPLICATE);
        }
        if (buildAllPartitions) {
            List<Long> oldPartitionIds = segment.getMultiPartitions().stream().map(SegmentPartition::getPartitionId)
                    .collect(Collectors.toList());
            NDataModel model = modelService.getModelById(modelId, project);
            List<String[]> oldPartitions = model.getMultiPartitionDesc().getPartitionValuesById(oldPartitionIds);
            List<String[]> allPartitions = model.getMultiPartitionDesc().getPartitions().stream()
                    .map(MultiPartitionDesc.PartitionInfo::getValues).collect(Collectors.toList());
            List<String[]> diffPartitions = MultiPartitionUtil.findDiffValues(allPartitions, oldPartitions);
            if (partitionValues == null) {
                partitionValues = Lists.newArrayList();
            }
            partitionValues.addAll(diffPartitions);
        }
        dfm.appendPartitions(df.getId(), segment.getId(), partitionValues);
        Set<Long> targetPartitions = getManager(NDataModelManager.class, project).getDataModelDesc(modelId)
                .getMultiPartitionDesc().getPartitionIdsByValues(partitionValues);
        return parallelBuildPartition(parallelBuild, project, modelId, segmentId, targetPartitions, priority, yarnQueue,
                tag);
    }

    private JobInfoResponse parallelBuildPartition(boolean parallelBuild, String project, String modelId,
            String segmentId, Set<Long> partitionIds, int priority, String yarnQueue, Object tag) {
        val jobIds = Lists.<String> newArrayList();
        if (parallelBuild) {
            checkConcurrentSubmit(partitionIds.size());
            partitionIds.forEach(partitionId -> {
                val jobParam = new JobParam(Sets.newHashSet(segmentId), null, modelId, getUsername(),
                        Sets.newHashSet(partitionId), null).withPriority(priority).withYarnQueue(yarnQueue)
                                .withTag(tag);
                val jobId = getManager(SourceUsageManager.class).licenseCheckWrap(project,
                        () -> getManager(JobManager.class, project).buildPartitionJob(jobParam));
                jobIds.add(jobId);
            });
        } else {
            val jobParam = new JobParam(Sets.newHashSet(segmentId), null, modelId, getUsername(), partitionIds, null)
                    .withPriority(priority).withYarnQueue(yarnQueue).withTag(tag);
            val jobId = getManager(SourceUsageManager.class).licenseCheckWrap(project,
                    () -> getManager(JobManager.class, project).buildPartitionJob(jobParam));
            jobIds.add(jobId);
        }
        return JobInfoResponse.of(jobIds, JobTypeEnum.SUB_PARTITION_BUILD.toString());
    }

    private void checkConcurrentSubmit(int partitionSize) {
        int runningJobLimit = getConfig().getMaxConcurrentJobLimit();
        int submitJobLimit = runningJobLimit * 5;
        if (partitionSize > submitJobLimit) {
            throw new KylinException(JOB_CONCURRENT_SUBMIT_LIMIT, submitJobLimit);
        }
    }

    @Override
    @Transaction(project = 0)
    public void refreshSegments(String project, String table, String refreshStart, String refreshEnd,
            String affectedStart, String affectedEnd) throws IOException {
        aclEvaluate.checkProjectOperationPermission(project);
        RefreshAffectedSegmentsResponse response = modelService.getRefreshAffectedSegmentsResponse(project, table,
                refreshStart, refreshEnd);
        if (!response.getAffectedStart().equals(affectedStart) || !response.getAffectedEnd().equals(affectedEnd)) {
            throw new KylinException(PERMISSION_DENIED,
                    MsgPicker.getMsg().getSegmentCanNotRefreshBySegmentChange());
        }
        TableDesc tableDesc = getManager(NTableMetadataManager.class, project).getTableDesc(table);
        SegmentRange segmentRange = SourceFactory.getSource(tableDesc).getSegmentRange(refreshStart, refreshEnd);
        segmentHelper.refreshRelatedModelSegments(project, table, segmentRange);
    }

    @Transaction(project = 0)
    public JobInfoResponse refreshSegmentPartition(PartitionsRefreshRequest param, String modelId) {
        val project = param.getProject();
        modelService.checkSegmentsExistById(modelId, project, new String[] { param.getSegmentId() });
        modelService.checkModelIsMLP(modelId, project);
        val dfm = getManager(NDataflowManager.class, project);
        val df = dfm.getDataflow(modelId);
        val segment = df.getSegment(param.getSegmentId());
        var partitions = param.getPartitionIds();
        aclEvaluate.checkProjectOperationPermission(project);
        modelService.checkModelPermission(project, modelId);

        if (CollectionUtils.isEmpty(param.getPartitionIds())) {
            partitions = modelService.getModelById(modelId, project).getMultiPartitionDesc()
                    .getPartitionIdsByValues(param.getSubPartitionValues());
            if (partitions.isEmpty() || partitions.size() != param.getSubPartitionValues().size()) {
                throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON);
            }
        }

        val oldPartitions = segment.getMultiPartitions().stream().map(SegmentPartition::getPartitionId)
                .collect(Collectors.toSet());
        if (!Sets.difference(partitions, oldPartitions).isEmpty()) {
            throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON);
        }
        val jobManager = getManager(JobManager.class, project);
        JobParam jobParam = new JobParam(Sets.newHashSet(segment.getId()), null, modelId, getUsername(), partitions,
                null).withIgnoredSnapshotTables(param.getIgnoredSnapshotTables()).withPriority(param.getPriority())
                        .withYarnQueue(param.getYarnQueue()).withTag(param.getTag());

        val jobId = getManager(SourceUsageManager.class).licenseCheckWrap(project,
                () -> jobManager.refreshSegmentJob(jobParam));
        return JobInfoResponse.of(Lists.newArrayList(jobId), JobTypeEnum.SUB_PARTITION_REFRESH.toString());
    }

    @Transaction(project = 0)
    public JobInfoResponse.JobInfo mergeSegmentsManually(MergeSegmentParams params) {
        val startAndEnd = modelService.checkMergeSegments(params);

        String project = params.getProject();
        String modelId = params.getModelId();

        val dfManager = getManager(NDataflowManager.class, project);
        val jobManager = getManager(JobManager.class, project);
        val indexPlan = modelService.getIndexPlan(modelId, project);
        val df = dfManager.getDataflow(indexPlan.getUuid());

        NDataSegment mergeSeg = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).mergeSegments(
                df, new SegmentRange.TimePartitionedSegmentRange(startAndEnd.getFirst(), startAndEnd.getSecond()),
                true);

        String jobId = getManager(SourceUsageManager.class).licenseCheckWrap(project,
                () -> jobManager.mergeSegmentJob(
                        new JobParam(mergeSeg, modelId, getUsername()).withPriority(params.getPriority())
                                .withYarnQueue(params.getYarnQueue()).withTag(params.getTag())));

        return new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_MERGE.toString(), jobId);
    }

    public JobInfoResponseWithFailure addIndexesToSegments(String project, String modelId, List<String> segmentIds,
            List<Long> indexIds, boolean parallelBuildBySegment, int priority) {
        return addIndexesToSegments(project, modelId, segmentIds, indexIds, parallelBuildBySegment, priority, false,
                null, null);
    }

    @Override
    @Transaction(project = 0)
    public JobInfoResponseWithFailure addIndexesToSegments(String project, String modelId, List<String> segmentIds,
            List<Long> indexIds, boolean parallelBuildBySegment, int priority, boolean partialBuild, String yarnQueue,
            Object tag) {
        aclEvaluate.checkProjectOperationPermission(project);
        modelService.checkModelPermission(project, modelId);
        val dfManger = getManager(NDataflowManager.class, project);
        NDataflow dataflow = dfManger.getDataflow(modelId);
        modelService.checkSegmentsExistById(modelId, project, segmentIds.toArray(new String[0]));
        if (parallelBuildBySegment) {
            return addIndexesToSegmentsParallelly(project, modelId, segmentIds, indexIds, dataflow, priority, yarnQueue,
                    tag);
        } else {
            val jobManager = getManager(JobManager.class, project);
            JobInfoResponseWithFailure result = new JobInfoResponseWithFailure();
            List<JobInfoResponse.JobInfo> jobs = new LinkedList<>();
            try {
                Set<Long> targetLayouts = indexIds == null ? null : Sets.newHashSet(indexIds);
                JobParam jobParam = new JobParam(Sets.newHashSet(segmentIds), targetLayouts, modelId, getUsername())
                        .withPriority(priority).withYarnQueue(yarnQueue).withTag(tag);
                if (partialBuild) {
                    jobParam.addExtParams(NBatchConstants.P_PARTIAL_BUILD, String.valueOf(true));
                }
                JobInfoResponse.JobInfo jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_BUILD.toString(),
                        getManager(SourceUsageManager.class).licenseCheckWrap(project,
                                () -> jobManager.addRelatedIndexJob(jobParam)));
                jobs.add(jobInfo);
            } catch (JobSubmissionException e) {
                result.addFailedSeg(dataflow, e);
            }
            result.setJobs(jobs);
            return result;
        }
    }

    private JobInfoResponseWithFailure addIndexesToSegmentsParallelly(String project, String modelId,
            List<String> segmentIds, List<Long> indexIds, NDataflow dataflow, int priority, String yarnQueue,
            Object tag) {
        JobInfoResponseWithFailure result = new JobInfoResponseWithFailure();
        List<JobInfoResponse.JobInfo> jobs = new LinkedList<>();
        val jobManager = getManager(JobManager.class, project);
        for (String segmentId : segmentIds) {
            try {
                JobInfoResponse.JobInfo jobInfo = new JobInfoResponse.JobInfo(JobTypeEnum.INDEX_BUILD.toString(),
                        getManager(SourceUsageManager.class).licenseCheckWrap(project,
                                () -> jobManager.addRelatedIndexJob(new JobParam(Sets.newHashSet(segmentId),
                                        indexIds == null ? null : new HashSet<>(indexIds), modelId, getUsername())
                                                .withPriority(priority).withYarnQueue(yarnQueue).withTag(tag))));
                jobs.add(jobInfo);
            } catch (JobSubmissionException e) {
                result.addFailedSeg(dataflow, e);
            }
        }
        result.setJobs(jobs);
        return result;
    }

}
