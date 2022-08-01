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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CONCURRENT_SUBMIT_LIMIT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_FAIL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_DUPLICATE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_LOCKED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_MERGE_CHECK_INDEX_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_MERGE_CHECK_PARTITION_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_NOT_EXIST_ID;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_REFRESH_SELECT_RANGE_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_STATUS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.spark.job.ExecutableAddCuboidHandler;
import org.apache.kylin.engine.spark.job.ExecutableAddSegmentHandler;
import org.apache.kylin.engine.spark.job.ExecutableMergeOrRefreshHandler;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.engine.spark.utils.ComputedColumnEvalUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableParams;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.SucceedChainedTestExecutable;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutPartition;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataLoadingRange;
import org.apache.kylin.metadata.cube.model.NDataLoadingRangeManager;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.util.ExpandableMeasureUtil;
import org.apache.kylin.metadata.query.QueryTimesResponse;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.query.util.KapQueryUtil;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.config.initialize.ModelBrokenListener;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.PartitionsRefreshRequest;
import org.apache.kylin.rest.request.SegmentTimeRequest;
import org.apache.kylin.rest.response.BuildIndexResponse;
import org.apache.kylin.rest.response.ExistedDataRangeResponse;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.RefreshAffectedSegmentsResponse;
import org.apache.kylin.rest.response.RelatedModelResponse;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.apache.kylin.rest.service.params.IncrementBuildSegmentParams;
import org.apache.kylin.rest.service.params.MergeSegmentParams;
import org.apache.kylin.rest.service.params.RefreshSegmentParams;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.streaming.jobs.StreamingJobListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;

public class ModelServiceBuildTest extends SourceTestCase {
    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final MockModelQueryService modelQueryService = Mockito.spy(new MockModelQueryService());

    @InjectMocks
    private final ModelBuildService modelBuildService = Mockito.spy(new ModelBuildService());

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());

    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());

    @InjectMocks
    private final TableExtService tableExtService = Mockito.spy(new TableExtService());

    @InjectMocks
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(new ProjectService());

    @InjectMocks
    private final SegmentHelper segmentHelper = new SegmentHelper();

    @InjectMocks
    private final FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @Mock
    private final AclTCRServiceSupporter aclTCRService = Mockito.spy(AclTCRServiceSupporter.class);

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private final static String[] timeZones = { "GMT+8", "CST", "PST", "UTC" };

    private StreamingJobListener eventListener = new StreamingJobListener();

    @Before
    public void setup() {
        super.setup();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "expandableMeasureUtil",
                new ExpandableMeasureUtil((model, ccDesc) -> {
                    String ccExpression = KapQueryUtil.massageComputedColumn(model, model.getProject(), ccDesc,
                            AclPermissionUtil.prepareQueryContextACLInfo(model.getProject(),
                                    semanticService.getCurrentUserGroups()));
                    ccDesc.setInnerExpression(ccExpression);
                    ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
                }));
        ReflectionTestUtils.setField(modelService, "projectService", projectService);
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);
        ReflectionTestUtils.setField(tableService, "jobService", jobService);
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "fusionModelService", fusionModelService);
        ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(modelService, "modelBuildService", modelBuildService);
        ReflectionTestUtils.setField(modelBuildService, "modelService", modelService);
        ReflectionTestUtils.setField(modelBuildService, "segmentHelper", segmentHelper);
        ReflectionTestUtils.setField(modelBuildService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);

        modelService.setSemanticUpdater(semanticService);
        modelService.setSegmentHelper(segmentHelper);
        modelService.setIndexPlanService(indexPlanService);
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);

        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
        EventBusFactory.getInstance().register(eventListener, true);
        EventBusFactory.getInstance().register(modelBrokenListener, false);
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().unregister(eventListener);
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testMergeSegment() {
        val dfId = new String("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val dfManager = NDataflowManager.getInstance(getTestConfig(), "default");
        val df = dfManager.getDataflow(dfId);
        // remove exist segment
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);
        Segments<NDataSegment> segments = new Segments<>();

        // first segment
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-02-01");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment1 = dfManager.appendSegment(df, segmentRange);
        dataSegment1.setStatus(SegmentStatusEnum.READY);
        dataSegment1.setSegmentRange(segmentRange);
        segments.add(dataSegment1);

        // second segment
        start = SegmentRange.dateToLong("2010-02-01");
        end = SegmentRange.dateToLong("2010-04-01");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment2 = dfManager.appendSegment(df, segmentRange);
        dataSegment2.setStatus(SegmentStatusEnum.READY);
        dataSegment2.setSegmentRange(segmentRange);
        segments.add(dataSegment2);

        // third segment
        start = SegmentRange.dateToLong("2010-04-01");
        end = SegmentRange.dateToLong("2010-05-01");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment3 = dfManager.appendSegment(df, segmentRange);
        dataSegment3.setStatus(SegmentStatusEnum.READY);
        dataSegment3.setSegmentRange(segmentRange);
        segments.add(dataSegment3);

        update.setToUpdateSegs(segments.toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        try {
            modelBuildService.mergeSegmentsManually(new MergeSegmentParams("default", dfId,
                    new String[] { dataSegment1.getId(), dataSegment3.getId() }));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(
                    "Can't merge the selected segments, as there are gap(s) in between. Please check and try again.",
                    e.getMessage());
        }

        modelBuildService.mergeSegmentsManually(new MergeSegmentParams("default", dfId,
                new String[] { dataSegment1.getId(), dataSegment2.getId(), dataSegment3.getId() }));
        val execManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val executables = getRunningExecutables("default", "741ca86a-1f13-46da-a59f-95fb68615e3a");
        Assert.assertEquals(1, executables.size());
        Assert.assertEquals(JobTypeEnum.INDEX_MERGE, executables.get(0).getJobType());
        AbstractExecutable job = executables.get(0);
        Assert.assertEquals(1, job.getTargetSegments().size());

        val mergedSegment = dfManager.getDataflow(dfId).getSegment(job.getTargetSegments().get(0));
        Assert.assertEquals(SegmentRange.dateToLong("2010-01-01"), mergedSegment.getSegRange().getStart());
        Assert.assertEquals(SegmentRange.dateToLong("2010-05-01"), mergedSegment.getSegRange().getEnd());

        try {
            //refresh exception
            modelBuildService.mergeSegmentsManually(
                    new MergeSegmentParams("default", dfId, new String[] { dataSegment2.getId() }));
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals("Can't remove, refresh or merge segment \"" + dataSegment2.displayIdName()
                    + "\", as it's LOCKED. Please try again later.", e.getMessage());
        }
        // clear segments
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);
    }

    @Test
    public void testMergeLoadingSegments() {
        val dfId = new String("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val dfManager = NDataflowManager.getInstance(getTestConfig(), "default");
        val df = dfManager.getDataflow(dfId);
        // remove exist segment
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        Segments<NDataSegment> segments = new Segments<>();

        // first segment
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-02-01");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment1 = dfManager.appendSegment(df, segmentRange);
        dataSegment1.setStatus(SegmentStatusEnum.NEW);
        dataSegment1.setSegmentRange(segmentRange);
        segments.add(dataSegment1);

        // second segment
        start = SegmentRange.dateToLong("2010-02-01");
        end = SegmentRange.dateToLong("2010-03-01");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment2 = dfManager.appendSegment(df, segmentRange);
        dataSegment2.setStatus(SegmentStatusEnum.READY);
        dataSegment2.setSegmentRange(segmentRange);
        segments.add(dataSegment2);

        update.setToUpdateSegs(segments.toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        thrown.expect(KylinException.class);
        thrown.expectMessage(
                SEGMENT_STATUS.getMsg(dataSegment1.displayIdName(), SegmentStatusEnumToDisplay.LOADING.name()));
        modelBuildService.mergeSegmentsManually(
                new MergeSegmentParams("default", dfId, new String[] { dataSegment1.getId(), dataSegment2.getId() }));

        // clear segments
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);
    }

    @Test
    public void testRefreshSegmentById_SegmentToRefreshIsLocked_Exception() {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        val df = dataflowManager.getDataflow(modelId);
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2010-01-02");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        Segments<NDataSegment> segments = new Segments<>();
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.NEW);
        dataSegment.setSegmentRange(segmentRange);
        segments.add(dataSegment);

        start = SegmentRange.dateToLong("2010-01-02");
        end = SegmentRange.dateToLong("2010-01-03");
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val dataSegment2 = dataflowManager.appendSegment(df, segmentRange);
        dataSegment2.setStatus(SegmentStatusEnum.READY);
        dataSegment2.setSegmentRange(segmentRange);
        segments.add(dataSegment2);
        update = new NDataflowUpdate(df.getUuid());
        update.setToAddOrUpdateLayouts(
                generateAllDataLayout(getProject(), modelId, Arrays.asList(dataSegment, dataSegment2)));

        update.setToUpdateSegs(segments.toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        //refresh normally
        modelBuildService.refreshSegmentById(new RefreshSegmentParams("default", "741ca86a-1f13-46da-a59f-95fb68615e3a",
                new String[] { dataSegment2.getId() }));
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, SEGMENT_LOCKED.getErrorMsg().getLocalizedString(),
                dataSegment2.displayIdName()));
        //refresh exception
        modelBuildService.refreshSegmentById(new RefreshSegmentParams("default", "741ca86a-1f13-46da-a59f-95fb68615e3a",
                new String[] { dataSegment2.getId() }));
    }

    private NDataLayout[] generateAllDataLayout(String project, String modelId, List<NDataSegment> segments) {
        List<NDataLayout> layouts = Lists.newArrayList();
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val df = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        indexManager.getIndexPlan(modelId).getAllLayouts().forEach(layout -> {
            for (NDataSegment segment : segments) {
                layouts.add(NDataLayout.newDataLayout(df, segment.getId(), layout.getId()));
            }
        });
        return layouts.toArray(new NDataLayout[0]);
    }

    @Test
    public void testRefreshSegmentById_isNotExist() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(SEGMENT_NOT_EXIST_ID.getMsg("not_exist_01"));
        //refresh exception
        modelBuildService.refreshSegmentById(new RefreshSegmentParams("default", "741ca86a-1f13-46da-a59f-95fb68615e3a",
                new String[] { "not_exist_01" }));
    }

    @Test
    public void testBuildSegmentsManually_IncrementBuild_ChangePartition() throws Exception {
        for (String timeZone : timeZones) {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
            DateFormat.cleanCache();

            String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
            String project = getProject();
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            modelManager.updateDataModel(modelId, copyForWrite -> {
                copyForWrite.setManagementType(ManagementType.MODEL_BASED);
                copyForWrite.setPartitionDesc(null);
            });
            String pattern = "yyyyMMdd";
            PartitionDesc partitionDesc = new PartitionDesc();
            partitionDesc.setPartitionDateColumn("TEST_KYLIN_FACT.CAL_DT");
            partitionDesc.setPartitionDateFormat(pattern);
            modelBuildService.incrementBuildSegmentsManually(project, modelId, "1577811661000", "1609430400000",
                    partitionDesc, null);
            NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            var dataflow = dataflowManager.getDataflow(modelId);
            Assert.assertEquals(1, dataflow.getSegments().size());
            Assert.assertEquals(DateFormat.getFormatTimeStamp("1577808000000", pattern),
                    dataflow.getSegments().get(0).getSegRange().getStart());
        }
    }

    @Test
    public void testBuildSegmentManually_PartitionValue_Not_Support() throws Exception {
        List<String[]> multiPartitionValues = Lists.newArrayList();
        multiPartitionValues.add(new String[] { "cn" });
        try {
            modelBuildService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "", true,
                    Sets.newHashSet(), multiPartitionValues);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(
                    "Model \"nmodel_basic\" hasn’t set a partition column yet. Please set it first and try again."));
        }
    }

    @Test
    public void testBuildSegmentsManually_NoPartition_Exception() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        modelManager.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
            copyForWrite.setPartitionDesc(null);
        });

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflow = dataflowManager.updateDataflow(dataflowUpdate);
        Assert.assertEquals(0, dataflow.getSegments().size());
        modelBuildService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "", true,
                Sets.newHashSet(), null, 0, false);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertTrue(dataflow.getSegments().get(0).getSegRange().isInfinite());
        val executables = getRunningExecutables("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, executables.size());
        AbstractExecutable job = executables.get(0);
        Assert.assertEquals(0, job.getPriority());
        Assert.assertTrue(((NSparkCubingJob) job).getHandler() instanceof ExecutableAddSegmentHandler);
        thrown.expectInTransaction(KylinException.class);
        thrown.expectMessageInTransaction(
                SEGMENT_STATUS.getMsg(dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments()
                        .get(0).displayIdName(), SegmentStatusEnumToDisplay.LOADING.name()));
        modelBuildService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
    }

    @Test
    @Ignore("Sorry, I don't know")
    public void testBuildSegmentsManually_NoPartition_FullSegExisted() throws Exception {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel modelDesc = modelManager.getDataModelDesc(modelId);
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        val request = new ModelRequest(JsonUtil.deepCopy(modelDesc, NDataModel.class));
        request.setSimplifiedMeasures(modelDesc.getEffectiveMeasures().values().stream()
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.getAllNamedColumns().forEach(c -> c.setName(c.getAliasDotColumn().replace(".", "_")));
        request.setSimplifiedDimensions(request.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toList()));
        request.setComputedColumnDescs(request.getComputedColumnDescs());
        request.setPartitionDesc(null);
        request.setProject(project);
        modelService.updateDataModelSemantic(project, request);
        try {
            modelBuildService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
        } catch (TransactionException exception) {
            Assert.assertTrue(exception.getCause() instanceof KylinException);
            Assert.assertEquals(JOB_CREATE_CHECK_FAIL.getErrorMsg().getLocalizedString(),
                    exception.getCause().getMessage());
        }
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);
    }

    @Test
    @Ignore("will create cube with model")
    public void testBuildSegmentsManuallyException1() throws Exception {
        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "match")
                .getDataModelDesc("match");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setUuid("new_model");
        modelRequest.setAlias("new_model");
        modelRequest.setManagementType(ManagementType.MODEL_BASED);
        modelRequest.setLastModified(0L);
        modelRequest.setProject("match");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Can not build segments, please define table index or aggregate index first!");
        modelService.createModel(modelRequest.getProject(), modelRequest);
        modelBuildService.buildSegmentsManually("match", "new_model", "0", "100");
    }

    @Test
    public void testCreateModelAndBuildManually() throws Exception {
        setupPushdownEnv();
        testGetLatestData();
        testCreateModel_PartitionNotNull();
        testBuildSegmentsManually_WithPushDown();
        testBuildSegmentsManually();
        testChangePartitionDesc();
        testChangePartitionDesc_OriginModelNoPartition();
        testChangePartitionDesc_NewModelNoPartitionColumn();
        cleanPushdownEnv();
    }

    private void testGetLatestData() {
        ExistedDataRangeResponse data = modelService.getLatestDataRange("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null);
        Assert.assertEquals(String.valueOf(Long.MAX_VALUE), data.getEndTime());
    }

    @Test
    public void testDetectViewPartitionDateFormatForbidden() throws Exception {
        setupPushdownEnv();

        final String table = "DEFAULT.TEST_KYLIN_FACT";
        final NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "default");
        final TableDesc tableDesc = tableMgr.getTableDesc(table);
        tableDesc.setTableType(TableDesc.TABLE_TYPE_VIEW);
        tableMgr.updateTableDesc(tableDesc);
        try {
            modelService.getLatestDataRange("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(MsgPicker.getMsg().getViewDateFormatDetectionError(), e.getMessage());
        }
    }

    @Test
    public void testDetectPartitionDateFormatError() throws Exception {
        setupPushdownEnv();
        final NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        modelManager.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.getPartitionDesc().setPartitionDateFormat("yyyyMMdd");
        });
        try {
            modelService.getLatestDataRange("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(MsgPicker.getMsg().getPushdownDatarangeError(), e.getMessage());
        }
    }

    public void testChangePartitionDesc() throws Exception {

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        val modelRequest = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", modelRequest.getPartitionDesc().getPartitionDateColumn());

        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copy -> {
            copy.getPartitionDesc().setPartitionDateColumn("TRANS_ID");
        });

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("TEST_KYLIN_FACT.TRANS_ID", model.getPartitionDesc().getPartitionDateColumn());
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "default").getIndexPlan(model.getUuid());
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), "default").updateIndexPlan(indexPlan.getUuid(),
                    copyForWrite -> {
                        copyForWrite.setIndexes(new ArrayList<>());
                    });
            return 0;
        }, "default");
        modelService.updateDataModelSemantic("default", modelRequest);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("yyyy-MM-dd", model.getPartitionDesc().getPartitionDateFormat());

    }

    public void testChangePartitionDesc_OriginModelNoPartition() throws Exception {

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        val modelRequest = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", modelRequest.getPartitionDesc().getPartitionDateColumn());

        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copy -> {
            copy.setPartitionDesc(null);
        });

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertNull(model.getPartitionDesc());

        modelService.updateDataModelSemantic("default", modelRequest);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("yyyy-MM-dd", model.getPartitionDesc().getPartitionDateFormat());

    }

    public void testChangePartitionDesc_NewModelNoPartitionColumn() throws Exception {

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("default");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.getPartitionDesc().setPartitionDateColumn("");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        val modelRequest = JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("yyyy-MM-dd", model.getPartitionDesc().getPartitionDateFormat());

        modelService.updateDataModelSemantic("default", modelRequest);

        model = modelMgr.getDataModelDescByAlias("nmodel_basic");

        Assert.assertEquals("", model.getPartitionDesc().getPartitionDateFormat());

    }

    public void testCreateModel_PartitionNotNull() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        model.setManagementType(ManagementType.MODEL_BASED);
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        modelRequest.setAlias("new_model");
        modelRequest.setUuid(null);
        modelRequest.setLastModified(0L);
        val newModel = modelService.createModel(modelRequest.getProject(), modelRequest);
        Assert.assertEquals("new_model", newModel.getAlias());
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val df = dfManager.getDataflow(newModel.getUuid());
        Assert.assertEquals(0, df.getSegments().size());

        modelManager.dropModel(newModel);
    }

    public void testBuildSegmentsManually() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        val jobInfo = modelBuildService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "1577811661000", "1609430400000", true, Sets.newHashSet(), null, 0, false);

        Assert.assertEquals(1, jobInfo.getJobs().size());
        Assert.assertEquals(jobInfo.getJobs().get(0).getJobName(), JobTypeEnum.INC_BUILD.name());
        modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals("yyyy-MM-dd", modelDesc.getPartitionDesc().getPartitionDateFormat());

        val executables = getRunningExecutables("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.TABLE_ORIENTED);
        modelManager.updateDataModelDesc(modelUpdate);

        String pattern = "yyyy-MM-dd";
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertEquals(0, executables.get(1).getPriority());
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertEquals(DateFormat.getFormatTimeStamp("1577808000000", pattern),
                dataflow.getSegments().get(0).getSegRange().getStart());
        Assert.assertEquals(DateFormat.getFormatTimeStamp("1609430400000", pattern),
                dataflow.getSegments().get(0).getSegRange().getEnd());

        // multi-partition model
        String multiPartitionModelUuid = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        dataflow = dataflowManager.getDataflow(multiPartitionModelUuid);
        dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        val jobInfo2 = modelBuildService.buildSegmentsManually("default", multiPartitionModelUuid, "1577811661000",
                "1609430400000", true, Sets.newHashSet(), null, 0, true);
        Assert.assertEquals(1, jobInfo2.getJobs().size());
        Assert.assertEquals(jobInfo2.getJobs().get(0).getJobName(), JobTypeEnum.INC_BUILD.name());
        val job2 = NExecutableManager.getInstance(getTestConfig(), "default")
                .getJob(jobInfo2.getJobs().get(0).getJobId());
        Assert.assertEquals(3, job2.getTargetPartitions().size());

    }

    public void testBuildSegmentsManually_WithPushDown() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel modelDesc = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        val minAndMaxTime = PushDownUtil.getMaxAndMinTime(modelUpdate.getPartitionDesc().getPartitionDateColumn(),
                modelUpdate.getRootFactTableName(), "default");
        val dateFormat = DateFormat.proposeDateFormat(minAndMaxTime.getFirst());
        modelBuildService.buildSegmentsManually("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                DateFormat.getFormattedDate(minAndMaxTime.getFirst(), dateFormat),
                DateFormat.getFormattedDate(minAndMaxTime.getSecond(), dateFormat));
        val executables = getRunningExecutables("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, dataflow.getSegments().size());

        java.text.DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault(Locale.Category.FORMAT));
        sdf.setTimeZone(TimeZone.getDefault());

        long t1 = sdf.parse("2012/01/01").getTime();
        long t2 = sdf.parse("2014/01/01").getTime();

        Assert.assertEquals(t1, dataflow.getSegments().get(0).getSegRange().getStart());
        Assert.assertEquals(t2, dataflow.getSegments().get(0).getSegRange().getEnd());
        val result = PushDownUtil.getMaxAndMinTimeWithTimeOut(modelUpdate.getPartitionDesc().getPartitionDateColumn(),
                modelUpdate.getRootFactTableName(), "default");
        Assert.assertNotNull(result);
    }

    @Test
    public void testGetRelatedModels_HasErrorJobs() {
        NExecutableManager executableManager = mock(NExecutableManager.class);
        when(modelService.getManager(NExecutableManager.class, "default")).thenReturn(executableManager);
        when(executableManager.getExecutablesByStatus(ExecutableState.ERROR)).thenReturn(mockJobs());
        List<RelatedModelResponse> responses = modelService.getRelateModels("default", "DEFAULT.TEST_KYLIN_FACT",
                "nmodel_basic_inner");
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals(true, responses.get(0).isHasErrorJobs());
    }

    private List<AbstractExecutable> mockJobs() {
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject("default");
        job1.setName("sparkjob1");
        job1.setTargetSubject("741ca86a-1f13-46da-a59f-95fb68615e3a");
        SucceedChainedTestExecutable job2 = new SucceedChainedTestExecutable();
        job2.setProject("default");
        job2.setName("sparkjob2");
        job2.setTargetSubject("model2");
        SucceedChainedTestExecutable job3 = new SucceedChainedTestExecutable();
        job3.setProject("default");
        job3.setName("sparkjob3");
        job3.setTargetSubject("model3");
        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);
        return jobs;
    }

    @Test
    public void testBuildIndexManually_TableOriented_exception() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t manually build indexes in model \"all_fixed_length\" under the current project settings.");
        modelBuildService.buildIndicesManually(modelId, project, 3, null, null);
    }

    @Test
    public void testBuildIndexManually() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dataflowManager.getDataflow(modelId);
        val dfUpdate = new NDataflowUpdate(df.getId());
        List<NDataLayout> tobeRemoveCuboidLayouts = Lists.newArrayList();
        Segments<NDataSegment> segments = df.getSegments();
        for (NDataSegment segment : segments) {
            tobeRemoveCuboidLayouts.addAll(segment.getLayoutsMap().values());
        }
        dfUpdate.setToRemoveLayouts(tobeRemoveCuboidLayouts.toArray(new NDataLayout[0]));
        dataflowManager.updateDataflow(dfUpdate);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelManager.updateDataModel(modelId,
                copyForWrite -> copyForWrite.setManagementType(ManagementType.MODEL_BASED));
        val response = modelBuildService.buildIndicesManually(modelId, project, 0, null, null);
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NORM_BUILD, response.getType());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(1, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);
        Assert.assertEquals(0, executables.get(0).getPriority());

    }

    @Test
    public void testBuildIndexManuallyWithoutLayout() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelManager.updateDataModel(modelId,
                copyForWrite -> copyForWrite.setManagementType(ManagementType.MODEL_BASED));
        val response = modelBuildService.buildIndicesManually(modelId, project, 3, null, null);
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_LAYOUT, response.getType());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(0, executables.size());
    }

    @Test
    public void testBuildIndexManuallyWithoutSegment() {
        val project = "default";
        val modelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dataflowManager.getDataflow(modelId);
        val dfUpdate = new NDataflowUpdate(df.getId());
        dfUpdate.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(dfUpdate);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelManager.updateDataModel(modelId,
                copyForWrite -> copyForWrite.setManagementType(ManagementType.MODEL_BASED));
        val response = modelBuildService.buildIndicesManually(modelId, project, 3, null, null);
        Assert.assertEquals(BuildIndexResponse.BuildIndexType.NO_SEGMENT, response.getType());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(0, executables.size());

    }

    //test refreshSegments:all Online model, all lag beghind model, One Online One lag behind model
    //first test exception
    @Test
    public void testRefreshSegments_AffectedSegmentRangeChanged_Exception() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t refresh at the moment, as the segment range has changed. Please try again later.");
        RefreshAffectedSegmentsResponse response = new RefreshAffectedSegmentsResponse();
        response.setAffectedStart("12");
        response.setAffectedEnd("120");
        doReturn(response).when(modelService).getRefreshAffectedSegmentsResponse("default", "DEFAULT.TEST_KYLIN_FACT",
                "0", "12223334");
        modelBuildService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "12223334", "0", "12223334");
    }

    @Test
    public void testGetAffectedSegmentsResponse_NoSegments_Exception() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage(SEGMENT_REFRESH_SELECT_RANGE_EMPTY.getMsg());
        List<NDataSegment> segments = modelService.getSegmentsByRange("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94", "default",
                "0", "" + Long.MAX_VALUE);
        Assert.assertTrue(CollectionUtils.isEmpty(segments));

        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        val loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_ENCODING");
        loadingRange.setColumnName("TEST_ENCODING.int_dict");
        loadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(0L, 12223334L));
        loadingRangeMgr.createDataLoadingRange(loadingRange);
        modelBuildService.refreshSegments("default", "DEFAULT.TEST_ENCODING", "0", "12223334", "0", "12223334");
    }

    @Test
    public void testGetAffectedSegmentsResponse_TwoOnlineModelHasNewSegment_Exception() throws IOException {
        prepareTwoOnlineModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        dfMgr.refreshSegment(df, df.getSegments().get(0).getSegRange());

        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getSegmentCanNotRefresh());
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        val loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        loadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));
        loadingRangeMgr.createDataLoadingRange(loadingRange);
        modelBuildService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "12223334", "0", "12223334");
    }

    @Test
    public void testGetAffectedSegmentsResponse_OneLagBehindAndOneOnlineModel_LagBehindHasRefreshingException()
            throws IOException {
        prepareOneLagBehindAndOneOnlineModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");

        val df = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        dfMgr.refreshSegment(df, df.getSegments().get(0).getSegRange());

        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t refresh some segments, as they are being built at the moment. Please try again later.");
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        val loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        loadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));
        loadingRangeMgr.createDataLoadingRange(loadingRange);
        modelBuildService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "12223334", "0", "12223334");
    }

    //now test cases without exception
    @Test
    public void testRefreshSegmentsByDataRange_TwoOnlineModelAndHasReadySegs() throws IOException {
        prepareTwoOnlineModels();
        modelBuildService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "9223372036854775807", "0",
                "9223372036854775807");

        val executables = getRunningExecutables("default", null);

        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
    }

    @Test
    public void testRefreshSegmentsByDataRange_TwoOnlineModelNoExistedSegmentAndFullBuild() throws IOException {
        prepareTwoOnlineModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val df1 = dfMgr.getDataflowByModelAlias("nmodel_basic");
        val df2 = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");

        //purge segments first
        NDataflowUpdate update1 = new NDataflowUpdate(df1.getUuid());
        update1.setToRemoveSegs(df1.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update1);

        NDataflowUpdate update2 = new NDataflowUpdate(df2.getUuid());
        update2.setToRemoveSegs(df2.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update2);

        modelBuildService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "9223372036854775807", "0",
                "9223372036854775807");

        val executables = getRunningExecutables("default", null);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
    }

    @Test
    public void testRefreshSegmentsByDataRange_TwoLagBehindModelAndNoReadySegs() throws IOException {
        prepareTwoLagBehindModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");

        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setToRemoveSegs(df_basic.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update1);
        df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        dfMgr.appendSegment(df_basic, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        NDataflowUpdate update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        update2.setToRemoveSegs(df_basic_inner.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update2);
        df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        val oldSeg = dfMgr.appendSegment(df_basic_inner, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        modelBuildService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "9223372036854775807", "0",
                "9223372036854775807");
        val executables = getRunningExecutables("default", null);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertTrue(((NSparkCubingJob) executables.get(1)).getHandler() instanceof ExecutableAddSegmentHandler);
        df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        Assert.assertNotSame(df_basic_inner.getSegments().get(0).getId(), oldSeg.getId());
    }

    @Test
    public void testRefreshSegmentsByDataRange_TwoLagBehindModelAndHasReadySegs() throws IOException {
        prepareTwoLagBehindModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");

        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setToRemoveSegs(df_basic.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update1);
        df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var firstSegment = dfMgr.appendSegment(df_basic, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        dfMgr.appendSegment(df_basic, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        update1 = new NDataflowUpdate(df_basic.getUuid());
        firstSegment.setStatus(SegmentStatusEnum.READY);
        update1.setToAddOrUpdateLayouts(
                generateAllDataLayout(getProject(), df_basic.getUuid(), Arrays.asList(firstSegment)));
        update1.setToUpdateSegs(firstSegment);
        dfMgr.updateDataflow(update1);

        NDataflowUpdate update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        update2.setToRemoveSegs(df_basic_inner.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update2);
        df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        firstSegment = dfMgr.appendSegment(df_basic_inner, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        dfMgr.appendSegment(df_basic_inner, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        firstSegment.setStatus(SegmentStatusEnum.READY);
        update2.setToUpdateSegs(firstSegment);
        update2.setToAddOrUpdateLayouts(
                generateAllDataLayout(getProject(), df_basic_inner.getUuid(), Arrays.asList(firstSegment)));
        dfMgr.updateDataflow(update2);

        modelBuildService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "20", "0", "20");
        val executables = getRunningExecutables("default", null);
        //refresh 2 ready segs and rebuild two new segs
        Assert.assertEquals(4, executables.size());
        executables.sort(Comparator.comparing(AbstractExecutable::getJobType));
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(1)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(((NSparkCubingJob) executables.get(2)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertTrue(((NSparkCubingJob) executables.get(3)).getHandler() instanceof ExecutableAddSegmentHandler);
    }

    @Test
    public void testRefreshSegmentsByDataRange_OneLagBehindOneOnlineModelAndHasReadySegs() throws IOException {
        prepareOneLagBehindAndOneOnlineModels();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");

        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setToRemoveSegs(df_basic.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update1);
        df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        var firstSegment = dfMgr.appendSegment(df_basic, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        dfMgr.appendSegment(df_basic, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        update1 = new NDataflowUpdate(df_basic.getUuid());
        firstSegment.setStatus(SegmentStatusEnum.READY);
        update1.setToUpdateSegs(firstSegment);
        update1.setToAddOrUpdateLayouts(
                generateAllDataLayout(getProject(), df_basic.getUuid(), Arrays.asList(firstSegment)));
        dfMgr.updateDataflow(update1);

        NDataflowUpdate update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        update2.setToRemoveSegs(df_basic_inner.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update2);
        df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        firstSegment = dfMgr.appendSegment(df_basic_inner, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        val secondSeg = dfMgr.appendSegment(df_basic_inner, new SegmentRange.TimePartitionedSegmentRange(10L, 20L));
        update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        firstSegment.setStatus(SegmentStatusEnum.READY);
        secondSeg.setStatus(SegmentStatusEnum.READY);
        update2.setToUpdateSegs(firstSegment, secondSeg);
        update2.setToAddOrUpdateLayouts(
                generateAllDataLayout(getProject(), df_basic_inner.getUuid(), Arrays.asList(firstSegment, secondSeg)));
        dfMgr.updateDataflow(update2);

        modelBuildService.refreshSegments("default", "DEFAULT.TEST_KYLIN_FACT", "0", "20", "0", "20");

        val executables = getRunningExecutables("default", null);
        executables.sort(Comparator.comparing(AbstractExecutable::getJobType));
        //refresh 2 ready segs in online model and one ready seg in lag behind and rebuild one new seg in lag behind
        Assert.assertEquals(4, executables.size());
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(1)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(2)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(((NSparkCubingJob) executables.get(3)).getHandler() instanceof ExecutableAddSegmentHandler);
    }

    private void prepareTwoOnlineModels() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("82fa7671-a935-45f5-8779-85703601f49a", "default");
            return null;
        }, "default");
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default");
            return null;
        }, "default");
    }

    private void prepareTwoLagBehindModels() {
        //all lag behind
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("82fa7671-a935-45f5-8779-85703601f49a", "default");
            return null;
        }, "default");
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default");
            return null;
        }, "default");
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        val df_basic_inner = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setStatus(RealizationStatusEnum.LAG_BEHIND);
        dfMgr.updateDataflow(update1);

        NDataflowUpdate update2 = new NDataflowUpdate(df_basic_inner.getUuid());
        update2.setStatus(RealizationStatusEnum.LAG_BEHIND);
        dfMgr.updateDataflow(update2);
    }

    private void prepareOneLagBehindAndOneOnlineModels() {
        //one ONLINE one Lag_behind
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("82fa7671-a935-45f5-8779-85703601f49a", "default");
            return null;
        }, "default");
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.dropModel("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", "default");
            return null;
        }, "default");
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df_basic = dfMgr.getDataflowByModelAlias("nmodel_basic");
        NDataflowUpdate update1 = new NDataflowUpdate(df_basic.getUuid());
        update1.setStatus(RealizationStatusEnum.LAG_BEHIND);
        dfMgr.updateDataflow(update1);
    }

    @Test
    public void testBuildMultiPartitionManually() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId1 = "73570f31-05a5-448f-973c-44209830dd01";

        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val dataflow = dataflowManager.getDataflow(modelId);
        val dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowManager.updateDataflow(dataflowUpdate);
        val buildPartitions = Lists.<String[]> newArrayList();
        buildPartitions.add(new String[] { "un" });
        buildPartitions.add(new String[] { "Africa" });
        buildPartitions.add(new String[] { "Austria" });
        val multiPartition1 = modelManager.getDataModelDesc(modelId).getMultiPartitionDesc();
        Assert.assertEquals(3, multiPartition1.getPartitions().size());
        modelBuildService.buildSegmentPartitionByValue(getProject(), modelId, segmentId1, buildPartitions, false, false,
                0, null, null);
        val multiPartition2 = modelManager.getDataModelDesc(modelId).getMultiPartitionDesc();
        // add two new partitions
        Assert.assertEquals(5, multiPartition2.getPartitions().size());
        val jobs1 = getRunningExecutables(getProject(), modelId);
        Assert.assertEquals(1, jobs1.size());

        val segmentId2 = "0db919f3-1359-496c-aab5-b6f3951adc0e";
        modelBuildService.buildSegmentPartitionByValue(getProject(), modelId, segmentId2, buildPartitions, true, false,
                0, null, null);
        val jobs2 = getRunningExecutables(getProject(), modelId);
        Assert.assertEquals(4, jobs2.size());

        val segmentId3 = "d2edf0c5-5eb2-4968-9ad5-09efbf659324";
        try {
            modelBuildService.buildSegmentPartitionByValue(getProject(), modelId, segmentId3, buildPartitions, true,
                    false, 0, null, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(JOB_CREATE_CHECK_MULTI_PARTITION_DUPLICATE.getMsg(), e.getMessage());
            Assert.assertEquals(4, getRunningExecutables(getProject(), modelId).size());
        }

        val segmentId4 = "ff839b0b-2c23-4420-b332-0df70e36c343";
        try {
            overwriteSystemProp("kylin.job.max-concurrent-jobs", "1");
            val buildPartitions2 = Lists.<String[]> newArrayList();
            buildPartitions2.add(new String[] { "ASIA" });
            buildPartitions2.add(new String[] { "EUROPE" });
            buildPartitions2.add(new String[] { "MIDDLE EAST" });
            buildPartitions2.add(new String[] { "AMERICA" });
            buildPartitions2.add(new String[] { "MOROCCO" });
            buildPartitions2.add(new String[] { "INDONESIA" });
            modelBuildService.buildSegmentPartitionByValue(getProject(), modelId, segmentId4, buildPartitions2, true,
                    false, 0, null, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(JOB_CONCURRENT_SUBMIT_LIMIT.getMsg(5), e.getMessage());
            Assert.assertEquals(4, getRunningExecutables(getProject(), modelId).size());
        }

        modelBuildService.buildSegmentPartitionByValue(getProject(), modelId, segmentId4, null, false, true, 0, null,
                null);
        val jobs4 = getRunningExecutables(getProject(), modelId);
        Assert.assertEquals(3, jobs4.get(0).getTargetPartitions().size());
    }

    @Test
    public void testRefreshMultiPartitionById() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val segmentId1 = "0db919f3-1359-496c-aab5-b6f3951adc0e";
        val segmentId2 = "ff839b0b-2c23-4420-b332-0df70e36c343";

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val dataflow = dataflowManager.getDataflow(modelId);
        val dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowManager.updateDataflow(dataflowUpdate);

        // refresh partition by id
        PartitionsRefreshRequest param1 = new PartitionsRefreshRequest(getProject(), segmentId1,
                Sets.newHashSet(7L, 8L), null, null, 0, null, null);
        modelBuildService.refreshSegmentPartition(param1, modelId);

        // refresh partition by value
        val partitionValues = Lists.<String[]> newArrayList(new String[] { "usa" }, new String[] { "un" });
        PartitionsRefreshRequest param2 = new PartitionsRefreshRequest(getProject(), segmentId2, null, partitionValues,
                null, 0, null, null);
        modelBuildService.refreshSegmentPartition(param2, modelId);

        // no target partition id in segment
        PartitionsRefreshRequest param3 = new PartitionsRefreshRequest(getProject(), segmentId1, Sets.newHashSet(99L),
                null, null, 0, null, null);
        try {
            modelBuildService.refreshSegmentPartition(param3, modelId);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(e.getMessage(), JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON.getMsg());
        }

        // no target partition value in segment
        partitionValues.add(new String[] { "nodata" });
        PartitionsRefreshRequest param4 = new PartitionsRefreshRequest(getProject(), segmentId1, null, partitionValues,
                null, 0, null, null);
        try {
            modelBuildService.refreshSegmentPartition(param4, modelId);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(e.getMessage(), JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON.getMsg());
        }

        // no target partition value or partition id
        PartitionsRefreshRequest param5 = new PartitionsRefreshRequest(getProject(), segmentId1, null, null, null, 0,
                null, null);
        try {
            modelBuildService.refreshSegmentPartition(param5, modelId);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(e.getMessage(), JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON.getMsg());
        }
    }

    @Test
    public void testMultiPartitionIndexBuild() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val project = "default";

        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfManager.getDataflow(modelId);
        // remove exist segment
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        // different segments with different partitions and layouts
        List<String> partitionValues = Lists.newArrayList("usa", "cn");
        NDataSegment dataSegment1 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-01-01",
                "2010-02-01", SegmentStatusEnum.READY);
        NDataLayout layout1 = generateLayoutForMultiPartition(modelId, dataSegment1.getId(), partitionValues, 1L);

        List<String> partitionValues2 = Lists.newArrayList("usa");
        NDataSegment dataSegment2 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-02-01",
                "2010-03-01", SegmentStatusEnum.READY);
        NDataLayout layout2 = generateLayoutForMultiPartition(modelId, dataSegment2.getId(), partitionValues2, 1L);
        NDataLayout layout3 = generateLayoutForMultiPartition(modelId, dataSegment2.getId(), partitionValues2, 100001L);

        List<NDataLayout> toAddCuboIds = Lists.newArrayList(layout1, layout2, layout3);
        val segments = Lists.newArrayList(dataSegment1, dataSegment2);
        val update2 = new NDataflowUpdate(df.getUuid());
        update2.setToAddOrUpdateLayouts(toAddCuboIds.toArray(new NDataLayout[] {}));
        update2.setToUpdateSegs(segments.toArray(new NDataSegment[] {}));
        dfManager.updateDataflow(update2);

        modelBuildService.addIndexesToSegments(project, modelId,
                Lists.newArrayList(dataSegment1.getId(), dataSegment2.getId()), Lists.newArrayList(80001L), false,
                ExecutablePO.DEFAULT_PRIORITY);
        val executables = getRunningExecutables(getProject(), modelId);
        val job = executables.get(0);
        Assert.assertEquals(1, executables.size());
        Assert.assertEquals(2, job.getTargetPartitions().size());
        Assert.assertEquals(3, ExecutableParams.getBuckets(job.getParam("buckets")).size());
    }

    @Test
    public void testBuildMultiPartitionSegments() throws Exception {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val project = "default";

        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), project);
        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        val model = dataflow.getModel();
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[dataflow.getSegments().size()]));
        dataflowManager.updateDataflow(dataflowUpdate);
        val buildPartitions = Lists.<String[]> newArrayList();
        buildPartitions.add(new String[] { "usa" });
        buildPartitions.add(new String[] { "Austria" });
        val segmentTimeRequests = Lists.<SegmentTimeRequest> newArrayList();
        segmentTimeRequests.add(new SegmentTimeRequest("1630425600000", "1630512000000"));

        IncrementBuildSegmentParams incrParams = new IncrementBuildSegmentParams(project, modelId, "1633017600000",
                "1633104000000", model.getPartitionDesc(), model.getMultiPartitionDesc(), segmentTimeRequests, true,
                buildPartitions);
        val jobInfo = modelBuildService.incrementBuildSegmentsManually(incrParams);

        Assert.assertEquals(2, jobInfo.getJobs().size());
        Assert.assertEquals(jobInfo.getJobs().get(0).getJobName(), JobTypeEnum.INC_BUILD.name());
        val executables = getRunningExecutables(project, modelId);
        Assert.assertEquals(2, executables.size());
        val job = executableManager.getJob(jobInfo.getJobs().get(0).getJobId());
        Assert.assertEquals(3, job.getTargetPartitions().size());
        Set<JobBucket> buckets = ExecutableParams.getBuckets(job.getParam(NBatchConstants.P_BUCKETS));
        Assert.assertEquals(45, buckets.size());
        NDataSegment segment = dataflowManager.getDataflow(modelId).getSegment(job.getTargetSegments().get(0));
        Assert.assertEquals(44, segment.getMaxBucketId());

        // build all partition values
        IncrementBuildSegmentParams incrParams2 = new IncrementBuildSegmentParams(project, modelId, "1633104000000",
                "1633190400000", model.getPartitionDesc(), model.getMultiPartitionDesc(), null, true, null)
                        .withBuildAllSubPartitions(true);
        val jobInfo2 = modelBuildService.incrementBuildSegmentsManually(incrParams2);
        Assert.assertEquals(1, jobInfo2.getJobs().size());
        Assert.assertEquals(jobInfo2.getJobs().get(0).getJobName(), JobTypeEnum.INC_BUILD.name());
        val job2 = executableManager.getJob(jobInfo2.getJobs().get(0).getJobId());
        Assert.assertEquals(4, job2.getTargetPartitions().size()); // usa,un,cn,Austria

        // change multi partition desc will clean all segments
        IncrementBuildSegmentParams incrParams3 = new IncrementBuildSegmentParams(project, modelId, "1633017600000",
                "1633104000000", model.getPartitionDesc(), null, null, true, null);
        val jobInfo3 = modelBuildService.incrementBuildSegmentsManually(incrParams3);
        val newModel = dataflowManager.getDataflow(modelId).getModel();
        Assert.assertEquals(1, jobInfo3.getJobs().size());
        Assert.assertFalse(newModel.isMultiPartitionModel());
    }

    @Test
    public void testRefreshMultiPartitionSegments() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val project = "default";
        val segmentId = "0db919f3-1359-496c-aab5-b6f3951adc0e";
        val refreshSegmentParams = new RefreshSegmentParams(project, modelId, new String[] { segmentId });
        modelBuildService.refreshSegmentById(refreshSegmentParams);

        val jobs = getRunningExecutables(getProject(), modelId);
        val job = jobs.get(0);
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(2, job.getTargetPartitions().size());
        Set<JobBucket> buckets = ExecutableParams.getBuckets(job.getParam(NBatchConstants.P_BUCKETS));
        Assert.assertEquals(30, buckets.size());

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val dataflow = dataflowManager.getDataflow(modelId);
        val segment = dataflow.getSegment(job.getTargetSegments().get(0));
        segment.getMultiPartitions().forEach(partition -> {
            Assert.assertEquals(PartitionStatusEnum.REFRESH, partition.getStatus());
        });
    }

    @Test
    public void testMergeMultiPartitionSegments() {
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        val project = "default";

        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfManager.getDataflow(modelId);

        // remove exist segment
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        // first segment
        List<String> partitionValues = Lists.newArrayList("usa", "cn");
        NDataSegment dataSegment1 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-01-01",
                "2010-02-01", SegmentStatusEnum.READY);
        NDataLayout layout1 = generateLayoutForMultiPartition(modelId, dataSegment1.getId(), partitionValues, 1L);

        NDataSegment dataSegment2 = generateSegmentForMultiPartition(modelId, partitionValues, "2010-02-01",
                "2010-03-01", SegmentStatusEnum.READY);
        NDataLayout layout2 = generateLayoutForMultiPartition(modelId, dataSegment2.getId(), partitionValues, 1L);

        List<String> partitionValues2 = Lists.newArrayList("usa");
        NDataSegment dataSegment3 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-03-01",
                "2010-04-01", SegmentStatusEnum.READY);
        NDataLayout layout3 = generateLayoutForMultiPartition(modelId, dataSegment3.getId(), partitionValues2, 1L);

        NDataSegment dataSegment4 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-04-01",
                "2010-05-01", SegmentStatusEnum.READY);
        NDataLayout layout4_1 = generateLayoutForMultiPartition(modelId, dataSegment4.getId(), partitionValues2, 1L);
        NDataLayout layout4_2 = generateLayoutForMultiPartition(modelId, dataSegment4.getId(), partitionValues2,
                10001L);

        NDataSegment dataSegment5 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-05-01",
                "2010-06-01", SegmentStatusEnum.READY);
        NDataSegment dataSegment6 = generateSegmentForMultiPartition(modelId, partitionValues2, "2010-06-01",
                "2010-07-01", SegmentStatusEnum.READY);

        List<NDataLayout> toAddCuboIds = Lists.newArrayList(layout1, layout2, layout3, layout4_1, layout4_2);
        val segments = Lists.newArrayList(dataSegment1, dataSegment2, dataSegment3, dataSegment4, dataSegment5,
                dataSegment6);

        val update2 = new NDataflowUpdate(df.getUuid());
        update2.setToAddOrUpdateLayouts(toAddCuboIds.toArray(new NDataLayout[] {}));
        update2.setToUpdateSegs(segments.toArray(new NDataSegment[] {}));
        dfManager.updateDataflow(update2);

        // empty layout in segment4
        try {
            modelBuildService.mergeSegmentsManually(new MergeSegmentParams(project, modelId,
                    new String[] { dataSegment5.getId(), dataSegment6.getId() }));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(SEGMENT_MERGE_CHECK_INDEX_ILLEGAL.getMsg(), e.getMessage());
        }

        // index is not aligned in segment3, segment4
        try {
            modelBuildService.mergeSegmentsManually(new MergeSegmentParams(project, modelId,
                    new String[] { dataSegment3.getId(), dataSegment4.getId() }));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(SEGMENT_MERGE_CHECK_INDEX_ILLEGAL.getMsg(), e.getMessage());
        }

        // partitions are not aligned in segment2, segment3
        try {
            modelBuildService.mergeSegmentsManually(new MergeSegmentParams(project, modelId,
                    new String[] { dataSegment2.getId(), dataSegment3.getId() }));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(SEGMENT_MERGE_CHECK_PARTITION_ILLEGAL.getMsg(), e.getMessage());
        }

        // success
        modelBuildService.mergeSegmentsManually(
                new MergeSegmentParams(project, modelId, new String[] { dataSegment1.getId(), dataSegment2.getId() }));
    }

    private NDataSegment generateSegmentForMultiPartition(String modelId, List<String> partitionValues, String start,
            String end, SegmentStatusEnum status) {
        val dfm = NDataflowManager.getInstance(getTestConfig(), getProject());
        val partitions = Lists.<String[]> newArrayList();
        partitionValues.forEach(value -> {
            partitions.add(new String[] { value });
        });
        long startTime = SegmentRange.dateToLong(start);
        long endTime = SegmentRange.dateToLong(end);
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(startTime, endTime);
        val df = dfm.getDataflow(modelId);
        val newSegment = dfm.appendSegment(df, segmentRange, status, partitions);
        newSegment.getMultiPartitions().forEach(partition -> {
            partition.setStatus(PartitionStatusEnum.READY);
        });
        return newSegment;
    }

    private NDataLayout generateLayoutForMultiPartition(String modelId, String segmentId, List<String> partitionValues,
            long layoutId) {
        val dfm = NDataflowManager.getInstance(getTestConfig(), getProject());
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());

        val model = modelManager.getDataModelDesc(modelId);
        val df = dfm.getDataflow(modelId);
        val partitions = Lists.<String[]> newArrayList();
        partitionValues.forEach(value -> {
            partitions.add(new String[] { value });
        });
        val partitionIds = model.getMultiPartitionDesc().getPartitionIdsByValues(partitions);
        NDataLayout layout = NDataLayout.newDataLayout(df, segmentId, layoutId);
        partitionIds.forEach(id -> {
            layout.getMultiPartition().add(new LayoutPartition(id));
        });
        return layout;
    }

    @Test
    public void testDeleteSegmentById_cleanIndexPlanToBeDeleted() {
        String modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        String project = "default";
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NDataModel dataModel = dataModelManager.getDataModelDesc(modelId);
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NIndexPlanManager.getInstance(getTestConfig(), project).updateIndexPlan(modelId, copyForWrite -> {
            val toBeDeletedSet = copyForWrite.getIndexes().stream().map(IndexEntity::getLayouts).flatMap(List::stream)
                    .filter(layoutEntity -> 1000001L == layoutEntity.getId()).collect(Collectors.toSet());
            copyForWrite.markIndexesToBeDeleted(modelId, toBeDeletedSet);
        });
        Assert.assertTrue(CollectionUtils.isNotEmpty(
                NIndexPlanManager.getInstance(getTestConfig(), project).getIndexPlan(modelId).getToBeDeletedIndexes()));
        val df1 = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df1.getStatus());
        modelService.deleteSegmentById(modelId, project, new String[] { "ef783e4d-e35f-4bd9-8afd-efd64336f04d" },
                false);
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), project).getIndexPlan(modelId);

        Assert.assertTrue(CollectionUtils.isEmpty(dataflow.getSegments()));
        Assert.assertTrue(CollectionUtils.isEmpty(indexPlan.getAllToBeDeleteLayoutId()));
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, dataflow.getStatus());
    }

    @Test
    public void testDeleteSegmentById_UnconsecutiveSegmentsToDelete_Exception() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel dataModel = dataModelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NDataModel modelUpdate = dataModelManager.copyForWrite(dataModel);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflow df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        Segments<NDataSegment> segments = new Segments<>();

        long start;
        long end;
        for (int i = 0; i <= 6; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            segments.add(dataSegment);
        }
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        //remove normally
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { segments.get(0).getId() }, false);
        //2 dataflows
        val df2 = dataflowManager.getDataflow(dataModel.getUuid());
        modelService.deleteSegmentById("741ca86a-1f13-46da-a59f-95fb68615e3a", "default",
                new String[] { segments.get(2).getId(), segments.get(3).getId() }, false);
        Assert.assertEquals(6, df2.getSegments().size());
    }

    @Test
    public void testGetCubes0ExistBrokenModel() {
        tableService.unloadTable(getProject(), "DEFAULT.TEST_KYLIN_FACT", false);
        val result = modelService.getCubes0(null, getProject());
        Assert.assertEquals(8, result.size());

        boolean notBrokenModel = result.stream()
                .filter(model -> "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94".equals(model.getUuid()))
                .allMatch(NDataModelResponse::isModelBroken);
        Assert.assertFalse(notBrokenModel);

        boolean brokenModel = result.stream()
                .filter(model -> "82fa7671-a935-45f5-8779-85703601f49a".equals(model.getUuid()))
                .allMatch(NDataModelResponse::isModelBroken);
        Assert.assertTrue(brokenModel);

        int joinTablesSize = result.stream()
                .filter(model -> "cb596712-3a09-46f8-aea1-988b43fe9b6c".equals(model.getUuid())).findFirst().get()
                .getOldParams().getJoinTables().size();
        Assert.assertEquals(1, joinTablesSize);
    }

    @Test
    public void testProposeDateFormat() {
        Assert.assertThrows(KylinException.class, () -> DateFormat.proposeDateFormat("not_exits"));
    }

}
