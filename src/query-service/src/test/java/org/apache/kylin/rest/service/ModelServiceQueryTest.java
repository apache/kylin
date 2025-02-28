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

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.engine.spark.utils.ComputedColumnEvalUtil;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.util.ExpandableMeasureUtil;
import org.apache.kylin.metadata.query.QueryTimesResponse;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.config.initialize.ModelBrokenListener;
import org.apache.kylin.rest.constant.ModelAttributeEnum;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.FusionModelResponse;
import org.apache.kylin.rest.response.NDataModelLiteResponse;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.service.params.ModelQueryParams;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.ModelTriple;
import org.apache.kylin.streaming.jobs.StreamingJobListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class ModelServiceQueryTest extends SourceTestCase {
    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @InjectMocks
    private final FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());

    @InjectMocks
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);
    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();
    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();
    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    private StreamingJobListener eventListener = new StreamingJobListener();

    protected String getProject() {
        return "default";
    }

    @Before
    public void setUp() {
        super.setUp();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "expandableMeasureUtil",
                new ExpandableMeasureUtil((model, ccDesc) -> {
                    String ccExpression = PushDownUtil.massageComputedColumn(model, model.getProject(), ccDesc,
                            AclPermissionUtil.createAclInfo(model.getProject(),
                                    semanticService.getCurrentUserGroups()));
                    ccDesc.setInnerExpression(ccExpression);
                    ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
                }));
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "fusionModelService", fusionModelService);

        modelService.setSemanticUpdater(semanticService);
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

        SparkJobFactoryUtils.initJobFactory();
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
    public void testQueryModels() {
        String project = "streaming_test";
        ModelQueryParams request = new ModelQueryParams(null, null, true, project, "ADMIN", Lists.newArrayList(), "", 0,
                8, "last_modify", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, false);
        val modelList = modelService.getModels(request);
        Assert.assertEquals(11, modelList.getTotalSize());
        Assert.assertEquals(8, modelList.getValue().size());

        ModelQueryParams requestSortByRecCountReverse = new ModelQueryParams(null, null, true, project, "ADMIN",
                Lists.newArrayList(), "", 0, 8, "recommendations_count", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, false);
        val modelListSortByRecCountReverse = modelService.getModels(requestSortByRecCountReverse);
        Assert.assertEquals("fusion_model", modelListSortByRecCountReverse.getValue().get(0).getAlias());

        ModelQueryParams requestSortByRecCount = new ModelQueryParams(null, null, true, project, "ADMIN",
                Lists.newArrayList(), "", 0, 8, "recommendations_count", false, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, false);
        val modelListSortByRecCount = modelService.getModels(requestSortByRecCount);
        Assert.assertEquals("fusion_model", modelListSortByRecCount.getValue().get(0).getAlias());

        // test model list is empty
        ModelQueryParams requestSortByRecCount2 = new ModelQueryParams(null, "not_exist", true, project, "ADMIN",
                Lists.newArrayList(), "", 0, 8, "recommendations_count", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, false);
        val modelListSortByRecCount2 = modelService.getModels(requestSortByRecCount2);
        Assert.assertEquals(0, modelListSortByRecCount2.getTotalSize());

        ModelQueryParams liteRequest = new ModelQueryParams(null, null, true, project, "ADMIN", Lists.newArrayList(),
                "", 0, 8, "last_modify", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, true);
        val modelListWithLite = modelService.getModels(liteRequest);
        Assert.assertEquals(11, modelListWithLite.getTotalSize());
        Assert.assertEquals(8, modelListWithLite.getValue().size());

        ModelQueryParams request1 = new ModelQueryParams(null, null, true, project, "ADMIN", Lists.newArrayList(), "",
                1, 10, "usage", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, false);
        val modelList1 = modelService.getModels(request1);
        Assert.assertEquals(11, modelList1.getTotalSize());
        Assert.assertEquals(1, modelList1.getValue().size());

        val modelResponse = modelList1.getValue().get(0);
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);

        val triple = new ModelTriple(dfMgr.getDataflow(modelResponse.getUuid()), modelResponse);
        Assert.assertTrue(triple.getLeft() instanceof NDataflow);
        Assert.assertTrue(triple.getMiddle() instanceof NDataModel);
        Assert.assertNull(triple.getRight());

        ModelQueryParams request2 = new ModelQueryParams(null, null, true, project, "ADMIN", Lists.newArrayList(), "",
                1, 5, "storage", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, false);
        val modelList2 = modelService.getModels(request2);
        Assert.assertEquals(11, modelList2.getTotalSize());
        Assert.assertEquals(5, modelList2.getValue().size());
        Assert.assertTrue(((NDataModelResponse) modelList2.getValue().get(0))
                .getStorage() >= ((NDataModelResponse) modelList2.getValue().get(4)).getStorage());

        ModelQueryParams request3 = new ModelQueryParams(null, null, true, project, "ADMIN", Lists.newArrayList(), "",
                1, 10, "expansionrate", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, false, false);
        val modelList3 = modelService.getModels(request3);
        Assert.assertEquals(1, modelList3.getValue().size());

        ModelQueryParams request4 = new ModelQueryParams(null, null, true, project, "ADMIN", Lists.newArrayList(), "",
                0, 10, "expansionrate", false, "ADMIN",
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID),
                974198646000L, System.currentTimeMillis(), true, false);
        val modelList4 = modelService.getModels(request4);
        Assert.assertEquals(10, modelList4.getValue().size());

        ModelQueryParams request5 = new ModelQueryParams(null, null, true, project, "ADMIN", Lists.newArrayList(), "",
                1, 6, "", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, false, false);
        val modelList5 = modelService.getModels(request5);
        Assert.assertEquals(5, modelList5.getValue().size());
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        ModelQueryParams request6 = new ModelQueryParams(null, null, true, project, "ADMIN", Lists.newArrayList(), "",
                1, 6, "", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, false, false);
        val modelList6 = modelService.getModels(request6);
        Assert.assertEquals(5, modelList6.getValue().size());

        // test model list and sortBy is empty when kylin.metadata.semi-automatic-mode=true
        ModelQueryParams requestSortByEmpty = new ModelQueryParams(null, "not_exist", true, project, "ADMIN",
                Lists.newArrayList(), "", 0, 8, null, false, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, false);
        val modelListSortByEmpty = modelService.getModels(requestSortByEmpty);
        Assert.assertEquals(0, modelListSortByEmpty.getTotalSize());

        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");

        // used for getModels without sortBy field
        ModelQueryParams request7 = new ModelQueryParams(null, null, true, project, "ADMIN", Lists.newArrayList(), "",
                0, 6, "", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, false, false);
        val modelList7 = modelService.getModels(request7);
        Assert.assertEquals(6, modelList7.getValue().size());
    }

    @Test
    public void testConvertToDataModelResponseBroken() {
        List<ModelAttributeEnum> modelAttributeSet = Lists.newArrayList(ModelAttributeEnum.BATCH);
        ModelQueryParams modelQueryParams = new ModelQueryParams("", null, true, "default", null, null, "", 0, 10, "",
                true, null, modelAttributeSet, null, null, true, false);

        val tripleList = modelQueryService.getModels(modelQueryParams);
        ModelTriple modelTriple = tripleList.get(0);
        NDataModel dataModel = modelTriple.getDataModel();

        NDataModelResponse nDataModelResponse = modelService.convertToDataModelResponseBroken(dataModel);
        Assert.assertEquals(ModelStatusToDisplayEnum.BROKEN, nDataModelResponse.getStatus());
    }

    @Test
    public void testGetFusionModel() {
        String project = "streaming_test";
        String modelName = "streaming_test";
        List<NDataModelResponse> models = modelService.getModels(modelName, project, false, null, Lists.newArrayList(),
                null, false, null, null, null, true);

        FusionModelResponse model = (FusionModelResponse) models.get(0);
        Assert.assertEquals(0, model.getAvailableIndexesCount());
        Assert.assertEquals(3, model.getTotalIndexes());
        Assert.assertEquals(5, model.getStreamingIndexes());
        Assert.assertEquals(10, model.getUsage());
        Assert.assertEquals(0, model.getStorage());
        Assert.assertEquals(0, model.getSource());

        String modelName1 = "AUTO_MODEL_P_LINEORDER_1";
        NDataModelResponse model1 = modelService
                .getModels(modelName1, project, false, null, Lists.newArrayList(), null, false, null, null, null, true)
                .get(0);
        Assert.assertEquals(0, model1.getAvailableIndexesCount());
        Assert.assertEquals(1, model1.getTotalIndexes());
        Assert.assertEquals(0, model1.getStorage());
        Assert.assertEquals(0, model1.getSource());

        String modelName2 = "model_streaming";
        ModelQueryParams liteRequest = new ModelQueryParams("4965c827-fbb4-4ea1-a744-3f341a3b030d", modelName2, true,
                project, "ADMIN", Lists.newArrayList(), "", 0, 10, "last_modify", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, true);

        DataResult<List<NDataModel>> modelResult2 = modelService.getModels(liteRequest);
        List<NDataModel> models2 = modelResult2.getValue();
        NDataModelLiteResponse model2 = (NDataModelLiteResponse) models2.get(0);

        Assert.assertEquals(12010, model2.getOldParams().getInputRecordCnt());
        Assert.assertEquals(1505415, model2.getOldParams().getInputRecordSizeBytes());
        Assert.assertEquals(396, model2.getOldParams().getSizeKB());

        ModelQueryParams request = new ModelQueryParams("4965c827-fbb4-4ea1-a744-3f341a3b030d", modelName2, true,
                project, "ADMIN", Lists.newArrayList(), "", 0, 10, "last_modify", true, null,
                Arrays.asList(ModelAttributeEnum.BATCH, ModelAttributeEnum.STREAMING, ModelAttributeEnum.HYBRID), null,
                null, true, false);
        DataResult<List<NDataModel>> modelResult3 = modelService.getModels(request);
        List<NDataModel> models3 = modelResult3.getValue();
        FusionModelResponse model3 = (FusionModelResponse) models3.get(0);

        Assert.assertEquals(12010, model3.getOldParams().getInputRecordCnt());
        Assert.assertEquals(1505415, model3.getOldParams().getInputRecordSizeBytes());
        Assert.assertEquals(396, model3.getOldParams().getSizeKB());
    }
}
