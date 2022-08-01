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

import static org.mockito.Mockito.doNothing;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.config.initialize.ModelUpdateListener;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.OwnerChangeRequest;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;

public class FusionModelServiceTest extends SourceTestCase {

    @InjectMocks
    private FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private MockModelQueryService modelQueryService = Mockito.spy(new MockModelQueryService());

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Autowired
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    private final ModelUpdateListener modelUpdateListener = new ModelUpdateListener();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        super.setup();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        EventBusFactory.getInstance().register(modelUpdateListener, true);
        ReflectionTestUtils.setField(fusionModelService, "modelService", modelService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateDataModelSemantic() throws Exception {
        String modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        String batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        var model = modelMgr.getDataModelDesc(modelId);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject("streaming_test");
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.getPartitionDesc().setPartitionDateColumn("P_LINEORDER_STREAMING.LO_SHIPMODE");
        List<NDataModel.NamedColumn> dimensions = request.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        dimensions.removeIf(column -> column.getAliasDotColumn().equalsIgnoreCase("P_LINEORDER_STREAMING.LO_PARTKEY"));
        request.setSimplifiedDimensions(dimensions);
        doNothing().when(modelService).validateFusionModelDimension(Mockito.any());
        fusionModelService.updateDataModelSemantic("streaming_test", request);

        model = modelMgr.getDataModelDesc(modelId);
        var batchModel = modelMgr.getDataModelDesc(batchId);
        Assert.assertEquals("P_LINEORDER_STREAMING.LO_SHIPMODE", model.getPartitionDesc().getPartitionDateColumn());
        Assert.assertEquals(5, batchModel.getEffectiveDimensions().size());
    }

    @Test
    public void testDropFusionModel() throws Exception {
        String modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        String batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";
        UnitOfWork.doInTransactionWithRetry(() -> {
            fusionModelService.dropModel(modelId, "streaming_test");
            return null;
        }, "streaming_test");
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        var model = modelMgr.getDataModelDesc(modelId);
        var batchModel = modelMgr.getDataModelDesc(batchId);
        Assert.assertNull(model);
        Assert.assertNull(batchModel);
        val fusionMgr = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        Assert.assertNull(fusionMgr.getFusionModel(modelId));
    }

    @Test
    public void testRenameFusionModelName() {
        String modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        String batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";
        String project = "streaming_test";
        String newModelName = "new_streaming";
        fusionModelService.renameDataModel(project, modelId, newModelName);
        Assert.assertEquals(newModelName,
                NDataModelManager.getInstance(getTestConfig(), project).getDataModelDesc(modelId).getAlias());
        Assert.assertEquals(FusionModel.getBatchName(newModelName, modelId),
                NDataModelManager.getInstance(getTestConfig(), project).getDataModelDesc(batchId).getAlias());
    }

    @Test
    public void testUpdateModelOwner() throws IOException {
        String modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        String batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";
        String project = "streaming_test";
        String newOwner = "test";

        Set<String> projectManagementUsers1 = Sets.newHashSet();
        projectManagementUsers1.add(newOwner);
        Mockito.doReturn(projectManagementUsers1).when(accessService).getProjectManagementUsers(project);

        OwnerChangeRequest request = new OwnerChangeRequest();
        request.setProject(project);
        request.setOwner(newOwner);
        fusionModelService.updateModelOwner(project, modelId, request);
        Assert.assertEquals(newOwner,
                NDataModelManager.getInstance(getTestConfig(), project).getDataModelDesc(modelId).getOwner());
        Assert.assertEquals(newOwner,
                NDataModelManager.getInstance(getTestConfig(), project).getDataModelDesc(batchId).getOwner());
    }

    @Test
    public void testDropStreamingTableWithModel() throws Exception {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "streaming_test");

        tableMgr.removeSourceTable("DEFAULT.SSB_TOPIC");
        List<NDataModelResponse> models = modelService.getModels("stream_merge1", "streaming_test", true, "", null, "",
                false);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(ModelStatusToDisplayEnum.BROKEN, models.get(0).getStatus());
        fusionModelService.dropModel("e78a89dd-847f-4574-8afa-8768b4228b73", "streaming_test");
        models = modelService.getModels("stream_merge1", "streaming_test", true, "", null, "", false);
        Assert.assertEquals(0, models.size());
        Set<IRealization> realizations = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getRealizationsByTable("streaming_test", "DEFAULT.SSB_TOPIC");
        Assert.assertEquals(0, realizations.size());
    }

    @Test
    public void testDropHybridTableWithModel() throws Exception {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "streaming_test");

        tableMgr.removeSourceTable("SSB.P_LINEORDER_STREAMING");
        List<NDataModelResponse> models = modelService.getModels("streaming_test", "streaming_test", true, "", null, "",
                false);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(ModelStatusToDisplayEnum.BROKEN, models.get(0).getStatus());
        fusionModelService.dropModel("b05034a8-c037-416b-aa26-9e6b4a41ee40", "streaming_test");
        models = modelService.getModels(" streaming_test", "streaming_test", true, "", null, "", false);
        Assert.assertEquals(0, models.size());
        Set<IRealization> realizations = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getRealizationsByTable("streaming_test", "SSB.P_LINEORDER_STREAMING");
        Assert.assertEquals(0, realizations.size());
    }

    @Test
    public void testDropHiveTableWithModel() throws Exception {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "streaming_test");
        NDataflowManager dataflowMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");

        val dataflow = dataflowMgr.getDataflow("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        Assert.assertTrue(dataflow.isStreaming());

        tableMgr.removeSourceTable("SSB.LINEORDER_HIVE");
        List<NDataModelResponse> models = modelService.getModels("model_streaming", "streaming_test", true, "", null,
                "", false);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(ModelStatusToDisplayEnum.BROKEN, models.get(0).getStatus());
        fusionModelService.dropModel("4965c827-fbb4-4ea1-a744-3f341a3b030d", "streaming_test");
        models = modelService.getModels("model_streaming", "streaming_test", true, "", null, "", false);
        Assert.assertEquals(0, models.size());
    }

    @Test
    public void testGetModelTypeWithTable() throws Exception {
        List<NDataModelResponse> models = modelService.getModels("batch", "streaming_test", true, "", null, "", false);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(NDataModel.ModelType.BATCH, models.get(0).getModelType());
    }

    @Test
    public void testConvertSegmentIdWithName_ByName() {
        val fusionId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        // check streaming segment of fusion model
        Pair pair = fusionModelService.convertSegmentIdWithName(fusionId, "streaming_test", null,
                new String[] { "1622186700000_1622186700000" });
        String[] originSegIds = { "3e560d22-b749-48c3-9f64-d4230207f120" };
        Assert.assertEquals(fusionId, pair.getFirst());
        Assert.assertTrue(ArrayUtils.isEquals(pair.getSecond(), originSegIds));

        // check batch segment of fusion model
        pair = fusionModelService.convertSegmentIdWithName(fusionId, "streaming_test", null,
                new String[] { "20200518111100_20210118111100" });
        String[] originBatchSegIds = { "86b5daaa-e295-4e8c-b877-f97bda69bee5" };
        Assert.assertEquals("cd2b9a23-699c-4699-b0dd-38c9412b3dfd", pair.getFirst());
        Assert.assertTrue(ArrayUtils.isEquals(pair.getSecond(), originBatchSegIds));

        // check segment of streaming model
        val streamingModelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        pair = fusionModelService.convertSegmentIdWithName(streamingModelId, "streaming_test", null,
                new String[] { "1613957110000_1613957120000" });
        String[] streamingSegIds = { "c380dd2a-43b8-4268-b73d-2a5f76236631" };
        Assert.assertEquals(streamingModelId, pair.getFirst());
        Assert.assertTrue(ArrayUtils.isEquals(pair.getSecond(), streamingSegIds));

        // check segment of batch model
        val batchModelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        pair = fusionModelService.convertSegmentIdWithName(batchModelId, "default", null,
                new String[] { "FULL_BUILD" });
        String[] batchSegIds = { "ef5e0663-feba-4ed2-b71c-21958122bbff" };
        Assert.assertEquals(batchModelId, pair.getFirst());
        Assert.assertTrue(ArrayUtils.isEquals(pair.getSecond(), batchSegIds));
    }

    @Test
    public void testConvertSegmentIdWithName_ByID() {
        val fusionId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        // check streaming segment of fusion model
        Pair pair = fusionModelService.convertSegmentIdWithName(fusionId, "streaming_test",
                new String[] { "3e560d22-b749-48c3-9f64-d4230207f120" }, null);
        String[] originSegIds = { "3e560d22-b749-48c3-9f64-d4230207f120" };
        Assert.assertEquals(fusionId, pair.getFirst());
        Assert.assertTrue(ArrayUtils.isEquals(pair.getSecond(), originSegIds));

        // check batch segment of fusion model
        pair = fusionModelService.convertSegmentIdWithName(fusionId, "streaming_test",
                new String[] { "86b5daaa-e295-4e8c-b877-f97bda69bee5" }, null);
        String[] originBatchSegIds = { "86b5daaa-e295-4e8c-b877-f97bda69bee5" };
        Assert.assertEquals("4965c827-fbb4-4ea1-a744-3f341a3b030d", pair.getFirst());
        Assert.assertTrue(ArrayUtils.isEquals(pair.getSecond(), originBatchSegIds));

        // check segment of streaming model
        val streamingModelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        pair = fusionModelService.convertSegmentIdWithName(streamingModelId, "streaming_test",
                new String[] { "c380dd2a-43b8-4268-b73d-2a5f76236631" }, null);
        String[] streamingSegIds = { "c380dd2a-43b8-4268-b73d-2a5f76236631" };
        Assert.assertEquals(streamingModelId, pair.getFirst());
        Assert.assertTrue(ArrayUtils.isEquals(pair.getSecond(), streamingSegIds));

        // check segment of batch model
        val batchModelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        pair = fusionModelService.convertSegmentIdWithName(batchModelId, "default",
                new String[] { "ef5e0663-feba-4ed2-b71c-21958122bbff" }, null);
        String[] batchSegIds = { "ef5e0663-feba-4ed2-b71c-21958122bbff" };
        Assert.assertEquals(batchModelId, pair.getFirst());
        Assert.assertTrue(ArrayUtils.isEquals(pair.getSecond(), batchSegIds));
    }

    @Test
    public void testGetBatchName() {
        val modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";

        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "streaming_test");
        val model = modelMgr.getDataModelDesc(modelId);
        val batchModel = modelMgr.getDataModelDesc(batchId);
        val alias = FusionModel.getBatchName(model.getAlias(), modelId);
        Assert.assertEquals(batchModel.getAlias(), alias);
    }

    @Test
    public void testDropModel() {
        val project = "streaming_test";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        fusionModelService.innerDopModel("334671fd-e383-4fc9-b5c2-94fce832f77a", project);
        val dataModel = modelManager.getDataModelDesc("b05034a8-c037-416b-aa26-9e6b4a41ee40");
        Assert.assertNull(dataModel);
        val dataModel1 = modelManager.getDataModelDesc("334671fd-e383-4fc9-b5c2-94fce832f77a");
        Assert.assertNull(dataModel1);

        fusionModelService.innerDopModel("4965c827-fbb4-4ea1-a744-3f341a3b030d", project);
        val dataModel2 = modelManager.getDataModelDesc("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        Assert.assertNull(dataModel2);
        val dataModel3 = modelManager.getDataModelDesc("cd2b9a23-699c-4699-b0dd-38c9412b3dfd");
        Assert.assertNull(dataModel3);

        fusionModelService.innerDopModel("4965c827-fbb4-4ea1-a744-3f341a3b030d", project);
        val dataModel4 = modelManager.getDataModelDesc("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        Assert.assertNull(dataModel4);
    }

    @Test
    public void testSetModelUpdateEnabled() {
        // broken streaming model
        var models = modelService.getModels("model_streaming_broken", "streaming_test", true, "", null, "", false);
        Assert.assertTrue(models.get(0).isModelUpdateEnabled());
        fusionModelService.setModelUpdateEnabled(DataResult.get(Arrays.asList(models.get(0)), 1));
        Assert.assertFalse(models.get(0).isModelUpdateEnabled());

        // batch model
        models = modelService.getModels("batch", "streaming_test", true, "", null, "", false);
        Assert.assertTrue(models.get(0).isModelUpdateEnabled());
        fusionModelService.setModelUpdateEnabled(DataResult.get(Arrays.asList(models.get(0)), 1));
        Assert.assertTrue(models.get(0).isModelUpdateEnabled());

        // streaming model and has segment
        models = modelService.getModels("model_streaming", "streaming_test", true, "", null, "", false);
        Assert.assertTrue(models.get(0).isModelUpdateEnabled());
        fusionModelService.setModelUpdateEnabled(DataResult.get(Arrays.asList(models.get(0)), 1));
        Assert.assertFalse(models.get(0).isModelUpdateEnabled());

        // streaming model and Running
        testSetModelUpdateEnabled(JobStatusEnum.RUNNING);

        // streaming model and Starting
        testSetModelUpdateEnabled(JobStatusEnum.STARTING);

        // streaming model and Stopping
        testSetModelUpdateEnabled(JobStatusEnum.STOPPING);
    }

    private void testSetModelUpdateEnabled(JobStatusEnum jobStatus) {
        val models = modelService.getModels("streaming_test", "streaming_test", true, "", null, "", false);
        Assert.assertTrue(models.get(0).isModelUpdateEnabled());
        val mgr = StreamingJobManager.getInstance(getTestConfig(), "streaming_test");
        mgr.updateStreamingJob(models.get(0).getId() + "_build", updater -> updater.setCurrentStatus(jobStatus));
        fusionModelService.setModelUpdateEnabled(DataResult.get(Arrays.asList(models.get(0)), 1));
        Assert.assertFalse(models.get(0).isModelUpdateEnabled());
    }

    @Test
    public void testModelExists() {
        Assert.assertTrue(fusionModelService.modelExists("stream_merge1", "streaming_test"));
    }
}
