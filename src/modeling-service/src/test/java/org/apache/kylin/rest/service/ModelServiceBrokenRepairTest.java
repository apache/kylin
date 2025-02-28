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

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.UpdateImpact;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.rest.config.initialize.ModelBrokenListener;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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

public class ModelServiceBrokenRepairTest extends SourceTestCase {

    private static final String PROJECT_NAME = "ssb";

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final MockModelQueryService modelQueryService = Mockito.spy(new MockModelQueryService());

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @InjectMocks
    private final FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());

    @InjectMocks
    private final TableExtService tableExtService = Mockito.spy(new TableExtService());

    @InjectMocks
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(new ProjectService());

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

    private static final String[] timeZones = { "GMT+8", "CST", "PST", "UTC" };

    private StreamingJobListener eventListener = new StreamingJobListener();

    private JdbcRawRecStore jdbcRawRecStore;

    private FavoriteRuleManager favoriteRuleManager;

    @Before
    public void setUp() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        String localMetaDir = "src/test/resources/ut_meta/broken_repair_test";
        createTestMetadata(localMetaDir);
        indexPlanService.setSemanticUpater(semanticService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        modelService.setSemanticUpdater(semanticService);
        modelService.setModelQuerySupporter(modelQueryService);
        modelService.setIndexPlanService(indexPlanService);
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRepairBrokenModelAfterReloadMoreThanOneTable() throws IOException {
        String modelId = "722b027b-8906-379b-cf4f-ac2055ee528b";
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT_NAME);
        ModelRequest modelRequest = JsonUtil.readValue(new File(
                "src/test/resources/ut_meta/broken_repair_test/model_request/model_request_broken_aftr_reload.json"),
                ModelRequest.class);
        modelRequest.setProject(PROJECT_NAME);
        modelService.detectInvalidIndexes(modelRequest);
        NDataModel brokenDataModelDesc = dataModelManager.getDataModelDescWithoutInit(modelId);
        Assert.assertEquals(1, brokenDataModelDesc.getComputedColumnUuids().size());
        Assert.assertEquals(29,
                brokenDataModelDesc.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist).count());
        Assert.assertEquals(8,
                brokenDataModelDesc.getAllMeasures().stream().filter(NDataModel.Measure::isTomb).count());

        Assert.assertEquals(NDataModel.class,
                UnitOfWork.doInTransactionWithRetry(() -> modelService.repairBrokenModel(PROJECT_NAME, modelRequest),
                        PROJECT_NAME).getClass());
    }

    @Test
    public void testRepairModelWithMultiPartitionCol() throws IOException {
        String modelId = "8dc4c289-c95d-0402-31e4-b123f9ef553f";
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT_NAME);
        ModelRequest modelRequest = JsonUtil.readValue(new File(
                "src/test/resources/ut_meta/broken_repair_test/model_request/model_request_broken_aftr_reload_2.json"),
                ModelRequest.class);
        modelRequest.setProject(PROJECT_NAME);
        modelService.detectInvalidIndexes(modelRequest);
        MultiPartitionDesc newMultiPartitionDesc = new MultiPartitionDesc();
        newMultiPartitionDesc.setColumns(new LinkedList<String>() {
            {
                add("LINEORDER.LO_ORDERDATE");
            }
        });
        newMultiPartitionDesc.setPartitionConditionBuilderClz(
                "org.apache.kylin.metadata.model.MultiPartitionDesc$DefaultMultiPartitionConditionBuilder");
        modelRequest.setMultiPartitionDesc(newMultiPartitionDesc);
        NDataModel brokenDataModelDesc = dataModelManager.getDataModelDescWithoutInit(modelId);
        Assert.assertEquals("LINEORDER.LO_ORDERDATE_TEST",
                brokenDataModelDesc.getMultiPartitionDesc().getColumns().getFirst());
        Assert.assertNull(brokenDataModelDesc.getMultiPartitionKeyMapping());

        NDataModel repairedDataModel = UnitOfWork.doInTransactionWithRetry(
                () -> modelService.repairBrokenModel(PROJECT_NAME, modelRequest), PROJECT_NAME);
        Assert.assertEquals(NDataModel.class, repairedDataModel.getClass());
        Assert.assertEquals("LINEORDER.LO_ORDERDATE",
                repairedDataModel.getMultiPartitionDesc().getColumns().getFirst());
    }

    @Test
    public void testSaveModelWithTimestampCC() throws IOException {
        String modelId = "3580693c-d1fd-7697-8ee8-0474f81c413d";
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT_NAME);
        Assert.assertNotNull(modelManager.getDataModelDesc(modelId));
        NDataModel dataModel = modelManager.getDataModelDesc(modelId);
        ModelRequest modelRequest = JsonUtil.readValue(new File(
                "src/test/resources/ut_meta/broken_repair_test/model_request/model_request_with_timestamp_cc.json"),
                ModelRequest.class);
        modelRequest.setProject(PROJECT_NAME);
        UpdateImpact updateImpact = semanticService.updateModelColumns(dataModel, modelRequest);
        Assert.assertEquals(0, updateImpact.getRemovedOrUpdatedCCs().size());
    }
}
