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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.config.initialize.ModelUpdateListener;
import org.apache.kylin.rest.request.IndexesToSegmentsRequest;
import org.apache.kylin.rest.service.params.IncrementBuildSegmentParams;
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

import lombok.val;
import lombok.var;

public class FusionModelServiceBuildTest extends SourceTestCase {
    @InjectMocks
    private FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private ModelBuildService modelBuildService = Mockito.spy(new ModelBuildService());

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
        ReflectionTestUtils.setField(modelBuildService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelBuildService, "modelService", modelService);
        ReflectionTestUtils.setField(fusionModelService, "modelBuildService", modelBuildService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
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
    public void testBuildSegmentsManually() throws Exception {
        String modelId = "334671fd-e383-4fc9-b5c2-94fce832f77a";
        String streamingId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        String project = "streaming_test";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        modelManager.updateDataModel(modelId, copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
            copyForWrite.setPartitionDesc(null);
        });

        NDataModel model = modelManager.getDataModelDesc(streamingId);
        IncrementBuildSegmentParams incrParams = new IncrementBuildSegmentParams(project, modelId, "1633017600000",
                "1633104000000", model.getPartitionDesc(), model.getMultiPartitionDesc(), null, true, null);
        fusionModelService.incrementBuildSegmentsManually(incrParams);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        var dataflow = dataflowManager.getDataflow(modelId);
        Assert.assertEquals(1, dataflow.getSegments().size());
        Assert.assertEquals("LINEORDER_HIVE.LO_PARTITIONCOLUMN",
                dataflow.getModel().getPartitionDesc().getPartitionDateColumn());

    }

    @Test
    public void testAddIndexesToSegments() {
        IndexesToSegmentsRequest buildSegmentsRequest = new IndexesToSegmentsRequest();
        buildSegmentsRequest.setProject("streaming_test");

        // test streaming of fusion model
        buildSegmentsRequest.setSegmentIds(Arrays.asList("3e560d22-b749-48c3-9f64-d4230207f120"));
        val fusionId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        try {
            fusionModelService.addIndexesToSegments(fusionId, buildSegmentsRequest);
        } catch (KylinException e) {
            Assert.assertEquals("KE-010022004", e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        // test batch of fusion model
        buildSegmentsRequest.setSegmentIds(Arrays.asList("86b5daaa-e295-4e8c-b877-f97bda69bee5"));
        try {
            fusionModelService.addIndexesToSegments(fusionId, buildSegmentsRequest);
        } catch (Exception e) {
            Assert.fail();
        }

        // test streaming model
        buildSegmentsRequest.setSegmentIds(Arrays.asList("c380dd2a-43b8-4268-b73d-2a5f76236631"));
        val streamingModelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        try {
            fusionModelService.addIndexesToSegments(streamingModelId, buildSegmentsRequest);
        } catch (KylinException e) {
            Assert.assertEquals("KE-010022004", e.getErrorCode().getCodeString());
        } catch (Exception e) {
            Assert.fail();
        }

        // test batch model
        buildSegmentsRequest.setSegmentIds(Arrays.asList("ef5e0663-feba-4ed2-b71c-21958122bbff"));
        val batchModelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        buildSegmentsRequest.setProject("default");
        try {
            fusionModelService.addIndexesToSegments(batchModelId, buildSegmentsRequest);
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
