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

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.engine.spark.utils.ComputedColumnEvalUtil;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.util.ExpandableMeasureUtil;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.QueryTimesResponse;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.query.util.KapQueryUtil;
import org.apache.kylin.rest.config.initialize.ModelBrokenListener;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.streaming.jobs.StreamingJobListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.clickhouse.MockSecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import lombok.val;
import lombok.var;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
@PowerMockIgnore({ "javax.management.*", "javax.script.*" })
public class ModelServiceWithSecondStorageTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Autowired
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private StreamingJobListener eventListener = new StreamingJobListener();

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        ApplicationContext applicationContext = PowerMockito.mock(ApplicationContext.class);

        // Use thenAnswer instead of thenReturn, a workaround for https://github.com/powermock/powermock/issues/992
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenAnswer((invocation -> userGroupInformation));
        PowerMockito.when(SpringContext.getApplicationContext()).thenAnswer(invocation -> applicationContext);
        PowerMockito.when(SpringContext.getBean(PermissionFactory.class))
                .thenAnswer((invocation -> PowerMockito.mock(PermissionFactory.class)));
        PowerMockito.when(SpringContext.getBean(PermissionGrantingStrategy.class))
                .thenAnswer(invocation -> PowerMockito.mock(PermissionGrantingStrategy.class));
        PowerMockito.when(SpringContext.getBean(SecondStorageUpdater.class))
                .thenAnswer(invocation -> new SecondStorageService());

        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
        getTestConfig().setProperty("kylin.query.engine.sparder-additional-files",
                "../../../build/conf/spark-executor-log4j.xml");

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
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);

        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);
        val result1 = new QueryTimesResponse();
        result1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        result1.setQueryTimes(10);
        val prjManager = NProjectManager.getInstance(getTestConfig());
        prjManager.forceDropProject("broken_test");
        prjManager.forceDropProject("bad_query_test");

        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
        EventBusFactory.getInstance().register(eventListener, true);
        EventBusFactory.getInstance().register(modelBrokenListener, false);
        ExecutableUtils.initJobFactory();
    }

    @After
    public void tearDown() {
        EventBusFactory.getInstance().unregister(eventListener);
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testChangePartitionDescAndSegWithSecondStorage() throws Exception {
        val model = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val project = "default";
        MockSecondStorage.mock("default", new ArrayList<>(), this);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            indexPlanManager.updateIndexPlan(model, indexPlan -> {
                indexPlan.createAndAddBaseIndex(indexPlan.getModel());
            });
            return null;
        }, project);
        SecondStorageUtil.initModelMetaData(project, model);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));

        val modelRequest = prepare();
        modelRequest.setWithSecondStorage(false);
        modelRequest.getPartitionDesc().setPartitionDateColumn(null);
        modelRequest.setWithBaseIndex(true);
        modelRequest.setSaveOnly(true);

        val indexResponse = modelService.updateDataModelSemantic(project, modelRequest);
        Assert.assertFalse(indexResponse.isCleanSecondStorage());
    }

    @Test
    public void testChangePartitionDescWithSecondStorage() throws Exception {
        val model = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val project = "default";
        MockSecondStorage.mock("default", new ArrayList<>(), this);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            indexPlanManager.updateIndexPlan(model, indexPlan -> {
                indexPlan.createAndAddBaseIndex(indexPlan.getModel());
            });
            return null;
        }, project);
        SecondStorageUtil.initModelMetaData("default", model);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));

        val modelRequest = prepare();
        modelRequest.setWithSecondStorage(true);
        modelRequest.getPartitionDesc().setPartitionDateColumn("TRANS_ID");
        modelService.updateDataModelSemantic("default", modelRequest);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, model));
    }

    private ModelRequest prepare() throws IOException {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        final String project = "default";
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        var model = modelMgr.getDataModelDescByAlias("nmodel_basic");
        val modelId = model.getId();

        modelMgr.updateDataModel(modelId, copyForWrite -> copyForWrite.setManagementType(ManagementType.MODEL_BASED));
        model = modelMgr.getDataModelDesc(modelId);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(project);
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }
}
