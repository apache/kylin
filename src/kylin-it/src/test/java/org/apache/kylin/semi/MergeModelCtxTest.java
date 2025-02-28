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

package org.apache.kylin.semi;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.LayoutRecDetailResponse;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.apache.kylin.rest.response.SuggestionResponse;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.IndexPlanService;
import org.apache.kylin.rest.service.ModelSemanticHelper;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelSmartService;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.service.OptRecService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SCD2SimplificationConvertUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.SemiAutoTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class MergeModelCtxTest extends SemiAutoTestBase {

    private RawRecService rawRecService;
    private NDataModelManager modelManager;
    private ProjectService projectService;
    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @Mock
    OptRecService optRecService = Mockito.spy(new OptRecService());
    @Mock
    ModelService modelService = Mockito.spy(ModelService.class);
    @Mock
    ModelSmartService modelSmartService = Mockito.spy(ModelSmartService.class);
    @Mock
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Override
    public String getProject() {
        return "ssb";
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        rawRecService = new RawRecService();
        projectService = new ProjectService();
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelService.setSemanticUpdater(semanticService);
        ReflectionTestUtils.setField(modelService, "indexPlanService", indexPlanService);
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        prepareACL();
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(optRecService, "modelService", modelService);
        ReflectionTestUtils.setField(rawRecService, "optRecService", optRecService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "modelChangeSupporters", Collections.singletonList(rawRecService));
        ReflectionTestUtils.setField(modelSmartService, "optRecService", optRecService);
        ReflectionTestUtils.setField(modelSmartService, "modelService", modelService);
        ReflectionTestUtils.setField(modelSmartService, "indexPlanService", indexPlanService);
        ReflectionTestUtils.setField(modelSmartService, "rawRecService", rawRecService);
        ReflectionTestUtils.setField(modelSmartService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(projectService, "projectModelSupporter", modelService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void tearDown() throws Exception {
        queryHistoryDAO.deleteAllQueryHistory();
        super.tearDown();
    }

    @Test
    public void testSkipMergingContextsHavingSqlHint() {
        //prepare original model
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        val smartContext = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { "SELECT MIN(LO_ORDERKEY) from SSB.P_LINEORDER;" }, true);

        Assert.assertEquals(1, modelManager.listAllModelIds().size());
        String uuid = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        //transfer auto mode to semi auto mode
        MetadataTestUtils.toSemiAutoMode(getProject());

        //suggest model
        List<String> sqls = Lists.newArrayList();
        sqls.add(
                "SELECT /*+ MODEL_PRIORITY(model1) */ SUM(LO_QUANTITY * 2) FROM SSB.P_LINEORDER GROUP BY LO_QUANTITY;");
        sqls.add("SELECT LO_CUSTKEY, LO_LINENUMBER FROM SSB.P_LINEORDER GROUP BY LO_CUSTKEY, LO_LINENUMBER;");
        AbstractContext proposeContext = modelSmartService.suggestModel(getProject(), sqls, true, false);
        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        Assert.assertEquals(2, modelContexts.size());

        SuggestionResponse suggestionResponse = modelSmartService.buildModelSuggestionResponse(proposeContext);
        List<SuggestionResponse.ModelRecResponse> reusedModels = suggestionResponse.getReusedModels();
        List<SuggestionResponse.ModelRecResponse> newModels = suggestionResponse.getNewModels();
        List<ModelRequest> reusedModelRequests = mockModelRequest(reusedModels);
        List<ModelRequest> newModelRequests = mockModelRequest(newModels);
        changeTheIndexRecOrder(reusedModelRequests);
        newModelRequests.forEach(request -> request.setWithBaseIndex(true));
        modelService.batchCreateModel(getProject(), newModelRequests, reusedModelRequests);

        Assert.assertEquals(1, modelManager.listAllModelIds().size());
        NDataModel dataModelDesc = modelManager.getDataModelDesc(uuid);
        Assert.assertEquals(3, dataModelDesc.getEffectiveDimensions().size());
        Assert.assertEquals(3, dataModelDesc.getEffectiveMeasures().size());
        Assert.assertEquals(1, dataModelDesc.getComputedColumnNames().size());
    }

    private List<ModelRequest> mockModelRequest(List<SuggestionResponse.ModelRecResponse> modelResponses) {
        List<ModelRequest> modelRequestList = Lists.newArrayList();
        modelResponses.forEach(model -> {
            ModelRequest modelRequest = new ModelRequest();
            modelRequest.setUuid(model.getUuid());
            modelRequest.setJoinTables(model.getJoinTables());
            modelRequest.setJoinsGraph(model.getJoinsGraph());
            modelRequest.setFactTableRefs(model.getFactTableRefs());
            modelRequest.setAllTableRefs(model.getAllTableRefs());
            modelRequest.setLookupTableRefs(model.getLookupTableRefs());
            modelRequest.setTableNameMap(model.getTableNameMap());
            modelRequest.setRootFactTableName(model.getRootFactTableName());
            modelRequest.setRootFactTableAlias(model.getRootFactTableAlias());
            modelRequest.setRootFactTableRef(model.getRootFactTableRef());
            modelRequest.setModelType(model.getModelType());

            modelRequest.setIndexPlan(model.getIndexPlan());
            modelRequest.setAllNamedColumns(model.getAllNamedColumns());
            modelRequest.setAllMeasures(model.getAllMeasures());
            modelRequest.setComputedColumnDescs(model.getComputedColumnDescs());
            modelRequest.setRecItems(model.getIndexes());

            modelRequest.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                    .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList()));
            modelRequest.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                    .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
            modelRequest.setSimplifiedJoinTableDescs(
                    SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(model.getJoinTables()));
            modelRequest.setAlias(model.getAlias());
            modelRequest.setManagementType(model.getManagementType());
            modelRequestList.add(modelRequest);
        });
        return modelRequestList;
    }

    /**
     * https://olapio.atlassian.net/browse/KE-23783
     * layout1 depends on m2, layout2 depends on m1, m2.id > m1.id, layout2.id > layout1.id
     */
    private void changeTheIndexRecOrder(List<ModelRequest> reusedModelRequests) {
        ModelRequest modelRequest = reusedModelRequests.get(0);
        List<LayoutRecDetailResponse> recItems = modelRequest.getRecItems();
        recItems.sort((rec1, rec2) -> {
            List<Integer> measureList1 = rec1.getMeasures().stream().map(recMeasure -> recMeasure.getMeasure().getId())
                    .sorted().collect(Collectors.toList());
            List<Integer> measureList2 = rec2.getMeasures().stream().map(recMeasure -> recMeasure.getMeasure().getId())
                    .sorted().collect(Collectors.toList());

            return measureList2.get(measureList2.size() - 1) - measureList1.get(measureList1.size() - 1);
        });
        modelRequest.setRecItems(recItems);
    }
}
