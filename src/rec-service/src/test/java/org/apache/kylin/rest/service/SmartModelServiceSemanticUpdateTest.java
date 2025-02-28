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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SCD2SimplificationConvertUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class SmartModelServiceSemanticUpdateTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Mock
    private ModelSmartService modelSmartService = Mockito.spy(new ModelSmartService());

    @InjectMocks
    private IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Before
    public void setupResource() throws Exception {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        modelService.setSemanticUpdater(semanticService);
        indexPlanService.setSemanticUpater(semanticService);
        modelService.setIndexPlanService(indexPlanService);

        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
    }

    @Before
    public void setup() {
        SparkJobFactoryUtils.initJobFactory();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testSCD2ModelWithAlias() throws Exception {
        getTestConfig().setProperty("kylin.query.non-equi-join-model-enabled", "true");
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), "scd2");
        val model = modelMgr.getDataModelDescByAlias("same_scd2_dim_tables");
        val req = newSemanticRequest(model.getId(), "scd2");
        val modelFromReq = modelService.convertToDataModel(req);
        Assert.assertEquals(2, modelFromReq.getJoinTables().size());
        val join = modelFromReq.getJoinTables().get(1).getAlias().equalsIgnoreCase("TEST_SCD2_1")
                ? modelFromReq.getJoinTables().get(1)
                : modelFromReq.getJoinTables().get(0);
        Assert.assertEquals("TEST_SCD2_1", join.getAlias());
        Assert.assertEquals(
                "\"TEST_KYLIN_FACT\".\"SELLER_ID\" = \"TEST_SCD2_1\".\"BUYER_ID\" "
                        + "AND \"TEST_KYLIN_FACT\".\"CAL_DT\" >= \"TEST_SCD2_1\".\"START_DATE\" "
                        + "AND \"TEST_KYLIN_FACT\".\"CAL_DT\" < \"TEST_SCD2_1\".\"END_DATE\"",
                join.getJoin().getNonEquiJoinCondition().getExpr());
    }

    private ModelRequest newSemanticRequest(String modelId, String project) throws Exception {
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val model = modelMgr.getDataModelDesc(modelId);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(project);
        request.setUuid(modelId);
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(model.getJoinTables()));
        List<NDataModel.NamedColumn> otherColumns = model.getAllNamedColumns().stream()
                .filter(column -> !column.isDimension()).collect(Collectors.toList());
        request.setOtherColumns(otherColumns);

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }
}
