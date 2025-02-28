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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.request.OptRecRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableList;

public class OptRecServiceCcConflictTest extends OptRecV2TestBase {
    @InjectMocks
    OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    ModelService modelService = Mockito.spy(new ModelService());
    @InjectMocks
    OptRecApproveService optRecApproveService = Mockito.spy(new OptRecApproveService());
    @Spy
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public OptRecServiceCcConflictTest() {
        super("../rec-service/src/test/resources/ut_rec_v2/cc_expr_conflict",
                new String[] { "a58135aa-20a6-a296-a55a-9d6e76b02f10" });
    }

    @Test
    public void testApproveWithDupCCExpr() throws Exception {
        List<Integer> addLayoutId = Lists.newArrayList(3, 8, 13);
        prepare(addLayoutId);
        OptRecRequest recRequest = buildOptRecRequest(addLayoutId);
        optRecApproveService.approve(getProject(), recRequest);

        NDataModel dataModel = getModel();
        Assert.assertFalse(dataModel.getComputedColumnDescs().isEmpty());
        ComputedColumnDesc computedColumnDesc = dataModel.getComputedColumnDescs().get(0);
        Assert.assertEquals("SSB.P_LINEORDER", computedColumnDesc.getTableIdentity());
        Assert.assertEquals("`P_LINEORDER`.`LO_EXTENDEDPRICE` * `P_LINEORDER`.`LO_DISCOUNT` + 1",
                computedColumnDesc.getInnerExpression());
        Assert.assertEquals(2, dataModel.getEffectiveDimensions().size());
        Assert.assertEquals(2, dataModel.getEffectiveMeasures().size());

        List<List<Integer>> layoutColOrder = ImmutableList.<List<Integer>> builder()
                .add(ImmutableList.of(100000, 100001)).build();
        checkIndexPlan(layoutColOrder, getIndexPlan());
    }

    private void prepare(List<Integer> addLayoutId) throws IOException {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        prepareEnv(addLayoutId);
    }

    private void checkIndexPlan(List<List<Integer>> layoutColOrder, IndexPlan actualPlan) {
        Assert.assertEquals(layoutColOrder.size(), actualPlan.getAllLayouts().size());
        Assert.assertEquals(layoutColOrder,
                actualPlan.getAllLayouts().stream().map(LayoutEntity::getColOrder).collect(Collectors.toList()));
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId) {
        return buildOptRecRequest(addLayoutId, ImmutableList.of(), ImmutableMap.of());
    }

    private OptRecRequest buildOptRecRequest(List<Integer> addLayoutId, List<Integer> removeLayoutId,
            Map<Integer, String> nameMap) {
        OptRecRequest recRequest = new OptRecRequest();
        recRequest.setModelId(getDefaultUUID());
        recRequest.setProject(getProject());
        recRequest.setRecItemsToAddLayout(addLayoutId);
        recRequest.setRecItemsToRemoveLayout(removeLayoutId);
        recRequest.setNames(nameMap);
        return recRequest;
    }
}
