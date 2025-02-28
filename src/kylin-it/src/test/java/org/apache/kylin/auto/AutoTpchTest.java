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

package org.apache.kylin.auto;

import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.graph.JoinsGraph;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.util.ExecAndComp.CompareLevel;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@SuppressWarnings("squid:S2699")
public class AutoTpchTest extends SuggestTestBase {

    @Test
    public void testTpch() throws Exception {
        new TestScenario(CompareLevel.SAME, "sql_tpch", 0, 22).execute();
    }

    @Test
    public void testReProposeCase() throws Exception {
        // run twice to verify KAP#7515
        for (int i = 0; i < 2; ++i) {
            new TestScenario(CompareLevel.SAME, "sql_tpch", 1, 2).execute();
        }
    }

    @Test
    public void testBatchProposeSQLAndReuseLeftJoinModel() throws Exception {
        // 1st round, recommend model with a single fact table
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                getProject());
        AbstractContext context = proposeWithSmartMaster(getProject(), Lists.newArrayList(
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, false, "sql_tpch/sql_tpch_reprosal/", 0, 1, null)));
        Assert.assertEquals(1, context.getAccelerateInfoMap().size());
        Set<NDataModel> selectedDataModels1 = Sets.newHashSet();
        context.getAccelerateInfoMap().forEach((s, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isFailed());
            accelerateInfo.getRelatedLayouts()
                    .forEach(layout -> selectedDataModels1.add(dataModelManager.getDataModelDesc(layout.getModelId())));
        });
        NDataModel proposedModel1 = selectedDataModels1.iterator().next();
        Assert.assertEquals(0, proposedModel1.getJoinTables().size());
        JoinsGraph graph1 = proposedModel1.getJoinsGraph();

        // 2nd round, reuse the model and increase more Joins which is through accelerating 2 different olapCtx
        AbstractContext context2 = proposeWithSmartMaster(getProject(), Lists.newArrayList(
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, false, "sql_tpch/sql_tpch_reprosal/", 1, 3, null)));
        Set<NDataModel> selectedDataModels2 = Sets.newHashSet();
        context2.getAccelerateInfoMap().forEach((s, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isFailed());
            accelerateInfo.getRelatedLayouts()
                    .forEach(layout -> selectedDataModels2.add(dataModelManager.getDataModelDesc(layout.getModelId())));
        });
        Assert.assertEquals(1, selectedDataModels2.size());
        NDataModel proposedModel2 = selectedDataModels2.iterator().next();
        Assert.assertEquals(6, proposedModel2.getJoinTables().size());
        JoinsGraph graph2 = proposedModel2.getJoinsGraph();
        Assert.assertTrue(graph1.match(graph2, new HashMap<String, String>()));

        // 3rd round, accelerate a sql that its join info equaled with current model, so it won't change previous model
        AbstractContext context3 = proposeWithSmartMaster(getProject(), Lists.newArrayList(
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, false, "sql_tpch/sql_tpch_reprosal/", 3, 4, null)));
        Set<NDataModel> selectedDataModels3 = Sets.newHashSet();
        context3.getAccelerateInfoMap().forEach((s, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isFailed());
            accelerateInfo.getRelatedLayouts()
                    .forEach(layout -> selectedDataModels3.add(dataModelManager.getDataModelDesc(layout.getModelId())));
        });
        Assert.assertEquals(1, selectedDataModels3.size());
        NDataModel proposedModel3 = selectedDataModels3.iterator().next();
        Assert.assertEquals(6, proposedModel3.getJoinTables().size());
        JoinsGraph graph3 = proposedModel3.getJoinsGraph();
        Assert.assertTrue(graph2.match(graph3, new HashMap<>()));
        Assert.assertTrue(graph3.match(graph2, new HashMap<>()));
    }

    @Test
    public void testBatchProposeSQLAndReuseInnerJoinModel() throws Exception {
        //1st round, propose initial model
        AbstractContext context = proposeWithSmartMaster(getProject(),
                Lists.newArrayList(new TestScenario(CompareLevel.SAME, "sql_tpch")));
        NDataModel originModel = context.getModelContexts().stream()
                .filter(ctx -> ctx.getTargetModel().getJoinTables().size() == 6).collect(Collectors.toList()).get(0)
                .getTargetModel();
        JoinsGraph originJoinGragh = originModel.getJoinsGraph();

        AbstractContext context2 = proposeWithSmartMaster(getProject(),
                Lists.newArrayList(new TestScenario(CompareLevel.SAME, "sql_tpch/sql_tpch_reprosal/", 3, 4)));
        AccelerateInfo accelerateInfo = context2.getAccelerateInfoMap().values().toArray(new AccelerateInfo[] {})[0];
        Assert.assertFalse(accelerateInfo.isFailed());

        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataModel dataModel = modelManager
                .getDataModelDesc(Lists.newArrayList(accelerateInfo.getRelatedLayouts()).get(0).getModelId());
        JoinsGraph accelerateJoinGragh = dataModel.getJoinsGraph();
        Assert.assertTrue(originJoinGragh.match(accelerateJoinGragh, new HashMap<>()));
        Assert.assertTrue(accelerateJoinGragh.match(originJoinGragh, new HashMap<>()));
    }

    @Test
    @Ignore("For development")
    public void testTemp() throws Exception {
        new TestScenario(CompareLevel.SAME, "query/temp").execute();

    }
}
