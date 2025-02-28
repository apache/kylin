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

package org.apache.kylin.rec;

import java.io.File;
import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class SmartSemiAutoTest extends AutoTestOnLearnKylinData {

    @Test
    public void testWithSource() throws Exception {
        String project = "newten";
        MetadataTestUtils.toSemiAutoMode(project);

        val modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        modelManager.listAllModelIds().forEach(id -> {
            dataflowManager.dropDataflow(id);
            indexPlanManager.dropIndexPlan(id);
            modelManager.dropModel(id);
        });

        val baseModel = JsonUtil.readValue(new File("src/test/resources/nsmart/default/model_desc/model.json"),
                NDataModel.class);
        baseModel.setProject(project);
        modelManager.createDataModelDesc(baseModel, "ADMIN");
        val indexPlan = new IndexPlan();
        indexPlan.setUuid(baseModel.getUuid());
        indexPlan.setProject(project);
        indexPlanManager.createIndexPlan(indexPlan);
        dataflowManager.createDataflow(indexPlan, "ADMIN");
        dataflowManager.updateDataflowStatus(baseModel.getUuid(), RealizationStatusEnum.ONLINE);

        val sql1 = "select sum(TEST_KYLIN_FACT.ITEM_COUNT) from TEST_KYLIN_FACT group by TEST_KYLIN_FACT.CAL_DT;";
        val sql2 = "select LEAF_CATEG_ID from TEST_KYLIN_FACT;";

        AbstractSemiContext context1 = (AbstractSemiContext) AccelerationUtil.genOptRec(getTestConfig(), project,
                new String[] { sql1 });
        List<AbstractContext.ModelContext> modelContexts1 = context1.getModelContexts();
        Assert.assertEquals(1, modelContexts1.size());
        AbstractContext.ModelContext modelContext1 = modelContexts1.get(0);
        Assert.assertEquals(1, modelContext1.getIndexRexItemMap().size());

        AbstractSemiContext context2 = (AbstractSemiContext) AccelerationUtil.genOptRec(getTestConfig(), project,
                new String[] { sql2 });
        List<AbstractContext.ModelContext> modelContexts2 = context2.getModelContexts();
        Assert.assertEquals(1, modelContexts2.size());
        AbstractContext.ModelContext modelContext2 = modelContexts2.get(0);
        Assert.assertEquals(1, modelContext2.getIndexRexItemMap().size());
    }
}
