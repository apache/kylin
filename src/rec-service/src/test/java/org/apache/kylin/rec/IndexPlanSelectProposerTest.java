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

import java.util.List;

import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Test;

public class IndexPlanSelectProposerTest extends AutoTestOnLearnKylinData {
    private final String[] sqls = { //
            "select test_kylin_fact.lstg_format_name,sum(test_kylin_fact.price) as GMV \n"
                    + " , count(*) as TRANS_CNT from test_kylin_fact \n"
                    + " where test_kylin_fact.lstg_format_name is not null \n"
                    + " group by test_kylin_fact.lstg_format_name \n" + " having sum(price)>5000 or count(*)>20 " };

    @Test
    public void test() {
        AbstractContext originContext = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten",
                new String[] { "select sum(price) from test_kylin_fact" });
        List<AbstractContext.ModelContext> modelContexts = originContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel originModel = modelContexts.get(0).getTargetModel();
        Assert.assertNotNull(originModel);

        String expectedModelId = originModel.getUuid();
        AbstractContext context = new SmartContext(getTestConfig(), "newten", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.getProposer("SQLAnalysisProposer").execute();

        // validate select the expected model
        smartMaster.getProposer("ModelSelectProposer").execute();
        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        Assert.assertEquals(expectedModelId, mdCtx.getTargetModel().getUuid());
        Assert.assertEquals(expectedModelId, mdCtx.getOriginModel().getUuid());

        // validate select the expected CubePlan
        smartMaster.getProposer("IndexPlanSelectProposer").execute();
        Assert.assertEquals(expectedModelId, mdCtx.getOriginIndexPlan().getUuid());
        Assert.assertEquals(expectedModelId, mdCtx.getTargetIndexPlan().getUuid());
    }
}
