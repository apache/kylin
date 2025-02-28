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

package org.apache.kylin.rec.model;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelCreateContext;
import org.apache.kylin.rec.ProposerJob;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class ModelRenameProposerTest extends NLocalWithSparkSessionTest {
    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void testAutoRenameWithOfflineModel() {
        String[] accSql1 = {
                "select count(*)  FROM TEST_ORDER LEFT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID" };
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare initial model
        val smartContext = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), accSql1);
        MetadataTestUtils.toSemiAutoMode(getProject());

        // offline model
        AbstractContext context = ProposerJob.propose(new ModelCreateContext(getTestConfig(), getProject(), accSql1));
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals("AUTO_MODEL_TEST_ORDER_2", modelContext.getTargetModel().getAlias());

        //online model
        AccelerationUtil.onlineModel(smartContext);
        context = ProposerJob.propose(new ModelCreateContext(getTestConfig(), getProject(), accSql1));
        modelContext = context.getModelContexts().get(0);
        Assert.assertEquals("AUTO_MODEL_TEST_ORDER_2", modelContext.getTargetModel().getAlias());

        // broken model
        NTableMetadataManager.getInstance(getTestConfig(), getProject()).removeSourceTable("DEFAULT.TEST_KYLIN_FACT");
        String[] accSql2 = { "select count(*)  FROM TEST_ORDER" };

        context = ProposerJob.propose(new ModelCreateContext(getTestConfig(), getProject(), accSql2));
        modelContext = context.getModelContexts().get(0);
        Assert.assertEquals("AUTO_MODEL_TEST_ORDER_2", modelContext.getTargetModel().getAlias());

    }

}
