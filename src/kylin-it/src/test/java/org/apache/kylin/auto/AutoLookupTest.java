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

import java.util.List;

import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.ExecAndCompExt;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import lombok.val;

public class AutoLookupTest extends SuggestTestBase {

    @Test
    public void testLookup() throws Exception {
        String modelQuery = "select sum(ITEM_COUNT) as ITEM_CNT\n" //
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" //
                + "INNER JOIN TEST_ORDER as TEST_ORDER\n" //
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" //
                + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n" //
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" //
                + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n" //
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" //
                + "INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT\n" //
                + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n" //
                + "INNER JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS\n" //
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES as TEST_SITES\n" //
                + "ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n" //
                + "INNER JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM\n" //
                + "ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n"
                + "INNER JOIN TEST_COUNTRY as BUYER_COUNTRY\n" //
                + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY\n" //
                + "INNER JOIN TEST_COUNTRY as SELLER_COUNTRY\n" //
                + "ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY limit 1";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { modelQuery },
                true);
        Assert.assertEquals(1, context.getModelContexts().size());
        Assert.assertNotNull(context.getModelContexts().get(0).getTargetModel());

        List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, getProject()).listUnderliningDataModels();
        Assert.assertEquals(1, models.size());
        NDataModel model = models.get(0);
        Assert.assertTrue(model.isLookupTable("DEFAULT.TEST_CATEGORY_GROUPINGS"));

        buildAllModels(kylinConfig, getProject());
        Assert.assertEquals(1, ExecAndCompExt.queryModelWithoutCompute(getProject(), modelQuery).toDF().count());

        // Use snapshot, no need to build
        String lookupQuery = "select leaf_categ_id from test_category_groupings group by leaf_categ_id limit 1";
        Assert.assertEquals(1, ExecAndCompExt.queryModelWithoutCompute(getProject(), lookupQuery).toDF().count());
    }

    @Test
    @Ignore("Seems no need")
    public void testLookupByStep() {
        {
            String modelQuery = "select sum(ITEM_COUNT) as ITEM_CNT\n" //
                    + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" //
                    + "INNER JOIN TEST_ORDER as TEST_ORDER\n" //
                    + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" //
                    + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n" //
                    + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" //
                    + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n" //
                    + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" //
                    + "INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT\n" //
                    + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n" //
                    + "INNER JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS\n"
                    + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                    + "INNER JOIN EDW.TEST_SITES as TEST_SITES\n" //
                    + "ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n" //
                    + "INNER JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM\n" //
                    + "ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n" //
                    + "INNER JOIN TEST_COUNTRY as BUYER_COUNTRY\n" //
                    + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY\n" //
                    + "INNER JOIN TEST_COUNTRY as SELLER_COUNTRY\n" //
                    + "ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY limit 1";
            val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { modelQuery },
                    true);
            Assert.assertEquals(1, context.getModelContexts().size());
            Assert.assertNotNull(context.getModelContexts().get(0).getTargetModel());

            List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, getProject())
                    .listUnderliningDataModels();
            Assert.assertEquals(1, models.size());
            NDataModel model = models.get(0);
            Assert.assertTrue(model.isLookupTable("DEFAULT.TEST_CATEGORY_GROUPINGS"));
        }

        {
            String lookupQuery = "select leaf_categ_id from test_category_groupings group by leaf_categ_id limit 1";
            val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), new String[] { lookupQuery },
                    true);
            Assert.assertEquals(1, context.getModelContexts().size());
            NDataModel model = context.getModelContexts().get(0).getTargetModel();
            Assert.assertNotNull(model);
            Assert.assertTrue(model.isFactTable("DEFAULT.TEST_CATEGORY_GROUPINGS"));

            List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, getProject())
                    .listUnderliningDataModels();
            Assert.assertEquals(2, models.size());
        }
    }

    @Test
    public void testNoLookupInBatch() throws Exception {
        String modelQuery = "select sum(ITEM_COUNT) as ITEM_CNT\n" //
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" //
                + "INNER JOIN TEST_ORDER as TEST_ORDER\n" //
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" //
                + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n" //
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" //
                + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n" //
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" //
                + "INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT\n" //
                + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n" //
                + "INNER JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS\n" //
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES as TEST_SITES\n" //
                + "ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n" //
                + "INNER JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM\n" //
                + "ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n" //
                + "INNER JOIN TEST_COUNTRY as BUYER_COUNTRY\n" //
                + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY\n" //
                + "INNER JOIN TEST_COUNTRY as SELLER_COUNTRY\n" //
                + "ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY limit 1";
        String lookupQuery = "select leaf_categ_id from test_category_groupings group by leaf_categ_id limit 1";
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, getProject(),
                new String[] { modelQuery, lookupQuery }, true);
        Assert.assertEquals(2, context.getModelContexts().size());

        List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, getProject()).listUnderliningDataModels();
        Assert.assertEquals(2, models.size());

        buildAllModels(kylinConfig, getProject());
        Assert.assertEquals(1, ExecAndCompExt.queryModelWithoutCompute(getProject(), modelQuery).toDF().count());
        Assert.assertEquals(1, ExecAndCompExt.queryModelWithoutCompute(getProject(), lookupQuery).toDF().count());
    }
}
