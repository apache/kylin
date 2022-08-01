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

package org.apache.kylin.engine.spark.job;

import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.junit.Assert;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

public class NSparkCubingUtilTest extends NLocalWithSparkSessionTest {

    @Test
    public void testToSegmentIds() {
        Set<String> expectedSegIds = Sets.newLinkedHashSet();
        expectedSegIds.add("2");
        expectedSegIds.add("1");
        expectedSegIds.add("3");
        NDataSegment firstSeg = NDataSegment.empty();
        NDataSegment secondSeg = NDataSegment.empty();
        NDataSegment thirdSeg = NDataSegment.empty();
        firstSeg.setId("2");
        secondSeg.setId("1");
        thirdSeg.setId("3");

        Assert.assertEquals(expectedSegIds, NSparkCubingUtil.toSegmentIds("2,1,3"));

        Segments<NDataSegment> segments = new Segments<>();
        segments.add(firstSeg);
        segments.add(secondSeg);
        segments.add(thirdSeg);
        segments.add(firstSeg);
        Assert.assertEquals(expectedSegIds, NSparkCubingUtil.toSegmentIds(segments));

        Set<NDataSegment> segmentSet = Sets.newLinkedHashSet();
        segmentSet.add(firstSeg);
        segmentSet.add(secondSeg);
        segmentSet.add(thirdSeg);
        segmentSet.add(firstSeg);
        Assert.assertEquals(expectedSegIds, NSparkCubingUtil.toSegmentIds(segmentSet));
    }

    @Test
    public void testToLayoutIds() {
        Set<Long> expectedLayoutIds = Sets.newLinkedHashSet();
        expectedLayoutIds.add(20001L);
        expectedLayoutIds.add(10001L);
        expectedLayoutIds.add(10002L);
        Assert.assertEquals(expectedLayoutIds, NSparkCubingUtil.toLayoutIds("20001,10001,10002"));

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        final List<IndexPlan> indexPlans = indexPlanManager.listAllIndexPlans();
        Set<LayoutEntity> layouts = Sets.newLinkedHashSet();
        LayoutEntity one = indexPlans.get(0).getLayoutEntity(20001L);
        LayoutEntity two = indexPlans.get(0).getLayoutEntity(10001L);
        LayoutEntity three = indexPlans.get(0).getLayoutEntity(10002L);
        LayoutEntity four = indexPlans.get(0).getLayoutEntity(20001L);
        layouts.add(one);
        layouts.add(two);
        layouts.add(three);
        layouts.add(four);
        Assert.assertEquals(expectedLayoutIds, NSparkCubingUtil.toLayoutIds(layouts));

        Assert.assertEquals(layouts, NSparkCubingUtil.toLayouts(indexPlans.get(0), expectedLayoutIds));
    }

    @Test
    public void testConvert() {
        Assert.assertTrue(isEqualsConvertedString("max(case when A.account_id = 0 then 'adf.d kylin.adfad adf' else 'ky.lin' end), A.seller_id",
                "max(case when A_0_DOT_0_account_id = 0 then 'adf.d kylin.adfad adf' else 'ky.lin' end), A_0_DOT_0_seller_id"));
        Assert.assertTrue(isEqualsConvertedString("'ky.lin'", "'ky.lin'"));
        Assert.assertTrue(isEqualsConvertedString("p1.3a + 3.1E12", "p1_0_DOT_0_3a + 3.1E12"));
        Assert.assertTrue(isEqualsConvertedString("p1.3a + 3.1", "p1_0_DOT_0_3a + 3.1"));
        Assert.assertTrue(isEqualsConvertedString("3.1 + 1p1.3a1", "3.1 + 1p1_0_DOT_0_3a1"));
        Assert.assertTrue(isEqualsConvertedString("TEST_KYLIN_FACT.CAL_DT > '2017-09-12' AND TEST_KYLIN_FACT.PRICE < 9.9",
                "TEST_KYLIN_FACT_0_DOT_0_CAL_DT > '2017-09-12' AND TEST_KYLIN_FACT_0_DOT_0_PRICE < 9.9"));
        Assert.assertTrue(isEqualsConvertedString("case when  table.11col  > 0.8 then 0.2 else null end",
                "case when  table_0_DOT_0_11col  > 0.8 then 0.2 else null end"));
        Assert.assertTrue(isEqualsConvertedString("TEST_KYLIN_FACT.PRICE*0.8", "TEST_KYLIN_FACT_0_DOT_0_PRICE*0.8"));
        Assert.assertTrue(isEqualsConvertedString("`TEST_KYLIN_FACT`.`ITEM_COUNT`*`TEST_KYLIN_FACT`.`PRICE`*0.8",
                "TEST_KYLIN_FACT_0_DOT_0_ITEM_COUNT*TEST_KYLIN_FACT_0_DOT_0_PRICE*0.8"));
        Assert.assertTrue(isEqualsConvertedString("`TEST_KYLIN_FACT`.`PRICE`*0.8", "TEST_KYLIN_FACT_0_DOT_0_PRICE*0.8"));
        Assert.assertTrue(isEqualsConvertedString("TEST_KYLIN_FACT.PRICE-0.8", "TEST_KYLIN_FACT_0_DOT_0_PRICE-0.8"));
        Assert.assertTrue(isEqualsConvertedString("TEST_KYLIN_FACT.LSTG_FORMAT_NAME+'0.8'",
                "TEST_KYLIN_FACT_0_DOT_0_LSTG_FORMAT_NAME+'0.8'"));
        Assert.assertTrue(isEqualsConvertedString("TEST_KYLIN_FACT.LSTG_FORMAT_NAME+'-0.8'",
                "TEST_KYLIN_FACT_0_DOT_0_LSTG_FORMAT_NAME+'-0.8'"));
        Assert.assertTrue(isEqualsConvertedString("TEST_KYLIN_FACT.LSTG_FORMAT_NAME+'TEST_KYLIN_FACT.LSTG_FORMAT_NAME*0.8'",
                "TEST_KYLIN_FACT_0_DOT_0_LSTG_FORMAT_NAME+'TEST_KYLIN_FACT.LSTG_FORMAT_NAME*0.8'"));

        Assert.assertTrue(isEqualsConvertedString("default_test.sumtwonum(7, `TEST_FUNCTIONS`.`PRICE2`) + default_test.sumtwonum(7, TEST_FUNCTIONS.PRICE2) ",
                "default_test.sumtwonum(7, TEST_FUNCTIONS_0_DOT_0_PRICE2) + default_test.sumtwonum(7, TEST_FUNCTIONS_0_DOT_0_PRICE2) "));

        Assert.assertTrue(isEqualsConvertedString("default_test.sumtwonum(max(case when default_test.sumtwonum(A.account_id,1)= 0 then 'adf.d kylin.adfad adf' else 'ky.lin' end), A.seller_id)",
                "default_test.sumtwonum(max(case when default_test.sumtwonum(A_0_DOT_0_account_id,1)= 0 then 'adf.d kylin.adfad adf' else 'ky.lin' end), A_0_DOT_0_seller_id)"));
    }

    private boolean isEqualsConvertedString(String original, String converted) {
        return converted.equals(NSparkCubingUtil.convertFromDot(original));
    }
}
