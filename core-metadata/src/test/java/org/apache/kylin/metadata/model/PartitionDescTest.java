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

package org.apache.kylin.metadata.model;

import java.io.IOException;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PartitionDescTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCustomYearMonthDayFieldPartitionConditionBuilder() throws IOException {
        DataModelManager dataModelManager = DataModelManager.getInstance(getTestConfig());
        DataModelDesc model = dataModelManager.getDataModelDesc("test_kylin_inner_join_model_desc");
        PartitionDesc yearMonthDayPartitionDesc = new PartitionDesc();
        yearMonthDayPartitionDesc.setPartitionDateColumn(
                "TEST_KYLIN_FACT.LSTG_SITE_ID,TEST_KYLIN_FACT.LEAF_CATEG_ID,TEST_KYLIN_FACT.SELLER_ID");
        yearMonthDayPartitionDesc.setPartitionConditionBuilderClz(
                "org.apache.kylin.metadata.model.PartitionDesc$CustomYearMonthDayFieldPartitionConditionBuilder");
        model.setPartitionDesc(yearMonthDayPartitionDesc);
        dataModelManager.updateDataModelDesc(model);
        PartitionDesc.CustomYearMonthDayFieldPartitionConditionBuilder builder = (PartitionDesc.CustomYearMonthDayFieldPartitionConditionBuilder) model
                .getPartitionDesc().getPartitionConditionBuilder();
        String dateRangeCondition = builder.buildDateRangeCondition(model.getPartitionDesc(), null,
                new SegmentRange(0l, 86400000l), null);
        Assert.assertEquals(
                "CONCAT(TEST_KYLIN_FACT.LSTG_SITE_ID,'-',TEST_KYLIN_FACT.LEAF_CATEG_ID,'-',TEST_KYLIN_FACT.SELLER_ID) < '1970-01-02'",
                dateRangeCondition);
    }

}
