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

package org.apache.kylin.cube;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DimensionRangeInfoTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testMergeRangeMap() {
        DataModelDesc model = DataModelManager.getInstance(getTestConfig()).getDataModelDesc("ci_inner_join_model");
        String colId = "TEST_KYLIN_FACT.CAL_DT";

        // normal merge
        {
            Map<String, DimensionRangeInfo> m1 = new HashMap<>();
            m1.put(colId, new DimensionRangeInfo("2012-01-01", "2012-05-31"));

            Map<String, DimensionRangeInfo> m2 = new HashMap<>();
            m2.put(colId, new DimensionRangeInfo("2012-06-01", "2013-06-30"));

            DimensionRangeInfo r1 = DimensionRangeInfo.mergeRangeMap(model, m1, m2).get(colId);
            Assert.assertEquals("2012-01-01", r1.getMin());
            Assert.assertEquals("2013-06-30", r1.getMax());
        }
        
        // missing column on one side
        {
            Map<String, DimensionRangeInfo> m1 = new HashMap<>();
            m1.put(colId, new DimensionRangeInfo("2012-01-01", "2012-05-31"));

            Map<String, DimensionRangeInfo> m2 = new HashMap<>();

            Assert.assertTrue(DimensionRangeInfo.mergeRangeMap(model, m1, m2).isEmpty());
        }
        
        // null min/max value (happens on empty segment, or all-null columns)
        {
            Map<String, DimensionRangeInfo> m1 = new HashMap<>();
            m1.put(colId, new DimensionRangeInfo(null, null));

            Map<String, DimensionRangeInfo> m2 = new HashMap<>();
            m2.put(colId, new DimensionRangeInfo("2012-06-01", "2013-06-30"));

            DimensionRangeInfo r1 = DimensionRangeInfo.mergeRangeMap(model, m1, m2).get(colId);
            Assert.assertEquals("2012-06-01", r1.getMin());
            Assert.assertEquals("2013-06-30", r1.getMax());
        }
        
    }
}
