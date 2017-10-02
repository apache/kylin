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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

public class DataModelDescTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void loadInnerModel() {
        DataModelDesc model = DataModelManager.getInstance(getTestConfig()).getDataModelDesc("ci_inner_join_model");
        assertNotNull(model);
    }

    @Test
    public void loadLeftModel() {
        DataModelDesc model = DataModelManager.getInstance(getTestConfig()).getDataModelDesc("ci_left_join_model");
        assertNotNull(model);
    }

    @Test
    public void testNoDupColInDimAndMeasure() {
        DataModelDesc model = DataModelManager.getInstance(getTestConfig()).getDataModelDesc("test_kylin_inner_join_model_desc");
        String[] metrics = model.getMetrics();
        TblColRef col = model.findColumn("edw.test_cal_dt.cal_dt");
        assertTrue(metrics.length == 2);
        assertTrue(ArrayUtils.contains(metrics, col.getIdentity()) == false);
    }

    @Test
    public void testGetCopyOf() throws JsonProcessingException {
        DataModelDesc desc = DataModelManager.getInstance(getTestConfig()).getDataModelDesc("test_kylin_inner_join_model_desc");
        DataModelDesc copyDesc = DataModelDesc.getCopyOf(desc);

        // uuid is different, set to equals for json comparison
        copyDesc.setUuid(desc.getUuid());
        copyDesc.setLastModified(desc.getLastModified());

        String descStr = JsonUtil.writeValueAsIndentString(desc);
        String copyStr = JsonUtil.writeValueAsIndentString(copyDesc);

        assertEquals(descStr, copyStr);
    }

    @Test
    public void testPartitionDescCopyOf() throws JsonProcessingException {
        PartitionDesc desc = DataModelManager.getInstance(getTestConfig()).getDataModelDesc("test_kylin_inner_join_model_desc").partitionDesc;
        PartitionDesc copyDesc = PartitionDesc.getCopyOf(desc);

        String descStr = JsonUtil.writeValueAsIndentString(desc);
        String copyStr = JsonUtil.writeValueAsIndentString(copyDesc);

        assertEquals(descStr, copyStr);
    }
}
