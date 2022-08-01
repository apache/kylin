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

package org.apache.kylin.tool;

import java.util.stream.Collectors;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.tool.garbage.DataflowCleanerCLI;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;

public class DataflowCleanerCLITest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void testDataflowCleaner() {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        var dataflow = dataflowManager.getDataflow(MODEL_ID);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var indexPlan = dataflow.getIndexPlan();
        indexPlanManager.updateIndexPlan(indexPlan.getId(),
                copyForWrite -> copyForWrite.removeLayouts(Sets.newHashSet(10001L, 10002L), true, false));

        for (NDataSegment segment : dataflow.getSegments()) {
            var layouts = segment.getSegDetails().getLayouts().stream().map(NDataLayout::getLayoutId)
                    .collect(Collectors.toList());
            Assert.assertTrue(layouts.contains(10001L));
            Assert.assertTrue(layouts.contains(10002L));
        }
        DataflowCleanerCLI.execute();

        dataflow = dataflowManager.getDataflow(MODEL_ID);
        for (NDataSegment segment : dataflow.getSegments()) {
            var layouts = segment.getSegDetails().getLayouts().stream().map(NDataLayout::getLayoutId)
                    .collect(Collectors.toList());
            Assert.assertFalse(layouts.contains(10001L));
            Assert.assertFalse(layouts.contains(10002L));
        }
    }

}
