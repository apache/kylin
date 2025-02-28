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

package org.apache.kylin.metadata.cube.model;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class NSegmentConfigHelperTest extends NLocalFileMetadataTestCase {
    private String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetSegmentConfig() {

        val model = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        // 1. MODEL_BASED && model segmentConfig is empty, get project segmentConfig
        val dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        dataModelManager.updateDataModel(model, copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });
        SegmentConfig segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);

        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(4, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());

        // 2. MODEL_BASED && model segmentConfig is not empty, get mergedSegmentConfig of project segmentConfig and model SegmentConfig
        dataModelManager.updateDataModel(model, copyForWrite -> {
            copyForWrite.setSegmentConfig(
                    new SegmentConfig(false, Lists.newArrayList(AutoMergeTimeEnum.WEEK), null, null, false));
        });
        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(1, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());
    }

}
