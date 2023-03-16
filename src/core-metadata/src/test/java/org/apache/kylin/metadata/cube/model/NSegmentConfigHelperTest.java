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

import org.apache.kylin.metadata.model.SegmentConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.RetentionRange;
import org.apache.kylin.metadata.model.VolatileRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;
import lombok.var;

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
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
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

        // 3. TABLE_ORIENTED && model segmentConfig is empty, dataLoadingRange segmentConfig is empty, get project segmentConfig
        dataModelManager.updateDataModel(model, copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.TABLE_ORIENTED);
            copyForWrite.setSegmentConfig(new SegmentConfig());
        });

        var dataLoadingRange = new NDataLoadingRange();
        val dataModel = dataModelManager.getDataModelDesc(model);
        dataLoadingRange.setColumnName(dataModel.getPartitionDesc().getPartitionDateColumn());
        dataLoadingRange.setTableName(dataModel.getRootFactTableName());
        dataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(4, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());

        // 4. TABLE_ORIENTED && model segmentConfig is empty, dataLoadingRange segmentConfig is not empty, get mergedSegmentConfig of project segmentConfig and dataLoadingRange SegmentConfig
        var copy = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
        copy.setSegmentConfig(new SegmentConfig(false, Lists.newArrayList(AutoMergeTimeEnum.WEEK), null, null, false));
        dataLoadingRange = dataLoadingRangeManager.updateDataLoadingRange(copy);

        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(1, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());

        // 5. TABLE_ORIENTED && model segmentConfig is not empty, dataLoadingRange segmentConfig is not empty, get mergedSegmentConfig of project segmentConfig and dataLoadingRange SegmentConfig and model segmentConfig
        dataModelManager.updateDataModel(model, copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.TABLE_ORIENTED);
            copyForWrite.setSegmentConfig(
                    new SegmentConfig(false, Lists.newArrayList(AutoMergeTimeEnum.WEEK), null, null, false));
        });

        copy = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
        copy.setSegmentConfig(new SegmentConfig(null, null, new VolatileRange(1, true, AutoMergeTimeEnum.DAY),
                new RetentionRange(2, true, AutoMergeTimeEnum.DAY), false));
        dataLoadingRange = dataLoadingRangeManager.updateDataLoadingRange(copy);

        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(1, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(1L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(true, segmentConfig.getRetentionRange().isRetentionRangeEnabled());

        // 6. TABLE_ORIENTED && model segmentConfig is not empty, dataLoadingRange segmentConfig is empty, get mergedSegmentConfig of project segmentConfig and model segmentConfig
        copy = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
        copy.setSegmentConfig(new SegmentConfig());
        dataLoadingRangeManager.updateDataLoadingRange(copy);

        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(1, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());
    }

}
