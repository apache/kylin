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

package org.apache.kylin.tool.garbage;

import static org.apache.kylin.common.KylinConfigBase.PATH_DELIMITER;

import java.util.List;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.metadata.sourceusage.SourceUsageRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SourceUsageCleanerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    private SourceUsageManager manager;

    private SourceUsageCleaner sourceUsageCleaner;

    @Before
    public void init() {
        createTestMetadata();
        manager = SourceUsageManager.getInstance(getTestConfig());
        sourceUsageCleaner = new SourceUsageCleaner();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testCleanupOnlyOneSourceUsage() {
        SourceUsageRecord record = new SourceUsageRecord();
        record.setCreateTime(0);
        manager.updateSourceUsage(record);
        List<SourceUsageRecord> allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
        sourceUsageCleaner.cleanup();
        allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
    }

    @Test
    public void testCleanupSourceUsages() {
        SourceUsageRecord record = new SourceUsageRecord();
        record.setCreateTime(0);
        SourceUsageRecord record1 = new SourceUsageRecord();
        record1.setResPath(ResourceStore.HISTORY_SOURCE_USAGE + PATH_DELIMITER + "aaa.json");
        record1.setCreateTime(1);
        manager.updateSourceUsage(record);
        manager.updateSourceUsage(record1);
        List<SourceUsageRecord> allRecords = manager.getAllRecords();
        Assert.assertEquals(2, allRecords.size());
        sourceUsageCleaner.cleanup();
        allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
        Assert.assertEquals(1, allRecords.get(0).getCreateTime());
    }

    @Test
    public void testCleanupUnexpiredSourceUsage() {
        SourceUsageRecord record = new SourceUsageRecord();
        record.setCreateTime(System.currentTimeMillis());
        manager.updateSourceUsage(record);
        List<SourceUsageRecord> allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
        sourceUsageCleaner.cleanup();
        allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
    }

    @Test
    public void testCleanupZeroSourceUsage() {
        List<SourceUsageRecord> allRecords = manager.getAllRecords();
        sourceUsageCleaner.cleanup();
        Assert.assertEquals(0, allRecords.size());
    }

}
