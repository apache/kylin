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
package org.apache.kylin.tool.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.tool.garbage.StorageCleaner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;

public class ProjectTemporaryTableCleanerHelperTest extends NLocalFileMetadataTestCase {
    private ProjectTemporaryTableCleanerHelper tableCleanerHelper = new ProjectTemporaryTableCleanerHelper();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testIsNeedClean() {
        //  return !isEmptyJobTmp || !isEmptyDiscardJob;
        Assert.assertFalse(tableCleanerHelper.isNeedClean(Boolean.TRUE, Boolean.TRUE));
        Assert.assertTrue(tableCleanerHelper.isNeedClean(Boolean.FALSE, Boolean.TRUE));
        Assert.assertTrue(tableCleanerHelper.isNeedClean(Boolean.TRUE, Boolean.FALSE));
        Assert.assertTrue(tableCleanerHelper.isNeedClean(Boolean.FALSE, Boolean.FALSE));
    }

    @Test
    public void testCollectDropDBTemporaryTableCmd() throws Exception {
        ISourceMetadataExplorer explr = SourceFactory
                .getSource(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("tdh"))
                .getSourceMetadataExplorer();
        List<StorageCleaner.FileTreeNode> jobTemps = Lists.newArrayList();
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_KYLIN_FACT_WITH_INT_DATE"));
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_CATEGORY_GROUPINGS"));
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_SELLER_TYPE_DIM"));
        Set<String> discardJobs = Sets.newConcurrentHashSet();
        discardJobs.add("TEST_KYLIN_FACT_WITH_INT_DATE_123456780");
        discardJobs.add("TEST_CATEGORY_GROUPINGS_123456781");
        discardJobs.add("TEST_SELLER_TYPE_DIM_123456782");

        String result = tableCleanerHelper.collectDropDBTemporaryTableCmd(KylinConfig.getInstanceFromEnv(), explr,
                jobTemps, discardJobs);
        Assert.assertTrue(result.isEmpty());

        Map<String, List<String>> dropTableDbNameMap = Maps.newConcurrentMap();
        tableCleanerHelper.putTableNameToDropDbTableNameMap(dropTableDbNameMap, "DEFAULT", "TEST_CATEGORY_GROUPINGS");
        tableCleanerHelper.putTableNameToDropDbTableNameMap(dropTableDbNameMap, "DEFAULT", "TEST_COUNTRY");
        tableCleanerHelper.putTableNameToDropDbTableNameMap(dropTableDbNameMap, "EDW", "TEST_CAL_DT");
        ProjectTemporaryTableCleanerHelper mockCleanerHelper = Mockito.mock(ProjectTemporaryTableCleanerHelper.class);

        Mockito.when(mockCleanerHelper.collectDropDBTemporaryTableNameMap(explr, jobTemps, discardJobs))
                .thenReturn(dropTableDbNameMap);
        Mockito.when(mockCleanerHelper.collectDropDBTemporaryTableCmd(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenCallRealMethod();
        result = mockCleanerHelper.collectDropDBTemporaryTableCmd(KylinConfig.getInstanceFromEnv(), explr, jobTemps,
                discardJobs);
        Assert.assertFalse(result.isEmpty());
    }

    @Test
    public void testIsMatchesTemporaryTables() {
        List<StorageCleaner.FileTreeNode> jobTemps = Lists.newArrayList();
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_KYLIN_FACT_WITH_INT_DATE"));
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_CATEGORY_GROUPINGS"));
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_SELLER_TYPE_DIM"));
        Set<String> discardJobs = Sets.newConcurrentHashSet();
        discardJobs.add("89289329392823_123456780");
        discardJobs.add("37439483439489_123456781");
        discardJobs.add("12309290434934_123456782");
        Assert.assertTrue(tableCleanerHelper.isMatchesTemporaryTables(jobTemps, discardJobs,
                "TEST_SELLER_TYPE_DIM_hive_tx_intermediate123456780"));
        Assert.assertTrue(tableCleanerHelper.isMatchesTemporaryTables(jobTemps, discardJobs,
                "TEST_CATEGORY_GROUPINGS_hive_tx_intermediate123456781"));
        Assert.assertFalse(tableCleanerHelper.isMatchesTemporaryTables(jobTemps, discardJobs,
                "TEST_SELLER_hive_tx_intermediate123456779"));

    }

}
