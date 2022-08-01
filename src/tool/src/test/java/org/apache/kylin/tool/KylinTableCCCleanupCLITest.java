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

import java.util.Arrays;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KylinTableCCCleanupCLITest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/table_cc_cleanup");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testCleanupWithHelp() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "AL_4144");
        TableDesc tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        new KylinTableCCCleanupCLI().execute(new String[] { "-h" });

        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));
    }

    @Test
    public void testCleanupWithCleanup() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "AL_4144");
        TableDesc tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        new KylinTableCCCleanupCLI().execute(new String[] { "-c" });

        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertFalse(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));
    }

    @Test
    public void testCleanupWithCleanupAndProject() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "AL_4144");
        TableDesc tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        new KylinTableCCCleanupCLI().execute(new String[] { "--cleanup", "-projects=default" });

        // still exists
        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        // still exists
        new KylinTableCCCleanupCLI().execute(new String[] { "-projects=AL_4144" });

        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        // not exists
        new KylinTableCCCleanupCLI().execute(new String[] { "--cleanup", "-projects=AL_4144" });

        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertFalse(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));
    }
}
