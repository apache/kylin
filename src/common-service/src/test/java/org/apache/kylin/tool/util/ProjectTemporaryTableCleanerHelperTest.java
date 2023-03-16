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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class ProjectTemporaryTableCleanerHelperTest extends NLocalFileMetadataTestCase {
    private final String TRANSACTIONAL_TABLE_NAME_SUFFIX = "_hive_tx_intermediate";
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
    public void testCollectDropDBTemporaryTableCmd() {
        String project = "default";
        {
            Set<String> tempTables = Sets.newHashSet();
            tempTables.add("SSB.CUSTOMER" + TRANSACTIONAL_TABLE_NAME_SUFFIX + "21f2e3a73312");
            String result = tableCleanerHelper.getDropTmpTableCmd(project, tempTables);
            Assert.assertFalse(result.isEmpty());
        }
        {
            Set<String> tempTables = Sets.newHashSet();
            tempTables.add("SSB.CUSTOMER");
            String result = tableCleanerHelper.getDropTmpTableCmd(project, tempTables);
            Assert.assertFalse(result.isEmpty());
        }
        {
            Set<String> tempTables = Sets.newHashSet();
            String result = tableCleanerHelper.getDropTmpTableCmd(project, tempTables);
            Assert.assertTrue(result.isEmpty());
        }
        {
            Set<String> tempTables = Sets.newHashSet();
            tempTables.add("SSB.CUSTOMER_TEST");
            String result = tableCleanerHelper.getDropTmpTableCmd(project, tempTables);
            Assert.assertFalse(result.isEmpty());
        }
    }

    @Test
    public void testGetJobTransactionalTable() throws IOException {
        String project = "default";
        String jobId = "job-5c5851ef8544";
        {
            Set<String> tables = tableCleanerHelper.getJobTransactionalTable(project, jobId);
            Assert.assertTrue(tables.isEmpty());
        }
        {
            createHDFSFile(project, jobId);
            Set<String> tables = tableCleanerHelper.getJobTransactionalTable(project, jobId);
            Assert.assertFalse(tables.isEmpty());
        }
    }

    private void createHDFSFile(String project, String jobId) throws IOException {
        KylinConfig config = getTestConfig();
        String dir = config.getJobTmpTransactionalTableDir(project, jobId);
        Path path = new Path(dir);
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
            fileSystem.setPermission(path, new FsPermission((short) 00777));
            path = new Path(dir + "/SSB.CUSTOMER");
            fileSystem.createNewFile(path);
            path = new Path(dir + "/SSB.CUSTOMER_HIVE_TX_INTERMEDIATE5c5851ef8544");
            fileSystem.createNewFile(path);
        }
    }
}
