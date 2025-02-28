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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import lombok.val;

public class SampleProjectToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testImportProjectSuccess() throws IOException {
        val project = "broken_test";
        val junitFolder = temporaryFolder.getRoot();
        val junitCoreMetaFolder = new File(junitFolder.getAbsolutePath() + "/core_meta");
        junitCoreMetaFolder.mkdir();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitCoreMetaFolder, MetadataType.ALL.name());
        UnitOfWork.doInTransactionWithRetry(() -> {
            NProjectManager.getInstance(getTestConfig()).forceDropProject(project);
            return true;
        }, project);
        boolean success = true;
        try {
            SampleProjectTool tool = new SampleProjectTool();
            tool.execute(new String[] { "-project", project, "-model", "AUTO_MODEL_TEST_ACCOUNT_1", "-dir",
                    junitFolder.getAbsolutePath() });
        } catch (Exception e) {
            success = false;
        }
        Assert.assertTrue(success);

        FileUtils.deleteDirectory(junitFolder.getAbsoluteFile());
    }

    @Test
    public void testImportProjectFail() throws IOException {
        val project = "broken_test";
        val junitFolder = temporaryFolder.getRoot();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitFolder, MetadataType.ALL.name());
        boolean success = true;
        try {
            SampleProjectTool tool = new SampleProjectTool();
            tool.execute(new String[] { "-project", project, "-model", "AUTO_MODEL_TEST_ACCOUNT_1", "-dir",
                    junitFolder.getAbsolutePath() });
        } catch (Exception e) {
            success = false;
        }
        Assert.assertFalse(success);
        FileUtils.deleteDirectory(junitFolder.getAbsoluteFile());
    }

}
