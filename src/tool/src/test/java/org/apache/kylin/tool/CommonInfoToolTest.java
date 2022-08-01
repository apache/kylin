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
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class CommonInfoToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testExportClientInfo() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        CommonInfoTool.exportClientInfo(mainDir);
        File linuxDir = new File(mainDir, "client/linux");
        Assert.assertTrue(new File(linuxDir, "disk_usage").exists() && new File(linuxDir, "disk_usage").length() > 200);
        Assert.assertTrue(new File(mainDir, "client/hadoop").exists());
        Assert.assertTrue(new File(mainDir, "client/hive").exists());
        Assert.assertTrue(new File(mainDir, "client/kerberos").exists());
    }

    @Test
    public void testExportHadoopEnv() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        CommonInfoTool.exportHadoopEnv(mainDir);

        File hadoopEnv = new File(mainDir, "hadoop_env");
        Assert.assertTrue(hadoopEnv.exists() && hadoopEnv.length() > 20);
    }

    @Test
    public void testExportKylinHomeDir() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        CommonInfoTool.exportKylinHomeDir(mainDir);

        File catalogInfo = new File(mainDir, "catalog_info");
        Assert.assertTrue(catalogInfo.exists() && catalogInfo.length() > 20);
    }

}
