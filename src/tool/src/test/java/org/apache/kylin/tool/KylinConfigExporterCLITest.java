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
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;

public class KylinConfigExporterCLITest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        createTestMetadata("src/test/resources/tool/config_exporter_cli");
    }

    @Test
    public void testExporterCLI() throws IOException {
        KapConfig kapConfig = KapConfig.wrap(getTestConfig());
        Assert.assertEquals("123", kapConfig.getColumnarSparkConf("spark.executor.cores"));
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        File exportFile = new File(mainDir, ".export_log");
        KylinConfigExporterCLI.execute(new String[] { exportFile.getAbsolutePath() });
        String result = FileUtils.readFileToString(exportFile, StandardCharsets.UTF_8);

        int totalSize = kapConfig.getKylinConfig().exportToProperties().size();
        int exportSize = result.split("\n").length;
        Assert.assertEquals(totalSize, exportSize);
    }

    @Test
    public void testIllegalArgument() throws IOException {
        KapConfig kapConfig = KapConfig.wrap(getTestConfig());
        Assert.assertEquals("123", kapConfig.getColumnarSparkConf("spark.executor.cores"));
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> KylinConfigExporterCLI.execute(new String[] { "as", "asdasd" }));
    }

    @Test
    public void testDuplicateExport() throws IOException {
        KapConfig kapConfig = KapConfig.wrap(getTestConfig());
        Assert.assertEquals("123", kapConfig.getColumnarSparkConf("spark.executor.cores"));
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        File exportFile = new File(mainDir, ".export_log");
        String[] args = { exportFile.getAbsolutePath() };
        KylinConfigExporterCLI.execute(args);
        Assert.assertThrows(IllegalStateException.class, () -> KylinConfigExporterCLI.execute(args));
    }
}
