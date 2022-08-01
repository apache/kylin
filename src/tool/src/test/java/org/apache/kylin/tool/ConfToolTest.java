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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.tool.util.ToolUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class ConfToolTest extends NLocalFileMetadataTestCase {

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
    public void testExtractConf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());

        File confDir = new File(ToolUtil.getKylinHome(), "conf");
        if (!confDir.exists()) {
            FileUtils.forceMkdir(confDir);
        }

        File configFile = new File(confDir, "a.conf");
        FileUtils.writeStringToFile(configFile, "a=1");

        ConfTool.extractConf(mainDir);

        FileUtils.deleteQuietly(configFile);

        File newConfDir = new File(mainDir, "conf");
        Assert.assertTrue(newConfDir.exists());
        Assert.assertTrue(new File(newConfDir, "a.conf").exists());
    }

    @Test
    public void testExtractHadoopConf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());

        File confDir = new File(ToolUtil.getKylinHome(), "hadoop_conf");
        if (!confDir.exists()) {
            FileUtils.forceMkdir(confDir);
        }

        File writeConfDir = new File(ToolUtil.getKylinHome(), "write_hadoop_conf");
        if (!writeConfDir.exists()) {
            FileUtils.forceMkdir(writeConfDir);
        }

        File configFile = new File(confDir, "a.conf");
        FileUtils.writeStringToFile(configFile, "a=1");

        File configFile2 = new File(writeConfDir, "b.conf");
        FileUtils.writeStringToFile(configFile2, "b=2");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String originValue = kylinConfig.getBuildConf();
        kylinConfig.setProperty("kylin.engine.submit-hadoop-conf-dir", writeConfDir.getAbsolutePath());
        ConfTool.extractHadoopConf(mainDir);
        kylinConfig.setProperty("kylin.engine.submit-hadoop-conf-dir", originValue);
        FileUtils.deleteQuietly(configFile);
        FileUtils.deleteQuietly(configFile2);

        File newConfDir = new File(mainDir, "hadoop_conf");
        Assert.assertTrue(newConfDir.exists());
        Assert.assertTrue(new File(newConfDir, "a.conf").exists());

        File newWriteConfDir = new File(mainDir, "write_hadoop_conf");
        Assert.assertTrue(newWriteConfDir.exists());
        Assert.assertTrue(new File(newWriteConfDir, "b.conf").exists());
    }

    @Test
    public void testExtractBin() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());

        File confDir = new File(ToolUtil.getKylinHome(), "bin");
        if (!confDir.exists()) {
            FileUtils.forceMkdir(confDir);
        }

        List<File> clearFileList = new ArrayList<>();
        File binFile = new File(confDir, "kylin.sh");
        if (!binFile.exists()) {
            clearFileList.add(binFile);
            FileUtils.writeStringToFile(binFile, "a=1");
        }

        File bootBinFile = new File(confDir, "bootstrap.sh");
        if (!bootBinFile.exists()) {
            clearFileList.add(bootBinFile);
            FileUtils.writeStringToFile(bootBinFile, "a=1");
        }

        ConfTool.extractBin(mainDir);

        for (File file : clearFileList) {
            FileUtils.deleteQuietly(file);
        }

        File newConfDir = new File(mainDir, "bin");
        Assert.assertTrue(newConfDir.exists());
        Assert.assertTrue(new File(newConfDir, "kylin.sh").exists());
        Assert.assertFalse(new File(newConfDir, "bootstrap.sh").exists());
    }
}
