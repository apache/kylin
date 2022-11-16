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

import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_TIMESTAMP_COMPARE;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.tool.constant.SensitiveConfigKeysConstant;
import org.apache.kylin.tool.obf.KylinConfObfuscatorTest;
import org.apache.kylin.tool.snapshot.SnapshotSourceTableStatsTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import lombok.val;
import lombok.var;

public class DiagClientToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        KylinConfObfuscatorTest.prepare();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        DiagClientTool diagClientTool = new DiagClientTool();

        getTestConfig().setProperty("kylin.diag.task-timeout", "180s");
        long start = System.currentTimeMillis();
        diagClientTool.execute(new String[] { "-destDir", mainDir.getAbsolutePath() });
        long duration = System.currentTimeMillis() - start;
        Assert.assertTrue(
                "In theory, the running time of this case should not exceed two minutes. "
                        + "If other data is added subsequently, which causes the running time of the "
                        + "diagnostic package to exceed two minutes, please adjust this test.",
                duration < 2 * 60 * 1000);

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if (!file2.getName().contains("_full_") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testExecuteWithInvalidParameter() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        try {
            DiagClientTool diagClientTool = new DiagClientTool();
            diagClientTool.execute(new String[] { "-destDir", mainDir.getAbsolutePath(), "-startTime", "1604999712000",
                    "-endTime", "1604998712000" });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof KylinException);
            Assert.assertEquals(e.getCause().getMessage(), PARAMETER_TIMESTAMP_COMPARE.getMsg());
        }

    }

    @Test
    public void testDestDirNotExist() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File existDir = new File(mainDir, "existDir");
        FileUtils.forceMkdir(existDir);
        File notExistDir = new File(mainDir, "notExistDir");

        DiagClientTool diagClientTool = new DiagClientTool();
        diagClientTool.execute(new String[] { "-destDir", existDir.getAbsolutePath() });
        diagClientTool.execute(new String[] { "-destDir", notExistDir.getAbsolutePath() });

        String existDirFileName = existDir.listFiles()[0].listFiles()[0].getName();
        String notExistDirFileName = notExistDir.listFiles()[0].listFiles()[0].getName();
        Assert.assertTrue(existDirFileName.endsWith(".zip"));
        Assert.assertTrue(notExistDirFileName.endsWith(".zip"));
    }

    @Test
    public void testObf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        DiagClientTool diagClientTool = new DiagClientTool();

        diagClientTool.execute(new String[] { "-destDir", mainDir.getAbsolutePath() });
        File zipFile = mainDir.listFiles()[0].listFiles()[0];
        File exportFile = new File(mainDir, "output");
        FileUtils.forceMkdir(exportFile);
        ZipFileUtils.decompressZipFile(zipFile.getAbsolutePath(), exportFile.getAbsolutePath());
        File baseDiagFile = exportFile.listFiles()[0];
        val properties = org.apache.kylin.common.util.FileUtils
                .readFromPropertiesFile(new File(baseDiagFile, "conf/kylin.properties"));
        Assert.assertTrue(properties.containsValue(SensitiveConfigKeysConstant.HIDDEN));

    }

    @Test
    public void testExportSourceTableStats() {
        DiagClientTool diagClientTool = new DiagClientTool();
        var result = new AtomicBoolean(false);
        try (val mockedStatic = Mockito.mockStatic(SnapshotSourceTableStatsTool.class)) {
            mockedStatic.when(() -> SnapshotSourceTableStatsTool.extractSnapshotAutoUpdate(Mockito.any()))
                    .thenReturn(true);
            diagClientTool.executorService = Executors.newScheduledThreadPool(1);
            diagClientTool.taskQueue = new LinkedBlockingQueue<>();
            diagClientTool.extractSnapshotAutoUpdate(new File("test"), new File("test"));
            result.set(true);
        } finally {
            if (diagClientTool.executorService != null) {
                ExecutorServiceUtil.shutdownGracefully(diagClientTool.executorService, 60);
            }
        }
        Assert.assertTrue(result.get());
    }
}
