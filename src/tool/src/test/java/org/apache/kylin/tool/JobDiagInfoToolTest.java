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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.tool.constant.SensitiveConfigKeysConstant;
import org.apache.kylin.tool.obf.KylinConfObfuscatorTest;
import org.apache.kylin.tool.snapshot.SnapshotSourceTableStatsTool;
import org.apache.kylin.tool.util.JobMetadataWriter;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import lombok.val;
import lombok.var;

public class JobDiagInfoToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        KylinConfObfuscatorTest.prepare();
    }

    public void createTestMetadata(String... overlay) {
        super.createTestMetadata(overlay);
        JobMetadataWriter.writeJobMetaData(getTestConfig());
    }

    @After
    public void tearDown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testGetJobByJobId() {
        val job = new JobDiagInfoTool().getJobByJobId("dd5a6451-0743-4b32-b84d-2ddc8052429f");
        Assert.assertEquals("newten", job.getProject());
        Assert.assertEquals(1574130051721L, job.getOutput().getCreateTime());
        Assert.assertEquals(1574818631319L, job.getOutput().getEndTime());
    }

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        getTestConfig().setProperty("kylin.diag.task-timeout", "180s");
        long start = System.currentTimeMillis();
        new JobDiagInfoTool().execute(
                new String[] { "-job", "dd5a6451-0743-4b32-b84d-2ddc8052429f", "-destDir", mainDir.getAbsolutePath() });
        long duration = System.currentTimeMillis() - start;
        Assert.assertTrue(
                "In theory, the running time of this case should not exceed two minutes. "
                        + "If other data is added subsequently, which causes the running time of the "
                        + "diagnostic package to exceed two minutes, please adjust this test.",
                duration < 2 * 60 * 1000);

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if (!file2.getName().contains("job") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
                HashSet<String> appFiles = new HashSet<>();
                val zipFile = new ZipFile(file2);
                zipFile.stream().map(ZipEntry::getName).filter(file -> (file.contains("job_history/application_")))
                        .forEach(appFiles::add);
                Assert.assertEquals(3, appFiles.size());
            }
        }

    }

    @Test
    public void testExecuteWithFalseIncludeMeta() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        // includeMeta false
        new JobDiagInfoTool().execute(new String[] { "-job", "dd5a6451-0743-4b32-b84d-2ddc8052429f", "-destDir",
                mainDir.getAbsolutePath(), "-includeMeta", "false" });

        boolean hasMetadataFile = new ZipFile(
                Objects.requireNonNull(Objects.requireNonNull(mainDir.listFiles())[0].listFiles())[0]).stream()
                .anyMatch(zipEntry -> zipEntry.getName().contains("metadata"));

        Assert.assertFalse(hasMetadataFile);
    }

    @Test
    public void testExecuteWithDefaultIncludeMeta() throws IOException {
        // default includeMeta(true)
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        new JobDiagInfoTool().execute(
                new String[] { "-job", "dd5a6451-0743-4b32-b84d-2ddc8052429f", "-destDir", mainDir.getAbsolutePath() });

        boolean hasMetadataFile = new ZipFile(
                Objects.requireNonNull(Objects.requireNonNull(mainDir.listFiles())[0].listFiles())[0]).stream()
                .anyMatch(zipEntry -> zipEntry.getName().contains("metadata"));
        Assert.assertTrue(hasMetadataFile);
    }

    @Test
    public void testWithNotExistsJobId() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        thrown.expect(new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof Exception)) {
                    return false;
                }

                Throwable e = ((Exception) o).getCause();

                if (!e.getClass().equals(KylinRuntimeException.class)) {
                    return false;
                }

                if (!e.getMessage().equals("Can not find the jobId: 9462fee8-e6cd-4d18-a5fc-b598a3c5edb5")) {
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        });
        new JobDiagInfoTool().execute(
                new String[] { "-job", "9462fee8-e6cd-4d18-a5fc-b598a3c5edb5", "-destDir", mainDir.getAbsolutePath() });

    }

    @Test
    public void testObf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        new JobDiagInfoTool().execute(
                new String[] { "-job", "dd5a6451-0743-4b32-b84d-2ddc8052429f", "-destDir", mainDir.getAbsolutePath() });
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
    public void exportSourceTableStats() throws IOException {
        val projectName = RandomUtil.randomUUIDStr();
        val config = KylinConfig.getInstanceFromEnv();
        val testOverride = Maps.<String, String> newLinkedHashMap();
        val projectManager = NProjectManager.getInstance(config);
        projectManager.createProject(projectName, "test", null, testOverride);
        val projectInstance = projectManager.getProject(projectName);
        val job = new DefaultExecutable();

        val jobDiagInfoTool = new JobDiagInfoTool();
        var result = jobDiagInfoTool.exportSourceTableStats(new File("test"), new File("test"), projectName, job);
        Assert.assertFalse(result);

        testOverride.put("kylin.snapshot.manual-management-enabled", "true");
        testOverride.put("kylin.snapshot.auto-refresh-enabled", "true");
        projectManager.updateProject(projectInstance, projectName, projectInstance.getDescription(), testOverride);
        job.setJobType(JobTypeEnum.INDEX_MERGE);
        result = jobDiagInfoTool.exportSourceTableStats(new File("test"), new File("test"), projectName, job);
        Assert.assertFalse(result);

        job.setJobType(JobTypeEnum.INC_BUILD);
        try (val mockedStatic = Mockito.mockStatic(SnapshotSourceTableStatsTool.class)) {
            mockedStatic.when(() -> SnapshotSourceTableStatsTool.extractSourceTableStats(Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any())).thenReturn(true);
            jobDiagInfoTool.executorService = Executors.newScheduledThreadPool(1);
            jobDiagInfoTool.taskQueue = new LinkedBlockingQueue<>();
            result = jobDiagInfoTool.exportSourceTableStats(new File("test"), new File("test"), projectName, job);
            Assert.assertTrue(result);
        } finally {
            if (jobDiagInfoTool.executorService != null) {
                ExecutorServiceUtil.shutdownGracefully(jobDiagInfoTool.executorService, 60);
            }
        }
    }

    @Test
    public void testGetDifferentConfigLevel() {
        final String PROJECT = "default";
        final String KYLIN_SPARK_EVENTLOG_DIR = "kylin.engine.spark-conf.spark.eventLog.dir";
        final String SPARK_EVENTLOG_DIR = "spark.eventLog.dir";
        JobDiagInfoTool tool = new JobDiagInfoTool();

        // Update model config
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        indexPlanManager.updateIndexPlan("82fa7671-a935-45f5-8779-85703601f49a", copyForWrite -> {
            LinkedHashMap<String, String> overrideProps = new LinkedHashMap<>();
            overrideProps.put(KYLIN_SPARK_EVENTLOG_DIR, "/path/to/dir1");
            copyForWrite.setOverrideProps(overrideProps);
        });
        // Update project config
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.updateProject(PROJECT, copyForWrite -> {
            LinkedHashMap<String, String> overrideProps = new LinkedHashMap<>();
            overrideProps.put(KYLIN_SPARK_EVENTLOG_DIR, "/path/to/dir2");
            copyForWrite.setOverrideKylinProps(overrideProps);
        });

        // For jobs related to model: index build, index refresh etc.
        KylinConfig modelConfig = tool.getConfigForModelOrProjectLevel("82fa7671-a935-45f5-8779-85703601f49a", PROJECT);
        Assert.assertEquals("/path/to/dir1", modelConfig.getSparkConfigOverride().get(SPARK_EVENTLOG_DIR));

        // For jobs without model: snapshot build, sampling etc.
        KylinConfig projectConfig = tool.getConfigForModelOrProjectLevel("SSB.LINEORDER", PROJECT);
        Assert.assertEquals("/path/to/dir2", projectConfig.getSparkConfigOverride().get(SPARK_EVENTLOG_DIR));
    }
}
