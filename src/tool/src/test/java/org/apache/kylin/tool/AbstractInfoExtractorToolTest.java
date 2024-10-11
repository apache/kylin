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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.tool.util.ToolUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.springframework.jdbc.core.JdbcTemplate;

import lombok.val;

public class AbstractInfoExtractorToolTest extends NLocalFileMetadataTestCase {
    private JdbcTemplate jdbcTemplate;
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        JobContextUtil.cleanUp();
        createTestMetadata();
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        JobContextUtil.getJobInfoDao(getTestConfig());
    }

    @After
    public void teardown() {
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("SHUTDOWN;");
        }

        JobContextUtil.cleanUp();

        cleanupTestMetadata();
    }

    @Test
    public void testGetOptions() throws ParseException {
        MockInfoExtractorTool mock = new MockInfoExtractorTool();
        OptionsHelper optionsHelper = new OptionsHelper();
        optionsHelper.parseOptions(mock.getOptions(), new String[] { "-destDir", "output", "-startTime", "2000" });
        Assert.assertEquals("output",
                mock.getStringOption(optionsHelper, MockInfoExtractorTool.OPTION_DEST, "destDir"));
        Assert.assertTrue(mock.getBooleanOption(optionsHelper, MockInfoExtractorTool.OPTION_COMPRESS, true));
        Assert.assertEquals(2000, mock.getLongOption(optionsHelper, MockInfoExtractorTool.OPTION_START_TIME, 1000L));

        Option OPTION_THREADS = OptionBuilder.getInstance().withArgName("threads").hasArg().isRequired(false)
                .withDescription("Specify number of threads for parallel extraction.").create("threads");
        Assert.assertEquals(4, mock.getLongOption(optionsHelper, OPTION_THREADS, 4));
    }

    @Test
    public void testAddFile() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        MockInfoExtractorTool mock = new MockInfoExtractorTool();
        mock.addFile(new File(ToolUtil.getKylinHome(), "kylin.properties"), mainDir);
        Assert.assertTrue(new File(mainDir, "kylin.properties").exists());
    }

    @Test
    public void testAddShellOutput() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        MockInfoExtractorTool mock = new MockInfoExtractorTool();
        mock.addShellOutput("echo \"hello world\"", mainDir, "hello");
        mock.addShellOutput("echo \"hello java\"", mainDir, "hello", true);

        Assert.assertTrue(new File(mainDir, "hello").exists());
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "hello")).startsWith("hello world"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "hello")).contains("hello java"));
    }

    @Test
    public void TestExtractCommitFile() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        overwriteSystemProp("KYLIN_HOME", mainDir.getAbsolutePath());
        MockInfoExtractorTool mock = new MockInfoExtractorTool();

        List<File> clearFileList = new ArrayList<>();
        File commitFile = new File(KylinConfig.getKylinHome(), "commit_SHA1");
        if (!commitFile.exists()) {
            String sha1 = "6a38664fe087f7f466ec4ad9ac9dc28415d99e52@KAP\nBuild with MANUAL at 2019-08-31 20:02:22";
            FileUtils.writeStringToFile(commitFile, sha1);
            clearFileList.add(commitFile);
        }

        File output = new File(mainDir, "output");
        FileUtils.forceMkdir(output);

        mock.extractCommitFile(output);

        for (File file : clearFileList) {
            FileUtils.deleteQuietly(file);
        }
        Assert.assertTrue(new File(output, "commit_SHA1").exists());
        Assert.assertTrue(FileUtils.readFileToString(new File(output, "commit_SHA1")).contains("6a38664fe087f7f4"));
    }

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        MockInfoExtractorTool mock = new MockInfoExtractorTool();

        mock.execute(new String[] { "-destDir", mainDir.getAbsolutePath(), "-systemProp", "true" });

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if (!file2.getName().contains("_base_") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testExportOtherMetadata() throws IOException, InterruptedException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        MockInfoExtractorTool mock = new MockInfoExtractorTool();
        ExecutablePO po = createJob();

        mock.init(mainDir);
        doExport(mock, mainDir, null, null);
        mock.waitAndClose();
        doCheck(mainDir);

        FileUtils.cleanDirectory(mainDir);
        Assert.assertEquals(0, mainDir.listFiles().length);

        mock.init(mainDir);
        doExport(mock, mainDir, po.getProject(), po.getId());
        mock.waitAndClose();
        doCheck(mainDir);
    }

    private void doExport(MockInfoExtractorTool tool, File mainDir, String project, String jobId) {
        File recordTime = new File(mainDir, "recordTime");
        if (project == null) {
            tool.exportJobInfo(0, System.currentTimeMillis(), recordTime);
        } else {
            tool.exportJobInfo(project, jobId, recordTime);
        }
        tool.exportFavoriteRule(project, recordTime);
        tool.exportAsyncTask(project, recordTime);
        tool.exportQueryHistoryOffset(project, recordTime);
    }

    private void doCheck(File mainDir) {
        File jobInfoDir = new File(mainDir, "job_info");
        Assert.assertTrue(jobInfoDir.listFiles().length >= 2);
        File favoriteRuleDir = new File(mainDir, "favorite_rule");
        Assert.assertTrue(favoriteRuleDir.listFiles().length >= 1);
        File asyncTaskDir = new File(mainDir, "async_task");
        Assert.assertTrue(asyncTaskDir.listFiles().length >= 1);
        File queryHistoryOffsetDir = new File(mainDir, "query_history_offset");
        Assert.assertTrue(queryHistoryOffsetDir.listFiles().length >= 1);
    }

    public ExecutablePO createJob() {
        DefaultExecutable job = new DefaultExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309gg");
        job.setProject("default");
        job.setPriority(1);
        val po = ExecutableManager.toPO(job, "default");
        return JobContextUtil.getJobInfoDao(getTestConfig()).addJob(po);
    }

    static class MockInfoExtractorTool extends AbstractInfoExtractorTool {

        @Override
        protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
            // Do nothing.
        }

        public void init(File dir) {
            executorService = Executors.newScheduledThreadPool(2);
            timerExecutorService = Executors.newScheduledThreadPool(2);
            taskQueue = new LinkedBlockingQueue<>();
            taskStartTime = new ConcurrentHashMap<>();
            exportDir = dir;
        }

        public void waitAndClose() throws InterruptedException {
            executorService.shutdown();
            awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());
        }
    }

}
