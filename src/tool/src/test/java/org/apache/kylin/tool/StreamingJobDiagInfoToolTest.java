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
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;
import org.apache.kylin.tool.constant.SensitiveConfigKeysConstant;
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

import lombok.val;

public class StreamingJobDiagInfoToolTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "streaming_test";
    private static final String JOB_ID_BUILD = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
    private static final String JOB_ID_MERGE = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
    private static final String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public TestName testName = new TestName();
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        JobContextUtil.cleanUp();
        copyConf();
        createStreamingExecutorLog(PROJECT, JOB_ID_BUILD);
        createStreamingDriverLog(PROJECT, JOB_ID_BUILD);
        createCheckpoint(MODEL_ID);
    }

    @After
    public void teardown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    public void createStreamingDriverLog(String project, String jobId) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        File file1_1 = temporaryFolder.newFile("driver." + 1628555550000L + ".log");
        File file1_2 = temporaryFolder.newFile("driver." + 1628555551000L + ".log");
        String jobLogDir1 = String.format(Locale.ROOT, "%s%s",
                KylinConfig.getInstanceFromEnv().getStreamingJobTmpOutputStorePath(project, jobId), "1628555550000");
        Path jobLogDirPath1 = new Path(jobLogDir1);
        fs.mkdirs(jobLogDirPath1);
        fs.copyFromLocalFile(new Path(file1_1.getAbsolutePath()), jobLogDirPath1);
        fs.copyFromLocalFile(new Path(file1_2.getAbsolutePath()), jobLogDirPath1);

        File file2_1 = temporaryFolder.newFile("driver." + 1628555560000L + ".log");
        File file2_2 = temporaryFolder.newFile("driver." + 1628555561000L + ".log");
        String jobLogDir2 = String.format(Locale.ROOT, "%s%s",
                KylinConfig.getInstanceFromEnv().getStreamingJobTmpOutputStorePath(project, jobId), "1628555560000");
        Path jobLogDirPath2 = new Path(jobLogDir2);
        fs.mkdirs(jobLogDirPath2);
        fs.copyFromLocalFile(new Path(file2_1.getAbsolutePath()), jobLogDirPath2);
        fs.copyFromLocalFile(new Path(file2_2.getAbsolutePath()), jobLogDirPath2);

    }

    public void createCheckpoint(String modelId) throws IOException {

        File file = temporaryFolder.newFile("medadata");

        String hdfsStreamCheckpointPath = String.format(Locale.ROOT, "%s%s%s",
                KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectoryWithoutScheme(), "streaming/checkpoint/",
                modelId);

        Path checkPointModelDirPath = new Path(hdfsStreamCheckpointPath);
        Path commitsPath = new Path(String.format(Locale.ROOT, "%s/%s", hdfsStreamCheckpointPath, "commits"));
        Path offsetsPath = new Path(String.format(Locale.ROOT, "%s/%s", hdfsStreamCheckpointPath, "offsets"));
        Path sourcesPath = new Path(String.format(Locale.ROOT, "%s/%s", hdfsStreamCheckpointPath, "sources"));

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        fs.mkdirs(checkPointModelDirPath);
        fs.mkdirs(commitsPath);
        fs.mkdirs(offsetsPath);
        fs.mkdirs(sourcesPath);

        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), checkPointModelDirPath);
    }

    public void createStreamingExecutorLog(String project, String jobId) throws IOException {

        File file = temporaryFolder.newFile("executor-1.1628555560000.log");

        String hdfsStreamLogProjectPath = String.format(Locale.ROOT, "%s%s%s/%s/%s",
                KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectoryWithoutScheme(), "streaming/spark_logs/",
                project, jobId, "1628555560000");

        Path jobLogDirPath = new Path(hdfsStreamLogProjectPath);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        fs.mkdirs(jobLogDirPath);
        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), jobLogDirPath);
    }

    public void copyConf() throws IOException {
        StorageURL metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        File path = new File(metadataUrl.getIdentifier());
        File confDir = new File(path.getParentFile(), "conf");
        FileUtils.forceMkdir(confDir);

        File confFile = new File(path.getParentFile(), "kylin.properties");
        Files.copy(confFile.toPath(), new File(confDir, "kylin.properties").toPath());

    }

    @Test
    public void testGetJobByJobId() {
        StreamingJobDiagInfoTool streamingJobDiagInfoTool = new StreamingJobDiagInfoTool();
        val job = streamingJobDiagInfoTool.getJobById(JOB_ID_BUILD);
        Assert.assertEquals(PROJECT, job.getProject());
        StreamingJobMeta job2 = streamingJobDiagInfoTool.getJobById("");
        Assert.assertNull(job2);
    }

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        getTestConfig().setProperty("kylin.diag.task-timeout", "180s");
        long start = System.currentTimeMillis();
        new StreamingJobDiagInfoTool()
                .execute(new String[] { "-streamingJob", JOB_ID_BUILD, "-destDir", mainDir.getAbsolutePath() });
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
                zipFile.stream().map(ZipEntry::getName)
                        .filter(file -> (file.contains("streaming_spark_logs/spark_") && !file.endsWith(".crc")))
                        .forEach(appFiles::add);
                Assert.assertEquals(9, appFiles.size());
            }
        }

        new StreamingJobDiagInfoTool()
                .execute(new String[] { "-streamingJob", JOB_ID_MERGE, "-destDir", mainDir.getAbsolutePath() });

        thrown.expect(KylinException.class);
        thrown.expectMessage("error parsing args");
        new StreamingJobDiagInfoTool().execute(new String[] { "-destDir", mainDir.getAbsolutePath() });

    }

    @Test
    public void testExecuteAll() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File mainDir2 = new File(temporaryFolder.getRoot(), testName.getMethodName().concat("2"));
        FileUtils.forceMkdir(mainDir2);

        getTestConfig().setProperty("kylin.diag.task-timeout", "180s");
        long start = System.currentTimeMillis();
        DiagClientTool diagClientTool = new DiagClientTool();

        // > 31 day
        diagClientTool.execute(new String[] { "-destDir", mainDir2.getAbsolutePath(), "-startTime", "1604999712000",
                "-endTime", String.valueOf(System.currentTimeMillis()) });

        // over all job start time
        diagClientTool.execute(new String[] { "-destDir", mainDir2.getAbsolutePath(), "-startTime", "1628555591000",
                "-endTime", "1628555961000" });

        // normal
        diagClientTool.execute(new String[] { "-destDir", mainDir.getAbsolutePath(), "-startTime", "1628555560000",
                "-endTime", "1628555560100" });

        long duration = System.currentTimeMillis() - start;
        Assert.assertTrue(
                "In theory, the running time of this case should not exceed two minutes. "
                        + "If other data is added subsequently, which causes the running time of the "
                        + "diagnostic package to exceed two minutes, please adjust this test.",
                duration < 2 * 60 * 1000);

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if (!file2.getName().contains("full") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
                HashSet<String> appFiles = new HashSet<>();
                val zipFile = new ZipFile(file2);
                zipFile.stream().map(ZipEntry::getName)
                        .filter(file -> (file.contains("streaming_spark_logs/spark_") && !file.endsWith(".crc")))
                        .forEach(appFiles::add);
                Assert.assertEquals(7, appFiles.size());
            }
        }
    }

    @Test
    public void testExecuteWithFalseIncludeMeta() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        // includeMeta false
        new StreamingJobDiagInfoTool().execute(new String[] { "-streamingJob", JOB_ID_BUILD, "-destDir",
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

        new StreamingJobDiagInfoTool()
                .execute(new String[] { "-streamingJob", JOB_ID_BUILD, "-destDir", mainDir.getAbsolutePath() });

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

                if (!e.getClass().equals(RuntimeException.class)) {
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
        new StreamingJobDiagInfoTool().execute(new String[] { "-streamingJob", "9462fee8-e6cd-4d18-a5fc-b598a3c5edb5",
                "-destDir", mainDir.getAbsolutePath() });

    }

    @Test
    public void testObf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        new StreamingJobDiagInfoTool()
                .execute(new String[] { "-streamingJob", JOB_ID_BUILD, "-destDir", mainDir.getAbsolutePath() });
        File zipFile = mainDir.listFiles()[0].listFiles()[0];
        File exportFile = new File(mainDir, "output");
        FileUtils.forceMkdir(exportFile);
        ZipFileUtils.decompressZipFile(zipFile.getAbsolutePath(), exportFile.getAbsolutePath());
        File baseDiagFile = exportFile.listFiles()[0];
        val properties = org.apache.kylin.common.util.FileUtils
                .readFromPropertiesFile(new File(baseDiagFile, "conf/kylin.properties"));
        Assert.assertTrue(properties.containsValue(SensitiveConfigKeysConstant.HIDDEN));

    }

}
