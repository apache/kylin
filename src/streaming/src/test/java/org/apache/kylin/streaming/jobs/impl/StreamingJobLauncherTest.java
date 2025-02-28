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
package org.apache.kylin.streaming.jobs.impl;

import static org.apache.kylin.common.exception.ServerErrorCode.JOB_START_FAILURE;
import static org.apache.kylin.common.persistence.ResourceStore.METASTORE_IMAGE;
import static org.apache.kylin.streaming.constants.StreamingConstants.DEFAULT_PARSER_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.metadata.FileSystemMetadataStore;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.streaming.DataParserInfo;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.util.ReflectionUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
@PrepareForTest(StreamingJobLauncher.class)
public class StreamingJobLauncherTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "streaming_test";
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBuildJobInit() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        Assert.assertFalse(launcher.isInitialized());
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        Assert.assertTrue(launcher.isInitialized());
        val mainClazz = ReflectionUtils.getField(launcher, "mainClazz");
        Assert.assertEquals(StreamingConstants.SPARK_STREAMING_ENTRY, mainClazz);

        val jobParams = ReflectionUtils.getField(launcher, "jobParams");
        Assert.assertNotNull(jobParams);

        val appArgs = (String[]) ReflectionUtils.getField(launcher, "appArgs");
        Assert.assertEquals(5, appArgs.length);
        Assert.assertEquals(PROJECT, appArgs[0]);
        Assert.assertEquals(modelId, appArgs[1]);
        Assert.assertEquals(StreamingConstants.STREAMING_DURATION_DEFAULT, appArgs[2]);
        Assert.assertEquals("", appArgs[3]);
        Assert.assertEquals(getTestConfig().getMetadataUrl().toString(), appArgs[4]);
    }

    @Test
    public void testMergeJobInit() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        Assert.assertFalse(launcher.isInitialized());
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        val mainClazz = ReflectionUtils.getField(launcher, "mainClazz");
        Assert.assertEquals(StreamingConstants.SPARK_STREAMING_MERGE_ENTRY, mainClazz);

        val jobParams = ReflectionUtils.getField(launcher, "jobParams");
        Assert.assertNotNull(jobParams);

        val appArgs = (String[]) ReflectionUtils.getField(launcher, "appArgs");
        Assert.assertEquals(5, appArgs.length);
        Assert.assertEquals(PROJECT, appArgs[0]);
        Assert.assertEquals(modelId, appArgs[1]);
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT, appArgs[2]);
        Assert.assertEquals(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT, appArgs[3]);
        Assert.assertEquals(getTestConfig().getMetadataUrl().toString(), appArgs[4]);
    }

    @Test
    public void testStop() {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);

        launcher.stop();
        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        val uuid = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name());
        val meta = mgr.getStreamingJobByUuid(uuid);
        Assert.assertEquals(StreamingConstants.ACTION_GRACEFUL_SHUTDOWN, meta.getAction());
    }

    @Test
    public void testStartBuildJob() throws Exception {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        val mockup = new MockupSparkLauncher();
        ReflectionUtils.setField(launcher, "launcher", mockup);
        launcher.startYarnJob();
        Assert.assertNull(mockup.sparkConf.get("spark.kerberos.keytab"));
        Assert.assertNull(mockup.sparkConf.get("spark.kerberos.principal"));
    }

    @Test
    public void testStartMergeJob() throws Exception {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        val mockup = new MockupSparkLauncher();
        ReflectionUtils.setField(launcher, "launcher", mockup);
        launcher.startYarnJob();
        Assert.assertNull(mockup.sparkConf.get("spark.kerberos.keytab"));
        Assert.assertNull(mockup.sparkConf.get("spark.kerberos.principal"));
    }

    @Test
    public void testStartYarnBuildJob() throws Exception {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        config.setProperty("kylin.kerberos.enabled", "true");
        config.setProperty("kylin.tool.mount-spark-log-dir", ".");
        val kapConfig = KapConfig.getInstanceFromEnv();

        config.setProperty("kylin.kerberos.enabled", "true");
        config.setProperty("kylin.kafka-jaas.enabled", "true");
        config.setProperty("kylin.streaming.spark-conf.spark.driver.extraJavaOptions",
                "-Djava.security.krb5.conf=./krb5.conf -Djava.security.auth.login.config=./kafka_jaas.conf");
        config.setProperty("kylin.streaming.spark-conf.spark.executor.extraJavaOptions",
                "-Djava.security.krb5.conf=./krb5.conf -Djava.security.auth.login.config=./kafka_jaas.conf");
        config.setProperty("kylin.streaming.spark-conf.spark.am.extraJavaOptions",
                "-Djava.security.krb5.conf=./krb5.conf -Djava.security.auth.login.config=./kafka_jaas.conf");
        val mockup = new MockupSparkLauncher();
        ReflectionUtils.setField(launcher, "launcher", mockup);
        FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()),
                "KafkaClient{ org.apache.kafka.common.security.scram.ScramLoginModule required;}",
                StandardCharsets.UTF_8);
        launcher.startYarnJob();
        Assert.assertNotNull(mockup.sparkConf.get("spark.driver.extraJavaOptions"));
        Assert.assertNotNull(mockup.sparkConf.get("spark.executor.extraJavaOptions"));
        Assert.assertNotNull(mockup.sparkConf.get("spark.am.extraJavaOptions"));
    }

    @Test
    public void testStartYarnMergeJob() throws Exception {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        try {
            config.setProperty("kylin.kerberos.enabled", "true");
            config.setProperty("kylin.tool.mount-spark-log-dir", ".");
            val kapConfig = KapConfig.getInstanceFromEnv();
            config.setProperty("kylin.kafka-jaas.enabled", "true");
            FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()),
                    "KafkaClient{ org.apache.kafka.common.security.scram.ScramLoginModule required}",
                    StandardCharsets.UTF_8);

            val mockup = new MockupSparkLauncher();
            ReflectionUtils.setField(launcher, "launcher", mockup);
            launcher.startYarnJob();
            Assert.assertNotNull(mockup.sparkConf.get("spark.kerberos.keytab"));
            Assert.assertNotNull(mockup.sparkConf.get("spark.kerberos.principal"));
            Assert.assertFalse(mockup.files.contains(kapConfig.getKafkaJaasConfPath()));
        } finally {
            FileUtils.deleteQuietly(new File(KapConfig.getInstanceFromEnv().getKafkaJaasConfPath()));
        }
    }

    @Test
    public void testLaunchMergeJobException_Local() {
        try {
            overwriteSystemProp("streaming.local", "true");
            val config = getTestConfig();

            val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
            val launcher = new StreamingJobLauncher();
            launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
            config.setProperty("kylin.env", "local");
            launcher.launch();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(JOB_START_FAILURE.toErrorCode().getCodeString(),
                    ((KylinException) e).getErrorCode().getCodeString());
        }
    }

    @Test
    public void testLaunchMergeJobException_Yarn() {
        try {
            overwriteSystemProp("streaming.local", "false");
            val config = getTestConfig();

            val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
            val launcher = new StreamingJobLauncher();
            launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
            config.setProperty("kylin.env", "prod");
            launcher.launch();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(JOB_START_FAILURE.toErrorCode().getCodeString(),
                    ((KylinException) e).getErrorCode().getCodeString());
        }
    }

    @Test
    public void testLaunchBuildJobException_Local() {
        try {
            overwriteSystemProp("streaming.local", "true");
            val config = getTestConfig();
            val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
            val launcher = new StreamingJobLauncher();
            launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
            config.setProperty("kylin.env", "local");
            launcher.launch();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(JOB_START_FAILURE.toErrorCode().getCodeString(),
                    ((KylinException) e).getErrorCode().getCodeString());
        }
    }

    @Test
    public void testLaunchBuildJobException_Yarn() {

        try {
            overwriteSystemProp("streaming.local", "true");
            val config = getTestConfig();
            val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
            val launcher = new StreamingJobLauncher();
            launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
            config.setProperty("kylin.env", "local");
            launcher.launch();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(JOB_START_FAILURE.toErrorCode().getCodeString(),
                    ((KylinException) e).getErrorCode().getCodeString());
        }
    }

    @Test
    public void testLaunchBuildJobLaunch_Yarn() throws Exception {
        overwriteSystemProp("streaming.local", "false");
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = Mockito.spy(new StreamingJobLauncher());
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        config.setProperty("kylin.env", "prod");
        Mockito.doNothing().when(launcher).startYarnJob();
        launcher.launch();
    }

    @Test
    public void testLaunchMergeJobLaunch_Yarn() throws Exception {
        overwriteSystemProp("streaming.local", "false");
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = Mockito.spy(new StreamingJobLauncher());
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        config.setProperty("kylin.env", "prod");
        Mockito.doNothing().when(launcher).startYarnJob();
        launcher.launch();
    }

    @Test
    public void testGetJobTmpMetaStoreUrlPath() {
        overwriteSystemProp("streaming.local", "true");
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-jobs-location", "/tmp/");
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);

        val jobTmpMetaStoreUrlPath = ReflectionUtils.invokeGetterMethod(launcher, "getJobTmpMetaStoreUrlPath");
        Assert.assertEquals(
                String.format(Locale.ROOT, "%s/%s/%s/meta", config.getStreamingBaseJobsLocation(), PROJECT, modelId),
                jobTmpMetaStoreUrlPath);
    }

    @Test
    public void testGetAvailableLatestDumpPath_Null() {
        overwriteSystemProp("streaming.local", "true");
        val config = getTestConfig();
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        config.setProperty("kylin.engine.streaming-jobs-location", mainDir.getAbsolutePath());
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);

        val availableMetaDumpPath = ReflectionUtils.invokeGetterMethod(launcher, "getAvailableLatestDumpPath");
        Assert.assertNull(availableMetaDumpPath);
    }

    @Test
    public void testGetAvailableLatestDumpPath_NotNull() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        overwriteSystemProp("streaming.local", "true");

        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-jobs-location", mainDir.getAbsolutePath());
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);

        val metaRootPath = String.format(Locale.ROOT, "%s/%s/%s/meta", config.getStreamingBaseJobsLocation(), PROJECT,
                modelId);
        FileUtils.forceMkdir(new File(metaRootPath));

        FileUtils.forceMkdir(new File(metaRootPath, "meta_" + System.currentTimeMillis()));

        val availableMetaDumpPath = ReflectionUtils.invokeGetterMethod(launcher, "getAvailableLatestDumpPath");
        Assert.assertNotNull(availableMetaDumpPath);
    }

    @Test
    public void testGetAvailableLatestDumpPath_CleanMeta() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        overwriteSystemProp("streaming.local", "true");

        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-jobs-location", mainDir.getAbsolutePath());
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);

        val metaRootPath = String.format(Locale.ROOT, "%s/%s/%s/meta", config.getStreamingBaseJobsLocation(), PROJECT,
                modelId);
        FileUtils.forceMkdir(new File(metaRootPath));

        val outDateTime = System.currentTimeMillis()
                - TimeUnit.MILLISECONDS.toMillis(config.getStreamingJobMetaRetainedTime()) * 2;
        val outDateMetaFile = new File(metaRootPath, "meta_" + outDateTime);
        FileUtils.forceMkdir(outDateMetaFile);
        Assert.assertTrue(outDateMetaFile.setLastModified(outDateTime));
        Assert.assertTrue(outDateMetaFile.exists());

        val availableMetaDumpPath = ReflectionUtils.invokeGetterMethod(launcher, "getAvailableLatestDumpPath");
        Assert.assertNull(availableMetaDumpPath);
    }

    @Test
    public void testGetJobTmpHdfsMetaStorageUrl() {
        overwriteSystemProp("streaming.local", "true");
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-jobs-location", "/tmp/");
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);

        val jobTmpMetaStoreUrl = (StorageURL) ReflectionUtils.invokeGetterMethod(launcher,
                "getJobTmpHdfsMetaStorageUrl");
        val params = jobTmpMetaStoreUrl.getAllParameters();
        Assert.assertEquals("true", params.get("zip"));
        Assert.assertEquals("true", params.get("snapshot"));

        val metaRootPath = String.format(Locale.ROOT, "%s/%s/%s/meta", config.getStreamingBaseJobsLocation(), PROJECT,
                modelId);
        val metaPath = metaRootPath + "/meta_" + ReflectionUtils.getField(launcher, "currentTimestamp");
        Assert.assertEquals(metaPath, params.get("path"));
    }

    @Test
    public void testGetMetadataDumpList() {
        overwriteSystemProp("streaming.local", "true");
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-jobs-location", "/tmp/");
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);

        val dumpSet = launcher.getMetadataDumpList();
        Assert.assertEquals(18, dumpSet.size());

        Assert.assertTrue(dumpSet.contains("DATAFLOW/e78a89dd-847f-4574-8afa-8768b4228b72"));
        Assert.assertTrue(dumpSet.contains("SEGMENT/c380dd2a-43b8-4268-b73d-2a5f76236631"));
        Assert.assertTrue(dumpSet.contains("SEGMENT/c380dd2a-43b8-4268-b73d-2a5f76236632"));
        Assert.assertTrue(dumpSet.contains("SEGMENT/c380dd2a-43b8-4268-b73d-2a5f76236633"));
        Assert.assertTrue(dumpSet.contains("SEGMENT/f9a7af68-ba3e-a8d3-2493-8d72fdc63fa4"));
        Assert.assertTrue(dumpSet.contains("LAYOUT/c380dd2a-43b8-4268-b73d-2a5f76236631"));
        Assert.assertTrue(dumpSet.contains("LAYOUT/c380dd2a-43b8-4268-b73d-2a5f76236632"));
        Assert.assertTrue(dumpSet.contains("LAYOUT/c380dd2a-43b8-4268-b73d-2a5f76236633"));
        Assert.assertTrue(dumpSet.contains("LAYOUT/f9a7af68-ba3e-a8d3-2493-8d72fdc63fa4"));
        Assert.assertTrue(dumpSet.contains("INDEX_PLAN/e78a89dd-847f-4574-8afa-8768b4228b72"));
        Assert.assertTrue(dumpSet.contains("PROJECT/streaming_test"));
        Assert.assertTrue(dumpSet.contains("MODEL/e78a89dd-847f-4574-8afa-8768b4228b72"));
        Assert.assertTrue(dumpSet.contains("TABLE_INFO/streaming_test.SSB.P_LINEORDER_STR"));
        Assert.assertTrue(dumpSet.contains("KAFKA_CONFIG/streaming_test.SSB.P_LINEORDER_STR"));
        Assert.assertTrue(dumpSet.contains("TABLE_INFO/streaming_test.SSB.PART"));
        Assert.assertTrue(dumpSet.contains(METASTORE_IMAGE));
        Assert.assertTrue(dumpSet.contains("STREAMING_JOB/e78a89dd-847f-4574-8afa-8768b4228b72_build"));
        Assert.assertTrue(dumpSet.contains("STREAMING_JOB/e78a89dd-847f-4574-8afa-8768b4228b72_merge"));

    }

    @Test
    public void testInitStorageUrl() {
        overwriteSystemProp("streaming.local", "true");
        val config = getTestConfig();
        config.setProperty("kylin.engine.streaming-jobs-location", "/tmp/");
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);

        ReflectionUtils.invokeGetterMethod(launcher, "initStorageUrl");

        val storageUrl = (StorageURL) ReflectionUtils.getField(launcher, "distMetaStorageUrl");

        Assert.assertEquals(config.getMetadataUrl().getScheme(), storageUrl.getScheme());
    }

    @Test
    public void testInitStorageUrl_JobCluster() {
        overwriteSystemProp("streaming.local", "false");
        val config = getTestConfig();

        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);

        config.setProperty("kylin.engine.streaming-jobs-location", "/tmp/");
        config.setMetadataUrl("xxx");
        config.setProperty("kylin.env", "utxxx");
        ReflectionUtils.invokeGetterMethod(launcher, "initStorageUrl");

        val storageUrl = (StorageURL) ReflectionUtils.getField(launcher, "distMetaStorageUrl");
        Assert.assertEquals(FileSystemMetadataStore.HDFS_SCHEME, storageUrl.getScheme());
    }

    @Test
    public void testStartYarnBuildJobWithoutExtraOpts() throws Exception {
        val config = getTestConfig();
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";

        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        config.setProperty("kylin.kerberos.enabled", "true");
        config.setProperty("kylin.tool.mount-spark-log-dir", ".");
        val kapConfig = KapConfig.getInstanceFromEnv();

        config.setProperty("kylin.kerberos.enabled", "true");
        config.setProperty("kylin.kafka-jaas.enabled", "true");
        val mockup = new MockupSparkLauncher();
        ReflectionUtils.setField(launcher, "launcher", mockup);
        FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()),
                "KafkaClient{ org.apache.kafka.common.security.scram.ScramLoginModule required;}",
                StandardCharsets.UTF_8);
        launcher.startYarnJob();
        Assert.assertNotNull(mockup.sparkConf.get("spark.driver.extraJavaOptions"));
        Assert.assertNotNull(mockup.sparkConf.get("spark.executor.extraJavaOptions"));
        Assert.assertNotNull(mockup.sparkConf.get("spark.yarn.am.extraJavaOptions"));
    }

    @Test
    public void testAddParserJar() throws Exception {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        val mockup = new MockupSparkLauncher();
        ReflectionTestUtils.setField(launcher, "launcher", mockup);
        val mockLaunch = PowerMockito.spy(launcher);
        PowerMockito.when(mockLaunch, "getParserName").thenReturn("org.apache.kylin.parser.TimedJsonStreamParser2");
        DataParserInfo dataParserInfo = new DataParserInfo(PROJECT, DEFAULT_PARSER_NAME, "default");
        PowerMockito.when(mockLaunch, "getDataParser", Mockito.anyString()).thenReturn(dataParserInfo);
        PowerMockito.doReturn("default").when(mockLaunch, "getParserJarPath", dataParserInfo);
        ReflectionTestUtils.invokeMethod(mockLaunch, "addParserJar", mockup);
        mockup.startApplication();
        Assert.assertTrue(mockup.jars.contains("default"));
    }

    static class MockupSparkLauncher extends SparkLauncher {
        private Map<String, String> sparkConf;
        private List<String> files;
        private List<String> jars;

        public SparkAppHandle startApplication(SparkAppHandle.Listener... listeners) {
            val builder = ReflectionUtils.getField(this, "builder");
            sparkConf = (Map<String, String>) ReflectionUtils.getField(builder, "conf");
            files = (List<String>) ReflectionUtils.getField(builder, "files");
            jars = (List<String>) ReflectionUtils.getField(builder, "jars");
            return null;
        }
    }
}
