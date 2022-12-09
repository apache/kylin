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
package org.apache.kylin.streaming.jobs;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.KAFKA_JAAS_FILE_KEYTAB_NOT_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.READ_KAFKA_JAAS_FILE_ERROR;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class StreamingJobUtilsTest extends StreamingTestCase {
    private static final String PROJECT = "streaming_test";
    private static final String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetStreamingKylinConfig() {
        val config = getTestConfig();
        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        val jobId = DATAFLOW_ID + "_build";

        val jobMeta = mgr.getStreamingJobByUuid(jobId);
        val params = jobMeta.getParams();
        params.put("kylin.streaming.spark-conf.spark.executor.memoryOverhead", "1g");
        params.put("kylin.streaming.kafka-conf.maxOffsetsPerTrigger", "300");
        params.put("kylin.streaming.table-refresh-interval", "1h");

        val kylinConfig = StreamingJobUtils.getStreamingKylinConfig(config, params, jobMeta.getModelId(), PROJECT);
        Assert.assertFalse(kylinConfig.getStreamingSparkConfigOverride().isEmpty());
        Assert.assertFalse(kylinConfig.getStreamingKafkaConfigOverride().isEmpty());
        Assert.assertEquals("1h", kylinConfig.getStreamingTableRefreshInterval());
    }

    @Test
    public void testGetStreamingKylinConfigOfProject() {
        val config = getTestConfig();
        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        val jobId = DATAFLOW_ID + "_build";

        val jobMeta = mgr.getStreamingJobByUuid(jobId);
        val params = jobMeta.getParams();
        config.setProperty("kylin.streaming.spark-conf.spark.executor.memoryOverhead", "1g");
        config.setProperty("kylin.streaming.kafka-conf.maxOffsetsPerTrigger", "300");
        config.setProperty("kylin.streaming.table-refresh-interval", "30m");

        val kylinConfig = StreamingJobUtils.getStreamingKylinConfig(config, params, "", PROJECT);
        Assert.assertFalse(kylinConfig.getStreamingSparkConfigOverride().isEmpty());
        Assert.assertFalse(kylinConfig.getStreamingKafkaConfigOverride().isEmpty());
        Assert.assertEquals("30m", kylinConfig.getStreamingTableRefreshInterval());
    }

    @Test
    public void testExtractKafkaSaslJaasConf() throws Exception {
        val kapConfig = KapConfig.getInstanceFromEnv();
        Assert.assertNull(StreamingJobUtils.extractKafkaJaasConf(true));
        getTestConfig().setProperty("kylin.kafka-jaas.enabled", "true");
        FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()),
                "KafkaClient{ org.apache.kafka.common.security.scram.ScramLoginModule required;}",
                StandardCharsets.UTF_8);
        val text = StreamingJobUtils.extractKafkaJaasConf(true);
        Assert.assertNotNull(text);

        getTestConfig().setProperty("kylin.kafka-jaas-conf", "kafka_err_jaas.conf");
        File file = new File(kapConfig.getKafkaJaasConfPath());

        FileUtils.write(file, "}4{", StandardCharsets.UTF_8);
        try {
            StreamingJobUtils.extractKafkaJaasConf(true);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("KE-010035217", ((KylinException) e).getErrorCode().getCodeString());
        } finally {
            FileUtils.deleteQuietly(new File(KapConfig.getInstanceFromEnv().getKafkaJaasConfPath()));
        }
    }

    @Test
    public void testCheckKeyTabFileUnderJaas() throws Exception {
        val kapConfig = KapConfig.getInstanceFromEnv();
        Assert.assertNull(StreamingJobUtils.extractKafkaJaasConf(true));
        Assert.assertNull(StreamingJobUtils.getJaasKeyTabAbsPath());
        KylinConfig kylinConfig = getTestConfig();
        kylinConfig.setProperty("kylin.kafka-jaas.enabled", "true");
        File testKeyTab = new File(KylinConfig.getKylinConfDir() + File.separator + "test.keytab");

        // jaas not exist
        Assert.assertThrows(READ_KAFKA_JAAS_FILE_ERROR.getMsg(), KylinException.class,
                () -> StreamingJobUtils.extractKafkaJaasConf(true));

        // jaas keytab key not exist
        FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()),
                "KafkaClient { " + "com.sun.security.auth.module.Krb5LoginModule required " + "useKeyTab=true "
                        + "storeKey=true " + "principal=\"kylin@DEV.COM\" " + "serviceName=\"kafka\";" + " };",
                StandardCharsets.UTF_8);
        Assert.assertNull(StreamingJobUtils.getJaasKeyTabAbsPath());

        // jaas exist but keytab not exist
        FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()),
                "KafkaClient { " + "com.sun.security.auth.module.Krb5LoginModule required " + "useKeyTab=true "
                        + "storeKey=true " + "keyTab=\"" + testKeyTab + "\" " + "principal=\"kylin@DEV.COM\" "
                        + "serviceName=\"kafka\";" + " };",
                StandardCharsets.UTF_8);
        Assert.assertThrows(KAFKA_JAAS_FILE_KEYTAB_NOT_EXISTS.getMsg(), KylinException.class,
                () -> StreamingJobUtils.extractKafkaJaasConf(true));

        // all exist
        FileUtils.write(testKeyTab, "test", StandardCharsets.UTF_8);
        val text = StreamingJobUtils.extractKafkaJaasConf(true);
        Assert.assertNotNull(text);
        String keyTabAbsPath = StreamingJobUtils.getJaasKeyTabAbsPath();
        Assert.assertEquals(testKeyTab.getAbsolutePath(), keyTabAbsPath);
        String executorJaasName = StreamingJobUtils.getExecutorJaasName();
        Assert.assertEquals(kapConfig.getKafkaJaasConf(), executorJaasName);
        String executorJaasPath = StreamingJobUtils.getExecutorJaasPath();
        Assert.assertEquals(HadoopUtil.getHadoopConfDir() + File.separator + executorJaasName, executorJaasPath);
        kylinConfig.setProperty("kylin.kafka-jaas-conf", "kafka_err_jaas.conf");
    }

    @Test
    public void testCreateExecutorJaas() throws Exception {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        String executorJaasPath = HadoopUtil.getHadoopConfDir() + File.separator + kapConfig.getKafkaJaasConf();
        File executorJaasFile = new File(executorJaasPath);
        executorJaasFile.deleteOnExit();
        StreamingJobUtils.createExecutorJaas();
        Assert.assertFalse(executorJaasFile.exists());
        getTestConfig().setProperty("kylin.kafka-jaas.enabled", "true");

        {
            String jaasContext = "KafkaClient { org.apache.kafka.common.security.scram.ScramLoginModule required; };";
            FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()), jaasContext, StandardCharsets.UTF_8);
            StreamingJobUtils.createExecutorJaas();
            Assert.assertTrue(executorJaasFile.exists());
            Assert.assertEquals(jaasContext, FileUtils.readFileToString(executorJaasFile, StandardCharsets.UTF_8));
        }

        {
            File testKeyTab = new File(KylinConfig.getKylinConfDir() + File.separator + "test.keytab");
            FileUtils.write(testKeyTab, "test", StandardCharsets.UTF_8);
            String jaasContext = "KafkaClient { " + "com.sun.security.auth.module.Krb5LoginModule required "
                    + "useKeyTab=true " + "storeKey=true " + "keyTab=\"" + testKeyTab.getAbsolutePath() + "\" "
                    + "principal=\"kylin@DEV.COM\" " + "serviceName=\"kafka\";" + " };";
            FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()), jaasContext, StandardCharsets.UTF_8);
            StreamingJobUtils.createExecutorJaas();
            Assert.assertTrue(executorJaasFile.exists());
            Assert.assertEquals(jaasContext.replace(testKeyTab.getAbsolutePath(), testKeyTab.getName()),
                    FileUtils.readFileToString(executorJaasFile, StandardCharsets.UTF_8));
        }
        executorJaasFile.deleteOnExit();
    }
}
