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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class StreamingJobUtilsTest extends StreamingTestCase {
    private static final String PROJECT = "streaming_test";
    private static final String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        Assert.assertNull(StreamingJobUtils.extractKafkaSaslJaasConf());
        getTestConfig().setProperty("kylin.kafka-jaas.enabled", "true");
        FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()),
                "KafkaClient{ org.apache.kafka.common.security.scram.ScramLoginModule required}");
        val text = StreamingJobUtils.extractKafkaSaslJaasConf();
        Assert.assertNotNull(text);

        getTestConfig().setProperty("kylin.kafka-jaas-conf", "kafka_err_jaas.conf");
        File file = new File(kapConfig.getKafkaJaasConfPath());

        FileUtils.write(file, "}4{");
        try {
            StreamingJobUtils.extractKafkaSaslJaasConf();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("KE-010035015", ((KylinException) e).getErrorCode().getCodeString());
        } finally {
            FileUtils.deleteQuietly(new File(KapConfig.getInstanceFromEnv().getKafkaJaasConfPath()));
        }
    }
}
