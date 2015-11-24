/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.job;

import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.engine.streaming.OneOffStreamingBuilder;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.storage.hbase.steps.HBaseMetadataTestCase;
import org.apache.kylin.storage.hbase.util.StorageCleanupJob;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 *  for streaming cubing case "test_streaming_table"
 */
public class BuildCubeWithStreamTest {

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithStreamTest.class);
    private static final String streamingName = "test_streaming_table_cube";
    private static final long startTime = DateFormat.stringToMillis("2015-01-01 00:00:00");
    private static final long endTime = DateFormat.stringToMillis("2015-01-03 00:00:00");
    private static final long batchInterval = 16 * 60 * 60 * 1000;//16 hours

    private KylinConfig kylinConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/sandbox");
        if (System.getProperty("hdp.version") == null) {
            throw new RuntimeException("No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.2.4.2-2");
        }

    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);
        DeployUtil.overrideJobJarLocations();

        kylinConfig = KylinConfig.getInstanceFromEnv();

        final StreamingConfig config = StreamingManager.getInstance(KylinConfig.getInstanceFromEnv()).getStreamingConfig(streamingName);

        //Use a random topic for kafka data stream
        KafkaConfig streamingConfig = KafkaConfigManager.getInstance(kylinConfig).getKafkaConfig(streamingName);
        streamingConfig.setTopic(UUID.randomUUID().toString());
        KafkaConfigManager.getInstance(kylinConfig).saveKafkaConfig(streamingConfig);

        DeployUtil.prepareTestDataForStreamingCube(startTime, endTime, config.getCubeName(), streamingConfig);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        logger.info("start time:" + startTime + " end time:" + endTime + " batch interval:" + batchInterval + " batch count:" + ((endTime - startTime) / batchInterval));
        for (long start = startTime; start < endTime; start += batchInterval) {
            logger.info(String.format("build batch:{%d, %d}", start, start + batchInterval));
            new OneOffStreamingBuilder(streamingName, start, start + batchInterval).build().run();
        }
    }
}
