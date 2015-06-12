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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.job.streaming.BootstrapConfig;
import org.apache.kylin.job.streaming.KafkaDataLoader;
import org.apache.kylin.job.streaming.StreamingBootstrap;
import org.apache.kylin.job.streaming.StreamingTableDataGenerator;
import org.apache.kylin.streaming.StreamingConfig;
import org.apache.kylin.streaming.StreamingManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.UUID;

/**
 *  for streaming cubing case "test_streaming_table"
 */
public class BuildCubeWithStreamTest {

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithStreamTest.class);
    private static final String streamingName = "test_streaming_table_cube";
    private static final long startTime = DateFormat.stringToMillis("2015-01-01 00:00:00");
    private static final long endTime = DateFormat.stringToMillis("2015-11-01 00:00:00");
    private static final long batchInterval = 12 * 60 * 60 * 1000;//12 hours

    private KylinConfig kylinConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty("hdp.version", "2.2.4.2-2"); // mapred-site.xml ref this

    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);
        DeployUtil.overrideJobJarLocations();

        kylinConfig = KylinConfig.getInstanceFromEnv();

        //Use a random toplic for kafka data stream
        StreamingConfig streamingConfig = StreamingManager.getInstance(kylinConfig).getStreamingConfig(streamingName);
        streamingConfig.setTopic(UUID.randomUUID().toString());
        StreamingManager.getInstance(kylinConfig).saveStreamingConfig(streamingConfig);

        loadDataIntoKafka();
    }

    private void loadDataIntoKafka() {
        //10 day's data,sorted
        List<String> data = StreamingTableDataGenerator.generate(10000, startTime, endTime);
        KafkaDataLoader.loadIntoKafka(streamingName, data);
    }

    @After
    public void after() {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        for (long start = startTime; start < endTime; start += batchInterval) {
            BootstrapConfig bootstrapConfig = new BootstrapConfig();
            bootstrapConfig.setStart(start);
            bootstrapConfig.setEnd(start + endTime);
            bootstrapConfig.setMargin(0);
            bootstrapConfig.setOneOff(true);
            bootstrapConfig.setPartitionId(0);
            bootstrapConfig.setStreaming(streamingName);
            StreamingBootstrap.getInstance(KylinConfig.getInstanceFromEnv()).start(bootstrapConfig);
        }
    }
}
