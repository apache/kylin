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

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.job.hadoop.cube.StorageCleanupJob;
import org.apache.kylin.job.streaming.BootstrapConfig;
import org.apache.kylin.job.streaming.StreamingBootstrap;
import org.apache.kylin.storage.hbase.HBaseMetadataTestCase;
import org.apache.kylin.streaming.StreamingConfig;
import org.apache.kylin.streaming.StreamingManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        DeployUtil.prepareTestDataForStreamingCube(startTime, endTime, streamingConfig);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        backup();
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    private static int cleanupOldStorage() throws Exception {
        String[] args = { "--delete", "true" };
        int exitCode = ToolRunner.run(new StorageCleanupJob(), args);
        return exitCode;
    }

    private static void backup() throws Exception {
        int exitCode = cleanupOldStorage();
        if (exitCode == 0) {
            exportHBaseData();
        }
    }

    private static void exportHBaseData() throws IOException {
        ExportHBaseData export = new ExportHBaseData();
        export.exportTables();
        export.tearDown();
    }

    @Test
    public void test() throws Exception {
        for (long start = startTime; start < endTime; start += batchInterval) {
            BootstrapConfig bootstrapConfig = new BootstrapConfig();
            bootstrapConfig.setStart(start);
            bootstrapConfig.setEnd(start + batchInterval);
            bootstrapConfig.setOneOff(true);
            bootstrapConfig.setPartitionId(0);
            bootstrapConfig.setStreaming(streamingName);
            StreamingBootstrap.getInstance(KylinConfig.getInstanceFromEnv()).start(bootstrapConfig);
        }
    }
}
