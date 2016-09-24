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

package org.apache.kylin.provision;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.job.DeployUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.job.streaming.Kafka10DataLoader;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.config.BrokerConfig;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.storage.hbase.util.ZookeeperJobLock;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  for streaming cubing case "test_streaming_table"
 */
public class BuildCubeWithStream {

    private static final Logger logger = LoggerFactory.getLogger(org.apache.kylin.provision.BuildCubeWithStream.class);

    private CubeManager cubeManager;
    private DefaultScheduler scheduler;
    protected ExecutableManager jobService;
    private static final String cubeName = "test_streaming_table_cube";

    private KafkaConfig kafkaConfig;
    private MockKafka kafkaServer;

    public void before() throws Exception {
        deployEnv();

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = ExecutableManager.getInstance(kylinConfig);
        scheduler = DefaultScheduler.createInstance();
        scheduler.init(new JobEngineConfig(kylinConfig), new ZookeeperJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        cubeManager = CubeManager.getInstance(kylinConfig);

        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final String factTable = cubeInstance.getFactTable();

        final StreamingManager streamingManager = StreamingManager.getInstance(kylinConfig);
        final StreamingConfig streamingConfig = streamingManager.getStreamingConfig(factTable);
        kafkaConfig = KafkaConfigManager.getInstance(kylinConfig).getKafkaConfig(streamingConfig.getName());

        String topicName = UUID.randomUUID().toString();
        String localIp = NetworkUtils.getLocalIp();
        BrokerConfig brokerConfig = kafkaConfig.getKafkaClusterConfigs().get(0).getBrokerConfigs().get(0);
        brokerConfig.setHost(localIp);
        kafkaConfig.setTopic(topicName);
        KafkaConfigManager.getInstance(kylinConfig).saveKafkaConfig(kafkaConfig);

        startEmbeddedKafka(topicName, brokerConfig);
    }

    private void startEmbeddedKafka(String topicName, BrokerConfig brokerConfig) {
        //Start mock Kakfa
        String zkConnectionStr = "sandbox:2181";
        ZkConnection zkConnection = new ZkConnection(zkConnectionStr);
        // Assert.assertEquals(ZooKeeper.States.CONNECTED, zkConnection.getZookeeperState());
        kafkaServer = new MockKafka(zkConnection, brokerConfig.getPort(), brokerConfig.getId());
        kafkaServer.start();

        kafkaServer.createTopic(topicName, 3, 1);
        kafkaServer.waitTopicUntilReady(topicName);

        MetadataResponse.TopicMetadata topicMetadata = kafkaServer.fetchTopicMeta(topicName);
        Assert.assertEquals(topicName, topicMetadata.topic());
    }

    private void generateStreamData(long startTime, long endTime, int numberOfRecords) throws IOException {
        Kafka10DataLoader dataLoader = new Kafka10DataLoader(kafkaConfig);
        DeployUtil.prepareTestDataForStreamingCube(startTime, endTime, numberOfRecords, cubeName, dataLoader);
        logger.info("Test data inserted into Kafka");
    }

    private void clearSegment(String cubeName) throws Exception {
        CubeInstance cube = cubeManager.getCube(cubeName);
        // remove all existing segments
        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToRemoveSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
        cubeManager.updateCube(cubeBuilder);
    }

    public void build() throws Exception {
        clearSegment(cubeName);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2013-01-01").getTime();

        int numberOfRecrods1 = 10000;
        generateStreamData(date1, date2, numberOfRecrods1);
        buildSegment(cubeName, 0, Long.MAX_VALUE);

        long date3 = f.parse("2013-04-01").getTime();
        int numberOfRecrods2 = 5000;
        generateStreamData(date2, date3, numberOfRecrods2);
        buildSegment(cubeName, 0, Long.MAX_VALUE);

        //merge
        mergeSegment(cubeName, 0, 15000);

        List<CubeSegment> segments = cubeManager.getCube(cubeName).getSegments();
        Assert.assertTrue(segments.size() == 1);

        CubeSegment toRefreshSeg = segments.get(0);
        HashMap<String, String> partitionOffsetMap = toRefreshSeg.getAdditionalInfo();

        refreshSegment(cubeName, toRefreshSeg.getSourceOffsetStart(), toRefreshSeg.getSourceOffsetEnd(), partitionOffsetMap);
        segments = cubeManager.getCube(cubeName).getSegments();
        Assert.assertTrue(segments.size() == 1);

    }

    private String mergeSegment(String cubeName, long startOffset, long endOffset) throws Exception {
        CubeSegment segment = cubeManager.mergeSegments(cubeManager.getCube(cubeName), 0, 0, startOffset, endOffset, false);
        DefaultChainedExecutable job = EngineFactory.createBatchMergeJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getId();
    }

    private String refreshSegment(String cubeName, long startOffset, long endOffset, HashMap<String, String> partitionOffsetMap) throws Exception {
        CubeSegment segment = cubeManager.refreshSegment(cubeManager.getCube(cubeName), 0, 0, startOffset, endOffset, false);
        segment.setAdditionalInfo(partitionOffsetMap);
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        CubeUpdate cubeBuilder = new CubeUpdate(cubeInstance);
        cubeBuilder.setToUpdateSegs(segment);
        cubeManager.updateCube(cubeBuilder);
        segment = cubeManager.getCube(cubeName).getSegmentById(segment.getUuid());
        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getId();
    }

    private String buildSegment(String cubeName, long startOffset, long endOffset) throws Exception {
        CubeSegment segment = cubeManager.appendSegment(cubeManager.getCube(cubeName), 0, 0, startOffset, endOffset, false);
        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getId();
    }

    protected void deployEnv() throws IOException {
        DeployUtil.overrideJobJarLocations();
        //DeployUtil.initCliWorkDir();
        //DeployUtil.deployMetadata();
    }

    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, HBaseMetadataTestCase.SANDBOX_TEST_DATA);
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException("No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.2.4.2-2");
        }
        HBaseMetadataTestCase.staticCreateTestMetadata(HBaseMetadataTestCase.SANDBOX_TEST_DATA);
    }

    public static void afterClass() throws Exception {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    public void after() {
        kafkaServer.stop();
    }

    protected void waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR) {
                break;
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            beforeClass();

            BuildCubeWithStream buildCubeWithStream = new BuildCubeWithStream();
            buildCubeWithStream.before();
            buildCubeWithStream.build();
            logger.info("Build is done");
            buildCubeWithStream.after();
            afterClass();
            logger.info("Going to exit");
            System.exit(0);
        } catch (Exception e) {
            logger.error("error", e);
            System.exit(1);
        }

    }

    protected int cleanupOldStorage() throws Exception {
        String[] args = { "--delete", "true" };

        //        KapStorageCleanupCLI cli = new KapStorageCleanupCLI();
        //        cli.execute(args);
        return 0;
    }

}
