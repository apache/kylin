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
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
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
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.config.BrokerConfig;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.storage.hbase.util.ZookeeperJobLock;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

/**
 *  for streaming cubing case "test_streaming_table", using multiple threads to build it concurrently.
 */
public class BuildCubeWithStream2 {

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithStream2.class);

    private CubeManager cubeManager;
    private DefaultScheduler scheduler;
    protected ExecutableManager jobService;
    private static final String cubeName = "test_streaming_table_cube";

    private KafkaConfig kafkaConfig;
    private MockKafka kafkaServer;
    private static boolean generateData = true;

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
        if (numberOfRecords <= 0)
            return;
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
        final long date1 = 0;
        final long date2 = f.parse("2013-01-01").getTime();

        new Thread(new Runnable() {
            @Override
            public void run() {

                Random rand = new Random();
                while (generateData == true) {
                    try {
                        generateStreamData(date1, date2, rand.nextInt(100));
                        sleep(rand.nextInt(rand.nextInt(100 * 1000))); // wait random time, from 0 to 100 seconds
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        List<FutureTask<ExecutableState>> futures = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            FutureTask futureTask = new FutureTask(new Callable<ExecutableState>() {
                @Override
                public ExecutableState call() {
                    ExecutableState result = null;
                    try {
                        result = buildSegment(cubeName, 0, Long.MAX_VALUE);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return result;
                }
            });

            executorService.submit(futureTask);
            futures.add(futureTask);
            Thread.sleep(2 * 60 * 1000); // sleep 2 mintues
        }

        generateData = false; // stop generating message to kafka
        executorService.shutdown();
        int succeedBuild = 0;
        for (int i = 0; i < futures.size(); i++) {
            ExecutableState result = futures.get(i).get(20, TimeUnit.MINUTES);
            logger.info("Checking building task " + i + " whose state is " + result);
            Assert.assertTrue(result == null || result == ExecutableState.SUCCEED || result == ExecutableState.DISCARDED );
            if (result == ExecutableState.SUCCEED)
                succeedBuild++;
        }

        logger.info(succeedBuild + " build jobs have been successfully completed.");
        List<CubeSegment> segments = cubeManager.getCube(cubeName).getSegments(SegmentStatusEnum.READY);
        Assert.assertTrue(segments.size() == succeedBuild);

    }


    private ExecutableState buildSegment(String cubeName, long startOffset, long endOffset) throws Exception {
        CubeSegment segment = cubeManager.appendSegment(cubeManager.getCube(cubeName), 0, 0, startOffset, endOffset);
        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getStatus();
    }

    protected void deployEnv() throws IOException {
        DeployUtil.overrideJobJarLocations();
        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
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
        DefaultScheduler.destroyInstance();
    }

    protected void waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR || job.getStatus() == ExecutableState.DISCARDED) {
                break;
            } else {
                try {
                    sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            beforeClass();

            BuildCubeWithStream2 buildCubeWithStream = new BuildCubeWithStream2();
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

}
