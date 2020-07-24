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

import static java.lang.Thread.sleep;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.ZKUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.job.DeployUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.zookeeper.ZookeeperJobLock;
import org.apache.kylin.job.streaming.Kafka10DataLoader;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.streaming.StreamingConfig;
import org.apache.kylin.metadata.streaming.StreamingManager;
import org.apache.kylin.rest.job.StorageCleanupJob;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.source.SourcePartition;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.config.BrokerConfig;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 *  for streaming cubing case "test_streaming_table"
 */
public class BuildCubeWithStream {

    private static final Logger logger = LoggerFactory.getLogger(org.apache.kylin.provision.BuildCubeWithStream.class);

    protected CubeManager cubeManager;
    private DefaultScheduler scheduler;
    protected ExecutableManager jobService;
    static final String cubeName = "test_streaming_table_cube";
    static final String joinTableCubeName = "test_streaming_join_table_cube";

    private KafkaConfig kafkaConfig;
    private MockKafka kafkaServer;
    private ZkConnection zkConnection;
    private final String kafkaZkPath = ZKUtil.getZkRootBasedPath("streaming") + "/" + RandomUtil.randomUUID().toString();
    protected static boolean simpleBuildMode = false;
    private volatile boolean generateData = true;
    private volatile boolean generateDataDone = false;

    private static final int BUILD_ROUND = 4;

    public void before() throws Exception {
        deployEnv();
        simpleBuildMode = isSimpleBuildMode();
        if (simpleBuildMode) {
            logger.info("Will use simple build mode");
        } else {
            logger.info("Will not use simple build mode");
        }

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = ExecutableManager.getInstance(kylinConfig);
        scheduler = DefaultScheduler.createInstance();
        scheduler.init(new JobEngineConfig(kylinConfig), new ZookeeperJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        cubeManager = CubeManager.getInstance(kylinConfig);

        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final String factTable = cubeInstance.getRootFactTable();

        final StreamingManager streamingManager = StreamingManager.getInstance(kylinConfig);
        final StreamingConfig streamingConfig = streamingManager.getStreamingConfig(factTable);
        kafkaConfig = KafkaConfigManager.getInstance(kylinConfig).getKafkaConfig(streamingConfig.getName());

        String topicName = RandomUtil.randomUUID().toString();
        BrokerConfig brokerConfig = kafkaConfig.getKafkaClusterConfigs().get(0).getBrokerConfigs().get(0);
        kafkaConfig.setTopic(topicName);
        KafkaConfigManager.getInstance(kylinConfig).updateKafkaConfig(kafkaConfig);

        startEmbeddedKafka(topicName, brokerConfig);
    }
    private static boolean isSimpleBuildMode() {
        String simpleModeStr = System.getProperty("simpleBuildMode");
        if (simpleModeStr == null)
            simpleModeStr = System.getenv("KYLIN_CI_SIMPLEBUILD");

        return "true".equalsIgnoreCase(simpleModeStr);
    }

    private void startEmbeddedKafka(String topicName, BrokerConfig brokerConfig) {
        //Start mock Kakfa
        String zkConnectionStr = ZKUtil.getZKConnectString(KylinConfig.getInstanceFromEnv()) + kafkaZkPath;
        System.out.println("zkConnectionStr" + zkConnectionStr);
        zkConnection = new ZkConnection(zkConnectionStr);
        // Assert.assertEquals(ZooKeeper.States.CONNECTED, zkConnection.getZookeeperState());
        kafkaServer = new MockKafka(zkConnection, brokerConfig.getHost() + ":" + brokerConfig.getPort(), brokerConfig.getId());
        kafkaServer.start();

        kafkaServer.createTopic(topicName, 3, 1);
        kafkaServer.waitTopicUntilReady(topicName);

        MetadataResponse.TopicMetadata topicMetadata = kafkaServer.fetchTopicMeta(topicName);
        Assert.assertEquals(topicName, topicMetadata.topic());
    }

    protected void generateStreamData(long startTime, long endTime, int numberOfRecords) throws IOException {
        Kafka10DataLoader dataLoader = new Kafka10DataLoader(kafkaConfig);
        DeployUtil.prepareTestDataForStreamingCube(startTime, endTime, numberOfRecords, cubeName, dataLoader);
        logger.info("Test data inserted into Kafka");
    }

    protected void clearSegment(String cubeName) throws Exception {
        CubeInstance cube = cubeManager.getCube(cubeName);
        cubeManager.updateCubeDropSegments(cube, cube.getSegments());
    }

    public void build() throws Exception {
        clearSegment(cubeName);
        clearSegment(joinTableCubeName);
        new Thread(new Runnable() {
            @Override
            public void run() {
                SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
                f.setTimeZone(TimeZone.getTimeZone("GMT"));
                long dateStart = 0;
                try {
                    dateStart = f.parse("2012-01-01").getTime();
                } catch (ParseException e) {
                }
                Random rand = new Random();
                while (generateData == true) {
                    long dateEnd = dateStart + 7 * 24 * 3600000;
                    try {
                        generateStreamData(dateStart, dateEnd, rand.nextInt(100));
                        dateStart = dateEnd;
                        sleep(rand.nextInt(rand.nextInt(30)) * 1000); // wait random time
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                generateDataDone = true;
            }
        }).start();
        ExecutorService executorService = Executors.newCachedThreadPool();

        List<FutureTask<ExecutableState>> futures = Lists.newArrayList();
        for (int i = 0; i < BUILD_ROUND; i++) {
            if (i == (BUILD_ROUND - 1)) {
                // stop generating message to kafka
                generateData = false;
                int waittime = 0;
                while (generateDataDone == false && waittime < 100) {
                    Thread.sleep(1000);
                    waittime++;
                }
                if (generateDataDone == false) {
                    throw new IllegalStateException("Timeout when wait all messages be sent to Kafka"); // ensure all messages have been flushed.
                }
            } else {
                Thread.sleep(30 * 1000); // wait for new messages
            }

            FutureTask futureTask = new FutureTask(new StreamOffsetCallable(cubeName, 0, Long.MAX_VALUE));

            executorService.submit(futureTask);
            futures.add(futureTask);
        }

        Thread.sleep(30 * 1000);

        // build joined lookup table streaming cube
        // range is normal streaming cube's first segment.start and last segment.end
        CubeInstance cube = cubeManager.getCube(cubeName);
        CubeSegment firstSeg = cube.getFirstSegment();
        CubeSegment lastSeg = cube.getLastSegment();
        SourcePartition sourcePartition = new SourcePartition(null, new SegmentRange(firstSeg.getSegRange().start, lastSeg.getSegRange().end), firstSeg.getSourcePartitionOffsetStart(), lastSeg.getSourcePartitionOffsetEnd());

        FutureTask futureTask = new FutureTask(new StreamSourcePartitionCallable(joinTableCubeName, sourcePartition));

        executorService.submit(futureTask);
        futures.add(futureTask);

        generateData = false;
        executorService.shutdown();
        int succeedBuild = 0;
        for (int i = 0; i < futures.size(); i++) {
            ExecutableState result = futures.get(i).get(20, TimeUnit.MINUTES);
            logger.info("Checking building task " + i + " whose state is " + result);
            Assert.assertTrue(
                    result == null || result == ExecutableState.SUCCEED || result == ExecutableState.DISCARDED);
            if (result == ExecutableState.SUCCEED)
                succeedBuild++;
        }

        logger.info(succeedBuild + " build jobs have been successfully completed.");
        List<CubeSegment> segments = cubeManager.getCube(cubeName).getSegments(SegmentStatusEnum.READY);
        List<CubeSegment> joinTableSegments = cubeManager.getCube(joinTableCubeName).getSegments(SegmentStatusEnum.READY);

        Assert.assertTrue(segments.size() + joinTableSegments.size() == succeedBuild);

        if (simpleBuildMode == false) {
            long endOffset = (Long) segments.get(segments.size() - 1).getSegRange().end.v;
            //merge
            ExecutableState result = mergeSegment(cubeName, new SegmentRange(0L, endOffset));
            Assert.assertTrue(result == ExecutableState.SUCCEED);

            segments = cubeManager.getCube(cubeName).getSegments();
            Assert.assertTrue(segments.size() == 1);

            SegmentRange.TSRange tsRange = segments.get(0).getTSRange();
            Assert.assertTrue(tsRange.duration() > 0);

            CubeSegment toRefreshSeg = segments.get(0);

            refreshSegment(cubeName, toRefreshSeg.getSegRange());
            segments = cubeManager.getCube(cubeName).getSegments();
            Assert.assertTrue(segments.size() == 1);
        }

        logger.info("Build is done");
    }

    private ExecutableState mergeSegment(String cubeName, SegmentRange segRange) throws Exception {
        CubeSegment segment = cubeManager.mergeSegments(cubeManager.getCube(cubeName), null, segRange, false);
        DefaultChainedExecutable job = EngineFactory.createBatchMergeJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getStatus();
    }

    private String refreshSegment(String cubeName, SegmentRange segRange) throws Exception {
        CubeSegment segment = cubeManager.refreshSegment(cubeManager.getCube(cubeName), null, segRange);
        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getId();
    }

    protected ExecutableState buildSegment(String cubeName, long startOffset, long endOffset) throws Exception {
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        ISource source = SourceManager.getSource(cubeInstance);
        SourcePartition partition = source.enrichSourcePartitionBeforeBuild(cubeInstance,
                new SourcePartition(null, new SegmentRange(startOffset, endOffset), null, null));

        return buildSegment(cubeName, partition);
    }

    protected ExecutableState buildSegment(String cubeName, SourcePartition partition) throws Exception {
        logger.info("SourcePartition: {}", partition.toString());
        CubeSegment segment = cubeManager.appendSegment(cubeManager.getCube(cubeName), partition);

        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getStatus();
    }

    protected void deployEnv() throws Exception {
        DeployUtil.overrideJobJarLocations();
//        DeployUtil.initCliWorkDir();
//        DeployUtil.deployMetadata();

        // prepare test data for joined lookup table
        DeployUtil.deployTablesInModelWithExclusiveTables("test_streaming_join_table_model", new String[]{"DEFAULT.STREAMING_TABLE"});
    }


    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, HBaseMetadataTestCase.SANDBOX_TEST_DATA);
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException(
                    "No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.4.0.0-169");
        }
        HBaseMetadataTestCase.staticCreateTestMetadata(HBaseMetadataTestCase.SANDBOX_TEST_DATA);
    }

    public void after() {
        if (kafkaServer != null) {
            kafkaServer.stop();
        }
        ZKUtil.cleanZkPath(kafkaZkPath);
        DefaultScheduler.destroyInstance();
    }

    protected void waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR
                    || job.getStatus() == ExecutableState.DISCARDED) {
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

    public void cleanup() throws Exception {
        cleanupOldStorage();
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    protected void cleanupOldStorage() throws Exception {
        String[] args = { "--delete", "true" };
        StorageCleanupJob cli = new StorageCleanupJob();
        cli.execute(args);
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        int exitCode = 0;

        BuildCubeWithStream buildCubeWithStream = null;
        try {
            beforeClass();
            buildCubeWithStream = new BuildCubeWithStream();
            buildCubeWithStream.before();
            buildCubeWithStream.build();
            logger.info("Build is done");
        } catch (Throwable e) {
            logger.error("error", e);
            exitCode = 1;
        } finally {
            if (buildCubeWithStream != null) {
                buildCubeWithStream.after();
                buildCubeWithStream.cleanup();
            }
            logger.info("Going to exit");
        }

        long millis = System.currentTimeMillis() - start;
        System.out.println("Time elapsed: " + (millis / 1000) + " sec - in " + BuildCubeWithStream.class.getName());

        System.exit(exitCode);
    }

    class StreamOffsetCallable implements Callable<ExecutableState> {
        private final String cubeName;
        private final long startOffset;
        private final long endOffset;

        public StreamOffsetCallable(String cubeName, long startOffset, long endOffset) {
            this.cubeName = cubeName;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public ExecutableState call() throws Exception {
            ExecutableState result = null;
            try {
                result = buildSegment(cubeName, startOffset, endOffset);
            } catch (Exception e) {
                // previous build hasn't been started, or other case.
                e.printStackTrace();
            }
            return result;
        }
    }

    class StreamSourcePartitionCallable implements Callable<ExecutableState> {
        private final String cubeName;
        private final SourcePartition sourcePartition;

        public StreamSourcePartitionCallable(String cubeName, SourcePartition sourcePartition) {
            this.cubeName = cubeName;
            this.sourcePartition = sourcePartition;
        }

        @Override
        public ExecutableState call() throws Exception {
            ExecutableState result = null;
            try {
                result = buildSegment(cubeName, sourcePartition);
            } catch (Exception e) {
                // previous build hasn't been started, or other case.
                e.printStackTrace();
            }
            return result;
        }
    }
}
