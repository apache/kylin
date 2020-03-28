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

package org.apache.kylin.it.provision;

import static java.lang.Thread.sleep;
import static org.apache.kylin.it.provision.BuildEngineUtil.LINE;
import static org.apache.kylin.it.provision.BuildEngineUtil.cleanupOldStorage;
import static org.apache.kylin.it.provision.BuildEngineUtil.isNRTSkip;
import static org.apache.kylin.it.provision.BuildEngineUtil.isBuildSkip;
import static org.apache.kylin.it.provision.BuildEngineUtil.isSimpleBuildMode;
import static org.apache.kylin.it.provision.BuildEngineUtil.printEnv;
import static org.apache.kylin.it.provision.BuildEngineUtil.waitForJob;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.job.DeployUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
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
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.source.SourcePartition;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Build the cube from Kafka Source with Near Real-time Streaming build engine
 * before the Integration Test
 */
public class BuildCubeWithStream {

    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithStream.class);

    protected CubeManager cubeManager;
    private DefaultScheduler scheduler;
    protected ExecutableManager jobService;
    static final String cubeName = "test_streaming_table_cube";
    static final String joinTableCubeName = "test_streaming_join_table_cube";

    private KafkaConfig kafkaConfig;
    protected static boolean simpleBuildMode = false;
    private volatile boolean generateData = true;
    private volatile boolean generateDataDone = false;

    /**
     * How many segment will be build
     */
    private static int BUILD_ROUND = 4;
    private static long DURATION_ROUND_DURATION = 60000L;
    private static int NUM_RECORD_EACH_ROUND = 100;
    private static long ROUND_EVENT_TIME_RANGE = 7 * 24 * 3600000L;

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        int exitCode = 0;

        BuildCubeWithStream buildCubeWithStream = null;
        try {
            beforeClass();
            buildCubeWithStream = new BuildCubeWithStream();
            buildCubeWithStream.before();
            buildCubeWithStream.build();
        } catch (Throwable e) {
            logger.error("Error ", e);
            exitCode = 1;
        } finally {
            if (buildCubeWithStream != null) {
                buildCubeWithStream.after();
            }
        }

        long millis = System.currentTimeMillis() - start;
        System.out.println("Time elapsed: " + (millis / 1000) + " sec - in " + BuildCubeWithStream.class.getName());
        System.exit(exitCode);
    }

    public void before() throws Exception {
        deployEnv();
        simpleBuildMode = isSimpleBuildMode();
        if (simpleBuildMode) {
            BUILD_ROUND = 1;
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
//      kafkaConfig.getKafkaClusterConfigs().get(0).getBrokerConfigs().get(0).setHost("sandbox-hdp.hortonworks.com");

        LocalDateTime localDateTime = LocalDateTime.now();
        String topicName = "Topic" + localDateTime.getHour() + localDateTime.getMinute();
        kafkaConfig.setTopic(topicName);
        kylinConfig.getCliCommandExecutor().execute("sh $KAFKA_HOME/bin/kafka-topics.sh --create" +
                " --topic " + topicName +
                " --zookeeper localhost:2181/lacus" +
                " --partitions 3" +
                " --replication-factor 1");
        KafkaConfigManager.getInstance(kylinConfig).updateKafkaConfig(kafkaConfig);
    }

    public void build() throws Exception {
        clearSegment(cubeName);
        clearSegment(joinTableCubeName);
        produceMessage();
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        List<FutureTask<ExecutableState>> futures = Lists.newArrayList();
        for (int i = 1; i <= BUILD_ROUND; i++) {
            Thread.sleep(DURATION_ROUND_DURATION);
            logger.info("\n{}Build at round {}", LINE, i);
            if (i == BUILD_ROUND) {
                generateData = false;
                int wait = 0;
                while (!generateDataDone && wait < 30) {
                    Thread.sleep(3000);
                    logger.debug("Waiting kafka thread to stop ...");
                    wait++;
                }
                if (!generateDataDone) {
                    throw new IllegalStateException("Timeout when wait all messages be sent to Kafka"); // ensure all messages have been flushed.
                }
            }
            StreamOffsetCallable callable = new StreamOffsetCallable(cubeName, 0, Long.MAX_VALUE);
            callable.seqId = i;
            FutureTask futureTask = new FutureTask(callable);
            executorService.submit(futureTask);
            futures.add(futureTask);
        }
        logger.info("Waiting for a few seconds...");
        Thread.sleep(DURATION_ROUND_DURATION);

        // build joined lookup table streaming cube
        // range is normal streaming cube's first segment.start and last segment.end
        CubeInstance cube = cubeManager.getCube(cubeName);
        CubeSegment firstSeg = cube.getFirstSegment();
        CubeSegment lastSeg = cube.getLastSegment();
        SourcePartition sourcePartition = new SourcePartition(null,
                new SegmentRange(firstSeg.getSegRange().start, lastSeg.getSegRange().end),
                firstSeg.getSourcePartitionOffsetStart(),
                lastSeg.getSourcePartitionOffsetEnd());

        FutureTask futureTask = new FutureTask(new StreamSourcePartitionCallable(joinTableCubeName, sourcePartition));

        executorService.submit(futureTask);
        futures.add(futureTask);

        executorService.shutdown();
        int succeedBuild = 0;
        logger.info("All building job [{}] submitted to thread pool, we will wait and check if them done as expected.", futures.size());
        for (int i = 0; i < futures.size(); i++) {
            FutureTask<ExecutableState> f = futures.get(i);
            logger.info("Waiting building job {} for at most 20 minutes.", i);
            ExecutableState result = f.get(20, TimeUnit.MINUTES);
            logger.info("Checking building task {} whose state is {}", i, result);
            Assert.assertTrue(
                    result == null || result == ExecutableState.SUCCEED || result == ExecutableState.DISCARDED);
            if (result == ExecutableState.SUCCEED)
                succeedBuild++;
        }

        logger.info("{} build jobs have been successfully completed.", succeedBuild);
        List<CubeSegment> segments = cubeManager.getCube(cubeName).getSegments(SegmentStatusEnum.READY);
        List<CubeSegment> joinTableSegments = cubeManager.getCube(joinTableCubeName).getSegments(SegmentStatusEnum.READY);
        logger.info("Checking if all segment's job has succeed.");
        Assert.assertEquals(segments.size() + joinTableSegments.size(), succeedBuild);

        if (!simpleBuildMode) {
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

    private void produceMessage() {
        Thread t = new Thread(() -> {
            SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
            f.setTimeZone(TimeZone.getTimeZone("GMT"));
            long dateStart = 0;
            try {
                dateStart = f.parse("2012-01-01").getTime();
            } catch (ParseException e) {
                // Always right.
            }
            while (generateData) {
                long dateEnd = dateStart + ROUND_EVENT_TIME_RANGE;
                try {
                    generateStreamData(dateStart, dateEnd, NUM_RECORD_EACH_ROUND);
                    dateStart = dateEnd;
                    sleep(DURATION_ROUND_DURATION / 5);
                } catch (Exception e) {
                    logger.error("Error", e);
                }
            }
            generateDataDone = true;
            logger.warn("Kafka producer closed.");
        });
        t.setName("Kafka-Producer");
        t.start();
    }

    // =====================================================================================
    // Util method
    // =====================================================================================

    /**
     * Insert some records into Kafka Broker and also save them into local temp file(for compare)
     */
    protected void generateStreamData(long startTime, long endTime, int numberOfRecords) throws IOException {
        Kafka10DataLoader dataLoader = new Kafka10DataLoader(kafkaConfig);
        DeployUtil.prepareTestDataForStreamingCube(startTime, endTime, numberOfRecords, cubeName, dataLoader);
        logger.info("Test data inserted into Kafka");
    }

    protected void clearSegment(String cubeName) throws Exception {
        CubeInstance cube = cubeManager.getCube(cubeName);
        cubeManager.updateCubeDropSegments(cube, cube.getSegments());
    }

    protected void deployEnv() throws Exception {
        DeployUtil.overrideJobJarLocations();
        DeployUtil.deployTablesInModelWithExclusiveTables("test_streaming_join_table_model", new String[]{"DEFAULT.STREAMING_TABLE"});
    }

    public static void beforeClass() throws Exception {
        isNRTSkip();
        isBuildSkip();
        printEnv();
        System.setProperty("kylin.hadoop.conf.dir", HBaseMetadataTestCase.SANDBOX_TEST_DATA);

        System.setProperty("HADOOP_USER_NAME", "root");
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
        try {
            DefaultScheduler.destroyInstance();
            cleanupOldStorage();
            HBaseMetadataTestCase.staticCleanupTestMetadata();
        } catch (Exception e) {
            logger.error("Error", e);
        }
    }

    private ExecutableState mergeSegment(String cubeName, SegmentRange segRange) throws Exception {
        CubeSegment segment = cubeManager.mergeSegments(cubeManager.getCube(cubeName), null, segRange, false);
        DefaultChainedExecutable job = EngineFactory.createBatchMergeJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(jobService, job.getId());
        return job.getStatus();
    }

    private String refreshSegment(String cubeName, SegmentRange segRange) throws Exception {
        CubeSegment segment = cubeManager.refreshSegment(cubeManager.getCube(cubeName), null, segRange);
        DefaultChainedExecutable job = EngineFactory.createBatchCubingJob(segment, "TEST");
        jobService.addJob(job);
        waitForJob(jobService, job.getId());
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
        waitForJob(jobService, job.getId());
        return job.getStatus();
    }

    class StreamOffsetCallable implements Callable<ExecutableState> {
        private final String cubeName;
        private final long startOffset;
        private final long endOffset;
        int seqId;

        public StreamOffsetCallable(String cubeName, long startOffset, long endOffset) {
            this.cubeName = cubeName;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public ExecutableState call() throws Exception {
            ExecutableState result = null;
            try {
                logger.info("\n{}Building segment[{}] from {} to {}", LINE, seqId, startOffset, endOffset);
                result = buildSegment(cubeName, startOffset, endOffset);
            } catch (Exception e) {
                // previous build hasn't been started, or other case.
                logger.error("Error at " + seqId, e);
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
                logger.info("\n{}Building segment with {}.", LINE, sourcePartition);
                result = buildSegment(cubeName, sourcePartition);
            } catch (Exception e) {
                // previous build hasn't been started, or other case.
                logger.error("Error", e);
            }
            return result;
        }
    }
}
