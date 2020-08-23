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

package org.apache.kylin.realtime;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.ZKUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.DeployUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.zookeeper.ZookeeperJobLock;
import org.apache.kylin.job.streaming.Kafka10DataLoader;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.provision.MockKafka;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.rest.job.StorageCleanupJob;
import org.apache.kylin.stream.coordinator.Coordinator;
import org.apache.kylin.stream.coordinator.StreamingUtils;
import org.apache.kylin.stream.core.client.ReceiverAdminClient;
import org.apache.kylin.stream.core.consumer.ConsumerStartMode;
import org.apache.kylin.stream.core.consumer.ConsumerStartProtocol;
import org.apache.kylin.stream.core.model.AssignRequest;
import org.apache.kylin.stream.core.model.ConsumerStatsResponse;
import org.apache.kylin.stream.core.model.HealthCheckInfo;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.PauseConsumersRequest;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.ResumeConsumerRequest;
import org.apache.kylin.stream.core.model.StartConsumersRequest;
import org.apache.kylin.stream.core.model.StopConsumersRequest;
import org.apache.kylin.stream.core.model.UnAssignRequest;
import org.apache.kylin.stream.core.model.stats.ReceiverCubeStats;
import org.apache.kylin.stream.core.model.stats.ReceiverStats;
import org.apache.kylin.stream.core.source.StreamingSourceConfig;
import org.apache.kylin.stream.core.source.StreamingSourceConfigManager;
import org.apache.kylin.stream.server.StreamingServer;
import org.apache.kylin.stream.source.kafka.KafkaSource;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildCubeWithStreamV2 extends KylinTestBase {
    private static final Logger logger = LoggerFactory.getLogger(BuildCubeWithStreamV2.class);

    private static final String CUBE_NAME = "test_streaming_v2_user_info_cube";
    private final String kafkaZkPath = ZKUtil.getZkRootBasedPath("streamingv2") + "/" + RandomUtil.randomUUID().toString();
    private final String messageFile = "src/test/resources/streaming_v2_user_info_messages.txt";

    private String topicName;
    private Coordinator coordinator;
    private StreamingServer streamingServer;
    private DefaultScheduler scheduler;
    private MockKafka kafkaServer;
    private KafkaProducer<byte[], byte[]> producer;
    private int replicaSetID;
    private volatile boolean generateDataDone = false;

    public BuildCubeWithStreamV2() {
        coordinator = Coordinator.getInstance();
        coordinator.setToLeader();

        streamingServer = StreamingServer.getInstance();
        streamingServer.setCoordinatorClient(coordinator);
        coordinator.setReceiverAdminClient(new MockedReceiverAdminClient(streamingServer));
    }

    public static void beforeClass() throws Exception {
        beforeClass(HBaseMetadataTestCase.SANDBOX_TEST_DATA);
    }

    public static void beforeClass(String confDir) throws Exception {
        logger.info("Adding to classpath: " + new File(confDir).getAbsolutePath());
        ClassUtil.addClasspath(new File(confDir).getAbsolutePath());

        System.setProperty(KylinConfig.KYLIN_CONF, confDir);
        System.setProperty("kylin.hadoop.conf.dir", confDir);
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException(
                    "No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.4.0.0-169");
        }

        // use mocked stream data search client
        System.setProperty("kylin.stream.stand-alone.mode", "true");

        // DeployUtil.deployMetadata();

        // setup cube conn and h2 conn
        setupAll();

        try {
            //check hdfs permission
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
            String hdfsWorkingDirectory = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
            Path coprocessorDir = new Path(hdfsWorkingDirectory);
            boolean success = fileSystem.mkdirs(coprocessorDir);
            if (!success) {
                throw new IOException("mkdir fails, please check hdfs permission");
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "failed to create kylin.env.hdfs-working-dir, Please make sure the user has right to access "
                            + KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(),
                    e);
        }

        cleanStreamZkRoot();
    }

    public static void afterClass() {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    public void before() throws Exception {
        deployEnv();

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        scheduler = DefaultScheduler.createInstance();
        scheduler.init(new JobEngineConfig(kylinConfig), new ZookeeperJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(CUBE_NAME);
        final String streamingTableName = cubeInstance.getRootFactTable();
        final StreamingSourceConfig sourceConfig = StreamingSourceConfigManager.getInstance(kylinConfig).getConfig(streamingTableName);

        topicName = KafkaSource.getTopicName(sourceConfig.getProperties());
        String bootstrapServers = KafkaSource.getBootstrapServers(sourceConfig.getProperties());

        String server = bootstrapServers.split(",")[0];
        int brokerId = 0;

        // start kafka broker and create topic
        startEmbeddedKafka(topicName, server, brokerId);

        // start streamingServer
        streamingServer.start();

        // add receiver to replicaSet
        ReplicaSet replicaSet = new ReplicaSet();
        replicaSet.addNode(new Node());
        coordinator.createReplicaSet(replicaSet);
        replicaSetID = replicaSet.getReplicaSetID();

        // becomeLeader
        streamingServer.becomeLeader();

        // enabse streaming cube
        CubeManager.getInstance(kylinConfig).updateCubeStatus(cubeInstance, RealizationStatusEnum.READY);

        // assign cube/start consumer from LATEST
        coordinator.assignCube(CUBE_NAME);

        // init producer
        Properties properties = new Properties();
        properties.put("key.serializer", ByteArraySerializer.class.getName());
        properties.put("value.serializer", ByteArraySerializer.class.getName());
        producer = new KafkaProducer<byte[], byte[]>(Kafka10DataLoader.constructDefaultKafkaProducerProperties(bootstrapServers, properties));
    }

    public void produce(int size) throws IOException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                BufferedReader messageReader = null;
                try {
                    logger.info("Generate data start");
                    messageReader = Files.newBufferedReader(Paths.get(messageFile));
                    String message;
                    int index = 0;
                    while((message = messageReader.readLine()) != null && index < size) {
                        //logger.info("----message{}-----{}", index, message);
                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topicName, String.valueOf(index).getBytes(StandardCharsets.UTF_8), message.getBytes(StandardCharsets.UTF_8));
                        producer.send(record);
                        index++;
                        Thread.sleep(50);
                    }
                    generateDataDone = true; // spend 500s
                    logger.info("Generate data done");
                } catch (Exception e) {
                    //
                } finally {
                    IOUtils.closeQuietly(messageReader);
                    producer.close();
                }
            }
        }).start();
    }

    public void beforeQuery() throws Exception {
        // send all message need 500s
        Thread.sleep(500 * 1000);

        int waittime = 0;
        // is generate data done, total wait 2 min
        while (generateDataDone == false && waittime < 2) {
            Thread.sleep(60 * 1000);
                waittime++;
        }

        if (generateDataDone == false) {
            throw new IllegalStateException("Timeout when wait all messages be sent to Kafka"); // ensure all messages have been flushed.
        }

        waittime = 0;
        boolean consumeDataDone = false;
        // is consume data done, total wait 5 min
        while ((consumeDataDone = isConsumeDataDone()) == false && waittime < 5) {
            Thread.sleep(60 * 1000);
            waittime++;
        }

        if (consumeDataDone == false) {
            throw new IllegalStateException("Exec timeout, data not be consumed completely"); // ensure all messages have been comsumed.
        }

        waittime = 0;
        boolean segmentBuildSuccess = false;
        // is segment build success, total wait 10 min
        while ((segmentBuildSuccess = isSegmentBuildSuccess()) == false && waittime < 5) {
            Thread.sleep(120 * 1000);
            waittime++;
        }

        if (segmentBuildSuccess == false) {
            throw new IllegalStateException("Build failed/timeout, no ready segment"); // ensure at least one segment has been built.
        }
    }

    public void query() throws Exception {
        boolean success = execAndCompSuccess("src/test/resources/query/sql_streaming_v2/compare_result", null, true)
                    && execSuccess("src/test/resources/query/sql_streaming_v2/not_compare_result");

        if (success == false) {
            throw new IllegalStateException("Build failed/timeout, no ready segment"); // ensure all query have been passed
        }
    }

    public void after() throws Exception {
        coordinator.unAssignCube(CUBE_NAME);
        streamingServer.removeFromReplicaSet();
        if (kafkaServer != null) {
            kafkaServer.stop();

        }
        ZKUtil.cleanZkPath(kafkaZkPath);
        DefaultScheduler.destroyInstance();
    }

    public void cleanup() throws Exception {
        cleanupOldStorage();
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    private boolean execAndCompSuccess(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
       try {
           execAndCompQuery(queryFolder, exclusiveQuerys, needSort);
           return true;
       } catch (Exception e) {
           logger.error("Exec query and compare result failed", e);
           return false;
       }
    }

    private boolean execSuccess(String queryFolder) throws Exception {
        try {
            batchExecuteQuery(queryFolder);
            return true;
        } catch (Exception e) {
            logger.error("Exec query failed", e);
            return false;
        }
    }

    private boolean isConsumeDataDone() throws Exception {
        return execAndCompSuccess("src/test/resources/query/sql_streaming_v2/count", null, false);
    }

    private boolean isSegmentBuildSuccess() {
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(CUBE_NAME);
        return cubeInstance.getLatestReadySegment() != null;
    }

    public void cleanupOldStorage() throws Exception {
        String[] args = { "--delete", "true" };
        StorageCleanupJob cli = new StorageCleanupJob();
        cli.execute(args);
    }

    private void startEmbeddedKafka(String topicName, String server, int brokerId) {
        ZkConnection zkConnection = new ZkConnection(ZKUtil.getZKConnectString(KylinConfig.getInstanceFromEnv()) + kafkaZkPath);

        // start kafka server
        kafkaServer = new MockKafka(zkConnection, server, brokerId);
        kafkaServer.start();

        // create topic
        kafkaServer.createTopic(topicName, 3, 1);
        kafkaServer.waitTopicUntilReady(topicName);

        MetadataResponse.TopicMetadata topicMetadata = kafkaServer.fetchTopicMeta(topicName);
        Assert.assertEquals(topicName, topicMetadata.topic());
    }

    private void deployEnv() throws Exception {
        DeployUtil.overrideJobJarLocations();
//        DeployUtil.initCliWorkDir();
//        DeployUtil.deployMetadata();
    }

    public static void cleanStreamZkRoot() {
        ZKUtil.cleanZkPath(StreamingUtils.STREAM_ZK_ROOT);
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        int exitCode = 0;

        BuildCubeWithStreamV2 buildCubeWithStreamV2 = null;
        try {
            beforeClass();
            buildCubeWithStreamV2 = new BuildCubeWithStreamV2();

            // start kafka server and create topic; start coordinator, streamingserver and consumer
            buildCubeWithStreamV2.before();

            // send message
            buildCubeWithStreamV2.produce(10000);

            // wait for
            buildCubeWithStreamV2.beforeQuery();

            // query
            buildCubeWithStreamV2.query();
        } catch (Throwable e) {
            logger.error("error", e);
            exitCode = 1;
        } finally {
            try {
                if (buildCubeWithStreamV2 != null) {
                    buildCubeWithStreamV2.after();
                    buildCubeWithStreamV2.cleanup();
                }
            } catch (Exception e) {
                //
            }

            logger.info("Going to exit");
        }

        long millis = System.currentTimeMillis() - start;
        System.out.println("Time elapsed: " + (millis / 1000) + " sec - in " + BuildCubeWithStreamV2.class.getName());

        System.exit(exitCode);
    }

    static class MockedReceiverAdminClient implements ReceiverAdminClient {
        private StreamingServer streamingServer;

        public MockedReceiverAdminClient(StreamingServer streamingServer) {
            this.streamingServer = streamingServer;
        }

        @Override
        public void assign(Node receiver, AssignRequest assignRequest) throws IOException {
            streamingServer.assign(assignRequest.getCubeName(), assignRequest.getPartitions());
            if (assignRequest.isStartConsumers()) {
                streamingServer.startConsumer(assignRequest.getCubeName(), new ConsumerStartProtocol(ConsumerStartMode.LATEST));
            }
        }

        @Override
        public void unAssign(Node receiver, UnAssignRequest unAssignRequest) throws IOException {
            streamingServer.unAssign(unAssignRequest.getCube());
        }

        @Override
        public void startConsumers(Node receiver, StartConsumersRequest startRequest) throws IOException {

        }

        @Override
        public ConsumerStatsResponse stopConsumers(Node receiver, StopConsumersRequest stopRequest) throws IOException {
            return null;
        }

        @Override
        public ConsumerStatsResponse pauseConsumers(Node receiver, PauseConsumersRequest request) throws IOException {
            return null;
        }

        @Override
        public ConsumerStatsResponse resumeConsumers(Node receiver, ResumeConsumerRequest request) throws IOException {
            return null;
        }

        @Override
        public void removeCubeSegment(Node receiver, String cubeName, String segmentName) throws IOException {

        }

        @Override
        public void makeCubeImmutable(Node receiver, String cubeName) throws IOException {

        }

        @Override
        public void segmentBuildComplete(Node receiver, String cubeName, String segmentName) throws IOException {
            streamingServer.remoteSegmentBuildComplete(cubeName, segmentName);
        }

        @Override
        public void addToReplicaSet(Node receiver, int replicaSetID) throws IOException {
            streamingServer.addToReplicaSet(replicaSetID);
        }

        @Override
        public void removeFromReplicaSet(Node receiver) throws IOException {

        }

        @Override
        public ReceiverStats getReceiverStats(Node receiver) throws IOException {
            return null;
        }

        @Override
        public ReceiverCubeStats getReceiverCubeStats(Node receiver, String cubeName) throws IOException {
            return null;
        }

        @Override
        public HealthCheckInfo healthCheck(Node receiver) throws IOException {
            return null;
        }
    }

}
