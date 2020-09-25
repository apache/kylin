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
package org.apache.kylin.stream.coordinator.coordinate;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.ZKUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.stream.coordinator.StreamMetadataStore;
import org.apache.kylin.stream.coordinator.StreamMetadataStoreFactory;
import org.apache.kylin.stream.coordinator.StreamingUtils;
import org.apache.kylin.stream.coordinator.ZookeeperStreamMetadataStore;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.SegmentBuildState;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.source.kafka.KafkaPosition;
import org.apache.kylin.stream.source.kafka.KafkaPositionHandler;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.mockito.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Make create stub easier.
 */
public class StreamingTestBase extends LocalFileMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StreamingTestBase.class);
    private static final int port = 12181;
    private static final int retryTimes = 10;
    private String connectStr;

    /**
     * mock zookeeper server for streaming metadata
     */
    TestingServer testingServer;

    /**
     * Real StreamMetadataStore based on fake zookeeper server
     */
    StreamMetadataStore metadataStore;

    @Before
    public void init() {
        logger.debug("Start zk and prepare meatdata.");
        staticCreateTestMetadata();
        int realPort = port;
        for (int i = 0; i <= retryTimes; i++) {
            try {
                testingServer = new TestingServer(realPort, false);
                testingServer.start();
            } catch (Exception e) { // maybe caused by port occupy
                logger.error("Failed start zookeeper server at " + realPort, e);
                realPort++;
                continue;
            }
            break;
        }
        Assume.assumeTrue(realPort - port < retryTimes);
        connectStr = "localhost:" + realPort;
        System.setProperty("kylin.env.zookeeper-connect-string", connectStr);
        metadataStore = StreamMetadataStoreFactory.getZKStreamMetaDataStore();
        initZookeeperMetadataStore();
    }

    @After
    public void tearDown() throws Exception {
        logger.debug("Tear down server.");
        ZKUtil.cleanZkPath(StreamingUtils.STREAM_ZK_ROOT);
        ((ZookeeperStreamMetadataStore) metadataStore).close();
        testingServer.stop();
        System.clearProperty("kylin.env.zookeeper-connect-string");
    }

    // ================================================================================
    // ================================ Init Metadata =================================

    // replica set info
    ReplicaSet rs1 = new ReplicaSet();
    ReplicaSet rs2 = new ReplicaSet();
    ReplicaSet rs3 = new ReplicaSet();
    ReplicaSet rs4 = new ReplicaSet();

    // receiver info
    Node n1 = new Node("Node-1", 1000);
    Node n2 = new Node("Node-2", 1000);
    Node n3 = new Node("Node-3", 1000);
    Node n4 = new Node("Node-4", 1000);
    Node n5 = new Node("Node-5", 1000);
    Node n6 = new Node("Node-6", 1000);
    Node n7 = new Node("Node-7", 1000);
    Node n8 = new Node("Node-8", 1000);

    // cube info
    String cubeName1 = "MockRealtimeCube_1";
    String cubeName2 = "MockRealtimeCube_2";
    String cubeName3 = "MockRealtimeCube_3";
    String cubeName4 = "MockRealtimeCube_4";

    // cube segment info
    String segment1 = "20190601120000_20190601130000";
    String segment2 = "20190601130000_20190601140000";

    // topic partition info
    Partition p1 = new Partition(1);
    Partition p2 = new Partition(2);
    Partition p3 = new Partition(3);
    Partition p4 = new Partition(4);
    Partition p5 = new Partition(5);
    Partition p6 = new Partition(6);

    String mockBuildJob1 = "mock_job_00001";
    String mockBuildJob2 = "mock_job_00002";
    String mockBuildJob3 = "mock_job_00003";
    String mockBuildJob4 = "mock_job_00004";

    private void initZookeeperMetadataStore() {

        // add receiver
        metadataStore.addReceiver(n1);
        metadataStore.addReceiver(n2);
        metadataStore.addReceiver(n3);
        metadataStore.addReceiver(n4);
        metadataStore.addReceiver(n5);
        metadataStore.addReceiver(n6);
        metadataStore.addReceiver(n7);
        metadataStore.addReceiver(n8);

        // add replica set and allocate receivers to replica set
        rs1.addNode(n1);
        rs1.addNode(n2);
        rs2.addNode(n3);
        rs2.addNode(n4);
        rs3.addNode(n5);
        rs3.addNode(n6);
        rs4.addNode(n7);
        rs4.addNode(n8);

        int rsId;
        rsId = metadataStore.createReplicaSet(rs1);
        rs1.setReplicaSetID(rsId);

        rsId = metadataStore.createReplicaSet(rs2);
        rs2.setReplicaSetID(rsId);

        rsId = metadataStore.createReplicaSet(rs3);
        rs3.setReplicaSetID(rsId);

        rsId = metadataStore.createReplicaSet(rs4);
        rs4.setReplicaSetID(rsId);

        createCubeMetadata(cubeName1, mockBuildJob1, true);
        createCubeMetadata(cubeName2, mockBuildJob2, false);
        createCubeMetadata(cubeName3, mockBuildJob3, true);
        createCubeMetadata(cubeName4, mockBuildJob4, true);
    }

    public void createCubeMetadata(String cubeName, String jobID, boolean addSegmentBuildState) {

        // add assignment for cube
        Map<Integer, List<Partition>> preAssignMap = new HashMap<>();
        preAssignMap.put(rs1.getReplicaSetID(), Lists.newArrayList(p1, p2));
        preAssignMap.put(rs2.getReplicaSetID(), Lists.newArrayList(p3, p4));
        preAssignMap.put(rs3.getReplicaSetID(), Lists.newArrayList(p5, p6));
        CubeAssignment originalAssignment = new CubeAssignment(cubeName, preAssignMap);
        metadataStore.saveNewCubeAssignment(originalAssignment);

        // add remote checkpoint for segment1
        // check StreamingServer#sendSegmentsToFullBuild
        KafkaPositionHandler kafkaPositionHandler = new KafkaPositionHandler();
        Map<Integer, Long> positionMap = new HashMap<>();
        positionMap.put(p1.getPartitionId(), 10001L);
        positionMap.put(p2.getPartitionId(), 10002L);

        String positionStr = kafkaPositionHandler.serializePosition(new KafkaPosition(positionMap));
        metadataStore.saveSourceCheckpoint(cubeName, segment1, rs1.getReplicaSetID(), positionStr);

        positionMap.clear();
        positionMap.put(p3.getPartitionId(), 10003L);
        positionMap.put(p4.getPartitionId(), 10004L);
        positionStr = kafkaPositionHandler.serializePosition(new KafkaPosition(positionMap));
        metadataStore.saveSourceCheckpoint(cubeName, segment1, rs2.getReplicaSetID(), positionStr);

        positionMap.clear();
        positionMap.put(p5.getPartitionId(), 10005L);
        positionMap.put(p6.getPartitionId(), 10006L);
        positionStr = kafkaPositionHandler.serializePosition(new KafkaPosition(positionMap));
        metadataStore.saveSourceCheckpoint(cubeName, segment1, rs3.getReplicaSetID(), positionStr);

        positionMap.clear();
        positionMap.put(p1.getPartitionId(), 20001L);
        positionMap.put(p2.getPartitionId(), 20002L);
        positionStr = kafkaPositionHandler.serializePosition(new KafkaPosition(positionMap));
        metadataStore.saveSourceCheckpoint(cubeName, segment2, rs1.getReplicaSetID(), positionStr);

        // notify by replica set data has been uploaded
        metadataStore.addCompleteReplicaSetForSegmentBuild(cubeName, segment1, rs1.getReplicaSetID());
        metadataStore.addCompleteReplicaSetForSegmentBuild(cubeName, segment1, rs2.getReplicaSetID());
        metadataStore.addCompleteReplicaSetForSegmentBuild(cubeName, segment1, rs3.getReplicaSetID());
        metadataStore.addCompleteReplicaSetForSegmentBuild(cubeName, segment2, rs1.getReplicaSetID());

        if (addSegmentBuildState) {
            // update segment build job info
            SegmentBuildState.BuildState buildState = new SegmentBuildState.BuildState();
            buildState.setBuildStartTime(System.currentTimeMillis() - 30 * 60 * 1000);// submit 30 minutes ago
            buildState.setJobId(jobID);
            buildState.setState(SegmentBuildState.BuildState.State.BUILDING);
            metadataStore.updateSegmentBuildState(cubeName, segment1, buildState);
        }
    }
    // ================================================================================
    // ============================= Prepare stub object ==============================

    CubingJob stubCubingJob(ExecutableState state) {
        CubingJob job = mock(CubingJob.class);
        when(job.getStatus()).thenReturn(state);
        return job;
    }

    ReceiverClusterManager stubReceiverClusterManager(StreamingCoordinator coordinator) {
        ReceiverClusterManager clusterManager = mock(ReceiverClusterManager.class);
        when(clusterManager.getCoordinator()).thenReturn(coordinator);
        return clusterManager;
    }

    KylinConfig stubKylinConfig() {
        KylinConfig kylinConfig = mock(KylinConfig.class);
        when(kylinConfig.getMaxBuildingSegments()).thenReturn(10);
        when(kylinConfig.getStreamingAssigner()).thenReturn("DefaultAssigner");

        // ZK part
        when(kylinConfig.getZKBaseSleepTimeMs()).thenReturn(5000);
        when(kylinConfig.getZKMaxRetries()).thenReturn(3);
        when(kylinConfig.getZookeeperConnectString()).thenReturn(connectStr);
        return kylinConfig;
    }

    StreamingCoordinator stubStreamingCoordinator(KylinConfig config, CubeManager cubeManager,
            ExecutableManager executableManager) {
        StreamingCoordinator coordinator = mock(StreamingCoordinator.class);
        when(coordinator.getConfig()).thenReturn(config);
        when(coordinator.getCubeManager()).thenReturn(cubeManager);
        when(coordinator.getExecutableManager()).thenReturn(executableManager);
        when(coordinator.getStreamMetadataStore()).thenReturn(metadataStore);
        return coordinator;
    }

    ExecutableManager stubExecutableManager(Map<String, CubingJob> cubingJobMap) {
        ExecutableManager executableManager = mock(ExecutableManager.class);
        for (Map.Entry<String, CubingJob> entry : cubingJobMap.entrySet()) {
            when(executableManager.getJob(eq(entry.getKey()))).thenReturn(entry.getValue());
        }
        return executableManager;
    }

    CubeManager stubCubeManager(CubeInstance cubeInstance, boolean promotedNewSegmentFailed) {
        CubeManager cubeManager = mock(CubeManager.class);
        try {
            when(cubeManager.getCube(anyString())).thenReturn(cubeInstance);
            if (promotedNewSegmentFailed) {
                doThrow(RuntimeException.class).when(cubeManager).promoteNewlyBuiltSegments(any(CubeInstance.class),
                        any(CubeSegment.class));
            } else {
                doNothing().when(cubeManager).promoteNewlyBuiltSegments(any(CubeInstance.class),
                        any(CubeSegment.class));
            }
            doNothing().when(cubeManager).promoteNewlyBuiltSegments(any(CubeInstance.class), any(CubeSegment.class));
        } catch (IOException ioe) {
            // a ugly workaroud for mock method with declaration of throwing checked exception
        }
        return cubeManager;
    }

    CubeInstance stubCubeInstance(CubeSegment cubSegment) {
        CubeInstance cubeInstance = mock(CubeInstance.class);
        CubeSegment readySegment = stubCubSegment(SegmentStatusEnum.READY, 0L, 1L);
        when(cubeInstance.latestCopyForWrite()).thenReturn(cubeInstance);
        @SuppressWarnings("unchecked")
        Segments<CubeSegment> segmentSegments = mock(Segments.class, RETURNS_DEEP_STUBS);

        Segments<CubeSegment> optimizedSegments = mock(Segments.class, RETURNS_DEEP_STUBS);

        when(segmentSegments.size()).thenReturn(1);
        when(cubeInstance.getBuildingSegments()).thenReturn(segmentSegments);
        when(cubeInstance.getName()).thenReturn(cubeName1);
        when(cubeInstance.getSegment(anyString(), Matchers.any())).thenReturn(cubSegment);

        when(optimizedSegments.size()).thenReturn(0);
        when(cubeInstance.getLatestReadySegment()).thenReturn(readySegment);
        when(cubeInstance.getSegments(SegmentStatusEnum.READY_PENDING)).thenReturn(optimizedSegments);
        when(cubeInstance.getSegments(SegmentStatusEnum.NEW)).thenReturn(segmentSegments);

        return cubeInstance;
    }

    CubeSegment stubCubSegment(SegmentStatusEnum statusEnum, long leftRange, long rightRange) {
        CubeSegment cubeSegment = mock(CubeSegment.class);
        when(cubeSegment.getTSRange()).thenReturn(new SegmentRange.TSRange(leftRange, rightRange));
        when(cubeSegment.getStatus()).thenReturn(statusEnum);
        return cubeSegment;
    }
}
