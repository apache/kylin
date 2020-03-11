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

package org.apache.kylin.stream.coordinator;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.ZKUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.stream.coordinator.exception.ClusterStateException;
import org.apache.kylin.stream.core.client.ReceiverAdminClient;
import org.apache.kylin.stream.core.model.ConsumerStatsResponse;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.PauseConsumersRequest;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.StartConsumersRequest;
import org.apache.kylin.stream.core.model.StopConsumersRequest;
import org.apache.kylin.stream.core.source.Partition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertSame;

import org.mockito.AdditionalMatchers;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.eq;

/**
 * Since Coordinator take the responsibility of the manager of whole receiver cluster,
 * it may require some carefully test for guarantee of consistent cluster state.
 */
public class CoordinatorTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    /**
     * mock zookeeper server for streaming metadata
     */
    private TestingServer testingServer;
    private Coordinator coordinator;
    private CubeInstance cube;
    private StreamMetadataStore metadataStore;

    ReplicaSet rs1 = new ReplicaSet();
    ReplicaSet rs2 = new ReplicaSet();
    ReplicaSet rs3 = new ReplicaSet();
    ReplicaSet rs4 = new ReplicaSet();
    Node n1 = new Node("Node-1", 1000);
    Node n2 = new Node("Node-2", 1000);
    Node n3 = new Node("Node-3", 1000);
    Node n4 = new Node("Node-4", 1000);
    Node n5 = new Node("Node-5", 1000);
    Node n6 = new Node("Node-6", 1000);
    Node n7 = new Node("Node-7", 1000);
    Node n8 = new Node("Node-8", 1000);

    String cubeName = "MockRealtimeCube";
    String positionRs1 = "{\"1\":10001,\"2\":10002}";
    String positionRs2 = "{\"3\":10003,\"4\":10004}";
    String positionRs3 = "{\"5\":10005,\"6\":10006}";

    Partition p1 = new Partition(1);
    Partition p2 = new Partition(2);
    Partition p3 = new Partition(3);
    Partition p4 = new Partition(4);
    Partition p5 = new Partition(5);
    Partition p6 = new Partition(6);

    @Before
    public void setUp() throws Exception {
        logger.info("Setup coordinator test env.");
        staticCreateTestMetadata();
        testingServer = new TestingServer(12181, false);
        testingServer.start();
        System.setProperty("kylin.env.zookeeper-connect-string", "localhost:12181");
        metadataStore = StreamMetadataStoreFactory.getZKStreamMetaDataStore();
        initZookeeperMetadataStore();
        mockCube();
    }

    @After
    public void tearDown() throws Exception {
        coordinator = null;
        ZKUtil.cleanZkPath(StreamingUtils.STREAM_ZK_ROOT);
        StreamingUtils.getZookeeperClient().close();
        testingServer.stop();// clear metadata
        System.clearProperty("kylin.env.zookeeper-connect-string");
    }

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

        // add replica set
        rs1.addNode(n1);
        rs1.addNode(n2);
        rs2.addNode(n3);
        rs2.addNode(n4);
        rs3.addNode(n5);
        rs3.addNode(n6);
        rs4.addNode(n7);
        rs4.addNode(n8);
        metadataStore.createReplicaSet(rs1);
        metadataStore.createReplicaSet(rs2);
        metadataStore.createReplicaSet(rs3);
        metadataStore.createReplicaSet(rs4);

        // add assignment for cube
        Map<Integer, List<Partition>> preAssignMap = new HashMap<>();
        preAssignMap.put(0, Lists.newArrayList(p1, p2));
        preAssignMap.put(1, Lists.newArrayList(p3, p4));
        preAssignMap.put(2, Lists.newArrayList(p5, p6));
        CubeAssignment originalAssignment = new CubeAssignment(cubeName, preAssignMap);
        metadataStore.saveNewCubeAssignment(originalAssignment);
    }

    /**
     * mock a receiver client which always return success
     */
    private ReceiverAdminClient mockSuccessReceiverAdminClient() throws IOException {
        ReceiverAdminClient receiverAdminClient = mock(ReceiverAdminClient.class);

        ConsumerStatsResponse consumerStatsResponse = new ConsumerStatsResponse();
        consumerStatsResponse.setConsumePosition(positionRs1);
        consumerStatsResponse.setCubeName(cubeName);

        when(receiverAdminClient.pauseConsumers(any(Node.class), any(PauseConsumersRequest.class)))
                .thenReturn(consumerStatsResponse);
        return receiverAdminClient;
    }

    private ReceiverAdminClient mockReceiverClientFailOnStopAndSync() throws IOException {
        ReceiverAdminClient receiverAdminClient = mock(ReceiverAdminClient.class);

        ConsumerStatsResponse consumerStatsResponse = new ConsumerStatsResponse();
        consumerStatsResponse.setConsumePosition(positionRs1);
        consumerStatsResponse.setCubeName(cubeName);
        when(receiverAdminClient.pauseConsumers(eq(n4), any(PauseConsumersRequest.class)))
                .thenThrow(new IOException("Mock Receiver Error"));
        when(receiverAdminClient.pauseConsumers(AdditionalMatchers.not(Matchers.eq(n4)),
                any(PauseConsumersRequest.class))).thenReturn(consumerStatsResponse);

        Mockito.doThrow(new IOException()).when(receiverAdminClient).startConsumers(eq(n1),
                any(StartConsumersRequest.class));
        return receiverAdminClient;
    }

    private ReceiverAdminClient mockReceiverClientFailOnStartNewComsumer() throws IOException {
        ReceiverAdminClient receiverAdminClient = mock(ReceiverAdminClient.class);

        ConsumerStatsResponse consumerStatsResponse = new ConsumerStatsResponse();
        consumerStatsResponse.setConsumePosition(positionRs1);
        consumerStatsResponse.setCubeName(cubeName);
        when(receiverAdminClient.pauseConsumers(any(Node.class), any(PauseConsumersRequest.class)))
                .thenReturn(consumerStatsResponse);
        Mockito.doThrow(new IOException()).when(receiverAdminClient).startConsumers(eq(n7),
                any(StartConsumersRequest.class));

        Mockito.doThrow(new IOException()).when(receiverAdminClient).stopConsumers(eq(n5),
                any(StopConsumersRequest.class));
        return receiverAdminClient;
    }

    private void mockCube() {
        cube = mock(CubeInstance.class);
        when(cube.getSourceType()).thenReturn(ISourceAware.ID_KAFKA);
        when(cube.getName()).thenReturn(cubeName);
    }

    @Test
    public void testReassignWithoutExeception() throws IOException {

        ReceiverAdminClient receiverAdminClient = mockSuccessReceiverAdminClient();
        coordinator = new Coordinator(metadataStore, receiverAdminClient);

        Map<Integer, List<Partition>> preAssignMap = metadataStore.getAssignmentsByCube(cubeName).getAssignments();
        Map<Integer, List<Partition>> newAssignMap = new HashMap<>();
        newAssignMap.put(1, Lists.newArrayList(p1, p2, p3));
        newAssignMap.put(2, Lists.newArrayList(p4, p5));
        newAssignMap.put(3, Lists.newArrayList(p6));

        CubeAssignment preAssigment = new CubeAssignment(cube.getName(), preAssignMap);
        CubeAssignment newAssigment = new CubeAssignment(cube.getName(), newAssignMap);

        coordinator.doReassign(cube, preAssigment, newAssigment);
    }

    @Test(expected = ClusterStateException.class)
    public void testReassignFailOnStopAndSync() throws IOException {

        ReceiverAdminClient receiverAdminClient = mockReceiverClientFailOnStopAndSync();
        coordinator = new Coordinator(metadataStore, receiverAdminClient);

        Map<Integer, List<Partition>> preAssignMap = metadataStore.getAssignmentsByCube(cubeName).getAssignments();

        Map<Integer, List<Partition>> newAssignMap = new LinkedHashMap<>();
        newAssignMap.put(1, Lists.newArrayList(p1, p2, p3));
        newAssignMap.put(2, Lists.newArrayList(p4, p5));
        newAssignMap.put(3, Lists.newArrayList(p6));

        CubeAssignment preAssigment = new CubeAssignment(cube.getName(), preAssignMap);
        CubeAssignment newAssigment = new CubeAssignment(cube.getName(), newAssignMap);

        try {
            coordinator.doReassign(cube, preAssigment, newAssigment);
        } catch (ClusterStateException rune) {
            assertSame(ClusterStateException.ClusterState.ROLLBACK_FAILED, rune.getClusterState());
            assertSame(ClusterStateException.TransactionStep.STOP_AND_SNYC, rune.getTransactionStep());
            System.out.println(rune.getMessage());
            throw rune;
        }
    }

    @Test(expected = ClusterStateException.class)
    public void testReassignFailOnStartNew() throws IOException {

        ReceiverAdminClient receiverAdminClient = mockReceiverClientFailOnStartNewComsumer();
        coordinator = new Coordinator(metadataStore, receiverAdminClient);

        Map<Integer, List<Partition>> preAssignMap = metadataStore.getAssignmentsByCube(cubeName).getAssignments();

        Map<Integer, List<Partition>> newAssignMap = new LinkedHashMap<>();
        newAssignMap.put(1, Lists.newArrayList(p1, p2, p3));
        newAssignMap.put(2, Lists.newArrayList(p4, p5));
        newAssignMap.put(3, Lists.newArrayList(p6));

        CubeAssignment preAssigment = new CubeAssignment(cube.getName(), preAssignMap);
        CubeAssignment newAssigment = new CubeAssignment(cube.getName(), newAssignMap);

        try {
            coordinator.doReassign(cube, preAssigment, newAssigment);
        } catch (ClusterStateException rune) {
            assertSame(ClusterStateException.ClusterState.ROLLBACK_FAILED, rune.getClusterState());
            assertSame(ClusterStateException.TransactionStep.START_NEW, rune.getTransactionStep());
            System.out.println(rune.getMessage());
            throw rune;
        }
    }

}