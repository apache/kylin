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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.stream.coordinator.exception.ClusterStateException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ZookeeperStreamMetadataStoreTest extends LocalFileMetadataTestCase {

    private TestingServer testingServer;
    private StreamMetadataStore metadataStore;

    @Before
    public void setUp() throws Exception {
        staticCreateTestMetadata();
        testingServer = new TestingServer(12181, false);
        testingServer.start();
        System.setProperty("kylin.stream.zookeeper", "localhost:12181");
        metadataStore = StreamMetadataStoreFactory.getZKStreamMetaDataStore();
    }

    @Test
    public void testSetCubeClusterState() {

        Map<String, Set<Integer>> failRs = Maps.newHashMap();
        failRs.put("asd", Sets.newHashSet(1, 2, 3));
        StreamMetadataStore.ReassignResult reassignResult = new StreamMetadataStore.ReassignResult(
                new ClusterStateException("test", ClusterStateException.ClusterState.ROLLBACK_FAILED,
                        ClusterStateException.TransactionStep.MAKE_IMMUTABLE, 10, failRs, null));
        metadataStore.setCubeClusterState(reassignResult);

        StreamMetadataStore.ReassignResult reassignResultFetched = metadataStore.getCubeClusterState("test");

        assertEquals(reassignResultFetched.cubeName, reassignResult.cubeName);
        assertEquals(reassignResultFetched.occurTimestamp, reassignResult.occurTimestamp);
        assertEquals(reassignResultFetched.clusterState, reassignResult.clusterState);
        assertEquals(reassignResultFetched.failedStep, reassignResult.failedStep);
        assertEquals(reassignResultFetched.failedRollback, reassignResult.failedRollback);

        reassignResult.failedStep = ClusterStateException.TransactionStep.ASSIGN_NEW;
        metadataStore.setCubeClusterState(reassignResult);
        reassignResultFetched = metadataStore.getCubeClusterState("test");
        assertEquals(ClusterStateException.TransactionStep.ASSIGN_NEW, reassignResultFetched.failedStep);
    }

    @After
    public void tearDown() throws Exception {
        testingServer.stop();
    }
}