package org.apache.kylin.rest;

import org.apache.kylin.rest.cluster.ClusterManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MockClusterManagerTest {
    @Test
    void testCheckServer() {
        ClusterManager clusterManager = new MockClusterManager();

        // Test valid server
        Assertions.assertFalse(clusterManager.checkServer("127.0.0.1:7070"));

        // Test null host
        Assertions.assertTrue(clusterManager.checkServer(null));

        // Test empty host
        Assertions.assertTrue(clusterManager.checkServer(""));
        Assertions.assertTrue(clusterManager.checkServer(" "));

        // Test host not found in servers
        Assertions.assertTrue(clusterManager.checkServer("192.168.1.1:8080"));
    }
}
