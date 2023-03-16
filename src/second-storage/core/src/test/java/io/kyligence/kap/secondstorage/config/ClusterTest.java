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
package io.kyligence.kap.secondstorage.config;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.common.util.EncryptUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterTest {
    @Test
    public void testPassword() {
        String password = "123456";
        ClusterInfo cluster = new ClusterInfo();
        cluster.setPassword(password);
        Assert.assertEquals(password, cluster.getPassword());

        cluster.setPassword(EncryptUtil.encryptWithPrefix(password));
        Assert.assertEquals(password, cluster.getPassword());

        Node node = new Node();
        node.setPassword(password);
        Assert.assertEquals(password, node.getPassword());

        node = new Node("node01", "127.0.0.1", 9000, "default", "123456", 222);
        Assert.assertEquals("node01", node.getName());
        Assert.assertEquals(222, node.getSSHPort());
        node.setSSHPort(22);
        Assert.assertEquals(22, node.getSSHPort());

        node.setPassword(EncryptUtil.encryptWithPrefix(password));
        Assert.assertEquals(password, node.getPassword());
    }

    @Test
    public void testTransformNode() {
        ClusterInfo cluster = new ClusterInfo();
        Map<String, List<Node>> clusterNode = new HashMap<>();
        clusterNode.put("pair0", Lists.newArrayList(new Node("node01", "127.0.0.1", 9000, "default", "123456", 222)));
        clusterNode.put("pair1", Lists.newArrayList(new Node("node01", "127.0.0.1", 9000, "default", "123456")));
        cluster.setCluster(clusterNode);
        cluster.transformNode();
        int size = cluster.getNodes().stream().filter(node -> {
            return node.getSSHPort() == 22;
        }).collect(Collectors.toList()).size();
        Assert.assertEquals(1, size);
    }
}
