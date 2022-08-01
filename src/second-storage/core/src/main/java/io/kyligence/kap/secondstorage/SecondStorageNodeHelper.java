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

package io.kyligence.kap.secondstorage;

import com.google.common.base.Preconditions;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SecondStorageNodeHelper {
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static final ConcurrentMap<String, Node> NODE_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, String> NODE2PAIR_INDEX = new ConcurrentHashMap<>();
    private static final Map<String, List<Node>> CLUSTER = new ConcurrentHashMap<>();
    private static Function<Node, String> node2url;
    private static BiFunction<List<Node>, QueryContext, String> shard2url;

    public static void initFromCluster(ClusterInfo cluster, Function<Node, String> node2url, BiFunction<List<Node>, QueryContext, String> shard2url) {
        synchronized (SecondStorageNodeHelper.class) {
            NODE2PAIR_INDEX.clear();
            cluster.getNodes().forEach(node -> NODE_MAP.put(node.getName(), node));
            CLUSTER.putAll(cluster.getCluster());
            SecondStorageNodeHelper.node2url = node2url;
            SecondStorageNodeHelper.shard2url = shard2url;

            // build lookup table for node to pair
            CLUSTER.forEach((pair, nodes) -> {
                nodes.forEach(node -> {
                    NODE2PAIR_INDEX.put(node.getName(), pair);
                });
            });
            initialized.set(true);
        }
    }

    public static List<String> resolve(List<String> names) {
        Preconditions.checkState(initialized.get());
        return names.stream().map(name -> {
            Preconditions.checkState(NODE_MAP.containsKey(name));
            return node2url.apply(NODE_MAP.get(name));
        }).collect(Collectors.toList());
    }

    public static String getPairByNode(String node) {
        Preconditions.checkArgument(NODE2PAIR_INDEX.containsKey(node), "Node %s doesn't exist", node);
        return NODE2PAIR_INDEX.get(node);
    }

    public static List<String> resolveToJDBC(List<String> names) {
        return resolve(names);
    }

    public static List<String> resolveShardToJDBC(List<Set<String>> shardNames, QueryContext queryContext) {
        Preconditions.checkState(initialized.get());
        return shardNames.stream().map(replicaNames -> {
            List<Node> replicas = replicaNames.stream().map(replicaName -> {
                Preconditions.checkState(NODE_MAP.containsKey(replicaName));
                return NODE_MAP.get(replicaName);
            }).collect(Collectors.toList());

            return shard2url.apply(replicas, queryContext);
        }).collect(Collectors.toList());
    }

    public static String resolve(String name) {
        Preconditions.checkState(initialized.get());
        return node2url.apply(NODE_MAP.get(name));
    }

    public static List<String> getPair(String pairName) {
        return CLUSTER.get(pairName).stream().map(Node::getName).collect(Collectors.toList());
    }

    private SecondStorageNodeHelper() {
    }

    public static Node getNode(String name) {
        return new Node(NODE_MAP.get(name));
    }

    public static List<Node> getALlNodes() {
        Preconditions.checkState(initialized.get());
        return new ArrayList<>(NODE_MAP.values());
    }

    public static List<Node> getALlNodesInProject(String project) {
        Optional<Manager<NodeGroup>> nodeGroupOptional = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
        Preconditions.checkState(nodeGroupOptional.isPresent(), "node group manager is not init");
        return nodeGroupOptional.get().listAll().stream()
                .flatMap(nodeGroup -> nodeGroup.getNodeNames().stream()).map(NODE_MAP::get)
                .distinct()
                .collect(Collectors.toList());
    }

    public static List<String> getAllNames() {
        Preconditions.checkState(initialized.get());
        return new ArrayList<>(NODE_MAP.keySet());
    }

    public static List<String> getAllPairs() {
        Preconditions.checkState(initialized.get());
        return new ArrayList<>(CLUSTER.keySet());
    }

    public static Map<Integer, List<String>> separateReplicaGroup(int replicaNum, String... pairs) {
        for (final String pair : pairs) {
            Preconditions.checkArgument(getPair(pair).size() == replicaNum, "Pair size is different from replica number");
        }
        Map<Integer, List<String>> nodeMap = new HashMap<>(replicaNum);
        for (final String pair : pairs) {
            List<String> pairNodes = getPair(pair);
            pairNodes.sort(String::compareTo);
            ListIterator<String> it = pairNodes.listIterator();
            while (it.hasNext()) {
                List<String> nodes = nodeMap.computeIfAbsent(it.nextIndex() % replicaNum, idx -> new ArrayList<>());
                nodes.add(it.next());
            }
        }

        return nodeMap;
    }

    /**
     * groups to shard
     * group [replica][shardSize] to shard[shardSize][replica]
     *
     * @param groups group
     * @return shards
     */
    public static List<Set<String>> groupsToShards(List<NodeGroup> groups) {
        int shardSize = groups.get(0).getNodeNames().size();
        // key is shard num, value is replica name
        Map<Integer, Set<String>> shards = new HashMap<>(shardSize);

        for (int shardNum = 0; shardNum < shardSize; shardNum++) {
            for (NodeGroup group : groups) {
                shards.computeIfAbsent(shardNum, key -> new HashSet<>()).add(group.getNodeNames().get(shardNum));
            }
        }

        return new ArrayList<>(shards.values());
    }

    public static void clear() {
        synchronized (SecondStorageNodeHelper.class) {
            initialized.set(false);
            NODE_MAP.clear();
            CLUSTER.clear();
        }
    }
}
