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

import com.google.common.collect.Sets;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SecondStorageQueryRouteUtil {
    private static final Map<String, Boolean> NODE_STATUS = new ConcurrentHashMap<>();

    private SecondStorageQueryRouteUtil() {
    }

    public static void setNodeStatus(String node, boolean status) {
        NODE_STATUS.put(node, status);
    }

    public static boolean getNodeStatus(String node) {
        return NODE_STATUS.getOrDefault(node, true);
    }

    public static List<Set<String>> getUsedShard(List<TablePartition> partitions, String project, Set<String> allSegIds) {
        // collect all node which partition used
        Set<String> allSegmentUsedNode = Sets.newHashSet();
        for (TablePartition partition : partitions) {
            if (allSegIds.contains(partition.getSegmentId())) {
                allSegmentUsedNode.addAll(partition.getShardNodes());
            }
        }

        if (allSegmentUsedNode.isEmpty()) {
            QueryContext.current().setRetrySecondStorage(false);
            throw new IllegalStateException("Segment node is empty.");
        }
        List<NodeGroup> nodeGroups = SecondStorage.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project).listAll();

        if (nodeGroups.isEmpty()) {
            QueryContext.current().setRetrySecondStorage(false);
            throw new IllegalStateException("Node groups is empty.");
        }

        List<Set<String>> shards = SecondStorageNodeHelper.groupsToShards(nodeGroups);
        List<Set<String>> segmentUsedShard = getSegmentUsedShard(shards, allSegmentUsedNode);
        filterAvailableReplica(segmentUsedShard);

        return segmentUsedShard;
    }

    /**
     * Get segment used nodes
     *
     * @param shards             shards
     * @param allSegmentUsedNode all segment used node
     * @return segment used nodes
     */
    private static List<Set<String>> getSegmentUsedShard(List<Set<String>> shards, Set<String> allSegmentUsedNode) {
        // filter which shards used by partitions
        return shards.stream().filter(replicas -> {
            for (String nodeName : allSegmentUsedNode) {
                if (replicas.contains(nodeName)) {
                    return true;
                }
            }
            return false;
        }).collect(Collectors.toList());
    }

    /**
     * filter available replica
     *
     * @param segmentUsedShard segments used shard
     */
    private static void filterAvailableReplica(List<Set<String>> segmentUsedShard) {
        for (Set<String> replicas : segmentUsedShard) {
            replicas.removeIf(replica -> !SecondStorageQueryRouteUtil.getNodeStatus(replica));

            if (replicas.isEmpty()) {
                QueryContext.current().setRetrySecondStorage(false);
                throw new IllegalStateException("One shard all replica has down");
            }
        }
    }

}
