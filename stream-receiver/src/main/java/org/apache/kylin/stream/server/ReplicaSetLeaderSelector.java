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

package org.apache.kylin.stream.server;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.kylin.stream.coordinator.StreamingUtils;
import org.apache.kylin.stream.core.model.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class ReplicaSetLeaderSelector extends LeaderSelectorListenerAdapter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSetLeaderSelector.class);
    private LeaderSelector leaderSelector;
    private int replicaSetID;
    private Node node;
    private List<LeaderChangeListener> leaderChangeListeners;

    public ReplicaSetLeaderSelector(CuratorFramework client, Node currNode, int replicaSetID) {
        this.node = currNode;
        this.replicaSetID = replicaSetID;
        String path = StreamingUtils.REPLICASETS_LEADER_ELECT + "/" + replicaSetID;
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
        leaderChangeListeners = Lists.newArrayList();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    public void start() {
        leaderSelector.start();
    }

    public void addLeaderChangeListener(LeaderChangeListener listener) {
        this.leaderChangeListeners.add(listener);
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        logger.info("become the leader of the replicaSet:{}", replicaSetID);
        for (LeaderChangeListener leaderChangeListener : leaderChangeListeners) {
            try {
                leaderChangeListener.becomeLeader();
            } catch (Exception e) {
                logger.error("error when call listener", e);
            }
        }
        while (true) {
            try {
                Thread.sleep(5 * 60 * 1000L);
            } catch (InterruptedException exception) {
                Thread.interrupted();
                break;
            }
            if (!leaderSelector.hasLeadership()) {
                break;
            }
        }
        logger.info("become the follower of the replicaSet:{}", replicaSetID);
        for (LeaderChangeListener leaderChangeListener : leaderChangeListeners) {
            try {
                leaderChangeListener.becomeFollower();
            } catch (Exception e) {
                logger.error("error when call listener", e);
            }
        }
    }

    public interface LeaderChangeListener {
        void becomeLeader();

        void becomeFollower();
    }
}
