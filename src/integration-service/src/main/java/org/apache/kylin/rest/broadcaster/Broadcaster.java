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

package org.apache.kylin.rest.broadcaster;

import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.config.initialize.BroadcastListener;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.tool.restclient.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class Broadcaster implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

    private KylinConfig config;
    private ClusterManager clusterManager;

    private ExecutorService eventPollExecutor;
    private ExecutorService eventHandlerExecutor;
    private BlockingQueue<Runnable> runnableQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<BroadcastEventReadyNotifier> eventQueue = new LinkedBlockingQueue<>();

    private BroadcastListener localHandler;
    private ConcurrentHashMap<String, RestClient> restClientMap = new ConcurrentHashMap<>();

    public static Broadcaster getInstance(KylinConfig config, BroadcastListener localHandler) {
        Broadcaster broadcaster = config.getManager(Broadcaster.class);
        broadcaster.localHandler = localHandler;
        return broadcaster;
    }

    // called by reflection
    static Broadcaster newInstance(KylinConfig config) {
        return new Broadcaster(config);
    }

    private Broadcaster(final KylinConfig config) {
        this.config = config;
        this.eventHandlerExecutor = new ThreadPoolExecutor(10, 10, 60L, TimeUnit.SECONDS, runnableQueue,
                new DaemonThreadFactory("BroadcastEvent-handler"), new ThreadPoolExecutor.DiscardPolicy());

        this.eventPollExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("BroadcastEvent-poll"));
        eventPollExecutor.submit(() -> consumeEvent());
    }

    public void announce(BroadcastEventReadyNotifier event) {
        if (eventQueue.contains(event)) {
            logger.debug("broadcast event queue has contain this event: {}", event);
            return;
        }
        eventQueue.offer(event);
    }

    public void consumeEvent() {
        try {
            while (true) {
                BroadcastEventReadyNotifier notifier = eventQueue.take();
                handleEvent(notifier);
            }
        } catch (InterruptedException e) {
            logger.error("consume broadcast event fail: ", e);
        }
    }

    private void handleEvent(BroadcastEventReadyNotifier notifier) {
        try {
            Set<String> notifyNodes = getBroadcastNodes(notifier);
            if (notifyNodes.isEmpty()) {
                logger.debug("no need broadcast the event {} to other node.", notifier);
                return;
            }

            CountDownLatch latch = new CountDownLatch(notifyNodes.size());
            String identity = AddressUtil.getLocalInstance();
            for (String node : notifyNodes) {

                eventHandlerExecutor.submit(() -> {
                    try {
                        if (identity.equals(node)) {
                            localHandle(notifier);
                        } else {
                            remoteHandle(node, notifier);
                        }
                        logger.info("Broadcast to {} notify.", node);
                    } catch (IOException e) {
                        logger.warn("Failed to notify.", e);
                    } finally {
                        latch.countDown();
                    }
                });

            }
            if (!latch.await(5, TimeUnit.SECONDS)) {
                logger.warn("Failed to broadcast due to timeout. current BroadcastEvent-handler task num {}",
                        runnableQueue.size());
            }

        } catch (Exception e) {
            logger.warn("failed to broadcast", e);
        }
    }

    private void localHandle(BroadcastEventReadyNotifier notifier) throws IOException {
        localHandler.handle(notifier);
    }

    private void remoteHandle(String node, BroadcastEventReadyNotifier notifier) throws IOException {
        RestClient client = restClientMap.get(node);
        if (client == null) {
            client = new RestClient(node);
            restClientMap.put(node, client);
        }
        RestClient finalClient = client;
        finalClient.notify(notifier);
    }

    private Set<String> getBroadcastNodes(BroadcastEventReadyNotifier notifier) {
        Set<String> nodes;
        switch (notifier.getBroadcastScope()) {
        case LEADER_NODES:
            nodes = getNodesByModes(ServerModeEnum.ALL, ServerModeEnum.JOB);
            break;
        case ALL_NODES:
            nodes = getNodesByModes(ServerModeEnum.ALL);
            break;
        case JOB_NODES:
            nodes = getNodesByModes(ServerModeEnum.JOB);
            break;
        case QUERY_NODES:
            nodes = getNodesByModes(ServerModeEnum.QUERY);
            break;
        case QUERY_AND_ALL:
            nodes = getNodesByModes(ServerModeEnum.QUERY, ServerModeEnum.ALL);
            break;
        default:
            nodes = getNodesByModes(ServerModeEnum.ALL, ServerModeEnum.JOB, ServerModeEnum.QUERY);
        }
        if (!notifier.needBroadcastSelf()) {
            String identity = AddressUtil.getLocalInstance();
            return nodes.stream().filter(node -> !node.equals(identity)).collect(Collectors.toSet());
        }
        return nodes;
    }

    private Set<String> getNodesByModes(ServerModeEnum... serverModeEnums) {
        if (ArrayUtils.isEmpty(serverModeEnums)) {
            return Collections.emptySet();
        }

        Set<String> serverModeNameSets = Stream.of(serverModeEnums).filter(Objects::nonNull)
                .map(ServerModeEnum::getName).collect(Collectors.toSet());
        if (clusterManager == null) {
            clusterManager = SpringContext.getApplicationContext().getBean(ClusterManager.class);
        }
        final List<ServerInfoResponse> nodes = clusterManager.getServersFromCache();
        Set<String> result = Sets.newHashSet();
        if (CollectionUtils.isEmpty(nodes)) {
            logger.warn("There is no available rest server; check the 'kylin.server.cluster-servers' config");
        } else {
            result = nodes.stream().filter(node -> serverModeNameSets.contains(node.getMode()))
                    .map(ServerInfoResponse::getHost).collect(Collectors.toSet());
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }
}
