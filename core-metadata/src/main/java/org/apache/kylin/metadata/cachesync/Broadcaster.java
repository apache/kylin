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

package org.apache.kylin.metadata.cachesync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.Closeable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.MoreObjects;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 * Broadcast metadata changes across all Kylin servers.
 * 
 * The origin server announce the event via Rest API to all Kylin servers including itself.
 * On target server, listeners are registered to process events. As part of processing, a 
 * listener can re-notify a new event to other local listeners.
 * 
 * A typical project schema change event:
 * - model is update on origin server, a "model" update event is announced
 * - on all servers, model listener is invoked, reload the model, and notify a "project_schema" update event
 * - all listeners respond to the "project_schema" update -- reload cube desc, clear project L2 cache, clear calcite data source etc
 */
public class Broadcaster implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

    public static final String SYNC_ALL = "all"; // the special entity to indicate clear all
    public static final String SYNC_PRJ_SCHEMA = "project_schema"; // the special entity to indicate project schema has change, e.g. table/model/cube_desc update
    public static final String SYNC_PRJ_DATA = "project_data"; // the special entity to indicate project data has change, e.g. cube/raw_table update
    public static final String SYNC_PRJ_ACL = "project_acl"; // the special entity to indicate query ACL has change, e.g. table_acl/learn_kylin update

    public static Broadcaster getInstance(KylinConfig config) {
        return config.getManager(Broadcaster.class);
    }

    // called by reflection
    static Broadcaster newInstance(KylinConfig config) {
        return new Broadcaster(config);
    }

    // ============================================================================

    static final Map<String, List<Listener>> staticListenerMap = Maps.newConcurrentMap();

    private KylinConfig config;
    private ExecutorService announceMainLoop;
    private ExecutorService announceThreadPool;
    private SyncErrorHandler syncErrorHandler;
    private BlockingDeque<BroadcastEvent> broadcastEvents = new LinkedBlockingDeque<>();
    private Map<String, List<Listener>> listenerMap = Maps.newConcurrentMap();
    private AtomicLong counter = new AtomicLong(); // a counter for testing purpose
    private final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    
    private Broadcaster(final KylinConfig config) {
        this.config = config;
        this.syncErrorHandler = getSyncErrorHandler(config);
        this.announceMainLoop = Executors.newSingleThreadExecutor(new DaemonThreadFactory());
        
        final String[] nodes = config.getRestServers();
        if (nodes == null || nodes.length < 1) {
            logger.warn("There is no available rest server; check the 'kylin.server.cluster-servers' config");
        }
        logger.debug("{} nodes in the cluster: {}", (nodes == null ? 0 : nodes.length), Arrays.toString(nodes));
        
        int corePoolSize = (nodes == null || nodes.length < 1)? 1 : nodes.length;
        int maximumPoolSize = (nodes == null || nodes.length < 1)? 10 : nodes.length * 2;
        this.announceThreadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS,
            workQueue, new DaemonThreadFactory());

        announceMainLoop.execute(new Runnable() {
            @Override
            public void run() {
                final Map<String, RestClient> restClientMap = Maps.newHashMap();

                while (!announceThreadPool.isShutdown()) {
                    try {
                        final BroadcastEvent broadcastEvent = broadcastEvents.takeFirst();

                        String[] restServers = config.getRestServers();
                        logger.debug("Servers in the cluster: {}", Arrays.toString(restServers));
                        for (final String node : restServers) {
                            if (restClientMap.containsKey(node) == false) {
                                restClientMap.put(node, new RestClient(node));
                            }
                        }

                        String toWhere = broadcastEvent.getTargetNode();
                        if (toWhere == null)
                            toWhere = "all";
                        logger.debug("Announcing new broadcast to {}: {}", toWhere, broadcastEvent);
                        
                        for (final String node : restServers) {
                            if (!(toWhere.equals("all") || toWhere.equals(node)))
                                continue;
                            
                            announceThreadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    RestClient restClient = restClientMap.get(node);
                                    try {
                                        restClient.wipeCache(broadcastEvent.getEntity(), broadcastEvent.getEvent(),
                                                broadcastEvent.getCacheKey());
                                    } catch (IOException e) {
                                        logger.error(
                                                "Announce broadcast event failed, targetNode {} broadcastEvent {}, error msg: {}",
                                                node, broadcastEvent, e);
                                        syncErrorHandler.handleAnnounceError(node, restClient, broadcastEvent);
                                    }
                                }
                            });
                        }
                    } catch (Exception e) {
                        logger.error("error running wiping", e);
                    }
                }
            }
        });
    }

    @Override
    public void close() {
        new Thread(this::stopAnnounce).start();
    }

    private SyncErrorHandler getSyncErrorHandler(KylinConfig config) {
        String clzName = config.getCacheSyncErrorHandler();
        if (StringUtils.isEmpty(clzName)) {
            clzName = DefaultSyncErrorHandler.class.getName();
        }
        return (SyncErrorHandler) ClassUtil.newInstance(clzName);
    }

    public KylinConfig getConfig() {
        return config;
    }
    
    public void stopAnnounce() {
        synchronized (workQueue) {
            while (!workQueue.isEmpty() || !broadcastEvents.isEmpty()){
                try {
                    workQueue.wait(100);
                } catch (InterruptedException e) {
                    logger.warn("InterruptedException is caught when waiting workQueue empty.", e);
                    Thread.currentThread().interrupt();
                }
            }
        }

        logger.info("AnnounceThreadPool shutdown.");
        announceThreadPool.shutdownNow();
        logger.info("AnnounceMainLoop shutdown.");
        announceMainLoop.shutdownNow();
    }

    // static listener survives cache wipe and goes after normal listeners
    public void registerStaticListener(Listener listener, String... entities) {
        doRegisterListener(staticListenerMap, listener, entities);
    }

    public void registerListener(Listener listener, String... entities) {
        doRegisterListener(listenerMap, listener, entities);
    }

    private static void doRegisterListener(Map<String, List<Listener>> lmap, Listener listener, String... entities) {
        synchronized (lmap) {
            // ignore re-registration
            List<Listener> all = lmap.get(SYNC_ALL);
            if (all != null && all.contains(listener)) {
                return;
            }

            for (String entity : entities) {
                if (!StringUtils.isBlank(entity))
                    addListener(lmap, entity, listener);
            }
            addListener(lmap, SYNC_ALL, listener);
            addListener(lmap, SYNC_PRJ_SCHEMA, listener);
            addListener(lmap, SYNC_PRJ_DATA, listener);
            addListener(lmap, SYNC_PRJ_ACL, listener);
        }
    }

    private static void addListener(Map<String, List<Listener>> lmap, String entity, Listener listener) {
        List<Listener> list = lmap.computeIfAbsent(entity, s -> new ArrayList<>());
        list.add(listener);
    }

    public void notifyClearAll() throws IOException {
        notifyListener(SYNC_ALL, Event.UPDATE, SYNC_ALL);
    }

    public void notifyProjectSchemaUpdate(String project) throws IOException {
        notifyListener(SYNC_PRJ_SCHEMA, Event.UPDATE, project);
    }

    public void notifyProjectDataUpdate(String project) throws IOException {
        notifyListener(SYNC_PRJ_DATA, Event.UPDATE, project);
    }

    public void notifyProjectACLUpdate(String project) throws IOException {
        notifyListener(SYNC_PRJ_ACL, Event.UPDATE, project);
    }

    public void notifyListener(String entity, Event event, String cacheKey) throws IOException {
        notifyListener(entity, event, cacheKey, true);
    }

    public void notifyNonStaticListener(String entity, Event event, String cacheKey) throws IOException {
        notifyListener(entity, event, cacheKey, false);
    }

    private void notifyListener(String entity, Event event, String cacheKey, boolean includeStatic) throws IOException {
        // prevents concurrent modification exception
        List<Listener> list = Lists.newArrayList();
        List<Listener> l1 = listenerMap.get(entity); // normal listeners first
        if (l1 != null)
            list.addAll(l1);

        if (includeStatic) {
            List<Listener> l2 = staticListenerMap.get(entity); // static listeners second
            if (l2 != null)
                list.addAll(l2);
        }

        if (list.isEmpty())
            return;

        logger.debug("Broadcasting {}, {}, {}", event, entity, cacheKey);

        switch (entity) {
        case SYNC_ALL:
            for (Listener l : list) {
                l.onClearAll(this);
            }
            config.clearManagers(); // clear all registered managers in config
            break;
        case SYNC_PRJ_SCHEMA:
            ProjectManager.getInstance(config).clearL2Cache(cacheKey);
            for (Listener l : list) {
                l.onProjectSchemaChange(this, cacheKey);
            }
            break;
        case SYNC_PRJ_DATA:
            ProjectManager.getInstance(config).clearL2Cache(cacheKey); // cube's first becoming ready leads to schema change too
            for (Listener l : list) {
                l.onProjectDataChange(this, cacheKey);
            }
            break;
        case SYNC_PRJ_ACL:
            ProjectManager.getInstance(config).clearL2Cache(cacheKey);
            for (Listener l : list) {
                l.onProjectQueryACLChange(this, cacheKey);
            }
            break;
        default:
            for (Listener l : list) {
                l.onEntityChange(this, entity, event, cacheKey);
            }
            break;
        }

        logger.debug("Done broadcasting {}, {}, {}", event, entity, cacheKey);
    }

    /**
     * Announce an event out to peer kylin servers
     */
    public void announce(String entity, String event, String key) {
        announce(new BroadcastEvent(entity, event, key));
    }

    public void announce(BroadcastEvent event) {
        if (broadcastEvents == null)
            return;

        try {
            counter.incrementAndGet();
            broadcastEvents.putLast(event);
        } catch (Exception e) {
            counter.decrementAndGet();
            logger.error("error putting BroadcastEvent", e);
        }
    }

    public long getCounterAndClear() {
        return counter.getAndSet(0);
    }

    // ============================================================================

    public static class DefaultSyncErrorHandler implements SyncErrorHandler {
        Broadcaster broadcaster;
        int maxRetryTimes;

        @Override
        public void init(Broadcaster broadcaster) {
            this.maxRetryTimes = broadcaster.getConfig().getCacheSyncRetrys();
            this.broadcaster = broadcaster;
        }

        @Override
        public void handleAnnounceError(String targetNode, RestClient restClient, BroadcastEvent event) {
            int retry = event.getRetryTime() + 1;

            // when sync failed, put back to queue to retry
            if (retry < maxRetryTimes) {
                event.setRetryTime(retry);
                event.setTargetNode(targetNode);
                broadcaster.announce(event);
            } else {
                logger.error("Announce broadcast event exceeds retry limit, abandon targetNode {} broadcastEvent {}",
                        targetNode, event);
            }
        }
    }

    public interface SyncErrorHandler {
        void init(Broadcaster broadcaster);

        void handleAnnounceError(String targetNode, RestClient restClient, BroadcastEvent event);
    }

    public enum Event {

        CREATE("create"), UPDATE("update"), DROP("drop");
        private String text;

        Event(String text) {
            this.text = text;
        }

        public String getType() {
            return text;
        }

        public static Event getEvent(String event) {
            for (Event one : values()) {
                if (one.getType().equalsIgnoreCase(event)) {
                    return one;
                }
            }

            return null;
        }
    }

    public abstract static class Listener {
        public void onClearAll(Broadcaster broadcaster) throws IOException {
        }

        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onProjectDataChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onProjectQueryACLChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
        }
    }

    public static class BroadcastEvent {
        private int retryTime;
        private String targetNode; // NULL means to all
        
        private String entity;
        private String event;
        private String cacheKey;

        public BroadcastEvent(String entity, String event, String cacheKey) {
            super();
            this.entity = entity;
            this.event = event;
            this.cacheKey = cacheKey;
        }

        public int getRetryTime() {
            return retryTime;
        }

        public void setRetryTime(int retryTime) {
            this.retryTime = retryTime;
        }

        public String getTargetNode() {
            return targetNode;
        }

        public void setTargetNode(String targetNode) {
            this.targetNode = targetNode;
        }

        public String getEntity() {
            return entity;
        }

        public String getEvent() {
            return event;
        }

        public String getCacheKey() {
            return cacheKey;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((event == null) ? 0 : event.hashCode());
            result = prime * result + ((cacheKey == null) ? 0 : cacheKey.hashCode());
            result = prime * result + ((entity == null) ? 0 : entity.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (this == obj) {
                return true;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BroadcastEvent other = (BroadcastEvent) obj;
            if (!StringUtils.equals(event, other.event)) {
                return false;
            }
            if (!StringUtils.equals(cacheKey, other.cacheKey)) {
                return false;
            }
            return StringUtils.equals(entity, other.entity);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("entity", entity).add("event", event).add("cacheKey", cacheKey)
                    .toString();
        }

    }
}
