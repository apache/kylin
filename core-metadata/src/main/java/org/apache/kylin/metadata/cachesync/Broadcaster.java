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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
public class Broadcaster {

    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

    public static final String SYNC_ALL = "all"; // the special entity to indicate clear all
    public static final String SYNC_PRJ_SCHEMA = "project_schema"; // the special entity to indicate project schema has change, e.g. table/model/cube_desc update
    public static final String SYNC_PRJ_DATA = "project_data"; // the special entity to indicate project data has change, e.g. cube/raw_table update

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, Broadcaster> CACHE = new ConcurrentHashMap<KylinConfig, Broadcaster>();

    public static Broadcaster getInstance(KylinConfig config) {

        synchronized (CACHE) {
            Broadcaster r = CACHE.get(config);
            if (r != null) {
                return r;
            }

            r = new Broadcaster(config);
            CACHE.put(config, r);
            if (CACHE.size() > 1) {
                logger.warn("More than one singleton exist");
            }
            return r;
        }
    }

    // call Broadcaster.getInstance().notifyClearAll() to clear cache
    public static void clearCache() {
        synchronized (CACHE) {
            CACHE.clear();
        }
    }

    // ============================================================================

    private KylinConfig config;

    private BlockingDeque<BroadcastEvent> broadcastEvents = new LinkedBlockingDeque<>();
    private Map<String, List<Listener>> listenerMap = Maps.newConcurrentMap();
    private AtomicLong counter = new AtomicLong();

    private Broadcaster(final KylinConfig config) {
        this.config = config;

        final String[] nodes = config.getRestServers();
        if (nodes == null || nodes.length < 1) {
            logger.warn("There is no available rest server; check the 'kylin.server.cluster-servers' config");
            broadcastEvents = null; // disable the broadcaster
            return;
        }
        logger.debug(nodes.length + " nodes in the cluster: " + Arrays.toString(nodes));

        Executors.newSingleThreadExecutor(new DaemonThreadFactory()).execute(new Runnable() {
            @Override
            public void run() {
                final List<RestClient> restClients = Lists.newArrayList();
                for (String node : config.getRestServers()) {
                    restClients.add(new RestClient(node));
                }
                final ExecutorService wipingCachePool = Executors.newFixedThreadPool(restClients.size(), new DaemonThreadFactory());
                while (true) {
                    try {
                        final BroadcastEvent broadcastEvent = broadcastEvents.takeFirst();
                        logger.info("Announcing new broadcast event: " + broadcastEvent);
                        for (final RestClient restClient : restClients) {
                            wipingCachePool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        restClient.wipeCache(broadcastEvent.getEntity(), broadcastEvent.getEvent(), broadcastEvent.getCacheKey());
                                    } catch (IOException e) {
                                        logger.warn("Thread failed during wipe cache at " + broadcastEvent, e);
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

    public void registerListener(Listener listener, String... entities) {
        synchronized (CACHE) {
            // ignore re-registration
            List<Listener> all = listenerMap.get(SYNC_ALL);
            if (all != null && all.contains(listener)) {
                return;
            }

            for (String entity : entities) {
                if (!StringUtils.isBlank(entity))
                    addListener(entity, listener);
            }
            addListener(SYNC_ALL, listener);
            addListener(SYNC_PRJ_SCHEMA, listener);
            addListener(SYNC_PRJ_DATA, listener);
        }
    }

    private void addListener(String entity, Listener listener) {
        List<Listener> list = listenerMap.get(entity);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(listener);
        listenerMap.put(entity, list);
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

    public void notifyListener(String entity, Event event, String cacheKey) throws IOException {
        synchronized (CACHE) {
            List<Listener> list = listenerMap.get(entity);
            if (list == null)
                return;

            logger.debug("Broadcasting metadata change: entity=" + entity + ", event=" + event + ", cacheKey=" + cacheKey + ", listeners=" + list);

            // prevents concurrent modification exception
            list = Lists.newArrayList(list);
            switch (entity) {
            case SYNC_ALL:
                for (Listener l : list) {
                    l.onClearAll(this);
                }
                clearCache(); // clear broadcaster too in the end
                break;
            case SYNC_PRJ_SCHEMA:
                ProjectManager.getInstance(config).clearL2Cache();
                for (Listener l : list) {
                    l.onProjectSchemaChange(this, cacheKey);
                }
                break;
            case SYNC_PRJ_DATA:
                ProjectManager.getInstance(config).clearL2Cache(); // cube's first becoming ready leads to schema change too
                for (Listener l : list) {
                    l.onProjectDataChange(this, cacheKey);
                }
                break;
            default:
                for (Listener l : list) {
                    l.onEntityChange(this, entity, event, cacheKey);
                }
                break;
            }

            logger.debug("Done broadcasting metadata change: entity=" + entity + ", event=" + event + ", cacheKey=" + cacheKey);
        }
    }

    /**
     * Broadcast an event out
     */
    public void queue(String entity, String event, String key) {
        if (broadcastEvents == null)
            return;

        try {
            counter.incrementAndGet();
            broadcastEvents.putLast(new BroadcastEvent(entity, event, key));
        } catch (Exception e) {
            counter.decrementAndGet();
            logger.error("error putting BroadcastEvent", e);
        }
    }

    public long getCounterAndClear() {
        return counter.getAndSet(0);
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

    abstract public static class Listener {
        public void onClearAll(Broadcaster broadcaster) throws IOException {
        }

        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onProjectDataChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) throws IOException {
        }
    }

    public static class BroadcastEvent {
        private String entity;
        private String event;
        private String cacheKey;

        public BroadcastEvent(String entity, String event, String cacheKey) {
            super();
            this.entity = entity;
            this.event = event;
            this.cacheKey = cacheKey;
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
            if (!StringUtils.equals(entity, other.entity)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("entity", entity).add("event", event).add("cacheKey", cacheKey).toString();
        }

    }
}
