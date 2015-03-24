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

package org.apache.kylin.common.restclient;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broadcast kylin event out
 * 
 * @author jianliu
 * 
 */
public class Broadcaster {

    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

    private BlockingDeque<BroadcastEvent> broadcastEvents = new LinkedBlockingDeque<>();

    private AtomicLong counter = new AtomicLong();

    static class BroadcasterHolder {
        static final Broadcaster INSTANCE = new Broadcaster();
    }

    private Broadcaster() {
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                final String[] nodes = KylinConfig.getInstanceFromEnv().getRestServers();
                if (nodes == null || nodes.length < 1) {//TODO if the node count is greater than 1, it means it is a cluster
                    logger.warn("There is no available rest server; check the 'kylin.rest.servers' config");
                    return;
                }
                final List<RestClient> restClients = Lists.newArrayList();
                for (String node : nodes) {
                    restClients.add(new RestClient(node));
                }
                final ExecutorService wipingCachePool = Executors.newFixedThreadPool(restClients.size());
                while (true) {
                    try {
                        final BroadcastEvent broadcastEvent = broadcastEvents.takeFirst();
                        logger.info("new broadcast event:" + broadcastEvent);
                        for (final RestClient restClient : restClients) {
                            wipingCachePool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        restClient.wipeCache(broadcastEvent.getType(), broadcastEvent.getAction(), broadcastEvent.getName());
                                    } catch (IOException e) {
                                        logger.warn("Thread failed during wipe cache at " + broadcastEvent);
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

    public static Broadcaster getInstance() {
        return BroadcasterHolder.INSTANCE;
    }

    /**
     * Broadcast the cubedesc event out
     * 
     * @param action
     *            event action
     */
    public void queue(String type, String action, String key) {
        try {
            counter.incrementAndGet();
            broadcastEvents.putFirst(new BroadcastEvent(type, action, key));
        } catch (Exception e) {
            counter.decrementAndGet();
            logger.error("error putting BroadcastEvent", e);
        }
    }

    public long getCounterAndClear() {
        return counter.getAndSet(0);
    }

    public static enum EVENT {
        CREATE("create"), UPDATE("update"), DROP("drop");
        private String text;

        private EVENT(String text) {
            this.text = text;
        }

        public String getType() {
            return text;
        }

        public static EVENT getEvent(String event) {
            for (EVENT one : values()) {
                if (one.getType().equalsIgnoreCase(event)) {
                    return one;
                }
            }

            return null;
        }
    }

    public static enum TYPE {
        ALL("all"), CUBE("cube"), CUBE_DESC("cube_desc"), PROJECT("project"), INVERTED_INDEX("inverted_index"), INVERTED_INDEX_DESC("ii_desc"), TABLE("table"), DATA_MODEL("data_model");
        private String text;

        private TYPE(String text) {
            this.text = text;
        }

        public String getType() {
            return text;
        }

        /**
         * @param type
         * @return
         */
        public static TYPE getType(String type) {
            for (TYPE one : values()) {
                if (one.getType().equalsIgnoreCase(type)) {
                    return one;
                }
            }

            return null;
        }
    }

    public static class BroadcastEvent {
        private String type;
        private String action;
        private String name;

        public BroadcastEvent(String type, String action, String name) {
            super();
            this.type = type;
            this.action = action;
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public String getAction() {
            return action;
        }

        public String getName() {
            return name;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((action == null) ? 0 : action.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
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
            if (!StringUtils.equals(action, other.action)) {
                return false;
            }
            if (!StringUtils.equals(name, other.name)) {
                return false;
            }
            if (!StringUtils.equals(type, other.type)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("type", type).add("name", name).add("action", action).toString();
        }

    }
}
