/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.common.restclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;

/**
 * Broadcast kylin event out
 * 
 * @author jianliu
 * 
 */
public class Broadcaster {

    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

    private static List<BroadcastEvent> broadcaseEvents = new ArrayList<BroadcastEvent>();

    static class BroadcasterHolder {
        static final Broadcaster INSTANCE = new Broadcaster();
    }

    private Broadcaster() {
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            public void run() {
                Broadcaster.flush();
            }
        };

        timer.schedule(task, new Date(), 10 * 1000);
    }

    public Broadcaster getInstance() {
        return BroadcasterHolder.INSTANCE;
    }

    public static void queueSyncMetadata() {
        queue(TYPE.METADATA.getType(), EVENT.CREATE.getType(), "metadata");
    }

    /**
     * Broadcast the cubedesc event out
     * 
     * @param action
     *            event action
     */
    public static synchronized void queue(String type, String action, String key) {
        BroadcastEvent event = BroadcasterHolder.INSTANCE.new BroadcastEvent(type, action, key);

        if (!broadcaseEvents.contains(event)) {
            broadcaseEvents.add(event);
        }
    }

    public static synchronized void flush() {
        String[] nodes = KylinConfig.getInstanceFromEnv().getRestServers();
        if (nodes == null)
            return;

        for (BroadcastEvent event : broadcaseEvents) {
            for (String nodeUri : nodes) {
                logger.debug("Broadcast nodeUri: " + nodeUri + ", type: " + event.getType() + ", action: " + event.getAction() + ", name: " + event.getName());
                WipeCacheThread thread = BroadcasterHolder.INSTANCE.new WipeCacheThread(nodeUri, event.getType(), event.getAction(), event.getName());
                thread.start();
            }
        }

        broadcaseEvents.clear();
    }

    public static String genEventkey(String type, String action, String name) {
        String time = String.valueOf(System.currentTimeMillis());
        return time + "_" + type + "_" + action + "_" + name;
    }

    protected class WipeCacheThread extends Thread {
        private String nodeUri;
        private String type;
        private String action;
        private String name;

        public WipeCacheThread(String nodeUri, String type, String action, String name) {
            this.nodeUri = nodeUri;
            this.type = type;
            this.action = action;
            this.name = name;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Thread#run()
         */
        @Override
        public void run() {
            RestClient restClient = new RestClient(nodeUri);
            try {
                restClient.wipeCache(this.type, this.action, this.name);
            } catch (IOException e) {
                logger.warn("Thread failed during wipe cache at " + type + "." + action + "." + name + ", " + e.toString());
            }
        }
    }

    public enum EVENT {
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

    public enum TYPE {
        CUBE("cube"), METADATA("metadata"), PROJECT("project");
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

    public class BroadcastEvent {
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

        public void setType(String type) {
            this.type = type;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((action == null) ? 0 : action.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            BroadcastEvent other = (BroadcastEvent) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (action == null) {
                if (other.action != null)
                    return false;
            } else if (!action.equals(other.action))
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            if (type == null) {
                if (other.type != null)
                    return false;
            } else if (!type.equals(other.type))
                return false;
            return true;
        }

        private Broadcaster getOuterType() {
            return Broadcaster.this;
        }

    }
}
