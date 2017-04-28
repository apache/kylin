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

package org.apache.kylin.metadata.badquery;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BadQueryHistoryManager {
    public static final Serializer<BadQueryHistory> BAD_QUERY_INSTANCE_SERIALIZER = new JsonSerializer<>(BadQueryHistory.class);
    private static final Logger logger = LoggerFactory.getLogger(BadQueryHistoryManager.class);

    private static final ConcurrentMap<KylinConfig, BadQueryHistoryManager> CACHE = new ConcurrentHashMap<>();
    private KylinConfig kylinConfig;

    private BadQueryHistoryManager(KylinConfig config) throws IOException {
        logger.info("Initializing BadQueryHistoryManager with config " + config);
        this.kylinConfig = config;
    }

    public static BadQueryHistoryManager getInstance(KylinConfig config) {
        BadQueryHistoryManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (BadQueryHistoryManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new BadQueryHistoryManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init BadQueryHistoryManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.kylinConfig);
    }

    public BadQueryHistory getBadQueriesForProject(String project) throws IOException {
        BadQueryHistory badQueryHistory = getStore().getResource(getResourcePathForProject(project), BadQueryHistory.class, BAD_QUERY_INSTANCE_SERIALIZER);
        if (badQueryHistory == null) {
            badQueryHistory = new BadQueryHistory(project);
        }

        logger.debug("Loaded " + badQueryHistory.getEntries().size() + " Bad Query(s)");
        return badQueryHistory;
    }

    public BadQueryHistory addEntryToProject(BadQueryEntry badQueryEntry, String project) throws IOException {
        if (StringUtils.isEmpty(project) || badQueryEntry.getAdj() == null || badQueryEntry.getSql() == null)
            throw new IllegalArgumentException();

        BadQueryHistory badQueryHistory = getBadQueriesForProject(project);
        NavigableSet<BadQueryEntry> entries = badQueryHistory.getEntries();
        if (entries.size() >= kylinConfig.getBadQueryHistoryNum()) {
            entries.pollFirst();
        }
        entries.add(badQueryEntry);

        getStore().putResource(badQueryHistory.getResourcePath(), badQueryHistory, BAD_QUERY_INSTANCE_SERIALIZER);
        return badQueryHistory;
    }

    public BadQueryHistory updateEntryToProject(BadQueryEntry badQueryEntry, String project) throws IOException {
        if (StringUtils.isEmpty(project) || badQueryEntry.getAdj() == null || badQueryEntry.getSql() == null)
            throw new IllegalArgumentException();

        BadQueryHistory badQueryHistory = getBadQueriesForProject(project);
        NavigableSet<BadQueryEntry> entries = badQueryHistory.getEntries();
        BadQueryEntry entry = entries.floor(badQueryEntry);
        entry.setAdj(badQueryEntry.getAdj());
        entry.setRunningSec(badQueryEntry.getRunningSec());
        entry.setServer(badQueryEntry.getServer());
        entry.setThread(badQueryEntry.getThread());
        getStore().putResource(badQueryHistory.getResourcePath(), badQueryHistory, BAD_QUERY_INSTANCE_SERIALIZER);

        return badQueryHistory;
    }

    public BadQueryHistory addEntryToProject(String sql, long startTime, String adj, float runningSecs, String server, String threadName, String user, String project) throws IOException {
        return addEntryToProject(new BadQueryEntry(sql, adj, startTime, runningSecs, server, threadName, user), project);
    }

    public BadQueryHistory updateEntryToProject(String sql, long startTime, String adj, float runningSecs, String server, String threadName, String user, String project) throws IOException {
        return updateEntryToProject(new BadQueryEntry(sql, adj, startTime, runningSecs, server, threadName, user), project);
    }

    public void removeBadQueryHistory(String project) throws IOException {
        getStore().deleteResource(getResourcePathForProject(project));
    }

    public String getResourcePathForProject(String project) {
        return ResourceStore.BAD_QUERY_RESOURCE_ROOT + "/" + project + MetadataConstants.FILE_SURFIX;
    }
}