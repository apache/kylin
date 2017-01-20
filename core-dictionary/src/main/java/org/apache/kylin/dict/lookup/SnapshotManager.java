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

package org.apache.kylin.dict.lookup;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.ReadableTable.TableSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * @author yangli9
 */
public class SnapshotManager {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, SnapshotManager> SERVICE_CACHE = new ConcurrentHashMap<KylinConfig, SnapshotManager>();

    public static SnapshotManager getInstance(KylinConfig config) {
        SnapshotManager r = SERVICE_CACHE.get(config);
        if (r == null) {
            synchronized (SnapshotManager.class) {
                r = SERVICE_CACHE.get(config);
                if (r == null) {
                    r = new SnapshotManager(config);
                    SERVICE_CACHE.put(config, r);
                    if (SERVICE_CACHE.size() > 1) {
                        logger.warn("More than one singleton exist");
                    }
                }
            }
        }
        return r;
    }

    public static void clearCache() {
        synchronized (SERVICE_CACHE) {
            SERVICE_CACHE.clear();
        }
    }

    // ============================================================================

    private KylinConfig config;
    private LoadingCache<String, SnapshotTable> snapshotCache; // resource

    // path ==>
    // SnapshotTable

    private SnapshotManager(KylinConfig config) {
        this.config = config;
        this.snapshotCache = CacheBuilder.newBuilder().removalListener(new RemovalListener<String, SnapshotTable>() {
            @Override
            public void onRemoval(RemovalNotification<String, SnapshotTable> notification) {
                SnapshotManager.logger.info("Snapshot with resource path " + notification.getKey() + " is removed due to " + notification.getCause());
            }
        }).maximumSize(config.getCachedSnapshotMaxEntrySize())//
                .expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<String, SnapshotTable>() {
                    @Override
                    public SnapshotTable load(String key) throws Exception {
                        SnapshotTable snapshotTable = SnapshotManager.this.load(key, true);
                        return snapshotTable;
                    }
                });
    }

    public void wipeoutCache() {
        snapshotCache.invalidateAll();
    }

    public SnapshotTable getSnapshotTable(String resourcePath) throws IOException {
        try {
            SnapshotTable r = snapshotCache.get(resourcePath);
            if (r == null) {
                r = load(resourcePath, true);
                snapshotCache.put(resourcePath, r);
            }
            return r;
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    public void removeSnapshot(String resourcePath) throws IOException {
        ResourceStore store = MetadataManager.getInstance(this.config).getStore();
        store.deleteResource(resourcePath);
        snapshotCache.invalidate(resourcePath);
    }

    public SnapshotTable buildSnapshot(ReadableTable table, TableDesc tableDesc) throws IOException {
        SnapshotTable snapshot = new SnapshotTable(table, tableDesc.getIdentity());
        snapshot.updateRandomUuid();

        String dup = checkDupByInfo(snapshot);
        if (dup != null) {
            logger.info("Identical input " + table.getSignature() + ", reuse existing snapshot at " + dup);
            return getSnapshotTable(dup);
        }

        if (snapshot.getSignature().getSize() / 1024 / 1024 > config.getTableSnapshotMaxMB()) {
            throw new IllegalStateException("Table snapshot should be no greater than " + config.getTableSnapshotMaxMB() //
                    + " MB, but " + tableDesc + " size is " + snapshot.getSignature().getSize());
        }

        snapshot.takeSnapshot(table, tableDesc);

        return trySaveNewSnapshot(snapshot);
    }

    public SnapshotTable rebuildSnapshot(ReadableTable table, TableDesc tableDesc, String overwriteUUID) throws IOException {
        SnapshotTable snapshot = new SnapshotTable(table, tableDesc.getIdentity());
        snapshot.setUuid(overwriteUUID);

        snapshot.takeSnapshot(table, tableDesc);

        SnapshotTable existing = getSnapshotTable(snapshot.getResourcePath());
        snapshot.setLastModified(existing.getLastModified());

        save(snapshot);
        snapshotCache.put(snapshot.getResourcePath(), snapshot);

        return snapshot;
    }

    public SnapshotTable trySaveNewSnapshot(SnapshotTable snapshotTable) throws IOException {

        String dupTable = checkDupByContent(snapshotTable);
        if (dupTable != null) {
            logger.info("Identical snapshot content " + snapshotTable + ", reuse existing snapshot at " + dupTable);
            return getSnapshotTable(dupTable);
        }

        save(snapshotTable);
        snapshotCache.put(snapshotTable.getResourcePath(), snapshotTable);

        return snapshotTable;
    }

    private String checkDupByInfo(SnapshotTable snapshot) throws IOException {
        ResourceStore store = MetadataManager.getInstance(this.config).getStore();
        String resourceDir = snapshot.getResourceDir();
        NavigableSet<String> existings = store.listResources(resourceDir);
        if (existings == null)
            return null;

        TableSignature sig = snapshot.getSignature();
        for (String existing : existings) {
            SnapshotTable existingTable = load(existing, false); // skip cache,
            // direct load from store
            if (existingTable != null && sig.equals(existingTable.getSignature()))
                return existing;
        }

        return null;
    }

    private String checkDupByContent(SnapshotTable snapshot) throws IOException {
        ResourceStore store = MetadataManager.getInstance(this.config).getStore();
        String resourceDir = snapshot.getResourceDir();
        NavigableSet<String> existings = store.listResources(resourceDir);
        if (existings == null)
            return null;

        for (String existing : existings) {
            SnapshotTable existingTable = load(existing, true); // skip cache, direct load from store
            if (existingTable != null && existingTable.equals(snapshot))
                return existing;
        }

        return null;
    }

    private void save(SnapshotTable snapshot) throws IOException {
        ResourceStore store = MetadataManager.getInstance(this.config).getStore();
        String path = snapshot.getResourcePath();
        store.putResource(path, snapshot, SnapshotTableSerializer.FULL_SERIALIZER);
    }

    private SnapshotTable load(String resourcePath, boolean loadData) throws IOException {
        logger.info("Loading snapshotTable from " + resourcePath + ", with loadData: " + loadData);
        ResourceStore store = MetadataManager.getInstance(this.config).getStore();

        SnapshotTable table = store.getResource(resourcePath, SnapshotTable.class, loadData ? SnapshotTableSerializer.FULL_SERIALIZER : SnapshotTableSerializer.INFO_SERIALIZER);

        if (loadData)
            logger.debug("Loaded snapshot at " + resourcePath);

        return table;
    }

}
