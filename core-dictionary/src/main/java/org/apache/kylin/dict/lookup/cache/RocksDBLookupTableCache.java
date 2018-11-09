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

package org.apache.kylin.dict.lookup.cache;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfoManager;
import org.apache.kylin.dict.lookup.IExtLookupTableCache;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.LookupProviderFactory;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class RocksDBLookupTableCache implements IExtLookupTableCache {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBLookupTableCache.class);

    private static final String CACHE_TYPE_ROCKSDB = "rocksdb";
    private static final String STATE_FILE = "STATE";
    private static final String DB_FILE = "db";

    private String basePath;
    private long maxCacheSizeInKB;
    private Cache<String, CachedTableInfo> tablesCache;

    private ConcurrentMap<String, Boolean> inBuildingTables = Maps.newConcurrentMap();

    private ExecutorService cacheBuildExecutor;
    private ScheduledExecutorService cacheStateCheckExecutor;
    private CacheStateChecker cacheStateChecker;

    // static cached instances
    private static final ConcurrentMap<KylinConfig, RocksDBLookupTableCache> SERVICE_CACHE = new ConcurrentHashMap<>();

    public static RocksDBLookupTableCache getInstance(KylinConfig config) {
        RocksDBLookupTableCache r = SERVICE_CACHE.get(config);
        if (r == null) {
            synchronized (RocksDBLookupTableCache.class) {
                r = SERVICE_CACHE.get(config);
                if (r == null) {
                    r = new RocksDBLookupTableCache(config);
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

    private RocksDBLookupTableCache(KylinConfig config) {
        this.config = config;
        init();
    }

    private void init() {
        this.basePath = getCacheBasePath(config);

        this.maxCacheSizeInKB = (long) (config.getExtTableSnapshotLocalCacheMaxSizeGB() * 1024 * 1024);
        this.tablesCache = CacheBuilder.newBuilder().removalListener(new RemovalListener<String, CachedTableInfo>() {
            @Override
            public void onRemoval(RemovalNotification<String, CachedTableInfo> notification) {
                logger.warn(notification.getValue() + " is removed " + "because of " + notification.getCause());
                notification.getValue().cleanStorage();
            }
        }).maximumWeight(maxCacheSizeInKB).weigher(new Weigher<String, CachedTableInfo>() {
            @Override
            public int weigh(String key, CachedTableInfo value) {
                return value.getSizeInKB();
            }
        }).build();
        restoreCacheState();
        cacheStateChecker = new CacheStateChecker();
        initExecutors();
    }

    protected static String getCacheBasePath(KylinConfig config) {
        String basePath = config.getExtTableSnapshotLocalCachePath();
        if ((!basePath.startsWith("/")) && (KylinConfig.getKylinHome() != null)) {
            basePath = KylinConfig.getKylinHome() + File.separator + basePath;
        }
        return basePath + File.separator + CACHE_TYPE_ROCKSDB;
    }

    private void restoreCacheState() {
        File dbBaseFolder = new File(basePath);
        if (!dbBaseFolder.exists()) {
            dbBaseFolder.mkdirs();
        }
        Map<String, File[]> tableSnapshotsFileMap = getCachedTableSnapshotsFolders(dbBaseFolder);
        for (Entry<String, File[]> tableSnapshotsEntry : tableSnapshotsFileMap.entrySet()) {
            for (File snapshotFolder : tableSnapshotsEntry.getValue()) {
                initSnapshotState(tableSnapshotsEntry.getKey(), snapshotFolder);
            }
        }
    }

    private Map<String, File[]> getCachedTableSnapshotsFolders(File dbBaseFolder) {
        Map<String, File[]> result = Maps.newHashMap();
        File[] tableFolders = dbBaseFolder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
        if (tableFolders == null) {
            return result;
        }

        for (File tableFolder : tableFolders) {
            String tableName = tableFolder.getName();
            File[] snapshotFolders = tableFolder.listFiles(new FileFilter() {
                @Override
                public boolean accept(File snapshotFile) {
                    return snapshotFile.isDirectory();
                }
            });
            result.put(tableName, snapshotFolders);
        }
        return result;
    }

    private void initSnapshotState(String tableName, File snapshotCacheFolder) {
        String snapshotID = snapshotCacheFolder.getName();
        File stateFile = getCacheStateFile(snapshotCacheFolder.getAbsolutePath());
        if (stateFile.exists()) {
            try {
                String stateStr = Files.toString(stateFile, Charsets.UTF_8);
                String resourcePath = ExtTableSnapshotInfo.getResourcePath(tableName, snapshotID);
                if (CacheState.AVAILABLE.name().equals(stateStr)) {
                    tablesCache.put(resourcePath, new CachedTableInfo(snapshotCacheFolder.getAbsolutePath()));
                }
            } catch (IOException e) {
                logger.error("error to read state file:" + stateFile.getAbsolutePath());
            }
        }
    }

    private void initExecutors() {
        this.cacheBuildExecutor = new ThreadPoolExecutor(0, 50, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory("lookup-cache-build-thread"));
        this.cacheStateCheckExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(
                "lookup-cache-state-checker"));
        cacheStateCheckExecutor.scheduleAtFixedRate(cacheStateChecker, 10, 10 * 60, TimeUnit.SECONDS); // check every 10 minutes
    }

    @Override
    public ILookupTable getCachedLookupTable(TableDesc tableDesc, ExtTableSnapshotInfo extTableSnapshotInfo, boolean buildIfNotExist) {
        String resourcePath = extTableSnapshotInfo.getResourcePath();
        if (inBuildingTables.containsKey(resourcePath)) {
            logger.info("cache is in building for snapshot:" + resourcePath);
            return null;
        }
        CachedTableInfo cachedTableInfo = tablesCache.getIfPresent(resourcePath);
        if (cachedTableInfo == null) {
            if (buildIfNotExist) {
                buildSnapshotCache(tableDesc, extTableSnapshotInfo, getSourceLookupTable(tableDesc, extTableSnapshotInfo));
            }
            logger.info("no available cache ready for the table snapshot:" + extTableSnapshotInfo.getResourcePath());
            return null;
        }

        String[] keyColumns = extTableSnapshotInfo.getKeyColumns();
        String dbPath = getSnapshotStorePath(extTableSnapshotInfo.getTableName(), extTableSnapshotInfo.getId());
        return new RocksDBLookupTable(tableDesc, keyColumns, dbPath);
    }

    private ILookupTable getSourceLookupTable(TableDesc tableDesc, ExtTableSnapshotInfo extTableSnapshotInfo) {
        return LookupProviderFactory.getExtLookupTableWithoutCache(tableDesc, extTableSnapshotInfo);
    }

    @Override
    public void buildSnapshotCache(final TableDesc tableDesc, final ExtTableSnapshotInfo extTableSnapshotInfo, final ILookupTable sourceTable) {
        if (extTableSnapshotInfo.getSignature().getSize() / 1024 > maxCacheSizeInKB * 2 / 3) {
            logger.warn("the size is to large to build to cache for snapshot:{}, size:{}, skip cache building",
                    extTableSnapshotInfo.getResourcePath(), extTableSnapshotInfo.getSignature().getSize());
            return;
        }
        final String[] keyColumns = extTableSnapshotInfo.getKeyColumns();
        final String cachePath = getSnapshotCachePath(extTableSnapshotInfo.getTableName(), extTableSnapshotInfo.getId());
        final String dbPath = getSnapshotStorePath(extTableSnapshotInfo.getTableName(), extTableSnapshotInfo.getId());
        final String snapshotResPath = extTableSnapshotInfo.getResourcePath();

        if (inBuildingTables.containsKey(snapshotResPath)) {
            logger.info("there is already snapshot cache in building for snapshot:{}, skip it", snapshotResPath);
            return;
        }
        if (inBuildingTables.putIfAbsent(snapshotResPath, true) == null) {
            cacheBuildExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        RocksDBLookupBuilder builder = new RocksDBLookupBuilder(tableDesc, keyColumns, dbPath);
                        builder.build(sourceTable);
                        saveSnapshotCacheState(extTableSnapshotInfo, cachePath);
                    } catch (Exception e) {
                        logger.error("error when build snapshot cache", e);
                    } finally {
                        inBuildingTables.remove(snapshotResPath);
                    }
                }
            });

        } else {
            logger.info("there is already snapshot cache in building for snapshot:{}, skip it", snapshotResPath);
        }
    }

    @Override
    public void removeSnapshotCache(ExtTableSnapshotInfo extTableSnapshotInfo) {
        tablesCache.invalidate(extTableSnapshotInfo.getResourcePath());
    }

    @Override
    public CacheState getCacheState(ExtTableSnapshotInfo extTableSnapshotInfo) {
        String resourcePath = extTableSnapshotInfo.getResourcePath();
        if (inBuildingTables.containsKey(resourcePath)) {
            return CacheState.IN_BUILDING;
        }
        File stateFile = getCacheStateFile(getSnapshotCachePath(extTableSnapshotInfo.getTableName(),
                extTableSnapshotInfo.getId()));
        if (!stateFile.exists()) {
            return CacheState.NONE;
        }
        try {
            String stateString = Files.toString(stateFile, Charsets.UTF_8);
            return CacheState.valueOf(stateString);
        } catch (IOException e) {
            logger.error("error when read state file", e);
        }
        return CacheState.NONE;
    }

    public long getTotalCacheSize() {
        return FileUtils.sizeOfDirectory(new File(getCacheBasePath(config)));
    }

    public void checkCacheState() {
        cacheStateChecker.run();
    }

    private void saveSnapshotCacheState(ExtTableSnapshotInfo extTableSnapshotInfo, String cachePath) {
        File stateFile = getCacheStateFile(getSnapshotCachePath(extTableSnapshotInfo.getTableName(),
                extTableSnapshotInfo.getId()));
        try {
            Files.write(CacheState.AVAILABLE.name(), stateFile, Charsets.UTF_8);
            tablesCache.put(extTableSnapshotInfo.getResourcePath(), new CachedTableInfo(cachePath));
        } catch (IOException e) {
            throw new RuntimeException("error when write cache state for snapshot:"
                    + extTableSnapshotInfo.getResourcePath());
        }
    }

    private File getCacheStateFile(String snapshotCacheFolder) {
        String stateFilePath = snapshotCacheFolder + File.separator + STATE_FILE;
        return new File(stateFilePath);
    }

    protected String getSnapshotStorePath(String tableName, String snapshotID) {
        return getSnapshotCachePath(tableName, snapshotID) + File.separator + DB_FILE;
    }

    protected String getSnapshotCachePath(String tableName, String snapshotID) {
        return basePath + File.separator + tableName + File.separator + snapshotID;
    }

    private class CacheStateChecker implements Runnable {

        @Override
        public void run() {
            try {
                String cacheBasePath = getCacheBasePath(config);
                logger.info("check snapshot local cache state, local path:{}", cacheBasePath);
                File baseFolder = new File(cacheBasePath);
                if (!baseFolder.exists()) {
                    return;
                }
                Map<String, File[]> tableSnapshotsFileMap = getCachedTableSnapshotsFolders(baseFolder);
                List<Pair<String, File>> allCachedSnapshots = Lists.newArrayList();
                for (Entry<String, File[]> tableSnapshotsEntry : tableSnapshotsFileMap.entrySet()) {
                    String tableName = tableSnapshotsEntry.getKey();
                    for (File file : tableSnapshotsEntry.getValue()) {
                        String snapshotID = file.getName();
                        allCachedSnapshots
                                .add(new Pair<>(ExtTableSnapshotInfo.getResourcePath(tableName, snapshotID), file));
                    }
                }

                final Set<String> activeSnapshotSet = ExtTableSnapshotInfoManager.getInstance(config).getAllExtSnapshotResPaths();

                List<Pair<String, File>> toRemovedCachedSnapshots = Lists.newArrayList(FluentIterable.from(
                        allCachedSnapshots).filter(new Predicate<Pair<String, File>>() {
                    @Override
                            public boolean apply(@Nullable Pair<String, File> input) {
                                long lastModified = input.getSecond().lastModified();
                                return !activeSnapshotSet.contains(input.getFirst()) && lastModified > 0
                                        && lastModified < (System.currentTimeMillis() - config.getExtTableSnapshotLocalCacheCheckVolatileRange());
                    }
                }));
                for (Pair<String, File> toRemovedCachedSnapshot : toRemovedCachedSnapshots) {
                    File snapshotCacheFolder = toRemovedCachedSnapshot.getSecond();
                    logger.info("removed cache file:{}, it is not referred by any cube",
                            snapshotCacheFolder.getAbsolutePath());
                    try {
                        FileUtils.deleteDirectory(snapshotCacheFolder);
                    } catch (IOException e) {
                        logger.error("fail to remove folder:" + snapshotCacheFolder.getAbsolutePath(), e);
                    }
                    tablesCache.invalidate(toRemovedCachedSnapshot.getFirst());
                }
            } catch (Exception e) {
                logger.error("error happens when check cache state", e);
            }

        }
    }

    private static class CachedTableInfo {
        private String cachePath;

        private long dbSize;

        public CachedTableInfo(String cachePath) {
            this.cachePath = cachePath;
            this.dbSize = FileUtils.sizeOfDirectory(new File(cachePath));
        }

        public int getSizeInKB() {
            return (int) (dbSize / 1024);
        }

        public void cleanStorage() {
            logger.info("clean cache storage for path:" + cachePath);
            try {
                FileUtils.deleteDirectory(new File(cachePath));
            } catch (IOException e) {
                logger.error("file delete fail:" + cachePath, e);
            }
        }
    }

    /**
     * A simple named thread factory.
     */
    private static class NamedThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        public NamedThreadFactory(String threadPrefix) {
            final SecurityManager s = System.getSecurityManager();
            this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = threadPrefix + "-";
        }

        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            t.setDaemon(true);
            return t;
        }
    }
}
