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
package org.apache.kylin.cache.fs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.cache.kylin.KylinCacheFileSystem;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.cache.CacheLoader;
import org.apache.kylin.guava30.shaded.common.cache.LoadingCache;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.util.concurrent.UncheckedExecutionException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A cache of file status.
 */
@Slf4j
public class ManagerOfCacheFileStatus {

    private final LoadingCache<Path, FileStatusEntry> statusCache;
    private final LoadingCache<Path, FileStatusEntry> childrenCache;
    private int countOfEvict; // for test

    public ManagerOfCacheFileStatus(long ttlSec, long maxSize, Function<Path, FileStatus> statusLoader,
            Function<Path, LocatedFileStatus[]> statusListLoader) {
        this.statusCache = CacheBuilder.newBuilder().maximumSize(maxSize).expireAfterAccess(ttlSec, TimeUnit.SECONDS)
                .recordStats().build(new CacheLoader<Path, FileStatusEntry>() {
                    @Override
                    public FileStatusEntry load(Path p) {
                        StopWatch w = StopWatch.createStarted();
                        long cacheTime = KylinCacheFileSystem.getAcceptCacheTimeLocally();

                        FileStatus status = statusLoader.apply(p);

                        log.debug("Slow fs operation took {} ms, now cached at {}: {}({})", w.getTime(), cacheTime,
                                "getFileStatus", p);
                        return new FileStatusEntry(p, status, null, cacheTime);
                    }
                });
        this.childrenCache = CacheBuilder.newBuilder().maximumSize(maxSize).expireAfterAccess(ttlSec, TimeUnit.SECONDS)
                .recordStats().build(new CacheLoader<Path, FileStatusEntry>() {
                    @Override
                    public FileStatusEntry load(Path p) {
                        StopWatch w = StopWatch.createStarted();
                        long cacheTime = KylinCacheFileSystem.getAcceptCacheTimeLocally();

                        LocatedFileStatus[] children = statusListLoader.apply(p);
                        for (LocatedFileStatus child : children) {
                            statusCache.put(child.getPath(),
                                    new FileStatusEntry(child.getPath(), child, null, cacheTime));
                        }

                        log.debug("Slow fs operation took {} ms, now cached at {}: got {} files from {}({})",
                                w.getTime(), cacheTime, children.length, "listFileStatus", p);
                        return new FileStatusEntry(p, null, children, cacheTime);
                    }
                });
    }

    @RequiredArgsConstructor
    private static class FileStatusEntry {
        final Path p;
        final FileStatus status;
        final LocatedFileStatus[] children;
        final long cacheCreateTime;
    }

    /**
     * Ensure the cache create time of a file is later than a required time.
     * In other word, user wants to see the refresh data after a required time.
     * Stale cache must be cleared by this method.
     */
    public void ensureCacheCreateTime(Path p, long requiredCacheTime) {
        if (requiredCacheTime <= 0)
            return;

        List<LoadingCache<Path, FileStatusEntry>> caches = ImmutableList.of(statusCache, childrenCache);
        for (LoadingCache<Path, FileStatusEntry> c : caches) {
            FileStatusEntry entry = c.asMap().get(p);
            if (entry != null && entry.cacheCreateTime < requiredCacheTime) {
                log.debug("(accept-cache-time:{}) Evict file status cache for {}", requiredCacheTime, p);
                c.asMap().remove(p);
                countOfEvict++;
            }
        }
    }

    public FileStatus getFileStatus(Path p) throws IOException {
        try {
            return this.statusCache.get(p).status;
        } catch (Exception e) {
            throw makeIOException(e);
        }
    }

    public LocatedFileStatus[] listChildren(Path p) throws IOException {
        try {
            FileStatusEntry entry = this.childrenCache.get(p);
            log.trace("Return cached at {}: got {} files from {}({})", entry.cacheCreateTime, entry.children.length,
                    "listFileStatus", p);
            return entry.children;
        } catch (Exception e) {
            throw makeIOException(e);
        }
    }

    protected static IOException makeIOException(Throwable e) {
        if (e instanceof UncheckedExecutionException || e instanceof ExecutionException)
            e = e.getCause();

        if (e instanceof IOException)
            return (IOException) e;
        else if (e.getCause() instanceof IOException)
            return (IOException) e.getCause();
        else
            return new IOException(e);
    }

    public long size() {
        return statusCache.size() + childrenCache.size();
    }

    public int countEvictions() {
        return countOfEvict;
    }
}
