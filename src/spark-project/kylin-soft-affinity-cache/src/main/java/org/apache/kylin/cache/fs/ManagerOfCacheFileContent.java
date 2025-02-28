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

import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A wrapper around alluxio CacheManager, enriching some file operations, like
 * 1) tracking cache time at file level
 * 2) deleting cache at file level, etc.
 */
@RequiredArgsConstructor
@Slf4j
public class ManagerOfCacheFileContent implements CacheManager {

    private final CacheManager under;
    private final ConcurrentHashMap<String, FileMeta> fileMetas = new ConcurrentHashMap<>();

    @Override
    public boolean put(PageId pageId, byte[] page, CacheContext cacheContext) {
        boolean ok = under.put(pageId, page, cacheContext);
        markFilePageCached(pageId);
        return ok;
    }

    @Override
    public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int offsetInBuffer,
            CacheContext cacheContext) {
        int bytesRead = under.get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer, cacheContext);
        if (bytesRead > 0)
            markFilePageCached(pageId);
        return bytesRead;
    }

    @Override
    public boolean delete(PageId pageId) {
        boolean ok = under.delete(pageId);
        markFilePageRemoved(pageId);
        return ok;
    }

    @Override
    public State state() {
        return under.state();
    }

    @Override
    public void close() throws Exception {
        under.close();
    }

    @RequiredArgsConstructor
    static class FileMeta {
        final String fileId;
        final long cacheCreateTime = System.currentTimeMillis();
        final BitSet cachedPageIndex = new BitSet();
    }

    public int countCachedPages(String fileId) {
        FileMeta meta = fileMetas.get(fileId);
        if (meta == null)
            return 0;
        else
            return meta.cachedPageIndex.cardinality();
    }

    public long countTotalCachedPages() {
        long sum = 0;
        for (FileMeta meta : fileMetas.values()) {
            sum += meta.cachedPageIndex.cardinality();
        }
        return sum;
    }

    protected void markFilePageCached(PageId pageId) {
        fileMetas.compute(pageId.getFileId(), (fileId, fmeta) -> {
            if (fmeta == null)
                fmeta = new FileMeta(fileId);
            fmeta.cachedPageIndex.set((int) pageId.getPageIndex());
            return fmeta;
        });
    }

    protected void markFilePageRemoved(PageId pageId) {
        fileMetas.compute(pageId.getFileId(), (fileId, fmeta) -> {
            if (fmeta == null)
                fmeta = new FileMeta(fileId);
            fmeta.cachedPageIndex.clear((int) pageId.getPageIndex());
            return fmeta;
        });
    }

}
