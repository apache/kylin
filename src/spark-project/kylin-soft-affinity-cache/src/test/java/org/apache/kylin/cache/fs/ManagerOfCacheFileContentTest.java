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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import alluxio.client.file.cache.PageId;

public class ManagerOfCacheFileContentTest {

    private ManagerOfCacheFileContent cacheManager;

    @Before
    public void setUp() {
        cacheManager = new ManagerOfCacheFileContent(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMarkFilePageRemoved() {
        // Add some dummy data to the cacheManager before testing
        PageId pageId1 = new PageId("fileId1", 0);
        PageId pageId2 = new PageId("fileId1", 1);
        PageId pageId3 = new PageId("fileId2", 0);
        PageId pageId4 = new PageId("fileId3", 0);

        cacheManager.markFilePageCached(pageId1);
        cacheManager.markFilePageCached(pageId2);
        cacheManager.markFilePageCached(pageId3);

        ConcurrentHashMap<String, ManagerOfCacheFileContent.FileMeta> fileMetas //
                = (ConcurrentHashMap<String, ManagerOfCacheFileContent.FileMeta>) ReflectionTestUtils
                        .getField(cacheManager, "fileMetas");
        ManagerOfCacheFileContent.FileMeta fileMeta1 = fileMetas.get("fileId1");
        ManagerOfCacheFileContent.FileMeta fileMeta2 = fileMetas.get("fileId2");

        assertNotNull(fileMeta1);
        assertNotNull(fileMeta2);

        cacheManager.markFilePageRemoved(pageId1);
        cacheManager.markFilePageRemoved(pageId2);
        cacheManager.markFilePageRemoved(pageId4);

        ManagerOfCacheFileContent.FileMeta fileMeta3 = fileMetas.get("fileId3");
        assertNotNull(fileMeta3);

        assertTrue(fileMeta1.cachedPageIndex.isEmpty());
        assertFalse(fileMeta2.cachedPageIndex.isEmpty());
        assertTrue(fileMeta3.cachedPageIndex.isEmpty());
    }
}
