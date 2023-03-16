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
import java.nio.charset.Charset;
import java.util.List;

import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CacheReloadCheckerTest extends NLocalFileMetadataTestCase {

    private final String resPath = "/mock/mock.json";

    private final String depPath1 = "/mock/mock/dep1.json";
    private final String depPath2 = "/mock/mock/dep2.json";
    private final String depPath3 = "/mock/mock/dep3.json";
    private final Charset charset = Charset.defaultCharset();

    @Before
    public void setup() throws IOException {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void test() {
        ResourceStore store = Mockito.mock(ResourceStore.class);
        CachedCrudAssist<RootPersistentEntity> crud = Mockito.mock(CachedCrudAssist.class);
        Cache<String, RootPersistentEntity> cache = Mockito.mock(Cache.class);

        Mockito.when(crud.getCache()).thenReturn(cache);
        CacheReloadChecker checker = new CacheReloadChecker(store, crud);

        List<RootPersistentEntity> dependencies = mockDependencies();

        RootPersistentEntity entity = Mockito.mock(RootPersistentEntity.class);
        Mockito.when(entity.getMvcc()).thenReturn(0L);
        Mockito.when(entity.getResourcePath()).thenReturn(resPath);
        Mockito.when(entity.getDependencies()).thenReturn(dependencies);

        Mockito.when(cache.getIfPresent("mock")).thenReturn(entity);
        Mockito.when(store.getResource(resPath))
                .thenReturn(new RawResource(resPath, ByteSource.wrap("version1".getBytes(charset)), 0L, 0));
        Mockito.when(store.getResource(depPath1))
                .thenReturn(new RawResource(depPath1, ByteSource.wrap("version1".getBytes(charset)), 0L, 0));
        Mockito.when(store.getResource(depPath2))
                .thenReturn(new RawResource(depPath2, ByteSource.wrap("version1".getBytes(charset)), 0L, 0));
        Mockito.when(store.getResource(depPath3))
                .thenReturn(new RawResource(depPath3, ByteSource.wrap("version1".getBytes(charset)), 0L, 0));

        Assert.assertFalse(checker.needReload("mock"));
        Mockito.when(store.getResource(depPath3)).thenReturn(null);
        Assert.assertTrue(checker.needReload("mock"));

        dependencies.remove(2);
        dependencies.add(new MissingRootPersistentEntity(depPath3));
        Assert.assertFalse(checker.needReload("mock"));

        Mockito.when(store.getResource(depPath3))
                .thenReturn(new RawResource(depPath3, ByteSource.wrap("version1".getBytes(charset)), 0L, 0));
        Assert.assertTrue(checker.needReload("mock"));
    }

    private List<RootPersistentEntity> mockDependencies() {
        List<RootPersistentEntity> lists = Lists.newArrayList();
        RootPersistentEntity dep1 = Mockito.mock(RootPersistentEntity.class);
        Mockito.when(dep1.getDependencies()).thenReturn(null);
        Mockito.when(dep1.getResourcePath()).thenReturn(depPath1);
        Mockito.when(dep1.getMvcc()).thenReturn(0L);
        RootPersistentEntity dep2 = Mockito.mock(RootPersistentEntity.class);
        Mockito.when(dep2.getDependencies()).thenReturn(null);
        Mockito.when(dep2.getResourcePath()).thenReturn(depPath2);
        Mockito.when(dep2.getMvcc()).thenReturn(0L);
        RootPersistentEntity dep3 = Mockito.mock(RootPersistentEntity.class);
        Mockito.when(dep3.getDependencies()).thenReturn(null);
        Mockito.when(dep3.getResourcePath()).thenReturn(depPath3);
        Mockito.when(dep3.getMvcc()).thenReturn(0L);
        lists.add(dep1);
        lists.add(dep2);
        lists.add(dep3);
        return lists;
    }

}
