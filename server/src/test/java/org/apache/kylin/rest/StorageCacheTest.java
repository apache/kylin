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

package org.apache.kylin.rest;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.apache.commons.lang.NotImplementedException;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.storage.ICachableStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.cache.AbstractCacheFledgedQuery;
import org.apache.kylin.storage.cache.CacheFledgedStaticQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Range;

public class StorageCacheTest extends ServiceTestBase {

    public class MockedCachableStorageQuery implements ICachableStorageQuery {
        private String uuid;

        public MockedCachableStorageQuery(String uuid) {
            this.uuid = uuid;
        }

        @Override
        public boolean isDynamic() {
            return false;
        }

        @Override
        public Range<Long> getVolatilePeriod() {
            throw new NotImplementedException();
        }

        @Override
        public String getStorageUUID() {
            return this.uuid;
        }

        @Override
        public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
            throw new NotImplementedException();
        }
    }

    @Autowired
    private CacheManager cacheManager;

    @Before
    public void setup() throws Exception {
        AbstractCacheFledgedQuery.setCacheManager(cacheManager);
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("1"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("2"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("3"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("4"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("5"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("6"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("7"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("8"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("9"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("10"));
        new CacheFledgedStaticQuery(new MockedCachableStorageQuery("11"));
    }

    @Test
    public void test1() {
        int oneM = 1 << 20;
        cacheManager.getCache("1").put(new Element("xx", new byte[oneM]));
        Element xx = cacheManager.getCache("1").get("xx");
        Assert.assertEquals(oneM, ((byte[]) xx.getObjectValue()).length);

        cacheManager.getCache("2").put(new Element("yy", new byte[3 * oneM]));
        Element yy = cacheManager.getCache("2").get("yy");
        Assert.assertEquals(3 * oneM, ((byte[]) yy.getObjectValue()).length);

        cacheManager.getCache("3").put(new Element("zz", new byte[10 * oneM]));
        Element zz = cacheManager.getCache("3").get("zz");
        Assert.assertEquals(null, zz);

        cacheManager.getCache("4").put(new Element("aa", new byte[oneM]));
        Element aa = cacheManager.getCache("4").get("aa");
        Assert.assertEquals(oneM, ((byte[]) aa.getObjectValue()).length);

        cacheManager.getCache("2").put(new Element("bb", new byte[3 * oneM]));
        Element bb = cacheManager.getCache("2").get("bb");
        Assert.assertEquals(3 * oneM, ((byte[]) bb.getObjectValue()).length);
    }

    @Test
    public void test2() {
    }
}
