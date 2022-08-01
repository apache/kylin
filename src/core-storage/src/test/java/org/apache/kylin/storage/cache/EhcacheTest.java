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

package org.apache.kylin.storage.cache;

import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

@Ignore("trial for dev")
public class EhcacheTest {

    @Test
    public void basicTest() throws InterruptedException {
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        Configuration conf = new Configuration();
        conf.setMaxBytesLocalHeap("100M");
        CacheManager cacheManager = CacheManager.create(conf);

        //Create a Cache specifying its configuration.
        Cache testCache = //Create a Cache specifying its configuration.
                new Cache(new CacheConfiguration("test", 0).//
                        memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU).//
                        eternal(false).//
                        timeToIdleSeconds(86400).//
                        diskExpiryThreadIntervalSeconds(0).//
                        //maxBytesLocalHeap(1000, MemoryUnit.MEGABYTES).//
                        persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)));

        cacheManager.addCache(testCache);

        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        byte[] blob = new byte[(1024 * 80 * 1024)];//400M
        Random random = new Random();
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }

        //        List<String> manyObjects = Lists.newArrayList();
        //        for (int i = 0; i < 10000; i++) {
        //            manyObjects.add(new String("" + i));
        //        }
        //        testCache.put(new Element("0", manyObjects));

        testCache.put(new Element("1", blob));
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        blob = new byte[(1024 * 80 * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }

        testCache.put(new Element("2", blob));
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.get("2") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        blob = new byte[(1024 * 80 * 1024)];//400M
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) random.nextInt();
        }
        testCache.put(new Element("3", blob));
        System.out.println(testCache.get("1") == null);
        System.out.println(testCache.get("2") == null);
        System.out.println(testCache.get("3") == null);
        System.out.println(testCache.getSize());
        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        System.out.println("runtime used memory: "
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        cacheManager.shutdown();
    }
}
