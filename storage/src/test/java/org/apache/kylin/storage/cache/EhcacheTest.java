package org.apache.kylin.storage.cache;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.junit.Test;

/**
 */
public class EhcacheTest {
    @Test
    public void basicTest() throws InterruptedException {
        CacheManager cacheManager = CacheManager.create();

        //Create a Cache specifying its configuration.
        Cache testCache = //Create a Cache specifying its configuration.
        new Cache(new CacheConfiguration("test", 0).//
                memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU).//
                eternal(false).//
                timeToIdleSeconds(86400).//
                diskExpiryThreadIntervalSeconds(0).//
                maxBytesLocalHeap(100, MemoryUnit.MEGABYTES).//
                persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)));

        cacheManager.addCache(testCache);

        //
        //        byte[] blob2 = new byte[(1024 * 400 * 1024)];//400M
        //
        //        testCache.put(new Element("1", blob));
        //        System.out.println(testCache.get("1") == null);
        //        System.out.println(testCache.getSize());
        //        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        //        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        //        testCache.put(new Element("2", blob));
        //        System.out.println(testCache.get("1") == null);
        //        System.out.println(testCache.getSize());
        //        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        //        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");
        //        testCache.put(new Element("3", blob));
        //        System.out.println(testCache.get("1") == null);
        //        System.out.println(testCache.get("2") == null);
        //        System.out.println(testCache.get("3") == null);
        //        System.out.println(testCache.getSize());
        //        System.out.println(testCache.getStatistics().getLocalHeapSizeInBytes());
        //        System.out.println("runtime used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024 + "M");

        cacheManager.shutdown();
    }
}
