package org.apache.kylin.storage.cache;

import com.google.common.collect.Lists;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 4/13/15.
 */
public class EhcacheTest {
    @Test
    public void basicTest() {
        CacheManager cacheManager = CacheManager.create();

        //Create a Cache specifying its configuration.
        Cache testCache = new Cache(new CacheConfiguration("test", 0).//
                memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LFU).//
                eternal(false).//
                timeToIdleSeconds(86400).//
                diskExpiryThreadIntervalSeconds(0).//
                maxBytesLocalHeap(500, MemoryUnit.MEGABYTES).//
                persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.LOCALTEMPSWAP)));

        cacheManager.addCache(testCache);
        testCache.put(new Element("x", Lists.<String> newArrayList()));

        List<String> v = (List<String>) testCache.get("x").getObjectValue();
        Assert.assertTrue(v.size() == 0);
        v.add("hi");

        List<String> v2 = (List<String>) testCache.get("x").getObjectValue();
        Assert.assertTrue(v2.size() == 1);


        cacheManager.shutdown();
    }
}
