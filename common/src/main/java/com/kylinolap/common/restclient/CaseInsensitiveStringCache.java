package com.kylinolap.common.restclient;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by qianzhou on 1/15/15.
 */
public class CaseInsensitiveStringCache<V> extends SingleValueCache<String, V>{

    public CaseInsensitiveStringCache(Broadcaster.TYPE syncType) {
        super(syncType, new ConcurrentSkipListMap<String, V>(String.CASE_INSENSITIVE_ORDER));
    }

    @Override
    public void put(String key, V value) {
        super.put(key, value);
    }

    @Override
    public void putLocal(String key, V value) {
        super.putLocal(key, value);
    }
}
