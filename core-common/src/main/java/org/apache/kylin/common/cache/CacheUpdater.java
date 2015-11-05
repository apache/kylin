package org.apache.kylin.common.cache;

import org.apache.kylin.common.restclient.AbstractRestCache;
import org.apache.kylin.common.restclient.Broadcaster;

/**
 */
@SuppressWarnings("rawtypes")
public interface CacheUpdater {
    void updateCache(Object key, Object value, Broadcaster.EVENT syncAction, Broadcaster.TYPE type, AbstractRestCache cache);
}
