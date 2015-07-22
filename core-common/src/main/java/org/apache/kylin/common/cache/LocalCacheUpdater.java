package org.apache.kylin.common.cache;

import org.apache.kylin.common.restclient.AbstractRestCache;
import org.apache.kylin.common.restclient.Broadcaster;

/**
 */
public class LocalCacheUpdater implements CacheUpdater {
    @Override
    public void updateCache(Object key, Object value, Broadcaster.EVENT syncAction, Broadcaster.TYPE type, AbstractRestCache cache) {
        if (syncAction == Broadcaster.EVENT.CREATE || syncAction == Broadcaster.EVENT.UPDATE) {
            cache.putLocal(key, value);
        } else if (syncAction == Broadcaster.EVENT.DROP) {
            cache.removeLocal(key);
        }
    }
}
