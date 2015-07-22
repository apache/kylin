package org.apache.kylin.common.cache;

import org.apache.kylin.common.restclient.AbstractRestCache;
import org.apache.kylin.common.restclient.Broadcaster;

/**
 */
public class RemoteCacheUpdater implements CacheUpdater {
    @Override
    public void updateCache(Object key, Object value, Broadcaster.EVENT syncAction, Broadcaster.TYPE type, AbstractRestCache cache) {
        Broadcaster.getInstance().queue(type.getType(), syncAction.getType(), key.toString());
    }
}
