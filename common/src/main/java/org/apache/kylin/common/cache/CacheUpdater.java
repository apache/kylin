package org.apache.kylin.common.cache;

import org.apache.kylin.common.restclient.AbstractRestCache;
import org.apache.kylin.common.restclient.Broadcaster;

/**
 * Created by Hongbin Ma(Binmahone) on 6/1/15.
 */
public interface CacheUpdater {
    void updateCache(Object key, Object value, Broadcaster.EVENT syncAction, Broadcaster.TYPE type, AbstractRestCache cache);
}
