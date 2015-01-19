package com.kylinolap.rest.service;

import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.cube.CubeDescManager;
import com.kylinolap.invertedindex.IIDescManager;
import com.kylinolap.metadata.project.ProjectManager;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Created by qianzhou on 1/19/15.
 */
@Component("cacheService")
public class CacheService extends BasicService {

    public void rebuildCache(Broadcaster.TYPE cacheType, String cacheKey) {
        final String log = "rebuild cache type: " + cacheType + " name:" + cacheKey;
        try {
            switch (cacheType) {
                case CUBE:
                    getCubeManager().loadCubeCache(getCubeManager().getCube(cacheKey));
                    break;
                case CUBE_DESC:
                    getCubeDescManager().reloadCubeDesc(cacheKey);
                    break;
                case PROJECT:
                    getProjectManager().reloadProject(cacheKey);
                    break;
                case INVERTED_INDEX:
                    getIIManager().loadIICache(cacheKey);
                    break;
                case INVERTED_INDEX_DESC:
                    getIIDescManager().reloadIIDesc(cacheKey);
                    break;
                case TABLE:
                    getMetadataManager().reloadTableCache(cacheKey);
                    IIDescManager.clearCache();
                    CubeDescManager.clearCache();
                    break;
                case DATA_MODEL:
                    getMetadataManager().reloadDataModelDesc(cacheKey);
                    IIDescManager.clearCache();
                    CubeDescManager.clearCache();
                    break;
                default:
                    throw new RuntimeException("invalid cacheType:" + cacheType);
            }
        } catch (IOException e) {
            throw new RuntimeException("error " + log, e);
        }

    }

    public void removeCache(Broadcaster.TYPE cacheType, String cacheKey) {
        final String log = "remove cache type: " + cacheType + " name:" + cacheKey;
        try {
            switch (cacheType) {
                case CUBE:
                    getCubeManager().removeCubeCacheLocal(cacheKey);
                    break;
                case CUBE_DESC:
                    getCubeDescManager().removeLocalCubeDesc(cacheKey);
                    break;
                case PROJECT:
                    ProjectManager.removeInstance(getConfig());
                    break;
                case INVERTED_INDEX:
                    getIIManager().removeIILocalCache(cacheKey);
                    break;
                case INVERTED_INDEX_DESC:
                    getIIDescManager().removeIIDescLocal(cacheKey);
                    break;
                case TABLE:
                    IIDescManager.clearCache();
                    CubeDescManager.clearCache();
                    break;
                case DATA_MODEL:
                    throw new UnsupportedOperationException(log);
                default:
                    throw new RuntimeException("invalid cacheType:" + cacheType);
            }
        } catch (IOException e) {
            throw new RuntimeException("error " + log, e);
        }
    }
}
