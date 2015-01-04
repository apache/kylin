package com.kylinolap.metadata.realization;

import com.google.common.collect.*;
import com.kylinolap.common.KylinConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Hongbin Ma(Binmahone) on 12/18/14.
 */
public class RealizationRegistry {
    private static final Logger logger = LoggerFactory.getLogger(RealizationRegistry.class);
    private static final ConcurrentHashMap<KylinConfig, RealizationRegistry> CACHE = new ConcurrentHashMap<KylinConfig, RealizationRegistry>();

    public static RealizationRegistry getInstance(KylinConfig config) {
        RealizationRegistry r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (RealizationRegistry.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new RealizationRegistry(config);
                CACHE.put(config, r);
                r.loadRealizations();
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton of RealizationRegistry exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeManager from " + config, e);
            }
        }
    }

    public static void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }

    // ============================================================================

    private Table<RealizationType, String, IRealization> realizationTable = HashBasedTable.create();
    private KylinConfig config;

    private RealizationRegistry(KylinConfig config) throws IOException {
        logger.info("Initializing RealizationRegistry with metadata url " + config);
        this.config = config;
    }

    private void loadRealizations() {
        // use reflection to load all realizations
        List<Throwable> es = Lists.newArrayList();
        List<String> realizationProviders = Lists.newArrayList("com.kylinolap.cube.CubeManager");
        for (String clsName : realizationProviders) {
            try {
                Class<?> cls = Class.forName(clsName);
                cls.getMethod("getInstance", KylinConfig.class).invoke(null, this.config);
            } catch (Exception | NoClassDefFoundError e) {
                es.add(e);
            }

            if (es.size() > 0) {
                for (Throwable exceptionOrError : es) {
                    logger.error("Create new store instance failed ", exceptionOrError);
                }
                throw new IllegalArgumentException("Failed to find metadata store by url: " + this.config.getMetadataUrl());
            }
        }
    }

    public synchronized void registerRealization(IRealization realization) {
        realizationTable.put(realization.getType(), realization.getName().toUpperCase(), realization);
    }

    public synchronized void unregisterRealization(IRealization realization) {
        realizationTable.remove(realization.getType(), realization.getName().toUpperCase());
    }

    public synchronized IRealization getRealization(RealizationType type, String name) {
        return realizationTable.get(type, name.toUpperCase());
    }

}
