package com.kylinolap.metadata.realization;

import com.google.common.collect.*;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.metadata.project.ProjectTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Hongbin Ma(Binmahone) on 12/18/14.
 */
public class RealizationRegistry {
    private static final Logger logger = LoggerFactory.getLogger(RealizationRegistry.class);
    private static final ConcurrentHashMap<KylinConfig, RealizationRegistry> CACHE = new ConcurrentHashMap<KylinConfig, RealizationRegistry>();

    private Table<RealizationType, String, IRealization> realizationTable = HashBasedTable.create();

    private KylinConfig config;

    private RealizationRegistry(KylinConfig config) throws IOException {
        logger.info("Initializing RealizationRegistry with metadata url " + config);
        this.config = config;

        //TODO: use reflection to load all realizations
    }

    public synchronized IRealization getRealization(RealizationType type, String name) {
        return realizationTable.get(type, name.toUpperCase());
    }

    public synchronized void registerRealization(IRealization realization) {
        realizationTable.put(realization.getType(), realization.getName().toUpperCase(), realization);
    }

    public synchronized void unregisterRealization(IRealization realization) {
        realizationTable.remove(realization.getType(), realization.getName().toUpperCase());
    }

    /**
     * Caller can only remove all realizations of a certain RealizationType
     *
     * @param type
     */
    public synchronized void resetRealizationOf(RealizationType type) {
        realizationTable.rowMap().remove(type);
    }


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
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeManager from " + config, e);
            }
        }
    }

    public static synchronized void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }
}
