package org.apache.kylin.storage.hybrid;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class HybridManager implements IRealizationProvider {
    public static final Serializer<HybridInstance> HYBRID_SERIALIZER = new JsonSerializer<HybridInstance>(HybridInstance.class);

    private static final Logger logger = LoggerFactory.getLogger(HybridManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, HybridManager> CACHE = new ConcurrentHashMap<KylinConfig, HybridManager>();

    public static HybridManager getInstance(KylinConfig config) {
        HybridManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (HybridManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new HybridManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one Hybrid Manager singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init Hybrid Manager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;

    private CaseInsensitiveStringCache<HybridInstance> hybridMap = new CaseInsensitiveStringCache<HybridInstance>(Broadcaster.TYPE.HYBRID);

    private HybridManager(KylinConfig config) throws IOException {
        logger.info("Initializing HybridManager with config " + config);
        this.config = config;

        loadAllHybridInstance();
    }

    private void loadAllHybridInstance() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.HYBRID_RESOURCE_ROOT, ".json");

        logger.debug("Loading Hybrid from folder " + store.getReadableResourcePath(ResourceStore.HYBRID_RESOURCE_ROOT));

        for (String path : paths) {
            loadHybridInstance(path);
        }

        logger.debug("Loaded " + paths.size() + " Hybrid(s)");
    }

    private synchronized HybridInstance loadHybridInstance(String path) throws IOException {
        ResourceStore store = getStore();

        HybridInstance hybridInstance = null;
        try {
            hybridInstance = store.getResource(path, HybridInstance.class, HYBRID_SERIALIZER);
            hybridInstance.setConfig(config);

            if (StringUtils.isBlank(hybridInstance.getName()))
                throw new IllegalStateException("HybridInstance name must not be blank, at " + path);

            if (hybridInstance.getHistoryRealization() == null || hybridInstance.getRealTimeRealization() == null) {

                throw new IllegalStateException("HybridInstance must have both historyRealization and realTimeRealization set, at " + path);
            }

            final String name = hybridInstance.getName();
            hybridMap.putLocal(name, hybridInstance);

            return hybridInstance;
        } catch (Exception e) {
            logger.error("Error during load hybrid instance " + path, e);
            return null;
        }
    }

    @Override
    public RealizationType getRealizationType() {
        return RealizationType.HYBRID;
    }

    @Override
    public IRealization getRealization(String name) {
        return getHybridInstance(name);
    }

    public HybridInstance getHybridInstance(String name) {
        return hybridMap.get(name);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }
}
