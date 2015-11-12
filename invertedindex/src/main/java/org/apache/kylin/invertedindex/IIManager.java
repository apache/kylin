/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.invertedindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * @author honma
 */
public class IIManager implements IRealizationProvider {

    private static String ALPHA_NUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static int HBASE_TABLE_LENGTH = 10;

    private static final Serializer<IIInstance> II_SERIALIZER = new JsonSerializer<IIInstance>(IIInstance.class);

    private static final Logger logger = LoggerFactory.getLogger(IIManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, IIManager> CACHE = new ConcurrentHashMap<KylinConfig, IIManager>();

    public static IIManager getInstance(KylinConfig config) {
        IIManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (IIManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new IIManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init IIManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    // ii name ==> IIInstance
    private CaseInsensitiveStringCache<IIInstance> iiMap;

    // for generation hbase table name of a new segment
    private Multimap<String, String> usedStorageLocation = HashMultimap.create();

    private IIManager(KylinConfig config) throws IOException {
        logger.info("Initializing IIManager with config " + config);
        this.config = config;
        this.iiMap = new CaseInsensitiveStringCache<IIInstance>(config, Broadcaster.TYPE.INVERTED_INDEX);
        loadAllIIInstance();
    }

    public List<IIInstance> listAllIIs() {
        return new ArrayList<IIInstance>(iiMap.values());
    }

    public IIInstance getII(String iiName) {
        return iiMap.get(iiName);
    }

    public List<IIInstance> getIIsByDesc(String descName) {

        List<IIInstance> list = listAllIIs();
        List<IIInstance> result = new ArrayList<IIInstance>();
        Iterator<IIInstance> it = list.iterator();
        while (it.hasNext()) {
            IIInstance ci = it.next();
            if (descName.equalsIgnoreCase(ci.getDescName())) {
                result.add(ci);
            }
        }
        return result;
    }
    

    public IIInstance createII(IIInstance ii) throws IOException {

        if (this.getII(ii.getName()) != null)
            throw new IllegalArgumentException("The II name '" + ii.getName() + "' already exists.");

        this.updateII(ii);

        // FIXME need to pass in project name
        String projectName = ProjectInstance.DEFAULT_PROJECT_NAME;
        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.INVERTED_INDEX, ii.getName(), projectName, ii.getOwner());
        return ii;
    }

    public void reloadIILocal(String iiName) {
        try {
            reloadIILocalAt(IIInstance.concatResourcePath(iiName));
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
    }

    public IIInstance dropII(String iiName, boolean deleteDesc) throws IOException {
        logger.info("Dropping II '" + iiName + "'");

        IIInstance ii = getII(iiName);

        if (deleteDesc && ii.getDescriptor() != null) {
            IIDescManager.getInstance(config).removeIIDesc(ii.getDescriptor());
        }

        removeII(ii);
        ProjectManager.getInstance(config).removeRealizationsFromProjects(RealizationType.INVERTED_INDEX, iiName);

        return ii;
    }

    private void removeII(IIInstance ii) throws IOException {
        getStore().deleteResource(ii.getResourcePath());
        iiMap.remove(ii.getName());
    }

    public void removeIILocal(String name) {
        iiMap.removeLocal(name);
        usedStorageLocation.removeAll(name.toUpperCase());
    }

    public void updateII(IIInstance ii) throws IOException {
        logger.info("Updating II instance : " + ii.getName());
        getStore().putResource(ii.getResourcePath(), ii, II_SERIALIZER);
        iiMap.put(ii.getName(), ii);

        //this is a duplicate call to take care of scenarios where REST cache service unavailable
        ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).clearL2Cache();
    }

    /**
     * @param IIInstance
     * @param startDate  (pass 0 if full build)
     * @param endDate    (pass 0 if full build)
     * @return
     */
    public IISegment buildSegment(IIInstance IIInstance, long startDate, long endDate) {
        IISegment segment = new IISegment();
        String incrementalSegName = IISegment.getSegmentName(startDate, endDate);
        segment.setUuid(UUID.randomUUID().toString());
        segment.setName(incrementalSegName);
        segment.setCreateTimeUTC(System.currentTimeMillis());
        segment.setDateRangeStart(startDate);
        segment.setDateRangeEnd(endDate);
        segment.setStatus(SegmentStatusEnum.NEW);
        segment.setStorageLocationIdentifier(generateStorageLocation());

        segment.setIIInstance(IIInstance);

        return segment;
    }

    private String generateStorageLocation() {
        while (true) {
            StringBuilder sb = new StringBuilder(IRealizationConstants.IIHbaseStorageLocationPrefix);
            for (int i = 0; i < HBASE_TABLE_LENGTH; i++) {
                int idx = (int) (Math.random() * ALPHA_NUM.length());
                sb.append(ALPHA_NUM.charAt(idx));
            }
            if (usedStorageLocation.containsValue(sb.toString())) {
                continue;
            } else {
                return sb.toString();
            }
        }
    }

    private void loadAllIIInstance() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.II_RESOURCE_ROOT, ".json");

        logger.debug("Loading II from folder " + store.getReadableResourcePath(ResourceStore.II_RESOURCE_ROOT));

        for (String path : paths) {
            reloadIILocalAt(path);
        }

        logger.debug("Loaded " + paths.size() + " II(s)");
    }

    private synchronized IIInstance reloadIILocalAt(String path) throws IOException {
        ResourceStore store = getStore();
        logger.debug("Loading IIInstance " + store.getReadableResourcePath(path));

        IIInstance ii = null;
        try {
            ii = store.getResource(path, IIInstance.class, II_SERIALIZER);
            ii.setConfig(config);

            if (StringUtils.isBlank(ii.getName()))
                throw new IllegalStateException("IIInstance name must not be blank");

            iiMap.putLocal(ii.getName(), ii);

            for (IISegment segment : ii.getSegments()) {
                usedStorageLocation.put(ii.getName().toUpperCase(), segment.getStorageLocationIdentifier());
            }

            return ii;
        } catch (Exception e) {
            logger.error("Error during load ii instance " + path, e);
            return null;
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    @Override
    public RealizationType getRealizationType() {
        return RealizationType.INVERTED_INDEX;
    }

    @Override
    public IRealization getRealization(String name) {
        return getII(name);
    }
}
