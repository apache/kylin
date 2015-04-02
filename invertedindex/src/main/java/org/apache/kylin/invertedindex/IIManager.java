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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    private CaseInsensitiveStringCache<IIInstance> iiMap = new CaseInsensitiveStringCache<IIInstance>(Broadcaster.TYPE.INVERTED_INDEX);

    // for generation hbase table name of a new segment
    private HashSet<String> usedStorageLocation = new HashSet<String>();

    private IIManager(KylinConfig config) throws IOException {
        logger.info("Initializing IIManager with config " + config);
        this.config = config;

        loadAllIIInstance();
    }

    public List<IIInstance> listAllIIs() {
        return new ArrayList<IIInstance>(iiMap.values());
    }

    public IIInstance getII(String iiName) {
        iiName = iiName.toUpperCase();
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

    public void buildInvertedIndexDictionary(IISegment iiSeg, String factColumnsPath) throws IOException {
        logger.info("Start building ii dictionary");
        DictionaryManager dictMgr = getDictionaryManager();
        IIDesc iiDesc = iiSeg.getIIInstance().getDescriptor();
        for (TblColRef column : iiDesc.listAllColumns()) {
            logger.info("Dealing with column {}", column);
            if (iiDesc.isMetricsCol(column)) {
                continue;
            }

            DictionaryInfo dict = dictMgr.buildDictionary(iiDesc.getModel(), "true", column, factColumnsPath);
            iiSeg.putDictResPath(column, dict.getResourcePath());
        }
        saveResource(iiSeg.getIIInstance());
    }

    /**
     * return null if no dictionary for given column
     */
    public Dictionary<?> getDictionary(IISegment iiSeg, TblColRef col) {
        DictionaryInfo info = null;
        try {
            DictionaryManager dictMgr = getDictionaryManager();
            // logger.info("Using metadata url " + metadataUrl +
            // " for DictionaryManager");
            String dictResPath = iiSeg.getDictResPath(col);
            if (dictResPath == null)
                return null;

            info = dictMgr.getDictionaryInfo(dictResPath);
            if (info == null)
                throw new IllegalStateException("No dictionary found by " + dictResPath + ", invalid II state; II segment" + iiSeg + ", col " + col);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get dictionary for II segment" + iiSeg + ", col" + col, e);
        }

        return info.getDictionaryObject();
    }

    public IIInstance createII(IIInstance ii) throws IOException {

        if (this.getII(ii.getName()) != null)
            throw new IllegalArgumentException("The II name '" + ii.getName() + "' already exists.");

        // other logic is the same as update.
        return updateII(ii);
    }

    public void updateIIStreamingOffset(String iiName, int partition, long offset) throws IOException {

    }


    public IIInstance updateII(IIInstance ii) throws IOException {
        logger.info("Updating II instance '" + ii.getName());

        // save resource
        saveResource(ii);

        logger.info("II with " + ii.getSegments().size() + " segments is saved");

        return ii;
    }

    public void loadIICache(String iiName) {
        try {
            loadIIInstance(IIInstance.concatResourcePath(iiName));
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
    }

    public void removeIICache(IIInstance ii) {
        iiMap.remove(ii.getName());

        for (IISegment segment : ii.getSegments()) {
            usedStorageLocation.remove(segment.getName());
        }
    }

    public void removeIILocalCache(String name) {
        iiMap.removeLocal(name);
        //TODO
        //        for (IISegment segment : ii.getSegments()) {
        //            usedStorageLocation.remove(segment.getName());
        //        }
    }

    private void saveResource(IIInstance ii) throws IOException {
        ResourceStore store = getStore();
        store.putResource(ii.getResourcePath(), ii, II_SERIALIZER);
        this.afterIIUpdated(ii);
    }

    private void afterIIUpdated(IIInstance updatedII) {
        iiMap.put(updatedII.getName(), updatedII);
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
            if (usedStorageLocation.contains(sb.toString())) {
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
            loadIIInstance(path);
        }

        logger.debug("Loaded " + paths.size() + " II(s)");
    }

    private synchronized IIInstance loadIIInstance(String path) throws IOException {
        ResourceStore store = getStore();
        logger.debug("Loading IIInstance " + store.getReadableResourcePath(path));

        IIInstance IIInstance = null;
        try {
            IIInstance = store.getResource(path, IIInstance.class, II_SERIALIZER);
            IIInstance.setConfig(config);

            if (StringUtils.isBlank(IIInstance.getName()))
                throw new IllegalStateException("IIInstance name must not be blank");

            iiMap.putLocal(IIInstance.getName(), IIInstance);

            for (IISegment segment : IIInstance.getSegments()) {
                usedStorageLocation.add(segment.getName());
            }

            return IIInstance;
        } catch (Exception e) {
            logger.error("Error during load ii instance " + path, e);
            return null;
        }
    }

    private DictionaryManager getDictionaryManager() {
        return DictionaryManager.getInstance(config);
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
