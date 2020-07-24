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

package org.apache.kylin.dict;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.IReadableTable.TableSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;

public class DictionaryManager {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryManager.class);

    private static final DictionaryInfo NONE_INDICATOR = new DictionaryInfo();

    public static DictionaryManager getInstance(KylinConfig config) {
        return config.getManager(DictionaryManager.class);
    }

    // called by reflection
    static DictionaryManager newInstance(KylinConfig config) throws IOException {
        return new DictionaryManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private LoadingCache<String, DictionaryInfo> dictCache; // resource

    private DictionaryManager(KylinConfig config) {
        this.config = config;
        this.dictCache = CacheBuilder.newBuilder()//
                .softValues()//
                .removalListener(new RemovalListener<String, DictionaryInfo>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, DictionaryInfo> notification) {
                        DictionaryManager.logger.info("Dict with resource path {} is removed due to {}",
                                notification.getKey(), notification.getCause());
                    }
                })//
                .maximumSize(config.getCachedDictMaxEntrySize())//
                .expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<String, DictionaryInfo>() {
                    @Override
                    public DictionaryInfo load(String key) throws Exception {
                        DictionaryInfo dictInfo = DictionaryManager.this.load(key, true);
                        if (dictInfo == null) {
                            return NONE_INDICATOR;
                        } else {
                            return dictInfo;
                        }
                    }
                });
    }

    public Dictionary<String> getDictionary(String resourcePath) throws IOException {
        DictionaryInfo dictInfo = getDictionaryInfo(resourcePath);
        return dictInfo == null ? null : dictInfo.getDictionaryObject();
    }

    public DictionaryInfo getDictionaryInfo(final String resourcePath) throws IOException {
        try {
            //when all the value for this column is NULL, the resourcePath will be NULL
            if (resourcePath == null) {
                return NONE_INDICATOR;
            }
            DictionaryInfo result = dictCache.get(resourcePath);
            if (result == NONE_INDICATOR) {
                return null;
            } else {
                return result;
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * Save the dictionary as it is.
     * More often you should consider using its alternative trySaveNewDict to save dict space
     */
    public DictionaryInfo forceSave(Dictionary<String> newDict, DictionaryInfo newDictInfo) throws IOException {
        initDictInfo(newDict, newDictInfo);
        logger.info("force to save dict directly");
        return saveNewDict(newDictInfo);
    }

    /**
     * @return may return another dict that is a super set of the input
     * @throws IOException
     */
    public DictionaryInfo trySaveNewDict(Dictionary<String> newDict, DictionaryInfo newDictInfo) throws IOException {

        initDictInfo(newDict, newDictInfo);

        if (config.isGrowingDictEnabled()) {
            logger.info("Growing dict is enabled, merge with largest dictionary");
            DictionaryInfo largestDictInfo = findLargestDictInfo(newDictInfo);
            if (largestDictInfo != null) {
                largestDictInfo = getDictionaryInfo(largestDictInfo.getResourcePath());
                Dictionary<String> largestDictObject = largestDictInfo.getDictionaryObject();
                if (largestDictObject.contains(newDict)) {
                    logger.info("dictionary content {}, is contained by  dictionary at {}", newDict,
                            largestDictInfo.getResourcePath());
                    return largestDictInfo;
                } else if (newDict.contains(largestDictObject)) {
                    logger.info("dictionary content {} is by far the largest, save it", newDict);
                    return saveNewDict(newDictInfo);
                } else {
                    logger.info("merge dict and save...");
                    return mergeDictionary(Lists.newArrayList(newDictInfo, largestDictInfo));
                }
            } else {
                logger.info("first dict of this column, save it directly");
                return saveNewDict(newDictInfo);
            }
        } else {
            DictionaryInfo dupDict = checkDupByContent(newDictInfo, newDict);
            if (dupDict != null) {
                logger.info("Identical dictionary content, reuse existing dictionary at {}", dupDict.getResourcePath());
                dupDict = updateExistingDictLastModifiedTime(dupDict.getResourcePath());
                return dupDict;
            }

            return saveNewDict(newDictInfo);
        }
    }

    private DictionaryInfo checkDupByContent(DictionaryInfo dictInfo, Dictionary<String> dict) throws IOException {
        ResourceStore store = getStore();
        NavigableSet<String> existings = store.listResources(dictInfo.getResourceDir());
        if (existings == null)
            return null;

        logger.info("{} existing dictionaries of the same column", existings.size());
        if (existings.size() > 100) {
            logger.warn("Too many dictionaries under {}, dict count: {}", dictInfo.getResourceDir(), existings.size());
        }

        for (String existing : existings) {
            try {
                if (existing.endsWith(".dict")) {
                    DictionaryInfo existingInfo = getDictionaryInfo(existing);
                    if (existingInfo != null && dict.equals(existingInfo.getDictionaryObject())) {
                        return existingInfo;
                    }
                }
            } catch (Exception ex) {
                logger.error("Tolerate exception checking dup dictionary " + existing, ex);
            }
        }

        return null;
    }

    private DictionaryInfo updateExistingDictLastModifiedTime(String dictPath) throws IOException {
        ResourceStore store = getStore();
        if (StringUtils.isBlank(dictPath))
            return NONE_INDICATOR;

        int retry = 7;
        while (retry-- > 0) {
            try {
                long now = System.currentTimeMillis();
                store.updateTimestamp(dictPath, now);
                logger.info("Update dictionary {} lastModifiedTime to {}", dictPath, now);
                return loadAndUpdateLocalCache(dictPath);
            } catch (WriteConflictException e) {
                if (retry <= 0) {
                    logger.error("Retry is out, till got error, abandoning...", e);
                    throw e;
                }
                logger.warn("Write conflict to update dictionary " + dictPath + " retry remaining " + retry
                        + ", will retry...");
            }
        }
        return loadAndUpdateLocalCache(dictPath);
    }

    private void initDictInfo(Dictionary<String> newDict, DictionaryInfo newDictInfo) {
        newDictInfo.setCardinality(newDict.getSize());
        newDictInfo.setDictionaryObject(newDict);
        newDictInfo.setDictionaryClass(newDict.getClass().getName());
    }

    private DictionaryInfo saveNewDict(DictionaryInfo newDictInfo) throws IOException {
        save(newDictInfo);
        updateDictCache(newDictInfo);
        return newDictInfo;
    }

    public DictionaryInfo mergeDictionary(List<DictionaryInfo> dicts) throws IOException {

        if (dicts.isEmpty())
            return null;

        if (dicts.size() == 1)
            return dicts.get(0);

        /**
         * AppendTrieDictionary needn't merge
         * more than one AppendTrieDictionary will generate when user use {@link SegmentAppendTrieDictBuilder}
         */
        for (DictionaryInfo dict : dicts) {
            if (dict.getDictionaryClass().equals(AppendTrieDictionary.class.getName())) {
                return dict;
            }
        }

        DictionaryInfo firstDictInfo = null;
        int totalSize = 0;
        for (DictionaryInfo info : dicts) {
            // check
            if (firstDictInfo == null) {
                firstDictInfo = info;
            } else {
                if (!firstDictInfo.isDictOnSameColumn(info)) {
                    // don't throw exception, just output warning as legacy cube segment may build dict on PK
                    logger.warn("Merging dictionaries are not structurally equal : {} and {}",
                            firstDictInfo.getResourcePath(), info.getResourcePath());
                }
            }
            totalSize += info.getInput().getSize();
        }

        if (firstDictInfo == null) {
            throw new IllegalArgumentException("DictionaryManager.mergeDictionary input cannot be null");
        }

        //identical
        DictionaryInfo newDictInfo = new DictionaryInfo(firstDictInfo);
        TableSignature signature = newDictInfo.getInput();
        signature.setSize(totalSize);
        signature.setLastModifiedTime(System.currentTimeMillis());
        signature.setPath("merged_with_no_original_path");

        //check for cases where merging dicts are actually same
        boolean identicalSourceDicts = true;
        for (int i = 1; i < dicts.size(); ++i) {
            if (!dicts.get(0).getDictionaryObject().equals(dicts.get(i).getDictionaryObject())) {
                identicalSourceDicts = false;
                break;
            }
        }

        if (identicalSourceDicts) {
            logger.info("Use one of the merging dictionaries directly");
            return dicts.get(0);
        } else {
            Dictionary<String> newDict = DictionaryGenerator
                    .mergeDictionaries(DataType.getType(newDictInfo.getDataType()), dicts);
            return trySaveNewDict(newDict, newDictInfo);
        }
    }

    public DictionaryInfo buildDictionary(TblColRef col, IReadableTable inpTable) throws IOException {
        return buildDictionary(col, inpTable, null);
    }

    public DictionaryInfo buildDictionary(TblColRef col, IReadableTable inpTable, String builderClass)
            throws IOException {
        if (!inpTable.exists())
            return null;

        logger.info("building dictionary for {}", col);

        DictionaryInfo dictInfo = createDictionaryInfo(col, inpTable);
        String dupInfo = checkDupByInfo(dictInfo);
        if (dupInfo != null) {
            logger.info("Identical dictionary input {}, reuse existing dictionary at {}", dictInfo.getInput(), dupInfo);
            DictionaryInfo dupDictInfo = updateExistingDictLastModifiedTime(dupInfo);
            return dupDictInfo;
        }

        logger.info("Building dictionary object {}", JsonUtil.writeValueAsString(dictInfo));

        Dictionary<String> dictionary;
        dictionary = buildDictFromReadableTable(inpTable, dictInfo, builderClass, col);
        return trySaveNewDict(dictionary, dictInfo);
    }

    private Dictionary<String> buildDictFromReadableTable(IReadableTable inpTable, DictionaryInfo dictInfo,
            String builderClass, TblColRef col) throws IOException {
        Dictionary<String> dictionary;
        IDictionaryValueEnumerator columnValueEnumerator = null;
        try {
            columnValueEnumerator = new TableColumnValueEnumerator(inpTable.getReader(),
                    dictInfo.getSourceColumnIndex());
            if (builderClass == null) {
                dictionary = DictionaryGenerator.buildDictionary(DataType.getType(dictInfo.getDataType()),
                        columnValueEnumerator);
            } else {
                IDictionaryBuilder builder = (IDictionaryBuilder) ClassUtil.newInstance(builderClass);
                dictionary = DictionaryGenerator.buildDictionary(builder, dictInfo, columnValueEnumerator);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create dictionary on " + col, ex);
        } finally {
            if (columnValueEnumerator != null)
                columnValueEnumerator.close();
        }
        return dictionary;
    }

    public DictionaryInfo saveDictionary(TblColRef col, IReadableTable inpTable, Dictionary<String> dictionary)
            throws IOException {
        DictionaryInfo dictInfo = createDictionaryInfo(col, inpTable);
        String dupInfo = checkDupByInfo(dictInfo);
        if (dupInfo != null) {
            logger.info("Identical dictionary input {}, reuse existing dictionary at {}", dictInfo.getInput(), dupInfo);
            DictionaryInfo dupDictInfo = updateExistingDictLastModifiedTime(dupInfo);
            return dupDictInfo;
        }

        return trySaveNewDict(dictionary, dictInfo);
    }

    private DictionaryInfo createDictionaryInfo(TblColRef col, IReadableTable inpTable) throws IOException {
        TableSignature inputSig = inpTable.getSignature();
        if (inputSig == null) // table does not exists
            throw new IllegalStateException("Input table does not exist: " + inpTable);

        DictionaryInfo dictInfo = new DictionaryInfo(col.getColumnDesc(), col.getDatatype(), inputSig);
        return dictInfo;
    }

    private String checkDupByInfo(DictionaryInfo dictInfo) throws IOException {
        final ResourceStore store = getStore();
        final List<DictionaryInfo> allResources = store.getAllResources(dictInfo.getResourceDir(),
                DictionaryInfoSerializer.INFO_SERIALIZER);

        TableSignature input = dictInfo.getInput();

        for (DictionaryInfo dictionaryInfo : allResources) {
            if (input.equals(dictionaryInfo.getInput())) {
                return dictionaryInfo.getResourcePath();
            }
        }
        return null;
    }

    private DictionaryInfo findLargestDictInfo(DictionaryInfo dictInfo) throws IOException {
        final ResourceStore store = getStore();
        final List<DictionaryInfo> allResources = store.getAllResources(dictInfo.getResourceDir(),
                DictionaryInfoSerializer.INFO_SERIALIZER);

        DictionaryInfo largestDict = null;
        for (DictionaryInfo dictionaryInfo : allResources) {
            if (largestDict == null) {
                largestDict = dictionaryInfo;
                continue;
            }

            if (largestDict.getCardinality() < dictionaryInfo.getCardinality()) {
                largestDict = dictionaryInfo;
            }
        }
        return largestDict;
    }

    public void removeDictionary(String resourcePath) throws IOException {
        logger.info("Remvoing dict: {}", resourcePath);
        ResourceStore store = getStore();
        store.deleteResource(resourcePath);
        dictCache.invalidate(resourcePath);
    }

    public void removeDictionaries(String srcTable, String srcCol) throws IOException {
        DictionaryInfo info = new DictionaryInfo();
        info.setSourceTable(srcTable);
        info.setSourceColumn(srcCol);

        ResourceStore store = getStore();
        NavigableSet<String> existings = store.listResources(info.getResourceDir());
        if (existings == null)
            return;

        for (String existing : existings)
            removeDictionary(existing);
    }

    void save(DictionaryInfo dict) throws IOException {
        ResourceStore store = getStore();
        String path = dict.getResourcePath();
        logger.info("Saving dictionary at {}", path);

        store.putBigResource(path, dict, System.currentTimeMillis(), DictionaryInfoSerializer.FULL_SERIALIZER);
    }

    private DictionaryInfo loadAndUpdateLocalCache(String dictPath) throws IOException {
        DictionaryInfo dictInfo = load(dictPath, true);
        updateDictCache(dictInfo);
        return dictInfo;
    }

    DictionaryInfo load(String resourcePath, boolean loadDictObj) throws IOException {
        ResourceStore store = getStore();

        if (loadDictObj) {
            logger.info("Loading dictionary at {}", resourcePath);
        }

        DictionaryInfo info = store.getResource(resourcePath,
                loadDictObj ? DictionaryInfoSerializer.FULL_SERIALIZER : DictionaryInfoSerializer.INFO_SERIALIZER);
        return info;
    }

    private void updateDictCache(DictionaryInfo newDictInfo) {
        dictCache.put(newDictInfo.getResourcePath(), newDictInfo);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(config);
    }

}
