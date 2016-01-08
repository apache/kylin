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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.ReadableTable.TableSignature;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

public class DictionaryManager {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryManager.class);

    private static final DictionaryInfo NONE_INDICATOR = new DictionaryInfo();

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, DictionaryManager> CACHE = new ConcurrentHashMap<KylinConfig, DictionaryManager>();

    public static DictionaryManager getInstance(KylinConfig config) {
        DictionaryManager r = CACHE.get(config);
        if (r == null) {
            r = new DictionaryManager(config);
            CACHE.put(config, r);
            if (CACHE.size() > 1) {
                logger.warn("More than one singleton exist");
            }
        }
        return r;
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    private LoadingCache<String, DictionaryInfo> dictCache; // resource

    // path ==>
    // DictionaryInfo

    private DictionaryManager(KylinConfig config) {
        this.config = config;
        this.dictCache = CacheBuilder.newBuilder().maximumSize(KylinConfig.getInstanceFromEnv().getCachedDictMaxEntrySize())//
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

    public Dictionary<?> getDictionary(String resourcePath) throws IOException {
        DictionaryInfo dictInfo = getDictionaryInfo(resourcePath);
        return dictInfo == null ? null : dictInfo.getDictionaryObject();
    }

    public DictionaryInfo getDictionaryInfo(final String resourcePath) throws IOException {
        try {
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
    public DictionaryInfo forceSave(Dictionary<?> newDict, DictionaryInfo newDictInfo) throws IOException {
        initDictInfo(newDict, newDictInfo);
        logger.info("force to save dict directly");
        return saveNewDict(newDictInfo);
    }

    /**
     * @return may return another dict that is a super set of the input
     * @throws IOException
     */
    public DictionaryInfo trySaveNewDict(Dictionary<?> newDict, DictionaryInfo newDictInfo) throws IOException {

        initDictInfo(newDict, newDictInfo);

        DictionaryInfo largestDictInfo = findLargestDictInfo(newDictInfo);
        if (largestDictInfo != null) {
            largestDictInfo = getDictionaryInfo(largestDictInfo.getResourcePath());
            Dictionary<?> largestDictObject = largestDictInfo.getDictionaryObject();
            if (largestDictObject.contains(newDict)) {
                logger.info("dictionary content " + newDict + ", is contained by  dictionary at " + largestDictInfo.getResourcePath());
                return largestDictInfo;
            } else if (newDict.contains(largestDictObject)) {
                logger.info("dictionary content " + newDict + " is by far the largest, save it");
                return saveNewDict(newDictInfo);
            } else {
                logger.info("merge dict and save...");
                return mergeDictionary(Lists.newArrayList(newDictInfo, largestDictInfo));
            }
        } else {
            logger.info("first dict of this column, save it directly");
            return saveNewDict(newDictInfo);
        }
    }

    private void initDictInfo(Dictionary<?> newDict, DictionaryInfo newDictInfo) {
        newDictInfo.setCardinality(newDict.getSize());
        newDictInfo.setDictionaryObject(newDict);
        newDictInfo.setDictionaryClass(newDict.getClass().getName());
    }

    private DictionaryInfo saveNewDict(DictionaryInfo newDictInfo) throws IOException {

        save(newDictInfo);
        dictCache.put(newDictInfo.getResourcePath(), newDictInfo);

        return newDictInfo;
    }

    public DictionaryInfo mergeDictionary(List<DictionaryInfo> dicts) throws IOException {

        if (dicts.size() == 0)
            return null;

        if (dicts.size() == 1)
            return dicts.get(0);

        DictionaryInfo firstDictInfo = null;
        int totalSize = 0;
        for (DictionaryInfo info : dicts) {
            // check
            if (firstDictInfo == null) {
                firstDictInfo = info;
            } else {
                if (!firstDictInfo.isDictOnSameColumn(info)) {
                    // don't throw exception, just output warning as legacy cube segment may build dict on PK
                    logger.warn("Merging dictionaries are not structurally equal : " + firstDictInfo.getResourcePath() + " and " + info.getResourcePath());
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

        //        String dupDict = checkDupByInfo(newDictInfo);
        //        if (dupDict != null) {
        //            logger.info("Identical dictionary input " + newDictInfo.getInput() + ", reuse existing dictionary at " + dupDict);
        //            return getDictionaryInfo(dupDict);
        //        }

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
            Dictionary<?> newDict = DictionaryGenerator.mergeDictionaries(DataType.getType(newDictInfo.getDataType()), dicts);
            return trySaveNewDict(newDict, newDictInfo);
        }
    }

    public DictionaryInfo buildDictionary(DataModelDesc model, boolean usingDict, TblColRef col, DistinctColumnValuesProvider factTableValueProvider) throws IOException {

        logger.info("building dictionary for " + col);

        TblColRef srcCol = decideSourceData(model, usingDict, col);
        String srcTable = srcCol.getTable();
        String srcColName = srcCol.getName();
        int srcColIdx = srcCol.getColumnDesc().getZeroBasedIndex();

        ReadableTable inpTable;
        if (model.isFactTable(srcTable)) {
            inpTable = factTableValueProvider.getDistinctValuesFor(srcCol);
        } else {
            TableDesc tableDesc = MetadataManager.getInstance(config).getTableDesc(srcTable);
            inpTable = SourceFactory.createReadableTable(tableDesc);
        }

        TableSignature inputSig = inpTable.getSignature();
        if (inputSig == null) // table does not exists
            return null;

        DictionaryInfo dictInfo = new DictionaryInfo(srcTable, srcColName, srcColIdx, col.getDatatype(), inputSig);

        String dupDict = checkDupByInfo(dictInfo);
        if (dupDict != null) {
            logger.info("Identical dictionary input " + dictInfo.getInput() + ", reuse existing dictionary at " + dupDict);
            return getDictionaryInfo(dupDict);
        }

        Dictionary<?> dictionary = DictionaryGenerator.buildDictionary(dictInfo, inpTable);
        return trySaveNewDict(dictionary, dictInfo);
    }

    /**
     * Decide a dictionary's source data, leverage PK-FK relationship.
     */
    public TblColRef decideSourceData(DataModelDesc model, boolean usingDict, TblColRef col) throws IOException {
        // Note FK on fact table is supported by scan the related PK on lookup table
        if (usingDict) {
            // FK on fact table and join type is inner, use PK from lookup instead
            if (model.isFactTable(col.getTable())) {
                TblColRef pkCol = model.findPKByFK(col, "inner");
                if (pkCol != null)
                    col = pkCol; // scan the counterparty PK on lookup table instead
            }
            return col;
        } else
            throw new IllegalArgumentException("Not using Dictionary ");
    }

    private String checkDupByInfo(DictionaryInfo dictInfo) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        ArrayList<String> existings = store.listResources(dictInfo.getResourceDir());
        if (existings == null || existings.isEmpty()) {
            return null;
        }
        Collections.sort(existings);

        final List<DictionaryInfo> allResources = MetadataManager.getInstance(config).getStore().getAllResources(existings.get(0), existings.get(existings.size() - 1), DictionaryInfo.class, DictionaryInfoSerializer.INFO_SERIALIZER);

        TableSignature input = dictInfo.getInput();

        for (DictionaryInfo dictionaryInfo : allResources) {
            if (input.equals(dictionaryInfo.getInput())) {
                return dictionaryInfo.getResourcePath();
            }
        }
        return null;
    }

    private DictionaryInfo findLargestDictInfo(DictionaryInfo dictInfo) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        ArrayList<String> dictInfos = store.listResources(dictInfo.getResourceDir());
        if (dictInfos == null || dictInfos.isEmpty()) {
            return null;
        }
        Collections.sort(dictInfos);

        final List<DictionaryInfo> allResources = MetadataManager.getInstance(config).getStore().getAllResources(dictInfos.get(0), dictInfos.get(dictInfos.size() - 1), DictionaryInfo.class, DictionaryInfoSerializer.INFO_SERIALIZER);

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
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        store.deleteResource(resourcePath);
        dictCache.invalidate(resourcePath);
    }

    public void removeDictionaries(String srcTable, String srcCol) throws IOException {
        DictionaryInfo info = new DictionaryInfo();
        info.setSourceTable(srcTable);
        info.setSourceColumn(srcCol);

        ResourceStore store = MetadataManager.getInstance(config).getStore();
        ArrayList<String> existings = store.listResources(info.getResourceDir());
        if (existings == null)
            return;

        for (String existing : existings)
            removeDictionary(existing);
    }

    void save(DictionaryInfo dict) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        String path = dict.getResourcePath();
        logger.info("Saving dictionary at " + path);

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        DictionaryInfoSerializer.FULL_SERIALIZER.serialize(dict, dout);
        dout.close();
        buf.close();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf.toByteArray());
        store.putResource(path, inputStream, System.currentTimeMillis());
        inputStream.close();
    }

    DictionaryInfo load(String resourcePath, boolean loadDictObj) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();

        logger.debug("Loading DictionaryInfo(loadDictObj:" + loadDictObj + ") from " + resourcePath);
        DictionaryInfo info = store.getResource(resourcePath, DictionaryInfo.class, loadDictObj ? DictionaryInfoSerializer.FULL_SERIALIZER : DictionaryInfoSerializer.INFO_SERIALIZER);

        //        if (loadDictObj)
        //            logger.debug("Loaded dictionary at " + resourcePath);

        return info;
    }

}
