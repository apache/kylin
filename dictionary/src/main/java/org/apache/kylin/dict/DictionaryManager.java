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

import com.google.common.base.Preconditions;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.dict.lookup.FileTable;
import org.apache.kylin.dict.lookup.HiveTable;
import org.apache.kylin.dict.lookup.ReadableTable;
import org.apache.kylin.dict.lookup.TableSignature;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
    private ConcurrentHashMap<String, DictionaryInfo> dictCache; // resource

    // path ==>
    // DictionaryInfo

    private DictionaryManager(KylinConfig config) {
        this.config = config;
        dictCache = new ConcurrentHashMap<String, DictionaryInfo>();
    }

    public Dictionary<?> getDictionary(String resourcePath) throws IOException {
        DictionaryInfo dictInfo = getDictionaryInfo(resourcePath);
        return dictInfo == null ? null : dictInfo.getDictionaryObject();
    }

    public DictionaryInfo getDictionaryInfo(String resourcePath) throws IOException {
        DictionaryInfo dictInfo = dictCache.get(resourcePath);
        if (dictInfo == null) {
            dictInfo = load(resourcePath, true);
            if (dictInfo == null)
                dictInfo = NONE_INDICATOR;
            dictCache.put(resourcePath, dictInfo);
        }
        return dictInfo == NONE_INDICATOR ? null : dictInfo;
    }

    public DictionaryInfo trySaveNewDict(Dictionary<?> newDict, DictionaryInfo newDictInfo) throws IOException {

        String dupDict = checkDupByContent(newDictInfo, newDict);
        if (dupDict != null) {
            logger.info("Identical dictionary content " + newDict + ", reuse existing dictionary at " + dupDict);
            return getDictionaryInfo(dupDict);
        }

        newDictInfo.setDictionaryObject(newDict);
        newDictInfo.setDictionaryClass(newDict.getClass().getName());

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

        String dupDict = checkDupByInfo(newDictInfo);
        if (dupDict != null) {
            logger.info("Identical dictionary input " + newDictInfo.getInput() + ", reuse existing dictionary at " + dupDict);
            return getDictionaryInfo(dupDict);
        }

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
            Dictionary<?> newDict = DictionaryGenerator.mergeDictionaries(DataType.getInstance(newDictInfo.getDataType()), dicts);
            newDictInfo.setCardinality(newDict.getSize());
            return trySaveNewDict(newDict, newDictInfo);
        }
    }

    public DictionaryInfo buildDictionary(DataModelDesc model, String dict, TblColRef col, String factColumnsPath) throws IOException {

        logger.info("building dictionary for " + col);

        Object[] tmp = decideSourceData(model, dict, col, factColumnsPath);
        String srcTable = (String) tmp[0];
        String srcCol = (String) tmp[1];
        int srcColIdx = (Integer) tmp[2];
        ReadableTable inpTable = (ReadableTable) tmp[3];

        if (!inpTable.exists())
            return null;

        DictionaryInfo dictInfo = new DictionaryInfo(srcTable, srcCol, srcColIdx, col.getDatatype(), inpTable.getSignature(), inpTable.getColumnDelimeter());

        String dupDict = checkDupByInfo(dictInfo);
        if (dupDict != null) {
            logger.info("Identical dictionary input " + dictInfo.getInput() + ", reuse existing dictionary at " + dupDict);
            return getDictionaryInfo(dupDict);
        }

        Dictionary<?> dictionary = DictionaryGenerator.buildDictionary(dictInfo, inpTable);
        dictInfo.setCardinality(dictionary.getSize());

        return trySaveNewDict(dictionary, dictInfo);
    }

    /**
     * Get column origin
     *
     * @return 1. source table name
     * 2. column name
     * 3. column cardinal in source table
     * 4. ReadableTable object
     */

    public Object[] decideSourceData(DataModelDesc model, String dict, TblColRef col, String factColumnsPath) throws IOException {
        String srcTable;
        String srcCol;
        int srcColIdx;
        ReadableTable table;
        MetadataManager metaMgr = MetadataManager.getInstance(config);

        // case of full table (dict on fact table)
        if (model == null) {
            srcTable = col.getTable();
            srcCol = col.getName();
            srcColIdx = col.getColumnDesc().getZeroBasedIndex();
            int nColumns = metaMgr.getTableDesc(col.getTable()).getColumnCount();
            table = new FileTable(factColumnsPath + "/" + col.getName(), nColumns);
            return new Object[] { srcTable, srcCol, srcColIdx, table };
        }

        // Decide source data of dictionary:
        // 1. If 'useDict' specifies pre-defined data set, use that
        // 2. Otherwise find a lookup table to scan through

        // Note FK on fact table is supported by scan the related PK on lookup table

        //String useDict = cube.getRowkey().getDictionary(col);

        // normal case, source from lookup table
        if ("true".equals(dict) || "string".equals(dict) || "number".equals(dict) || "any".equals(dict)) {
            // FK on fact table and join type is inner, use PK from lookup instead
            if (model.isFactTable(col.getTable())) {
                TblColRef pkCol = model.findPKByFK(col, "inner");
                if (pkCol != null)
                    col = pkCol; // scan the counterparty PK on lookup table
                // instead
            }
            srcTable = col.getTable();
            srcCol = col.getName();
            srcColIdx = col.getColumnDesc().getZeroBasedIndex();
            if (model.isFactTable(col.getTable())) {
                table = new FileTable(factColumnsPath + "/" + col.getName(), -1);
            } else {
                table = new HiveTable(metaMgr, col.getTable());
            }
        }
        // otherwise could refer to a data set, e.g. common_indicators.txt
        // (LEGACY PATH, since distinct values are collected from fact table)
        else {
            String dictDataSetPath = unpackDataSet(this.config.getTempHDFSDir(), dict);
            if (dictDataSetPath == null)
                throw new IllegalArgumentException("Unknown dictionary data set '" + dict + "', referred from " + col);
            srcTable = "PREDEFINED";
            srcCol = dict;
            srcColIdx = 0;
            table = new FileTable(dictDataSetPath, -1);
        }

        return new Object[] { srcTable, srcCol, srcColIdx, table };
    }

    private String unpackDataSet(String tempHDFSDir, String dataSetName) throws IOException {

        InputStream in = this.getClass().getResourceAsStream("/org/apache/kylin/dict/" + dataSetName + ".txt");
        if (in == null) // data set resource not found
            return null;

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        IOUtils.copy(in, buf);
        in.close();
        byte[] bytes = buf.toByteArray();

        Path tmpDataSetPath = new Path(tempHDFSDir + "/dict/temp_dataset/" + dataSetName + "_" + bytes.length + ".txt");

        FileSystem fs = HadoopUtil.getFileSystem(tempHDFSDir);
        boolean writtenNewFile = false;
        if (fs.exists(tmpDataSetPath) == false || fs.getFileStatus(tmpDataSetPath).getLen() != bytes.length) {
            fs.mkdirs(tmpDataSetPath.getParent());
            FSDataOutputStream out = fs.create(tmpDataSetPath);
            IOUtils.copy(new ByteArrayInputStream(bytes), out);
            out.close();
            writtenNewFile = true;
        }

        String qualifiedPath = tmpDataSetPath.makeQualified(fs.getUri(), new Path("/")).toString();
        if (writtenNewFile)
            logger.info("Dictionary temp data set file written to " + qualifiedPath);
        return qualifiedPath;
    }

    private String checkDupByInfo(DictionaryInfo dictInfo) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        ArrayList<String> existings = store.listResources(dictInfo.getResourceDir());
        if (existings == null || existings.isEmpty()) {
            return null;
        }
        Collections.sort(existings);

        final List<DictionaryInfo> allResources = MetadataManager.getInstance(config).getStore().getAllResources(existings.get(0),
                existings.get(existings.size() - 1),
                DictionaryInfo.class,
                DictionaryInfoSerializer.INFO_SERIALIZER);

        TableSignature input = dictInfo.getInput();

        for (DictionaryInfo dictionaryInfo : allResources) {
            if (input.equals(dictionaryInfo.getInput())) {
                return dictionaryInfo.getResourcePath();
            }
        }
        return null;
    }

    private String checkDupByContent(DictionaryInfo dictInfo, Dictionary<?> dict) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        ArrayList<String> dictInfos = store.listResources(dictInfo.getResourceDir());
        if (dictInfos == null || dictInfos.isEmpty()) {
            return null;
        }
        Collections.sort(dictInfos);

        final List<DictionaryInfo> allResources = MetadataManager.getInstance(config).getStore().getAllResources(dictInfos.get(0),
                dictInfos.get(dictInfos.size() - 1),
                DictionaryInfo.class,
                DictionaryInfoSerializer.FULL_SERIALIZER);

        for (DictionaryInfo dictionaryInfo : allResources) {
            if (dict.equals(dictionaryInfo.getDictionaryObject())) {
                return dictionaryInfo.getResourcePath();
            }
        }
        return null;
    }

    public void removeDictionary(String resourcePath) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();
        store.deleteResource(resourcePath);
        dictCache.remove(resourcePath);
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
        store.putResource(path, dict, DictionaryInfoSerializer.FULL_SERIALIZER);
    }

    DictionaryInfo load(String resourcePath, boolean loadDictObj) throws IOException {
        ResourceStore store = MetadataManager.getInstance(config).getStore();

        logger.debug("Going to load DictionaryInfo from " + resourcePath);
        DictionaryInfo info = store.getResource(resourcePath, DictionaryInfo.class, loadDictObj ? DictionaryInfoSerializer.FULL_SERIALIZER : DictionaryInfoSerializer.INFO_SERIALIZER);

        if (loadDictObj)
            logger.debug("Loaded dictionary at " + resourcePath);

        return info;
    }

}
