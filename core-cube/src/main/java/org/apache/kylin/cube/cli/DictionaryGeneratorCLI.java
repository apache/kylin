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

package org.apache.kylin.cube.cli;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.dict.DictionaryProvider;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.dict.lookup.SnapshotTableSerializer;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class DictionaryGeneratorCLI {

    private DictionaryGeneratorCLI() {
    }

    private static final Logger logger = LoggerFactory.getLogger(DictionaryGeneratorCLI.class);

    public static void processSegment(KylinConfig config, String cubeName, String segmentID, String uuid,
            DistinctColumnValuesProvider factTableValueProvider, DictionaryProvider dictProvider) throws IOException {
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeSegment segment = cube.getSegmentById(segmentID);

        int retryTime = 0;
        while (retryTime < 3) {
            if (retryTime > 0) {
                logger.info("Rebuild dictionary and snapshot for Cube: {}, Segment: {}, {} times.", cubeName, segmentID,
                        retryTime);
            }

            processSegment(config, segment, uuid, factTableValueProvider, dictProvider);

            if (isAllDictsAndSnapshotsReady(config, cubeName, segmentID)) {
                break;
            }
            retryTime++;
        }

        if (retryTime >= 3) {
            logger.error("Not all dictionaries and snapshots ready for cube segment: {}", segmentID);
        } else {
            logger.info("Succeed to build all dictionaries and snapshots for cube segment: {}", segmentID);
        }
    }

    private static void processSegment(KylinConfig config, CubeSegment cubeSeg, String uuid,
            DistinctColumnValuesProvider factTableValueProvider, DictionaryProvider dictProvider) throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(config);

        // dictionary
        for (TblColRef col : cubeSeg.getCubeDesc().getAllColumnsNeedDictionaryBuilt()) {
            logger.info("Building dictionary for {}", col);
            IReadableTable inpTable = factTableValueProvider.getDistinctValuesFor(col);

            Dictionary<String> preBuiltDict = null;
            if (dictProvider != null) {
                preBuiltDict = dictProvider.getDictionary(col);
            }

            if (preBuiltDict != null) {
                logger.debug("Dict for '{}' has already been built, save it", col.getName());
                cubeMgr.saveDictionary(cubeSeg, col, inpTable, preBuiltDict);
            } else {
                logger.debug("Dict for '{}' not pre-built, build it from {}", col.getName(), inpTable);
                cubeMgr.buildDictionary(cubeSeg, col, inpTable);
            }
        }

        // snapshot
        Set<String> toSnapshot = Sets.newHashSet();
        Set<TableRef> toCheckLookup = Sets.newHashSet();
        for (DimensionDesc dim : cubeSeg.getCubeDesc().getDimensions()) {
            TableRef table = dim.getTableRef();
            if (cubeSeg.getModel().isLookupTable(table)) {
                // only the snapshot desc is not ext type, need to take snapshot
                if (!cubeSeg.getCubeDesc().isExtSnapshotTable(table.getTableIdentity())) {
                    toSnapshot.add(table.getTableIdentity());
                    toCheckLookup.add(table);
                }
            }
        }

        for (String tableIdentity : toSnapshot) {
            logger.info("Building snapshot of {}", tableIdentity);
            cubeMgr.buildSnapshotTable(cubeSeg, tableIdentity, uuid);
        }

        CubeInstance updatedCube = cubeMgr.getCube(cubeSeg.getCubeInstance().getName());
        cubeSeg = updatedCube.getSegmentById(cubeSeg.getUuid());
        for (TableRef lookup : toCheckLookup) {
            logger.info("Checking snapshot of {}", lookup);
            try {
                JoinDesc join = cubeSeg.getModel().getJoinsTree().getJoinByPKSide(lookup);
                ILookupTable table = cubeMgr.getLookupTable(cubeSeg, join);
                if (table != null) {
                    IOUtils.closeStream(table);
                }
            } catch (Throwable th) {
                throw new RuntimeException(String.format(Locale.ROOT, "Checking snapshot of %s failed.", lookup), th);
            }
        }
    }

    private static boolean isAllDictsAndSnapshotsReady(KylinConfig config, String cubeName, String segmentID) {
        CubeInstance cube = CubeManager.getInstance(config).reloadCube(cubeName);
        CubeSegment segment = cube.getSegmentById(segmentID);
        ResourceStore store = ResourceStore.getStore(config);

        // check dicts
        logger.info("Begin to check if all dictionaries exist of Segment: {}", segmentID);
        Map<String, String> dictionaries = segment.getDictionaries();
        for (Map.Entry<String, String> entry : dictionaries.entrySet()) {
            String dictResPath = entry.getValue();
            String dictKey = entry.getKey();
            try {
                DictionaryInfo dictInfo = store.getResource(dictResPath, DictionaryInfoSerializer.INFO_SERIALIZER);
                if (dictInfo == null) {
                    logger.warn("Dictionary=[key: {}, resource path: {}] doesn't exist in resource store", dictKey,
                            dictResPath);
                    return false;
                }
            } catch (IOException e) {
                logger.warn("Dictionary=[key: {}, path: {}] failed to check, details: {}", dictKey, dictResPath, e);
                return false;
            }
        }

        // check snapshots
        logger.info("Begin to check if all snapshots exist of Segment: {}", segmentID);
        Map<String, String> snapshots = segment.getSnapshots();
        for (Map.Entry<String, String> entry : snapshots.entrySet()) {
            String snapshotKey = entry.getKey();
            String snapshotResPath = entry.getValue();
            try {
                SnapshotTable snapshot = store.getResource(snapshotResPath, SnapshotTableSerializer.INFO_SERIALIZER);
                if (snapshot == null) {
                    logger.info("SnapshotTable=[key: {}, resource path: {}] doesn't exist in resource store",
                            snapshotKey, snapshotResPath);
                    return false;
                }
            } catch (IOException e) {
                logger.warn("SnapshotTable=[key: {}, resource path: {}]  failed to check, details: {}", snapshotKey,
                        snapshotResPath, e);
                return false;
            }
        }

        logger.info("All dictionaries and snapshots exist checking succeed for Cube Segment: {}", segmentID);
        return true;
    }
}
