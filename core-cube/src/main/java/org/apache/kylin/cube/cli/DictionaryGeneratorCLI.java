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
import java.util.Set;

import org.apache.hadoop.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.DictionaryProvider;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class DictionaryGeneratorCLI {

    private DictionaryGeneratorCLI(){}

    private static final Logger logger = LoggerFactory.getLogger(DictionaryGeneratorCLI.class);

    public static void processSegment(KylinConfig config, String cubeName, String segmentID, String uuid,
            DistinctColumnValuesProvider factTableValueProvider, DictionaryProvider dictProvider) throws IOException {
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeSegment segment = cube.getSegmentById(segmentID);

        processSegment(config, segment, uuid, factTableValueProvider, dictProvider);
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

}
