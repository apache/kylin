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
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.DictionaryProvider;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class DictionaryGeneratorCLI {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryGeneratorCLI.class);

    public static void processSegment(KylinConfig config, String cubeName, String segmentID, DistinctColumnValuesProvider factTableValueProvider, DictionaryProvider dictProvider) throws IOException {
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeSegment segment = cube.getSegmentById(segmentID);

        processSegment(config, segment, factTableValueProvider, dictProvider);
    }

    private static void processSegment(KylinConfig config, CubeSegment cubeSeg, DistinctColumnValuesProvider factTableValueProvider, DictionaryProvider dictProvider) throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(config);

        // dictionary
        for (TblColRef col : cubeSeg.getCubeDesc().getAllColumnsNeedDictionaryBuilt()) {
            logger.info("Building dictionary for " + col);
            IReadableTable inpTable = factTableValueProvider.getDistinctValuesFor(col);
            
            Dictionary<String> preBuiltDict = null;
            if (dictProvider != null) {
                preBuiltDict = dictProvider.getDictionary(col);
            }
        
            if (preBuiltDict != null) {
                logger.debug("Dict for '" + col.getName() + "' has already been built, save it");
                cubeMgr.saveDictionary(cubeSeg, col, inpTable, preBuiltDict);
            } else {
                logger.debug("Dict for '" + col.getName() + "' not pre-built, build it from " + inpTable.toString());
                cubeMgr.buildDictionary(cubeSeg, col, inpTable);
            }
        }

        // snapshot
        Set<String> toSnapshot = Sets.newHashSet();
        Set<TableRef> toCheckLookup = Sets.newHashSet();
        for (DimensionDesc dim : cubeSeg.getCubeDesc().getDimensions()) {
            TableRef table = dim.getTableRef();
            if (cubeSeg.getModel().isLookupTable(table)) {
                toSnapshot.add(table.getTableIdentity());
                toCheckLookup.add(table);
            }
        }

        for (String tableIdentity : toSnapshot) {
            logger.info("Building snapshot of " + tableIdentity);
            cubeMgr.buildSnapshotTable(cubeSeg, tableIdentity);
        }
        
        CubeInstance updatedCube = cubeMgr.getCube(cubeSeg.getCubeInstance().getName());
        cubeSeg = updatedCube.getSegmentById(cubeSeg.getUuid());
        for (TableRef lookup : toCheckLookup) {
            logger.info("Checking snapshot of " + lookup);
            JoinDesc join = cubeSeg.getModel().getJoinsTree().getJoinByPKSide(lookup);
            cubeMgr.getLookupTable(cubeSeg, join);
        }
    }

}
