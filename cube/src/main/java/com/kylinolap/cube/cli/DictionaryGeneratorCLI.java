/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.cube.cli;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.model.DimensionDesc;
import com.kylinolap.metadata.model.realization.TblColRef;

public class DictionaryGeneratorCLI {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryGeneratorCLI.class);

    public static void processSegment(KylinConfig config, String cubeName, String segmentName, String factColumnsPath) throws IOException {
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeSegment segment = cube.getSegment(segmentName, CubeSegmentStatusEnum.NEW);

        processSegment(config, segment, factColumnsPath);
    }

    private static void processSegment(KylinConfig config, CubeSegment cubeSeg, String factColumnsPath) throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(config);

        for (DimensionDesc dim : cubeSeg.getCubeDesc().getDimensions()) {
            // dictionary
            for (TblColRef col : dim.getColumnRefs()) {
                if (cubeSeg.getCubeDesc().getRowkey().isUseDictionary(col)) {
                    logger.info("Building dictionary for " + col);
                    cubeMgr.buildDictionary(cubeSeg, col, factColumnsPath);
                }
            }

            // build snapshot
            if (dim.getTable() != null && !dim.getTable().equalsIgnoreCase(cubeSeg.getCubeDesc().getFactTable())) {
                // CubeSegment seg = cube.getTheOnlySegment();
                logger.info("Building snapshot of " + dim.getTable());
                cubeMgr.buildSnapshotTable(cubeSeg, dim.getTable());
                logger.info("Checking snapshot of " + dim.getTable());
                cubeMgr.getLookupTable(cubeSeg, dim); // load the table for
                                                      // sanity check
            }
        }
    }

}
