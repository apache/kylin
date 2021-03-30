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

package org.apache.kylin.cube.util;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.measure.map.bitmap.BitmapMapMeasureType;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class KeyValueBuilder implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(KeyValueBuilder.class);

    public static final String HIVE_NULL = "\\N";
    public static final String ZERO = "0";
    public static final String ONE = "1";

    private Set<String> nullStrs;
    private CubeJoinedFlatTableEnrich flatDesc;
    private CubeDesc cubeDesc;
    private String segmentStartTime;

    public KeyValueBuilder(CubeJoinedFlatTableEnrich intermediateTableDesc) {
        flatDesc = intermediateTableDesc;
        cubeDesc = flatDesc.getCubeDesc();
        // flatDesc.getSegment() may be null for testing
        segmentStartTime = flatDesc.getSegment() == null ? "0"
                : getSegmentStartTime((CubeSegment) flatDesc.getSegment());
        logger.info("The start time for segment {} is {}", flatDesc.getSegment(), segmentStartTime);
        initNullStrings();
    }

    private void initNullStrings() {
        nullStrs = Sets.newHashSet();
        nullStrs.add(HIVE_NULL);
        String[] nullStrings = cubeDesc.getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullStrs.add(s);
            }
        }
    }

    public boolean isNull(String v) {
        return nullStrs.contains(v);
    }

    private String getCell(int i, String[] flatRow) {
        if (i >= flatRow.length) {
            return null;
        }

        if (isNull(flatRow[i]))
            return null;
        else
            return flatRow[i];
    }

    public String[] buildKey(String[] row) {
        int keySize = flatDesc.getRowKeyColumnIndexes().length;
        String[] key = new String[keySize];

        for (int i = 0; i < keySize; i++) {
            key[i] = getCell(flatDesc.getRowKeyColumnIndexes()[i], row);
        }

        return key;
    }

    public String[] buildValueOf(int idxOfMeasure, String[] row) {
        MeasureDesc measure = cubeDesc.getMeasures().get(idxOfMeasure);
        FunctionDesc function = measure.getFunction();
        int[] colIdxOnFlatTable = flatDesc.getMeasureColumnIndexes()[idxOfMeasure];

        int paramCount = function.getParameterCount();
        List<String> inputToMeasure = Lists.newArrayListWithExpectedSize(paramCount);

        // pick up parameter values
        ParameterDesc param = function.getParameter();
        int colParamIdx = 0; // index among parameters of column type
        for (int i = 0; i < paramCount; i++, param = param.getNextParameter()) {
            String value;
            if (param.isColumnType()) {
                value = getCell(colIdxOnFlatTable[colParamIdx++], row);
                if (function.isCount() && value == null) {
                    value = ZERO;
                } else if (function.isCount()) {
                    value = ONE;
                }
            } else {
                value = param.getValue();
                if (function.isCount()) {
                    value = ONE;
                }
            }
            inputToMeasure.add(value);
        }
        if (BitmapMapMeasureType.DATATYPE_BITMAP_MAP.equalsIgnoreCase(function.getReturnType())) {
            inputToMeasure.add(segmentStartTime);
        }

        return inputToMeasure.toArray(new String[inputToMeasure.size()]);
    }

    /**
     * Use the segment start time as the map key, the time unit depends on the partition columns
     * If the partition_time_column is null, the unit is day;
     *                            otherwise, the unit is second
     */
    private String getSegmentStartTime(CubeSegment segment) {
        long startTime = segment.getTSRange().start.v;
        DataModelDesc model = segment.getModel();
        PartitionDesc partitionDesc = model.getPartitionDesc();
        if (partitionDesc == null || !partitionDesc.isPartitioned()) {
            return "0";
        } else if (partitionDesc.partitionColumnIsTimeMillis()) {
            return "" + startTime;
        } else if (partitionDesc.getPartitionTimeColumnRef() != null) {
            return "" + startTime / 1000L;
        } else if (partitionDesc.getPartitionDateColumnRef() != null) {
            return "" + startTime / 86400000L;
        }
        return "0";
    }
}
