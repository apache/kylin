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

package org.apache.kylin.storage.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;

public class CubeTupleConverter {

    private final List<IDerivedColumnFiller> derivedColumnFillers;

    public CubeTupleConverter() {
        derivedColumnFillers = new ArrayList<IDerivedColumnFiller>();
    }

    public void addDerivedColumnFiller(IDerivedColumnFiller derivedColumnFiller) {
        derivedColumnFillers.add(derivedColumnFiller);
    }

    public List<IDerivedColumnFiller> getDerivedColumnFillers() {
        return derivedColumnFillers;
    }

    // ============================================================================

    public static IDerivedColumnFiller newDerivedColumnFiller(List<TblColRef> rowColumns, TblColRef[] hostCols, CubeDesc.DeriveInfo deriveInfo, TupleInfo tupleInfo, CubeManager cubeMgr, CubeSegment cubeSegment) {

        int[] hostIndex = new int[hostCols.length];
        for (int i = 0; i < hostCols.length; i++) {
            hostIndex[i] = rowColumns.indexOf(hostCols[i]);
        }
        String[] derivedFieldNames = new String[deriveInfo.columns.length];
        for (int i = 0; i < deriveInfo.columns.length; i++) {
            derivedFieldNames[i] = tupleInfo.getFieldName(deriveInfo.columns[i]);
        }

        switch (deriveInfo.type) {
        case LOOKUP:
            LookupStringTable lookupTable = cubeMgr.getLookupTable(cubeSegment, deriveInfo.dimension);
            return new LookupFiller(hostIndex, lookupTable, deriveInfo, derivedFieldNames);
        case PK_FK:
            // composite key are split, see CubeDesc.initDimensionColumns()
            return new PKFKFiller(hostIndex[0], derivedFieldNames[0]);
        default:
            throw new IllegalArgumentException();
        }
    }

    public interface IDerivedColumnFiller {
        public void fillDerivedColumns(List<String> rowValues, Tuple tuple);
    }

    static class PKFKFiller implements IDerivedColumnFiller {
        final int hostIndex;
        final String derivedFieldName;

        public PKFKFiller(int hostIndex, String derivedFieldName) {
            this.hostIndex = hostIndex;
            this.derivedFieldName = derivedFieldName;
        }

        @Override
        public void fillDerivedColumns(List<String> rowValues, Tuple tuple) {
            String value = rowValues.get(hostIndex);
            tuple.setDimensionValue(derivedFieldName, value);
        }
    }

    static class LookupFiller implements IDerivedColumnFiller {

        final int[] hostIndex;
        final int hostLen;
        final Array<String> lookupKey;
        final LookupStringTable lookupTable;
        final int[] derivedIndex;
        final int derivedLen;
        final String[] derivedFieldNames;

        public LookupFiller(int[] hostIndex, LookupStringTable lookupTable, CubeDesc.DeriveInfo deriveInfo, String[] derivedFieldNames) {
            this.hostIndex = hostIndex;
            this.hostLen = hostIndex.length;
            this.lookupKey = new Array<String>(new String[hostLen]);
            this.lookupTable = lookupTable;
            this.derivedIndex = new int[deriveInfo.columns.length];
            this.derivedLen = derivedIndex.length;
            this.derivedFieldNames = derivedFieldNames;

            for (int i = 0; i < derivedLen; i++) {
                derivedIndex[i] = deriveInfo.columns[i].getColumn().getZeroBasedIndex();
            }
        }

        @Override
        public void fillDerivedColumns(List<String> rowValues, Tuple tuple) {
            for (int i = 0; i < hostLen; i++) {
                lookupKey.data[i] = rowValues.get(hostIndex[i]);
            }

            String[] lookupRow = lookupTable.getRow(lookupKey);

            if (lookupRow != null) {
                for (int i = 0; i < derivedLen; i++) {
                    String value = lookupRow[derivedIndex[i]];
                    tuple.setDimensionValue(derivedFieldNames[i], value);
                }
            } else {
                for (int i = 0; i < derivedLen; i++) {
                    tuple.setDimensionValue(derivedFieldNames[i], null);
                }
            }
        }

    }
}
