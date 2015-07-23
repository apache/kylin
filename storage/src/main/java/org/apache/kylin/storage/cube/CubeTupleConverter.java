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

package org.apache.kylin.storage.cube;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;

import com.google.common.collect.Lists;

public class CubeTupleConverter {

    final CubeSegment cubeSeg;
    final Cuboid cuboid;
    final TupleInfo tupleInfo;
    final List<IDerivedColumnFiller> derivedColFillers;

    final int[] gtColIdx;
    final int[] tupleIdx;
    final Object[] tmpValues;
    final int nSelectedDims;

    public CubeTupleConverter(CubeSegment cubeSeg, Cuboid cuboid, //
            Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo) {
        this.cubeSeg = cubeSeg;
        this.cuboid = cuboid;
        this.tupleInfo = returnTupleInfo;
        this.derivedColFillers = Lists.newArrayList();

        List<TblColRef> cuboidDims = cuboid.getColumns();
        List<MeasureDesc> cuboidMeasures = cuboid.getCube().getMeasures();

        nSelectedDims = selectedDimensions.size();
        gtColIdx = new int[selectedDimensions.size() + selectedMetrics.size()];
        tupleIdx = new int[selectedDimensions.size() + selectedMetrics.size()];
        tmpValues = new Object[selectedDimensions.size() + selectedMetrics.size()];

        int iii = 0;

        // pre-calculate dimension index mapping to tuple
        for (int i = 0; i < cuboidDims.size(); i++) {
            TblColRef col = cuboidDims.get(i);
            if (selectedDimensions.contains(col)) {
                gtColIdx[iii] = i;
                tupleIdx[iii] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
                iii++;
            }
        }

        // pre-calculate metrics index mapping to tuple
        for (int i = 0; i < cuboidMeasures.size(); i++) {
            FunctionDesc aggrFunc = cuboidMeasures.get(i).getFunction();
            if (contains(selectedMetrics, aggrFunc)) {
                gtColIdx[iii] = cuboidDims.size() + i;
                // a rewrite metrics is identified by its rewrite field name
                if (aggrFunc.needRewrite()) {
                    String rewriteFieldName = aggrFunc.getRewriteFieldName();
                    tupleIdx[iii] = tupleInfo.hasField(rewriteFieldName) ? tupleInfo.getFieldIndex(rewriteFieldName) : -1;
                }
                // a non-rewrite metrics (like sum, or dimension playing as metrics) is like a dimension column
                else {
                    TblColRef col = aggrFunc.getParameter().getColRefs().get(0);
                    tupleIdx[iii] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
                }
                iii++;
            }
        }

        // prepare derived columns and filler
        Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedInfo = cuboid.getCube().getHostToDerivedInfo(cuboidDims, null);
        for (Entry<Array<TblColRef>, List<DeriveInfo>> entry : hostToDerivedInfo.entrySet()) {
            TblColRef[] hostCols = entry.getKey().data;
            for (DeriveInfo deriveInfo : entry.getValue()) {
                IDerivedColumnFiller filler = newDerivedColumnFiller(hostCols, deriveInfo);
                if (filler != null) {
                    derivedColFillers.add(filler);
                }
            }
        }
    }

    private boolean contains(Set<FunctionDesc> selectedMetrics, FunctionDesc aggrFunc) {
        if (aggrFunc.isCountDistinct()) {
            // need special attention for count distinct and its holistic version, they are equals() but not exactly the same
            for (FunctionDesc selected : selectedMetrics) {
                if (selected.equals(aggrFunc) && selected.isHolisticCountDistinct() == aggrFunc.isHolisticCountDistinct())
                    return true;
            }
            return false;
        } else
            return selectedMetrics.contains(aggrFunc);
    }

    public void translateResult(GTRecord record, Tuple tuple) {

        record.getValues(gtColIdx, tmpValues);

        // dimensions
        for (int i = 0; i < nSelectedDims; i++) {
            int ti = tupleIdx[i];
            if (ti >= 0) {
                tuple.setDimensionValue(ti, toString(tmpValues[i]));
            }
        }

        // measures
        for (int i = nSelectedDims; i < gtColIdx.length; i++) {
            int ti = tupleIdx[i];
            if (ti >= 0) {
                tuple.setMeasureValue(ti, tmpValues[i]);
            }
        }

        // derived
        for (IDerivedColumnFiller filler : derivedColFillers) {
            filler.fillDerivedColumns(tmpValues, tuple);
        }
    }

    private interface IDerivedColumnFiller {
        public void fillDerivedColumns(Object[] tmpValues, Tuple tuple);
    }

    private IDerivedColumnFiller newDerivedColumnFiller(TblColRef[] hostCols, final DeriveInfo deriveInfo) {
        boolean allHostsPresent = true;
        final int[] hostTmpIdx = new int[hostCols.length];
        for (int i = 0; i < hostCols.length; i++) {
            hostTmpIdx[i] = indexOnTheTmpValues(hostCols[i]);
            allHostsPresent = allHostsPresent && hostTmpIdx[i] >= 0;
        }

        boolean needCopyDerived = false;
        final int[] derivedTupleIdx = new int[deriveInfo.columns.length];
        for (int i = 0; i < deriveInfo.columns.length; i++) {
            TblColRef col = deriveInfo.columns[i];
            derivedTupleIdx[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
            needCopyDerived = needCopyDerived || derivedTupleIdx[i] >= 0;
        }

        if ((allHostsPresent && needCopyDerived) == false)
            return null;

        switch (deriveInfo.type) {
        case LOOKUP:
            return new IDerivedColumnFiller() {
                CubeManager cubeMgr = CubeManager.getInstance(cubeSeg.getCubeInstance().getConfig());
                LookupStringTable lookupTable = cubeMgr.getLookupTable(cubeSeg, deriveInfo.dimension);
                int[] derivedColIdx = initDerivedColIdx();
                Array<String> lookupKey = new Array<String>(new String[hostTmpIdx.length]);

                private int[] initDerivedColIdx() {
                    int[] idx = new int[deriveInfo.columns.length];
                    for (int i = 0; i < idx.length; i++) {
                        idx[i] = deriveInfo.columns[i].getColumnDesc().getZeroBasedIndex();
                    }
                    return idx;
                }

                @Override
                public void fillDerivedColumns(Object[] tmpValues, Tuple tuple) {
                    for (int i = 0; i < hostTmpIdx.length; i++) {
                        lookupKey.data[i] = CubeTupleConverter.toString(tmpValues[hostTmpIdx[i]]);
                    }

                    String[] lookupRow = lookupTable.getRow(lookupKey);

                    if (lookupRow != null) {
                        for (int i = 0; i < derivedTupleIdx.length; i++) {
                            if (derivedTupleIdx[i] >= 0) {
                                String value = lookupRow[derivedColIdx[i]];
                                tuple.setDimensionValue(derivedTupleIdx[i], value);
                            }
                        }
                    } else {
                        for (int i = 0; i < derivedTupleIdx.length; i++) {
                            if (derivedTupleIdx[i] >= 0) {
                                tuple.setDimensionValue(derivedTupleIdx[i], null);
                            }
                        }
                    }
                }
            };
        case PK_FK:
            return new IDerivedColumnFiller() {
                @Override
                public void fillDerivedColumns(Object[] tmpValues, Tuple tuple) {
                    // composite keys are split, so only copy [0] is enough, see CubeDesc.initDimensionColumns()
                    tuple.setDimensionValue(derivedTupleIdx[0], CubeTupleConverter.toString(tmpValues[hostTmpIdx[0]]));
                }
            };
        default:
            throw new IllegalArgumentException();
        }
    }

    private int indexOnTheTmpValues(TblColRef col) {
        List<TblColRef> cuboidDims = cuboid.getColumns();
        int cuboidIdx = cuboidDims.indexOf(col);
        for (int i = 0; i < gtColIdx.length; i++) {
            if (gtColIdx[i] == cuboidIdx)
                return i;
        }
        return -1;
    }

    private static String toString(Object o) {
        return o == null ? null : o.toString();
    }
}
