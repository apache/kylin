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

package org.apache.kylin.storage.gtrecord;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureType.IAdvMeasureFiller;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * convert GTRecord to tuple
 */
public class CubeTupleConverter {

    final CubeSegment cubeSeg;
    final Cuboid cuboid;
    final TupleInfo tupleInfo;
    private final List<IDerivedColumnFiller> derivedColFillers;

    private final int[] gtColIdx;
    private final int[] tupleIdx;
    private final Object[] gtValues;
    private final MeasureType<?>[] measureTypes;

    private final List<IAdvMeasureFiller> advMeasureFillers;
    private final List<Integer> advMeasureIndexInGTValues;

    private final int nSelectedDims;

    private final int[] dimensionIndexOnTuple;

    public CubeTupleConverter(CubeSegment cubeSeg, Cuboid cuboid, //
                              Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo) {
        this.cubeSeg = cubeSeg;
        this.cuboid = cuboid;
        this.tupleInfo = returnTupleInfo;
        this.derivedColFillers = Lists.newArrayList();

        List<TblColRef> cuboidDims = cuboid.getColumns();
        CuboidToGridTableMapping mapping = cuboid.getCuboidToGridTableMapping();

        nSelectedDims = selectedDimensions.size();
        gtColIdx = new int[selectedDimensions.size() + selectedMetrics.size()];
        tupleIdx = new int[selectedDimensions.size() + selectedMetrics.size()];
        gtValues = new Object[selectedDimensions.size() + selectedMetrics.size()];

        // measure types don't have this many, but aligned length make programming easier
        measureTypes = new MeasureType[selectedDimensions.size() + selectedMetrics.size()];

        advMeasureFillers = Lists.newArrayListWithCapacity(1);
        advMeasureIndexInGTValues = Lists.newArrayListWithCapacity(1);

        // dimensionIndexOnTuple is for SQL with limit
        List<Integer> temp = Lists.newArrayList();
        for (TblColRef dim : cuboid.getColumns()) {
            if (tupleInfo.hasColumn(dim)) {
                temp.add(tupleInfo.getColumnIndex(dim));
            }
        }
        dimensionIndexOnTuple = new int[temp.size()];
        for (int i = 0; i < temp.size(); i++) {
            dimensionIndexOnTuple[i] = temp.get(i);
        }
        
        ////////////

        int i = 0;

        // pre-calculate dimension index mapping to tuple
        for (TblColRef dim : selectedDimensions) {
            int dimIndex = mapping.getIndexOf(dim);
            gtColIdx[i] = dimIndex;
            tupleIdx[i] = tupleInfo.hasColumn(dim) ? tupleInfo.getColumnIndex(dim) : -1;

            //            if (tupleIdx[iii] == -1) {
            //                throw new IllegalStateException("dim not used in tuple:" + dim);
            //            }

            i++;
        }

        for (FunctionDesc metric : selectedMetrics) {
            int metricIndex = mapping.getIndexOf(metric);
            gtColIdx[i] = metricIndex;

            if (metric.needRewrite()) {
                String rewriteFieldName = metric.getRewriteFieldName();
                tupleIdx[i] = tupleInfo.hasField(rewriteFieldName) ? tupleInfo.getFieldIndex(rewriteFieldName) : -1;
            } else {
                // a non-rewrite metrics (like sum, or dimension playing as metrics) is like a dimension column
                TblColRef col = metric.getParameter().getColRefs().get(0);
                tupleIdx[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
            }

            MeasureType<?> measureType = metric.getMeasureType();
            if (measureType.needAdvancedTupleFilling()) {
                Map<TblColRef, Dictionary<String>> dictionaryMap = buildDictionaryMap(measureType.getColumnsNeedDictionary(metric));
                advMeasureFillers.add(measureType.getAdvancedTupleFiller(metric, returnTupleInfo, dictionaryMap));
                advMeasureIndexInGTValues.add(i);
            } else {
                measureTypes[i] = measureType;
            }

            i++;
        }

        // prepare derived columns and filler
        Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedInfo = cuboid.getCubeDesc().getHostToDerivedInfo(cuboidDims, null);
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

    public Comparator<ITuple> getTupleDimensionComparator() {
        return new Comparator<ITuple>() {
            @Override
            public int compare(ITuple o1, ITuple o2) {
                Preconditions.checkNotNull(o1);
                Preconditions.checkNotNull(o2);
                for (int i = 0; i < dimensionIndexOnTuple.length; i++) {
                    int index = dimensionIndexOnTuple[i];

                    if (index == -1) {
                        //TODO: 
                        continue;
                    }

                    Comparable a = (Comparable) o1.getAllValues()[index];
                    Comparable b = (Comparable) o2.getAllValues()[index];

                    if (a == null && b == null) {
                        continue;
                    } else if (a == null) {
                        return 1;
                    } else if (b == null) {
                        return -1;
                    } else {
                        int temp = a.compareTo(b);
                        if (temp != 0) {
                            return temp;
                        } else {
                            continue;
                        }
                    }
                }

                return 0;
            }
        };
    }

    // load only needed dictionaries
    private Map<TblColRef, Dictionary<String>> buildDictionaryMap(List<TblColRef> columnsNeedDictionary) {
        Map<TblColRef, Dictionary<String>> result = Maps.newHashMap();
        for (TblColRef col : columnsNeedDictionary) {
            result.put(col, cubeSeg.getDictionary(col));
        }
        return result;
    }

    public List<IAdvMeasureFiller> translateResult(GTRecord record, Tuple tuple) {

        record.getValues(gtColIdx, gtValues);

        // dimensions
        for (int i = 0; i < nSelectedDims; i++) {
            int ti = tupleIdx[i];
            if (ti >= 0) {
                tuple.setDimensionValue(ti, toString(gtValues[i]));
            }
        }

        // measures
        for (int i = nSelectedDims; i < gtColIdx.length; i++) {
            int ti = tupleIdx[i];
            if (ti >= 0 && measureTypes[i] != null) {
                measureTypes[i].fillTupleSimply(tuple, ti, gtValues[i]);
            }
        }

        // derived
        for (IDerivedColumnFiller filler : derivedColFillers) {
            filler.fillDerivedColumns(gtValues, tuple);
        }

        // advanced measure filling, due to possible row split, will complete at caller side
        if (advMeasureFillers.isEmpty()) {
            return null;
        } else {
            for (int i = 0; i < advMeasureFillers.size(); i++) {
                Object measureValue = gtValues[advMeasureIndexInGTValues.get(i)];
                advMeasureFillers.get(i).reload(measureValue);
            }
            return advMeasureFillers;
        }
    }

    private interface IDerivedColumnFiller {
        public void fillDerivedColumns(Object[] gtValues, Tuple tuple);
    }

    private IDerivedColumnFiller newDerivedColumnFiller(TblColRef[] hostCols, final DeriveInfo deriveInfo) {
        boolean allHostsPresent = true;
        final int[] hostTmpIdx = new int[hostCols.length];
        for (int i = 0; i < hostCols.length; i++) {
            hostTmpIdx[i] = indexOnTheGTValues(hostCols[i]);
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
                    public void fillDerivedColumns(Object[] gtValues, Tuple tuple) {
                        for (int i = 0; i < hostTmpIdx.length; i++) {
                            lookupKey.data[i] = CubeTupleConverter.toString(gtValues[hostTmpIdx[i]]);
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
                    public void fillDerivedColumns(Object[] gtValues, Tuple tuple) {
                        // composite keys are split, so only copy [0] is enough, see CubeDesc.initDimensionColumns()
                        tuple.setDimensionValue(derivedTupleIdx[0], CubeTupleConverter.toString(gtValues[hostTmpIdx[0]]));
                    }
                };
            default:
                throw new IllegalArgumentException();
        }
    }

    private int indexOnTheGTValues(TblColRef col) {
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
