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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureType.IAdvMeasureFiller;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.source.ReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Convert Object[] (decoded GTRecord) to tuple
 */
public class CubeTupleConverter implements ITupleConverter {

    private static final Logger logger = LoggerFactory.getLogger(CubeTupleConverter.class);

    final CubeSegment cubeSeg;
    final Cuboid cuboid;
    final TupleInfo tupleInfo;
    private final List<IDerivedColumnFiller> derivedColFillers;

    private final int[] gtColIdx;
    private final int[] tupleIdx;
    private final MeasureType<?>[] measureTypes;

    private final List<IAdvMeasureFiller> advMeasureFillers;
    private final List<Integer> advMeasureIndexInGTValues;

    private final int nSelectedDims;

    public CubeTupleConverter(CubeSegment cubeSeg, Cuboid cuboid, //
            Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics, int[] gtColIdx, TupleInfo returnTupleInfo) {
        this.cubeSeg = cubeSeg;
        this.cuboid = cuboid;
        this.gtColIdx = gtColIdx;
        this.tupleInfo = returnTupleInfo;
        this.derivedColFillers = Lists.newArrayList();

        nSelectedDims = selectedDimensions.size();
        tupleIdx = new int[selectedDimensions.size() + selectedMetrics.size()];

        // measure types don't have this many, but aligned length make programming easier
        measureTypes = new MeasureType[selectedDimensions.size() + selectedMetrics.size()];

        advMeasureFillers = Lists.newArrayListWithCapacity(1);
        advMeasureIndexInGTValues = Lists.newArrayListWithCapacity(1);

        ////////////

        int i = 0;

        // pre-calculate dimension index mapping to tuple
        for (TblColRef dim : selectedDimensions) {
            tupleIdx[i] = tupleInfo.hasColumn(dim) ? tupleInfo.getColumnIndex(dim) : -1;
            i++;
        }

        for (FunctionDesc metric : selectedMetrics) {
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
        Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedInfo = cuboid.getCubeDesc().getHostToDerivedInfo(cuboid.getColumns(), null);
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

    // load only needed dictionaries
    private Map<TblColRef, Dictionary<String>> buildDictionaryMap(List<TblColRef> columnsNeedDictionary) {
        Map<TblColRef, Dictionary<String>> result = Maps.newHashMap();
        for (TblColRef col : columnsNeedDictionary) {
            result.put(col, cubeSeg.getDictionary(col));
        }
        return result;
    }

    @Override
    public List<IAdvMeasureFiller> translateResult(Object[] gtValues, Tuple tuple) {
        assert gtValues.length == gtColIdx.length;

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
                LookupStringTable lookupTable = getLookupTable(cubeSeg, deriveInfo.join);
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

    public LookupStringTable getLookupTable(CubeSegment cubeSegment, JoinDesc join) {
        long ts = System.currentTimeMillis();

        MetadataManager metaMgr = MetadataManager.getInstance(cubeSeg.getCubeInstance().getConfig());
        SnapshotManager snapshotMgr = SnapshotManager.getInstance(cubeSeg.getCubeInstance().getConfig());

        String tableName = join.getPKSide().getTableIdentity();
        String[] pkCols = join.getPrimaryKey();
        String snapshotResPath = cubeSegment.getSnapshotResPath(tableName);
        if (snapshotResPath == null)
            throw new IllegalStateException("No snaphot for table '" + tableName + "' found on cube segment" + cubeSegment.getCubeInstance().getName() + "/" + cubeSegment);

        try {
            SnapshotTable snapshot = snapshotMgr.getSnapshotTable(snapshotResPath);
            TableDesc tableDesc = metaMgr.getTableDesc(tableName);
            EnhancedStringLookupTable enhancedStringLookupTable = new EnhancedStringLookupTable(tableDesc, pkCols, snapshot);
            logger.info("Time to get lookup up table for {} is {} ", join.getPKSide().getTableName(), (System.currentTimeMillis() - ts));
            return enhancedStringLookupTable;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load lookup table " + tableName + " from snapshot " + snapshotResPath, e);
        }
    }

    private static class EnhancedStringLookupTable extends LookupStringTable {

        public EnhancedStringLookupTable(TableDesc tableDesc, String[] keyColumns, ReadableTable table) throws IOException {
            super(tableDesc, keyColumns, table);
        }

        @Override
        protected void init() throws IOException {
            this.data = new HashMap<>();
            super.init();
        }
    }

    private static String toString(Object o) {
        return o == null ? null : o.toString();
    }
}
