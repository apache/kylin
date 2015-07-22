package org.apache.kylin.storage.hbase;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
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
    final RowKeyDecoder rowKeyDecoder;
    final List<RowValueDecoder> rowValueDecoders;
    final List<IDerivedColumnFiller> derivedColFillers;

    final int[] dimensionTupleIdx;
    final int[][] metricsMeasureIdx;
    final int[][] metricsTupleIdx;

    public CubeTupleConverter(CubeSegment cubeSeg, Cuboid cuboid, List<RowValueDecoder> rowValueDecoders, TupleInfo tupleInfo) {
        this.cubeSeg = cubeSeg;
        this.cuboid = cuboid;
        this.tupleInfo = tupleInfo;
        this.rowKeyDecoder = new RowKeyDecoder(this.cubeSeg);
        this.rowValueDecoders = rowValueDecoders;
        this.derivedColFillers = Lists.newArrayList();

        List<TblColRef> dimCols = cuboid.getColumns();

        // pre-calculate dimension index mapping to tuple
        dimensionTupleIdx = new int[dimCols.size()];
        for (int i = 0; i < dimCols.size(); i++) {
            TblColRef col = dimCols.get(i);
            dimensionTupleIdx[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
        }

        // pre-calculate metrics index mapping to tuple
        metricsMeasureIdx = new int[rowValueDecoders.size()][];
        metricsTupleIdx = new int[rowValueDecoders.size()][];
        for (int i = 0; i < rowValueDecoders.size(); i++) {
            RowValueDecoder decoder = rowValueDecoders.get(i);
            MeasureDesc[] measures = decoder.getMeasures();
            BitSet selectedMeasures = decoder.getProjectionIndex();
            metricsMeasureIdx[i] = new int[selectedMeasures.cardinality()];
            metricsTupleIdx[i] = new int[selectedMeasures.cardinality()];
            for (int j = 0, mi = selectedMeasures.nextSetBit(0); j < metricsMeasureIdx[i].length; j++, mi = selectedMeasures.nextSetBit(mi + 1)) {
                FunctionDesc aggrFunc = measures[mi].getFunction();
                int tupleIdx;
                // a rewrite metrics is identified by its rewrite field name
                if (aggrFunc.needRewrite()) {
                    String rewriteFieldName = aggrFunc.getRewriteFieldName();
                    tupleIdx = tupleInfo.hasField(rewriteFieldName) ? tupleInfo.getFieldIndex(rewriteFieldName) : -1;
                }
                // a non-rewrite metrics (like sum, or dimension playing as metrics) is like a dimension column
                else {
                    TblColRef col = aggrFunc.getParameter().getColRefs().get(0);
                    tupleIdx = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
                }
                metricsMeasureIdx[i][j] = mi;
                metricsTupleIdx[i][j] = tupleIdx;
            }
        }

        // prepare derived columns and filler
        Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedInfo = cuboid.getCube().getHostToDerivedInfo(dimCols, null);
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

    public void translateResult(Result hbaseRow, Tuple tuple) {
        try {
            byte[] rowkey = hbaseRow.getRow();
            rowKeyDecoder.decode(rowkey);
        } catch (IOException ex) {
            throw new RuntimeException("Cannot translate hbase result " + hbaseRow);
        }

        // dimensions
        List<String> dimensionValues = rowKeyDecoder.getValues();
        for (int i = 0; i < dimensionValues.size(); i++) {
            int tupleIdx = dimensionTupleIdx[i];
            if (tupleIdx >= 0) {
                tuple.setDimensionValue(tupleIdx, dimensionValues.get(i));
            }
        }

        // derived
        for (IDerivedColumnFiller filler : derivedColFillers) {
            filler.fillDerivedColumns(dimensionValues, tuple);
        }

        // measures
        for (int i = 0; i < rowValueDecoders.size(); i++) {
            RowValueDecoder rowValueDecoder = rowValueDecoders.get(i);
            rowValueDecoder.decode(hbaseRow);
            Object[] measureValues = rowValueDecoder.getValues();

            int[] measureIdx = metricsMeasureIdx[i];
            int[] tupleIdx = metricsTupleIdx[i];
            for (int j = 0; j < measureIdx.length; j++) {
                tuple.setMeasureValue(tupleIdx[j], measureValues[measureIdx[j]]);
            }
        }
    }

    private interface IDerivedColumnFiller {
        public void fillDerivedColumns(List<String> rowValues, Tuple tuple);
    }

    private IDerivedColumnFiller newDerivedColumnFiller(TblColRef[] hostCols, final DeriveInfo deriveInfo) {
        List<TblColRef> rowColumns = cuboid.getColumns();
        
        final int[] hostColIdx = new int[hostCols.length];
        for (int i = 0; i < hostCols.length; i++) {
            hostColIdx[i] = rowColumns.indexOf(hostCols[i]);
        }
        
        boolean needCopyDerived = false;
        final int[] derivedTupleIdx = new int[deriveInfo.columns.length];
        for (int i = 0; i < deriveInfo.columns.length; i++) {
            TblColRef col = deriveInfo.columns[i];
            derivedTupleIdx[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
            needCopyDerived = needCopyDerived || derivedTupleIdx[i] >= 0;
        }
        
        if (needCopyDerived == false)
            return null;
        
        switch (deriveInfo.type) {
        case LOOKUP:
            return new IDerivedColumnFiller() {
                CubeManager cubeMgr = CubeManager.getInstance(cubeSeg.getCubeInstance().getConfig());
                LookupStringTable lookupTable = cubeMgr.getLookupTable(cubeSeg, deriveInfo.dimension);
                int[] derivedColIdx = initDerivedColIdx();
                Array<String> lookupKey = new Array<String>(new String[hostColIdx.length]);
                
                private int[] initDerivedColIdx() {
                    int[] idx = new int[deriveInfo.columns.length];
                    for (int i = 0; i < idx.length; i++) {
                        idx[i] = deriveInfo.columns[i].getColumnDesc().getZeroBasedIndex();
                    }
                    return idx;
                }
                
                @Override
                public void fillDerivedColumns(List<String> rowValues, Tuple tuple) {
                    for (int i = 0; i < hostColIdx.length; i++) {
                        lookupKey.data[i] = rowValues.get(hostColIdx[i]);
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
                public void fillDerivedColumns(List<String> rowValues, Tuple tuple) {
                    // composite keys are split, so only copy [0] is enough, see CubeDesc.initDimensionColumns()
                    tuple.setDimensionValue(derivedTupleIdx[0], rowValues.get(hostColIdx[0]));
                }
            };
        default:
            throw new IllegalArgumentException();
        }
    }

}
