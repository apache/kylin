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
package org.apache.kylin.cube.inmemcubing;

import com.google.common.base.Preconditions;

import org.apache.kylin.aggregation.MeasureCodec;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.LongMutable;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class InMemCubeBuilderInputConverter {

    private static final LongMutable ONE = new LongMutable(1l);
    
    private final CubeDesc cubeDesc;
    private final CubeJoinedFlatTableDesc intermediateTableDesc;
    private final MeasureDesc[] measureDescs;
    private final MeasureCodec measureCodec;
    private final int measureCount;
    private final ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private final Map<Integer, Dictionary<String>> topNLiteralColDictMap;
    private final GTInfo gtInfo;
    

    public InMemCubeBuilderInputConverter(CubeDesc cubeDesc, Map<Integer, Dictionary<String>> topNLiteralColDictMap, GTInfo gtInfo) {
        this.cubeDesc = cubeDesc;
        this.gtInfo = gtInfo;
        this.intermediateTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);
        this.measureCount = cubeDesc.getMeasures().size();
        this.measureDescs = cubeDesc.getMeasures().toArray(new MeasureDesc[measureCount]);
        this.measureCodec = new MeasureCodec(cubeDesc.getMeasures());
        this.topNLiteralColDictMap = Preconditions.checkNotNull(topNLiteralColDictMap, "topNLiteralColDictMap cannot be null");
    }
    
    public final GTRecord convert(List<String> row) {
        final GTRecord record = new GTRecord(gtInfo);
        convert(row, record);
        return record;
    }

    public final void convert(List<String> row, GTRecord record) {
        Object[] dimensions = buildKey(row);
        Object[] metricsValues = buildValue(row);
        Object[] recordValues = new Object[dimensions.length + metricsValues.length];
        System.arraycopy(dimensions, 0, recordValues, 0, dimensions.length);
        System.arraycopy(metricsValues, 0, recordValues, dimensions.length, metricsValues.length);
        record.setValues(recordValues);
    }

    private Object[] buildKey(List<String> row) {
        int keySize = intermediateTableDesc.getRowKeyColumnIndexes().length;
        Object[] key = new Object[keySize];

        for (int i = 0; i < keySize; i++) {
            key[i] = row.get(intermediateTableDesc.getRowKeyColumnIndexes()[i]);
        }

        return key;
    }

    private Object[] buildValue(List<String> row) {

        Object[] values = new Object[measureCount];
        for (int i = 0; i < measureCount; i++) {
            MeasureDesc measureDesc = measureDescs[i];
            int[] flatTableIdx = intermediateTableDesc.getMeasureColumnIndexes()[i];
            FunctionDesc function = cubeDesc.getMeasures().get(i).getFunction();
            if (flatTableIdx == null) {
                values[i] = measureCodec.getSerializer(i).valueOf(measureDesc.getFunction().getParameter().getValue());
            } else if (function.isCount() || function.isHolisticCountDistinct()) {
                // note for holistic count distinct, this value will be ignored
                values[i] = ONE;
            } else if (function.isTopN()) {
                // encode the key column with dict, and get the counter column;
                int keyColIndex = flatTableIdx[flatTableIdx.length - 1];
                Dictionary<String> literalColDict = topNLiteralColDictMap.get(keyColIndex);
                int keyColEncoded = literalColDict.getIdFromValue(row.get(keyColIndex));
                valueBuf.clear();
                valueBuf.putInt(literalColDict.getSizeOfId());
                valueBuf.putInt(keyColEncoded);
                if (flatTableIdx.length == 1) {
                    // only literalCol, use 1.0 as counter
                    valueBuf.putDouble(1.0);
                } else {
                    // get the counter column value
                    valueBuf.putDouble(Double.valueOf(row.get(flatTableIdx[0])));
                }

                values[i] = measureCodec.getSerializer(i).valueOf(valueBuf.array());

            } else if (flatTableIdx.length == 1) {
                values[i] = measureCodec.getSerializer(i).valueOf(toBytes(row.get(flatTableIdx[0])));
            } else {

                byte[] result = null;
                for (int x = 0; x < flatTableIdx.length; x++) {
                    byte[] split = toBytes(row.get(flatTableIdx[x]));
                    if (result == null) {
                        result = Arrays.copyOf(split, split.length);
                    } else {
                        byte[] newResult = new byte[result.length + split.length];
                        System.arraycopy(result, 0, newResult, 0, result.length);
                        System.arraycopy(split, 0, newResult, result.length, split.length);
                        result = newResult;
                    }
                }
                values[i] = measureCodec.getSerializer(i).valueOf(result);
            }
        }
        return values;
    }

    private byte[] toBytes(String v) {
        return v == null ? null : Bytes.toBytes(v);
    }

}
