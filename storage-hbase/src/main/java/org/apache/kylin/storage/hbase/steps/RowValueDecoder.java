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

package org.apache.kylin.storage.hbase.steps;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;

import org.apache.hadoop.hbase.client.Result;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.metadata.measure.DoubleMutable;
import org.apache.kylin.metadata.measure.LongMutable;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.storage.hbase.util.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class RowValueDecoder implements Cloneable {

    private static final Logger logger = LoggerFactory.getLogger(RowValueDecoder.class);

    private final HBaseColumnDesc hbaseColumn;
    private final byte[] hbaseColumnFamily;
    private final byte[] hbaseColumnQualifier;

    private final MeasureCodec codec;
    private final BitSet projectionIndex;
    private final MeasureDesc[] measures;
    private final Object[] values;

    public RowValueDecoder(HBaseColumnDesc hbaseColumn) {
        this.hbaseColumn = hbaseColumn;
        this.hbaseColumnFamily = Bytes.toBytes(hbaseColumn.getColumnFamilyName());
        this.hbaseColumnQualifier = Bytes.toBytes(hbaseColumn.getQualifier());
        this.projectionIndex = new BitSet();
        this.measures = hbaseColumn.getMeasures();
        this.codec = new MeasureCodec(measures);
        this.values = new Object[measures.length];
    }

    public void decodeAndConvertJavaObj(Result hbaseRow) {
        decode(hbaseRow, true);
    }

    public void decode(Result hbaseRow) {
        decode(hbaseRow, false);
    }

    private void decode(Result hbaseRow, boolean convertToJavaObject) {
        ByteBuffer buffer = Results.getValueAsByteBuffer(hbaseRow, hbaseColumnFamily, hbaseColumnQualifier);
        decode(buffer, convertToJavaObject);
    }

    public void decodeAndConvertJavaObj(byte[] bytes) {
        decode(ByteBuffer.wrap(bytes), true);
    }

    public void decode(byte[] bytes) {
        decode(ByteBuffer.wrap(bytes), false);
    }

    private void decode(ByteBuffer buffer, boolean convertToJavaObject) {
        codec.decode(buffer, values);
        if (convertToJavaObject) {
            convertToJavaObjects(values, values, convertToJavaObject);
        }
    }

    private void convertToJavaObjects(Object[] mapredObjs, Object[] results, boolean convertToJavaObject) {
        for (int i = 0; i < mapredObjs.length; i++) {
            Object o = mapredObjs[i];

            if (o instanceof LongMutable)
                o = ((LongMutable) o).get();
            else if (o instanceof DoubleMutable)
                o = ((DoubleMutable) o).get();

            results[i] = o;
        }
    }

    public void setProjectIndex(int bitIndex) {
        projectionIndex.set(bitIndex);
    }

    public BitSet getProjectionIndex() {
        return projectionIndex;
    }

    public HBaseColumnDesc getHBaseColumn() {
        return hbaseColumn;
    }

    public Object[] getValues() {
        return values;
    }

    public MeasureDesc[] getMeasures() {
        return measures;
    }

    // result is in order of <code>CubeDesc.getMeasures()</code>
    public void loadCubeMeasureArray(Object result[]) {
        int[] measureIndex = hbaseColumn.getMeasureIndex();
        for (int i = 0; i < measureIndex.length; i++) {
            result[measureIndex[i]] = values[i];
        }
    }

    public boolean hasMemHungryCountDistinct() {
        for (int i = projectionIndex.nextSetBit(0); i >= 0; i = projectionIndex.nextSetBit(i + 1)) {
            FunctionDesc func = measures[i].getFunction();
            if (func.isCountDistinct() && !func.isHolisticCountDistinct()) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasMemHungryCountDistinct(Collection<RowValueDecoder> rowValueDecoders) {
        for (RowValueDecoder decoder : rowValueDecoders) {
            if (decoder.hasMemHungryCountDistinct())
                return true;
        }
        return false;
    }

    public static MeasureDesc findTopN(Collection<RowValueDecoder> rowValueDecoders) {
        for (RowValueDecoder decoder : rowValueDecoders) {
            for (int i = decoder.projectionIndex.nextSetBit(0); i >= 0; i = decoder.projectionIndex.nextSetBit(i + 1)) {
                MeasureDesc measure = decoder.measures[i];
                FunctionDesc func = measure.getFunction();
                if (func.isTopN())
                    return measure;
            }
        }
        return null;
    }

}
