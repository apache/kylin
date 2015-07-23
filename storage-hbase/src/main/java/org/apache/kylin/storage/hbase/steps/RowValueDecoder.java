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

import org.apache.hadoop.hbase.client.Result;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.metadata.measure.DoubleMutable;
import org.apache.kylin.metadata.measure.LongMutable;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;

/**
 */
public class RowValueDecoder implements Cloneable {

    private final HBaseColumnDesc hbaseColumn;
    private final byte[] hbaseColumnFamily;
    private final byte[] hbaseColumnQualifier;

    private final MeasureCodec codec;
    private final BitSet projectionIndex;
    private final MeasureDesc[] measures;
    private Object[] values;

    public RowValueDecoder(HBaseColumnDesc hbaseColumn) {
        this.hbaseColumn = hbaseColumn;
        this.hbaseColumnFamily = Bytes.toBytes(hbaseColumn.getColumnFamilyName());
        this.hbaseColumnQualifier = Bytes.toBytes(hbaseColumn.getQualifier());
        this.projectionIndex = new BitSet();
        this.measures = hbaseColumn.getMeasures();
        this.codec = new MeasureCodec(measures);
        this.values = new Object[measures.length];
    }

    public void decode(Result hbaseRow) {
        decode(hbaseRow, true);
    }

    public void decode(Result hbaseRow, boolean convertToJavaObject) {
        decode(hbaseRow.getValueAsByteBuffer(hbaseColumnFamily, hbaseColumnQualifier), convertToJavaObject);
    }

    public void decode(byte[] bytes) {
        decode(bytes, true);
    }

    public void decode(byte[] bytes, boolean convertToJavaObject) {
        decode(ByteBuffer.wrap(bytes), convertToJavaObject);
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

    public void setIndex(int bitIndex) {
        projectionIndex.set(bitIndex);
    }

    public HBaseColumnDesc getHBaseColumn() {
        return hbaseColumn;
    }

    public BitSet getProjectionIndex() {
        return projectionIndex;
    }

    public Object[] getValues() {
        return values;
    }

    public MeasureDesc[] getMeasures() {
        return measures;
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

}
