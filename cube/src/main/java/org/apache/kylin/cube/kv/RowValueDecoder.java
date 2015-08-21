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

package org.apache.kylin.cube.kv;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

/**
 * 
 * @author xjiang
 * 
 */
public class RowValueDecoder implements Cloneable {

    private final HBaseColumnDesc hbaseColumn;
    private final MeasureCodec codec;
    private final BitSet projectionIndex;
    private final MeasureDesc[] measures;
    private final List<String> names;
    private Object[] values;

    public RowValueDecoder(RowValueDecoder rowValueDecoder) {
        this.hbaseColumn = rowValueDecoder.getHBaseColumn();
        this.projectionIndex = rowValueDecoder.getProjectionIndex();
        this.names = new ArrayList<String>();
        this.measures = hbaseColumn.getMeasures();
        for (MeasureDesc measure : measures) {
            this.names.add(measure.getFunction().getRewriteFieldName());
        }
        this.codec = new MeasureCodec(measures);
        this.values = new Object[measures.length];
    }

    public RowValueDecoder(HBaseColumnDesc hbaseColumn) {
        this.hbaseColumn = hbaseColumn;
        this.projectionIndex = new BitSet();
        this.names = new ArrayList<String>();
        this.measures = hbaseColumn.getMeasures();
        for (MeasureDesc measure : measures) {
            this.names.add(measure.getFunction().getRewriteFieldName());
        }
        this.codec = new MeasureCodec(measures);
        this.values = new Object[measures.length];
    }

    public void decode(byte[] bytes) {
        codec.decode(ByteBuffer.wrap(bytes), values);
        convertToJavaObjects(values, values);
    }

    private void convertToJavaObjects(Object[] mapredObjs, Object[] results) {
        for (int i = 0; i < mapredObjs.length; i++) {
            Object o = mapredObjs[i];

            if (o instanceof LongWritable)
                o = ((LongWritable) o).get();
            else if (o instanceof IntWritable)
                o = ((IntWritable) o).get();
            else if (o instanceof DoubleWritable)
                o = ((DoubleWritable) o).get();
            else if (o instanceof FloatWritable)
                o = ((FloatWritable) o).get();

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

    public List<String> getNames() {
        return names;
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
