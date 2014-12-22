/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase.coprocessor.endpoint;

import com.google.common.collect.Lists;
import com.kylinolap.common.util.BytesSerializer;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.metadata.measure.MeasureAggregator;
import com.kylinolap.metadata.measure.fixedlen.FixedLenMeasureCodec;
import com.kylinolap.invertedindex.index.TableRecordInfo;
import com.kylinolap.invertedindex.index.TableRecordInfoDigest;
import com.kylinolap.metadata.model.DataType;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorConstants;
import org.apache.hadoop.io.LongWritable;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author honma
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class EndpointAggregators {

    public static EndpointAggregators fromFunctions(TableRecordInfo tableInfo, List<FunctionDesc> metrics) {
        String[] funcNames = new String[metrics.size()];
        String[] dataTypes = new String[metrics.size()];
        int[] refColIndex = new int[metrics.size()];

        for (int i = 0; i < metrics.size(); i++) {
            FunctionDesc functionDesc = metrics.get(i);

            //TODO: what if funcionDesc's type is different from tablDesc? cause scale difference
            funcNames[i] = functionDesc.getExpression();
            dataTypes[i] = functionDesc.getReturnType();

            if (functionDesc.isCount()) {
                refColIndex[i] = -1;
            } else {
                refColIndex[i] = tableInfo.findMetric(functionDesc.getParameter().getValue());
                if (refColIndex[i] < 0) {
                    throw new IllegalStateException("Column " + functionDesc.getParameter().getColRefs().get(0) + " is not found in II");
                }
            }
        }

        return new EndpointAggregators(funcNames, dataTypes, refColIndex, tableInfo.getDigest());
    }

    final String[] funcNames;
    final String[] dataTypes;
    final int[] refColIndex;
    final TableRecordInfoDigest tableRecordInfo;

    final transient FixedLenMeasureCodec[] measureSerializers;
    final transient Object[] metricValues;

    final LongWritable one = new LongWritable(1);

    public EndpointAggregators(String[] funcNames, String[] dataTypes, int[] refColIndex, TableRecordInfoDigest tableInfo) {
        this.funcNames = funcNames;
        this.dataTypes = dataTypes;
        this.refColIndex = refColIndex;
        this.tableRecordInfo = tableInfo;

        this.metricValues = new Object[funcNames.length];
        this.measureSerializers = new FixedLenMeasureCodec[funcNames.length];
        for (int i = 0; i < this.measureSerializers.length; ++i) {
            this.measureSerializers[i] = FixedLenMeasureCodec.get(DataType.getInstance(dataTypes[i]));
        }
    }

    public TableRecordInfoDigest getTableRecordInfo() {
        return tableRecordInfo;
    }

    public boolean isEmpty() {
        return !((funcNames != null) && (funcNames.length != 0));
    }

    public MeasureAggregator[] createBuffer() {
        MeasureAggregator[] aggrs = new MeasureAggregator[funcNames.length];
        for (int j = 0; j < aggrs.length; j++) {
            //all fixed length measures can be aggregated as long
            aggrs[j] = MeasureAggregator.create(funcNames[j], "long");
        }
        return aggrs;
    }

    public void aggregate(MeasureAggregator[] measureAggrs, byte[] row) {
        int rawIndex = 0;
        int columnCount = tableRecordInfo.getColumnCount();

        //normal column values to aggregate
        for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
            if (tableRecordInfo.isMetrics(columnIndex)) {
                for (int metricIndex = 0; metricIndex < refColIndex.length; ++metricIndex) {
                    if (refColIndex[metricIndex] == columnIndex) {
                        measureAggrs[metricIndex].aggregate(measureSerializers[metricIndex].read(row, rawIndex));
                    }
                }
            }
            rawIndex += tableRecordInfo.length(columnIndex);
        }

        //aggregate for "count"
        for (int i = 0; i < refColIndex.length; ++i) {
            if (refColIndex[i] == -1) {
                measureAggrs[i].aggregate(one);
            }
        }
    }

    /**
     * @param aggrs
     * @param buffer byte buffer to get the metric data
     * @return length of metric data
     */
    public int serializeMetricValues(MeasureAggregator[] aggrs, byte[] buffer) {
        for (int i = 0; i < funcNames.length; i++) {
            metricValues[i] = aggrs[i].getState();
        }

        int metricBytesOffset = 0;
        for (int i = 0; i < measureSerializers.length; i++) {
            measureSerializers[i].write(metricValues[i], buffer, metricBytesOffset);
            metricBytesOffset += measureSerializers[i].getLength();
        }
        return metricBytesOffset;
    }

    public List<String> deserializeMetricValues(byte[] metricBytes, int offset) {
        List<String> ret = Lists.newArrayList();
        int metricBytesOffset = offset;
        for (int i = 0; i < measureSerializers.length; i++) {
            String valueString = measureSerializers[i].toString(measureSerializers[i].read(metricBytes, metricBytesOffset));
            metricBytesOffset += measureSerializers[i].getLength();
            ret.add(valueString);
        }
        return ret;
    }

    public static byte[] serialize(EndpointAggregators o) {
        ByteBuffer buf = ByteBuffer.allocate(CoprocessorConstants.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }

    public static EndpointAggregators deserialize(byte[] bytes) {
        return serializer.deserialize(ByteBuffer.wrap(bytes));
    }

    private static final Serializer serializer = new Serializer();

    private static class Serializer implements BytesSerializer<EndpointAggregators> {

        @Override
        public void serialize(EndpointAggregators value, ByteBuffer out) {
            BytesUtil.writeAsciiStringArray(value.funcNames, out);
            BytesUtil.writeAsciiStringArray(value.dataTypes, out);
            BytesUtil.writeIntArray(value.refColIndex, out);
            BytesUtil.writeByteArray(TableRecordInfoDigest.serialize(value.tableRecordInfo), out);
        }

        @Override
        public EndpointAggregators deserialize(ByteBuffer in) {
            String[] funcNames = BytesUtil.readAsciiStringArray(in);
            String[] dataTypes = BytesUtil.readAsciiStringArray(in);
            int[] refColIndex = BytesUtil.readIntArray(in);
            byte[] temp = BytesUtil.readByteArray(in);
            TableRecordInfoDigest tableInfo = TableRecordInfoDigest.deserialize(temp);
            return new EndpointAggregators(funcNames, dataTypes, refColIndex, tableInfo);
        }

    }


}

