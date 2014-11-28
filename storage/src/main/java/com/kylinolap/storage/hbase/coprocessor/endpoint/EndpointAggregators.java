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

import com.kylinolap.common.util.BytesSerializer;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.cube.invertedindex.TableRecordInfoDigest;
import com.kylinolap.cube.measure.MeasureAggregator;
import com.kylinolap.cube.measure.fixedlen.FixedLenMeasureCodec;
import com.kylinolap.metadata.model.DataType;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorConstants;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author honma
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class EndpointAggregators {

    public static EndpointAggregators fromFunctions(List<FunctionDesc> metrics, TableRecordInfoDigest tableInfo) {
        String[] funcNames = new String[metrics.size()];
        String[] dataTypes = new String[metrics.size()];

        for (int i = 0; i < metrics.size(); i++) {
            funcNames[i] = metrics.get(i).getExpression();
            dataTypes[i] = metrics.get(i).getReturnType();
        }

        return new EndpointAggregators(funcNames, dataTypes, tableInfo);
    }

    final String[] funcNames;
    final String[] dataTypes;
    final TableRecordInfoDigest tableInfo;

    final transient FixedLenMeasureCodec[] measureSerializers;
    final transient Object[] metricValues;
    final transient byte[] metricBytes;
    transient int metricBytesOffset = 0;

    public EndpointAggregators(String[] funcNames, String[] dataTypes, TableRecordInfoDigest tableInfo) {
        this.funcNames = funcNames;
        this.dataTypes = dataTypes;
        this.tableInfo = tableInfo;

        this.metricBytes = new byte[CoprocessorConstants.SERIALIZE_BUFFER_SIZE];
        this.metricValues = new Object[funcNames.length];
        this.measureSerializers = new FixedLenMeasureCodec[funcNames.length];
        for (int i = 0; i < this.measureSerializers.length; ++i) {
            this.measureSerializers[i] = FixedLenMeasureCodec.get(DataType.getInstance(dataTypes[i]));
        }
    }

    public MeasureAggregator[] createBuffer() {
        MeasureAggregator[] aggrs = new MeasureAggregator[funcNames.length];
        for (int j = 0; j < aggrs.length; j++) {
            //all fixed length measures can be aggregated as long
            aggrs[j++] = MeasureAggregator.create(funcNames[j], "long");
        }
        return aggrs;
    }

    public void aggregate(MeasureAggregator[] measureAggrs, byte[] row) {
        int rawIndex = 0;
        int metricIndex = 0;
        int columnCount = tableInfo.getColumnCount();

        for (int i = 0; i < columnCount; ++i) {
            if (tableInfo.isMetrics(i)) {
                measureAggrs[metricIndex].aggregate(measureSerializers[metricIndex].read(row, rawIndex));
                metricIndex++;
            }
            rawIndex += tableInfo.length(i);
        }
    }

    public byte[] getMetricValues(MeasureAggregator[] aggrs) {
        for (int i = 0; i < funcNames.length; i++) {
            metricValues[i] = aggrs[i++].getState();
        }

        metricBytesOffset = 0;
        for (int i = 0; i < funcNames.length; i++) {
            measureSerializers[i].write(metricValues[i], metricBytes, metricBytesOffset);
        }
        return metricBytes;
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
            BytesUtil.writeByteArray(TableRecordInfoDigest.serialize(value.tableInfo), out);

        }

        @Override
        public EndpointAggregators deserialize(ByteBuffer in) {
            String[] funcNames = BytesUtil.readAsciiStringArray(in);
            String[] dataTypes = BytesUtil.readAsciiStringArray(in);
            TableRecordInfoDigest tableInfo = TableRecordInfoDigest.deserialize(in);
            return new EndpointAggregators(funcNames, dataTypes, tableInfo);
        }

    }


}

