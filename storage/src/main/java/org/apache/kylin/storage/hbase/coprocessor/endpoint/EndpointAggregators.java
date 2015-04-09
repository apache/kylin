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

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.invertedindex.index.RawTableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.measure.fixedlen.FixedLenMeasureCodec;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorConstants;

import com.google.common.collect.Lists;

/**
 * @author honma
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class EndpointAggregators {

    private enum MetricType {
        Count, DimensionAsMetric, DistinctCount, Normal
    }

    private final static class MetricInfo {
        private MetricType type;
        private int refIndex = -1;
        private int precision = -1;

        public MetricInfo(MetricType type, int refIndex, int precision) {
            this.type = type;
            this.refIndex = refIndex;
            this.precision = precision;
        }

        public MetricInfo(MetricType type, int refIndex) {
            this.type = type;
            this.refIndex = refIndex;
        }

        public MetricInfo(MetricType type) {
            this.type = type;
        }

    }

    private static MetricInfo generateMetricInfo(int index, FunctionDesc functionDesc) {
        if (functionDesc.isCount()) {
            return new MetricInfo(MetricType.Count);
        } else if (functionDesc.isDimensionAsMetric()) {
            return new MetricInfo(MetricType.DimensionAsMetric);
        } else {
            Preconditions.checkState(index >= 0, "Column " + functionDesc.getParameter().getValue() + " is not found in II");
            if (functionDesc.isCountDistinct()) {
                return new MetricInfo(MetricType.DistinctCount, index, functionDesc.getReturnDataType().getPrecision());
            } else {
                return new MetricInfo(MetricType.Normal, index);
            }
        }
    }


    public static EndpointAggregators fromFunctions(TableRecordInfo tableInfo, List<FunctionDesc> metrics) {
        final int metricSize = metrics.size();
        String[] funcNames = new String[metricSize];
        String[] dataTypes = new String[metricSize];
        MetricInfo[] metricInfos = new MetricInfo[metricSize];

        for (int i = 0; i < metricSize; i++) {
            FunctionDesc functionDesc = metrics.get(i);

            //TODO: what if funcionDesc's type is different from tablDesc? cause scale difference
            funcNames[i] = functionDesc.getExpression();
            dataTypes[i] = functionDesc.getReturnType();
            int index = tableInfo.findFactTableColumn(functionDesc.getParameter().getValue());
            metricInfos[i] = generateMetricInfo(index, functionDesc);
        }

        return new EndpointAggregators(funcNames, dataTypes, metricInfos, tableInfo.getDigest());
    }

    final String[] funcNames;
    final String[] dataTypes;
    final MetricInfo[] metricInfos;

    final transient TableRecordInfoDigest tableRecordInfoDigest;
    final transient RawTableRecord rawTableRecord;
    final transient ImmutableBytesWritable byteBuffer;
    final transient HyperLogLogPlusCounter[] hllcs;
    final transient FixedLenMeasureCodec[] measureSerializers;
    final transient Object[] metricValues;

    final LongWritable ONE = new LongWritable(1);

    private EndpointAggregators(String[] funcNames, String[] dataTypes, MetricInfo[] metricInfos, TableRecordInfoDigest tableInfo) {
        this.funcNames = funcNames;
        this.dataTypes = dataTypes;
        this.metricInfos = metricInfos;
        this.tableRecordInfoDigest = tableInfo;
        this.rawTableRecord = tableInfo.createTableRecordBytes();
        this.byteBuffer = new ImmutableBytesWritable();

        this.hllcs = new HyperLogLogPlusCounter[this.metricInfos.length];
        this.metricValues = new Object[funcNames.length];
        this.measureSerializers = new FixedLenMeasureCodec[funcNames.length];
        for (int i = 0; i < this.measureSerializers.length; ++i) {
            this.measureSerializers[i] = FixedLenMeasureCodec.get(DataType.getInstance(dataTypes[i]));
        }
    }

    public TableRecordInfoDigest getTableRecordInfoDigest() {
        return tableRecordInfoDigest;
    }

    public boolean isEmpty() {
        return !((funcNames != null) && (funcNames.length != 0));
    }

    public MeasureAggregator[] createBuffer() {
        MeasureAggregator[] aggrs = new MeasureAggregator[funcNames.length];
        for (int i = 0; i < aggrs.length; i++) {
            if (metricInfos[i].type == MetricType.DistinctCount) {
                aggrs[i] = MeasureAggregator.create(funcNames[i], dataTypes[i]);
            } else {
                //all other fixed length measures can be aggregated as long
                aggrs[i] = MeasureAggregator.create(funcNames[i], "long");
            }
        }
        return aggrs;
    }

    /**
     * this method is heavily called at coprocessor side,
     * Make sure as little object creation as possible
     */
    public void aggregate(MeasureAggregator[] measureAggrs, byte[] row) {

        rawTableRecord.setBytes(row, 0, row.length);

        for (int metricIndex = 0; metricIndex < metricInfos.length; ++metricIndex) {
            final MetricInfo metricInfo = metricInfos[metricIndex];
            if (metricInfo.type == MetricType.Count) {
                measureAggrs[metricIndex].aggregate(ONE);
                continue;
            }

            if (metricInfo.type == MetricType.DimensionAsMetric) {
                continue;
            }

            MeasureAggregator aggregator = measureAggrs[metricIndex];
            FixedLenMeasureCodec measureSerializer = measureSerializers[metricIndex];

            //get the raw bytes
            rawTableRecord.getValueBytes(metricInfo.refIndex, byteBuffer);

            if (metricInfo.type == MetricType.Normal) {
                aggregator.aggregate(measureSerializer.read(byteBuffer.get(), byteBuffer.getOffset()));
            } else if (metricInfo.type == MetricType.DistinctCount) {
                //TODO: for unified dictionary, this is okay. but if different data blocks uses different dictionary, we'll have to aggregate original data
                HyperLogLogPlusCounter hllc = hllcs[metricIndex];
                if (hllc == null) {
                    int precision = metricInfo.precision;
                    hllc = new HyperLogLogPlusCounter(precision);
                }
                hllc.clear();
                hllc.add(byteBuffer.get(), byteBuffer.getOffset(), byteBuffer.getLength());
                aggregator.aggregate(hllc);
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

    public List<Object> deserializeMetricValues(byte[] metricBytes, int offset) {
        List<Object> ret = Lists.newArrayList();
        int metricBytesOffset = offset;
        for (int i = 0; i < measureSerializers.length; i++) {
            measureSerializers[i].read(metricBytes, metricBytesOffset);
            Object valueString = measureSerializers[i].getValue();
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

            BytesUtil.writeVInt(value.metricInfos.length, out);
            for (int i = 0; i < value.metricInfos.length; ++i) {
                MetricInfo metricInfo = value.metricInfos[i];
                BytesUtil.writeAsciiString(metricInfo.type.toString(), out);
                BytesUtil.writeVInt(metricInfo.refIndex, out);
                BytesUtil.writeVInt(metricInfo.precision, out);
            }

            BytesUtil.writeByteArray(TableRecordInfoDigest.serialize(value.tableRecordInfoDigest), out);
        }

        @Override
        public EndpointAggregators deserialize(ByteBuffer in) {

            String[] funcNames = BytesUtil.readAsciiStringArray(in);
            String[] dataTypes = BytesUtil.readAsciiStringArray(in);

            int metricInfoLength = BytesUtil.readVInt(in);
            MetricInfo[] infos = new MetricInfo[metricInfoLength];
            for (int i = 0; i < infos.length; ++i) {
                MetricType type = MetricType.valueOf(BytesUtil.readAsciiString(in));
                int refIndex = BytesUtil.readVInt(in);
                int presision = BytesUtil.readVInt(in);
                infos[i] = new MetricInfo(type, refIndex, presision);
            }

            byte[] temp = BytesUtil.readByteArray(in);
            TableRecordInfoDigest tableInfo = TableRecordInfoDigest.deserialize(temp);

            return new EndpointAggregators(funcNames, dataTypes, infos, tableInfo);
        }

    }

}
