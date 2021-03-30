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

package org.apache.kylin.stream.core.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.zip.DataFormatException;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.stream.core.query.ResponseResultSchema;
import org.apache.kylin.stream.core.storage.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordsSerializer {
    private static final Logger logger = LoggerFactory.getLogger(RecordsSerializer.class);
    private static final int ROWVALUE_BUFFER_SIZE = 1024 * 1024;

    private ResponseResultSchema schema;
    private DataTypeSerializer[] metricsSerializers;

    public RecordsSerializer(ResponseResultSchema schema) {
        this.schema = schema;
        DataType[] metricsDataTypes = schema.getMetricsDataTypes();
        this.metricsSerializers = new DataTypeSerializer[metricsDataTypes.length];
        for (int i = 0; i < metricsSerializers.length; i++) {
            metricsSerializers[i] = DataTypeSerializer.create(metricsDataTypes[i]);
        }
    }

    public Pair<byte[], Long> serialize(Iterator<Record> records, int storagePushDownLimit) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(ROWVALUE_BUFFER_SIZE);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(ROWVALUE_BUFFER_SIZE);
        long finalRowCnt = 0;
        while (records.hasNext()) {
            Record record = records.next();
            buffer.clear();
            serializeRecord(record, buffer);
            buffer.flip();
            outputStream.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            finalRowCnt++;
            if (finalRowCnt >= storagePushDownLimit) {
                //read one more rexcord than limit
                logger.info(
                        "The finalScanner aborted because storagePushDownLimit is satisfied, storagePushDownLimit is:{}",
                        storagePushDownLimit);
                break;
            }
        }

        byte[] compressedAllRows = CompressionUtils.compress(outputStream.toByteArray());
        return new Pair<>(compressedAllRows, finalRowCnt);
    }

    @SuppressWarnings("unchecked")
    private void serializeRecord(Record record, ByteBuffer out) {
        String[] dimValues = record.getDimensions();
        Object[] metricValues = record.getMetrics();
        for (int i = 0; i < dimValues.length; i++) {
            BytesUtil.writeUTFString(dimValues[i], out);
        }
        for (int i = 0; i < metricValues.length; i++) {
            metricsSerializers[i].serialize(metricValues[i], out);
        }
    }

    private void deserializeRecord(Record resultRecord, ByteBuffer in) {
        for (int i = 0; i < schema.getDimensionCount(); i++) {
            resultRecord.setDimension(i, BytesUtil.readUTFString(in));
        }
        for (int i = 0; i < schema.getMetricsCount(); i++) {
            resultRecord.setMetric(i, metricsSerializers[i].deserialize(in));
        }
    }

    public Iterator<Record> deserialize(final byte[] recordsBytes) throws IOException,
            DataFormatException {
        final byte[] decompressedData = CompressionUtils.decompress(recordsBytes);
        return new Iterator<Record>() {
            private ByteBuffer inputBuffer = null;
            private Record oneRecord = new Record(schema.getDimensionCount(), schema.getMetricsCount());

            @Override
            public boolean hasNext() {
                if (inputBuffer == null) {
                    inputBuffer = ByteBuffer.wrap(decompressedData);
                }

                return inputBuffer.position() < inputBuffer.limit();
            }

            @Override
            public Record next() {

                deserializeRecord(oneRecord, inputBuffer);
                return oneRecord;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("UnSupport Operation");
            }
        };
    }
}
