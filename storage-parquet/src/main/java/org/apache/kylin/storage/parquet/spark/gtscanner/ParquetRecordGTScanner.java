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

package org.apache.kylin.storage.parquet.spark.gtscanner;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.measure.bitmap.BitmapSerializer;
import org.apache.kylin.measure.dim.DimCountDistincSerializer;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnSerializer;
import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.measure.percentile.PercentileSerializer;
import org.apache.kylin.measure.raw.RawSerializer;
import org.apache.kylin.measure.topn.TopNCounterSerializer;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * this class tracks resource 
 */
public abstract class ParquetRecordGTScanner implements IGTScanner {

    private Iterator<Object[]> iterator;
    private GTInfo info;
    private GTRecord gtrecord;
    private ImmutableBitSet columns;

    private long maxScannedBytes;
    private long scannedRows;
    private long scannedBytes;

    public ParquetRecordGTScanner(GTInfo info, Iterator<Object[]> iterator, GTScanRequest scanRequest,
            long maxScannedBytes) {
        this.iterator = iterator;
        this.info = info;
        this.gtrecord = new GTRecord(info);
        this.columns = scanRequest.getColumns();
        this.maxScannedBytes = maxScannedBytes;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public void close() throws IOException {
    }

    public long getTotalScannedRowCount() {
        return scannedRows;
    }

    public long getTotalScannedRowBytes() {
        return scannedBytes;
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return Iterators.transform(iterator, new com.google.common.base.Function<Object[], GTRecord>() {
            private int maxColumnLength = -1;
            private ByteBuffer byteBuf = null;
            private final Map<Integer, Integer> dictCols = Maps.newHashMap();
            private final Map<Integer, Integer> binaryCols = Maps.newHashMap();
            private final Map<Integer, Integer> otherCols = Maps.newHashMap();
            @Nullable
            @Override
            @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
            public GTRecord apply(@Nullable Object[] input) {
                if (maxColumnLength <= 0) {
                    maxColumnLength = info.getMaxColumnLength(ParquetRecordGTScanner.this.columns);
                    for (int i = 0; i < ParquetRecordGTScanner.this.columns.trueBitCount(); i++) {
                        int c = ParquetRecordGTScanner.this.columns.trueBitAt(i);
                        DataTypeSerializer serializer = info.getCodeSystem().getSerializer(c);
                        if (serializer instanceof DictionaryDimEnc.DictionarySerializer) {
                            dictCols.put(i, c);
                        } else if (serializer instanceof TopNCounterSerializer || serializer instanceof HLLCSerializer
                                || serializer instanceof BitmapSerializer
                                || serializer instanceof ExtendedColumnSerializer
                                || serializer instanceof PercentileSerializer
                                || serializer instanceof DimCountDistincSerializer
                                || serializer instanceof RawSerializer) {
                            binaryCols.put(i, c);
                        } else {
                            otherCols.put(i, c);
                        }
                    }

                    byteBuf = ByteBuffer.allocate(maxColumnLength);
                }
                byteBuf.clear();
                GTUtil.setValuesParquet(gtrecord, byteBuf, dictCols, binaryCols, otherCols, input);

                scannedBytes += maxColumnLength;
                if ((++scannedRows % GTScanRequest.terminateCheckInterval == 1) && Thread.interrupted()) {
                    throw new KylinTimeoutException("Query timeout");
                }
                if (scannedBytes > maxScannedBytes) {
                    throw new ResourceLimitExceededException(
                            "Partition scanned bytes " + scannedBytes + " exceeds threshold " + maxScannedBytes
                                    + ", consider increase kylin.storage.partition.max-scan-bytes");
                }
                return gtrecord;
            }
        });
    }



}
