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
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

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

    private ImmutableBitSet[] columnBlocks;

    public ParquetRecordGTScanner(GTInfo info, Iterator<Object[]> iterator, GTScanRequest scanRequest,
                                  long maxScannedBytes) {
        this.iterator = iterator;
        this.info = info;
        this.gtrecord = new GTRecord(info);
        this.columns = scanRequest.getColumns();
        this.maxScannedBytes = maxScannedBytes;
        this.columnBlocks = getParquetCoveredColumnBlocks(scanRequest);
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
            @Nullable
            @Override
            public GTRecord apply(@Nullable Object[] input) {
                gtrecord.setValuesParquet(ParquetRecordGTScanner.this.columns, new ByteArray(info.getMaxColumnLength(ParquetRecordGTScanner.this.columns)), input);

                scannedBytes += info.getMaxColumnLength(ParquetRecordGTScanner.this.columns);
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

    abstract protected ImmutableBitSet getParquetCoveredColumns(GTScanRequest scanRequest);

    abstract protected ImmutableBitSet[] getParquetCoveredColumnBlocks(GTScanRequest scanRequest);

}
