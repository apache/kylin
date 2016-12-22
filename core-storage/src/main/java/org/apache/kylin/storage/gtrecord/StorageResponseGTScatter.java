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

package org.apache.kylin.storage.gtrecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.IGTScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * scatter the blob returned from region server to a iterable of gtrecords
 */
public class StorageResponseGTScatter implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(StorageResponseGTScatter.class);

    private GTInfo info;
    private IPartitionStreamer partitionStreamer;
    private Iterator<byte[]> blocks;
    private ImmutableBitSet columns;
    private long totalScannedCount;
    private int storagePushDownLimit = -1;

    public StorageResponseGTScatter(GTInfo info, IPartitionStreamer partitionStreamer, ImmutableBitSet columns, long totalScannedCount, int storagePushDownLimit) {
        this.info = info;
        this.partitionStreamer = partitionStreamer;
        this.blocks = partitionStreamer.asByteArrayIterator();
        this.columns = columns;
        this.totalScannedCount = totalScannedCount;
        this.storagePushDownLimit = storagePushDownLimit;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public long getScannedRowCount() {
        return totalScannedCount;
    }

    @Override
    public void close() throws IOException {
        //If upper consumer failed while consuming the GTRecords, the consumer should call IGTScanner's close method to ensure releasing resource
        partitionStreamer.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        Iterator<Iterator<GTRecord>> shardSubsets = Iterators.transform(blocks, new EndpointResponseGTScatterFunc());
        if (storagePushDownLimit != Integer.MAX_VALUE) {
            return new SortedIteratorMergerWithLimit<GTRecord>(shardSubsets, storagePushDownLimit, GTRecord.getPrimaryKeyComparator()).getIterator();
        } else {
            return Iterators.concat(shardSubsets);
        }
    }

    class EndpointResponseGTScatterFunc implements Function<byte[], Iterator<GTRecord>> {
        @Nullable
        @Override
        public Iterator<GTRecord> apply(@Nullable final byte[] input) {

            return new Iterator<GTRecord>() {
                private ByteBuffer inputBuffer = null;
                //rotate between two buffer GTRecord to support SortedIteratorMergerWithLimit, which will peek one more GTRecord
                private GTRecord firstRecord = null;

                @Override
                public boolean hasNext() {
                    if (inputBuffer == null) {
                        inputBuffer = ByteBuffer.wrap(input);
                        firstRecord = new GTRecord(info);
                    }

                    return inputBuffer.position() < inputBuffer.limit();
                }

                @Override
                public GTRecord next() {
                    firstRecord.loadColumns(columns, inputBuffer);
                    return firstRecord;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

}
