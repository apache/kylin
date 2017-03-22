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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * scatter the blob returned from region server to a iterable of gtrecords
 */
public class StorageResponseGTScatter implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(StorageResponseGTScatter.class);

    private final GTInfo info;
    private IPartitionStreamer partitionStreamer;
    private final Iterator<byte[]> blocks;
    private final ImmutableBitSet columns;
    private final ImmutableBitSet groupByDims;
    private final boolean needSorted; // whether scanner should return sorted records

    public StorageResponseGTScatter(GTScanRequest scanRequest, IPartitionStreamer partitionStreamer, StorageContext context) {
        this.info = scanRequest.getInfo();
        this.partitionStreamer = partitionStreamer;
        this.blocks = partitionStreamer.asByteArrayIterator();
        this.columns = scanRequest.getColumns();
        this.groupByDims = scanRequest.getAggrGroupBy();
        this.needSorted = (context.getFinalPushDownLimit() != Integer.MAX_VALUE) || context.isStreamAggregateEnabled();
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public void close() throws IOException {
        //If upper consumer failed while consuming the GTRecords, the consumer should call IGTScanner's close method to ensure releasing resource
        partitionStreamer.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        List<PartitionResultIterator> partitionResults = Lists.newArrayList();
        while (blocks.hasNext()) {
            partitionResults.add(new PartitionResultIterator(blocks.next(), info, columns));
        }

        if (partitionResults.size() == 1) {
            return partitionResults.get(0);
        }

        if (!needSorted) {
            logger.debug("Using Iterators.concat to merge partition results");
            return Iterators.concat(partitionResults.iterator());
        }

        logger.debug("Using SortMergedPartitionResultIterator to merge partition results");
        return new SortMergedPartitionResultIterator(partitionResults, info, GTRecord.getComparator(groupByDims));
    }
}
