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
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Merge-sort {@code GTRecord}s in all partitions, assume each partition contains sorted elements.
 */
public class SortMergedPartitionResultIterator extends UnmodifiableIterator<GTRecord> {

    final GTRecord record ; // reuse to avoid object creation
    PriorityQueue<PeekingIterator<GTRecord>> heap;

    SortMergedPartitionResultIterator(
            List<PartitionResultIterator> partitionResults,
            GTInfo info, final Comparator<GTRecord> comparator) {

        this.record = new GTRecord(info);
        Comparator<PeekingIterator<GTRecord>> heapComparator = new Comparator<PeekingIterator<GTRecord>>() {
            public int compare(PeekingIterator<GTRecord> o1, PeekingIterator<GTRecord> o2) {
                return comparator.compare(o1.peek(), o2.peek());
            }
        };
        this.heap = new PriorityQueue<>(partitionResults.size(), heapComparator);

        for (PartitionResultIterator it : partitionResults) {
            if (it.hasNext()) {
                heap.offer(Iterators.peekingIterator(it));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public GTRecord next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        // get smallest record
        PeekingIterator<GTRecord> it = heap.poll();
        // WATCH OUT! record got from PartitionResultIterator.next() may changed later,
        // so we must make a shallow copy of it.
        record.shallowCopyFrom(it.next());

        if (it.hasNext()) {
            heap.offer(it);
        }

        return record;
    }
}
