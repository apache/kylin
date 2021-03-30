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

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Iterators;
import org.apache.kylin.shaded.com.google.common.collect.PeekingIterator;
import org.apache.kylin.shaded.com.google.common.collect.UnmodifiableIterator;

/**
 * Merge-sort {@code GTRecord}s in all partitions, assume each partition contains sorted elements.
 */
public class SortMergedPartitionResultIterator extends UnmodifiableIterator<GTRecord> {
    private static final Logger logger = LoggerFactory.getLogger(SortMergedPartitionResultIterator.class);

    private final Iterator<PartitionResultIterator> iterators;
    private final Comparator<GTRecord> comparator;
    private final GTRecord record; // reuse to avoid object creation

    private boolean initted = false;
    private PriorityQueue<PeekingIterator<GTRecord>> heap;

    SortMergedPartitionResultIterator(Iterator<PartitionResultIterator> iterators, GTInfo info,
            final Comparator<GTRecord> comparator) {
        this.iterators = iterators;
        this.record = new GTRecord(info);
        this.comparator = comparator;
    }

    @Override
    public boolean hasNext() {
        if (!initted) {

            Comparator<PeekingIterator<GTRecord>> heapComparator = new Comparator<PeekingIterator<GTRecord>>() {
                public int compare(PeekingIterator<GTRecord> o1, PeekingIterator<GTRecord> o2) {
                    return comparator.compare(o1.peek(), o2.peek());
                }
            };

            this.heap = new PriorityQueue<>(3, heapComparator);
            int total = 0, actual = 0;

            while (iterators.hasNext()) {
                total++;
                PartitionResultIterator it = iterators.next();
                if (it.hasNext()) {
                    actual++;
                    heap.offer(Iterators.peekingIterator(it));
                }
            }
            logger.debug("Using SortMergedPartitionResultIterator to merge {} partition results out of {} partitions",
                    actual, total);
            initted = true;
        }

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
