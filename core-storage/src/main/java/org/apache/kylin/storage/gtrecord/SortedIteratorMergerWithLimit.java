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

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import com.google.common.base.Preconditions;

/**
 * the limit here correspond to the the limit in sql
 * if the SQL ends with "limit N", then each shard will return N "smallest" records
 * The query sever side will use a heap to pick right records.
 * 
 * There're two usage of SortedIteratorMergerWithLimit in kylin
 * One at GTRecord level and the other at Tuple Level
 * The first is to deal with cuboid shards among the same segment
 * and the second is to deal with multiple segments
 * 
 * Let's use single-segment as an example:
 * suppose we have a "limit 2" in SQL, and we have three shards in the segment
 * the first returns (1,2), the second returns (1,3) and the third returns (2,3)
 * each subset is guaranteed to be sorted. (that's why it's called "SortedIterator Merger")
 * SortedIteratorMergerWithLimit will merge these three subsets and return (1,1,2,2)
 * 
 */
public class SortedIteratorMergerWithLimit<E extends Cloneable> extends SortedIteratorMerger<E> {
    private int limit;
    private Comparator<E> comparator;

    public SortedIteratorMergerWithLimit(Iterator<Iterator<E>> shardSubsets, int limit, Comparator<E> comparator) {
        super(shardSubsets, comparator);
        this.limit = limit;
        this.comparator = comparator;
    }

    protected Iterator<E> getIteratorInternal(PriorityQueue<PeekingImpl<E>> heap) {
        return new MergedIteratorWithLimit<E>(heap, limit, comparator);
    }

    static class MergedIteratorWithLimit<E extends Cloneable> implements Iterator<E> {

        private final PriorityQueue<PeekingImpl<E>> heap;
        private final Comparator<E> comparator;

        private boolean nextFetched = false;
        private E fetched = null;
        private E last = null;

        private int limit;
        private int limitProgress = 0;

        private PeekingImpl<E> lastSource = null;

        public MergedIteratorWithLimit(PriorityQueue<PeekingImpl<E>> heap, int limit, Comparator<E> comparator) {
            this.heap = heap;
            this.limit = limit;
            this.comparator = comparator;
        }

        @Override
        public boolean hasNext() {
            if (nextFetched) {
                return true;
            }

            if (lastSource != null && lastSource.hasNext()) {
                if (lastSource.hasNext()) {
                    heap.offer(lastSource);
                } else {
                    lastSource = null;
                }
            }

            if (!heap.isEmpty()) {
                PeekingImpl<E> first = heap.poll();
                E current = first.next();
                try {
                    //clone is protected on Object, have to use reflection to call the overwritten clone method in subclasses
                    current = (E) current.getClass().getMethod("clone").invoke(current);
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }

                lastSource = first;

                Preconditions.checkState(current != null);

                if (last == null || comparator.compare(current, last) != 0) {
                    if (++limitProgress > limit) {
                        return false;
                    }
                }
                nextFetched = true;
                fetched = current;

                return true;
            } else {
                return false;
            }
        }

        @Override
        public E next() {
            if (!nextFetched) {
                throw new IllegalStateException("Should hasNext() before next()");
            }

            //TODO: remove this check when validated
            if (last != null) {
                if (comparator.compare(last, fetched) > 0)
                    throw new IllegalStateException("Not sorted! last: " + last + " fetched: " + fetched);
            }

            last = fetched;
            nextFetched = false;

            return fetched;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
