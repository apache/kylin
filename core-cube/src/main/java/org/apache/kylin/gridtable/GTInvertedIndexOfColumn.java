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

package org.apache.kylin.gridtable;

import java.util.NavigableMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.kylin.common.util.ByteArray;

import com.google.common.collect.Maps;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

public class GTInvertedIndexOfColumn {

    final private IGTComparator comparator;
    final private ReentrantReadWriteLock rwLock;

    private int nBlocks;
    private NavigableMap<ByteArray, ConciseSet> rangeIndex;
    private ConciseSet nullIndex;

    public GTInvertedIndexOfColumn(IGTComparator comparator) {
        this.comparator = comparator;
        this.rwLock = new ReentrantReadWriteLock();
        this.rangeIndex = Maps.newTreeMap(comparator);
        this.nullIndex = new ConciseSet();
    }

    public void add(Iterable<ByteArray> codes, int blockId) {
        rwLock.writeLock().lock();
        try {
            for (ByteArray code : codes) {
                if (comparator.isNull(code)) {
                    nullIndex.add(blockId);
                    continue;
                }
                ConciseSet set = rangeIndex.get(code);
                if (set == null) {
                    set = new ConciseSet();
                    rangeIndex.put(code.copy(), set);
                }
                set.add(blockId);
            }

            if (blockId >= nBlocks) {
                nBlocks = blockId + 1;
            }

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public ConciseSet getNull() {
        rwLock.readLock().lock();
        try {
            return nullIndex.clone();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public ConciseSet getEquals(ByteArray code) {
        rwLock.readLock().lock();
        try {
            ConciseSet set = rangeIndex.get(code);
            if (set == null)
                return new ConciseSet();
            else
                return set.clone();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public ConciseSet getIn(Iterable<ByteArray> codes) {
        rwLock.readLock().lock();
        try {
            ConciseSet r = new ConciseSet();
            for (ByteArray code : codes) {
                ConciseSet set = rangeIndex.get(code);
                if (set != null)
                    r.addAll(set);
            }
            return r;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public ConciseSet getRange(ByteArray from, boolean fromInclusive, ByteArray to, boolean toInclusive) {
        rwLock.readLock().lock();
        try {
            ConciseSet r = new ConciseSet();
            if (from == null && to == null) {
                r.add(nBlocks);
                r.complement();
                return r;
            }
            NavigableMap<ByteArray, ConciseSet> subMap;
            if (from == null) {
                subMap = rangeIndex.headMap(to, toInclusive);
            } else if (to == null) {
                subMap = rangeIndex.tailMap(from, fromInclusive);
            } else {
                subMap = rangeIndex.subMap(from, fromInclusive, to, toInclusive);
            }
            for (ConciseSet set : subMap.values()) {
                r.addAll(set);
            }
            return r;
        } finally {
            rwLock.readLock().unlock();
        }
    }
}
