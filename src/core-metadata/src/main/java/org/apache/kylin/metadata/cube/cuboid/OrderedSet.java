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

package org.apache.kylin.metadata.cube.cuboid;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.ComparisonChain;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;

public class OrderedSet<T> implements Set<T> {
    // HashMap default value
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private final Set<T> set = Sets.newHashSet();
    private final Map<T, Integer> insertingMap = Maps.newHashMap();

    @Override
    public boolean add(T element) {
        val result = set.add(element);
        if (result) {
            insertingMap.putIfAbsent(element, set.size());
        }
        return result;

    }

    @Override
    public boolean remove(Object element) {
        val result = set.remove(element);
        if (result) {
            insertingMap.remove(element);
        }
        return result;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return set.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        val iterator = c.iterator();
        while (iterator.hasNext()) {
            val element = iterator.next();
            add(element);
        }
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        val iterator = c.iterator();
        while (iterator.hasNext()) {
            val element = iterator.next();
            remove(element);
        }
        return false;
    }

    @Override
    public void clear() {
        set.clear();
        insertingMap.clear();
    }

    @Override
    public Iterator<T> iterator() {
        int capacity = capacity();
        return set.stream()
                .sorted((cuboid1, cuboid2) -> ComparisonChain.start()
                        .compare(hash(cuboid1, capacity), hash(cuboid2, capacity))
                        .compare(insertingMap.get(cuboid1), insertingMap.get(cuboid2)).result())
                .iterator();
    }

    @Override
    public Object[] toArray() {
        return set.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return set.toArray(a);
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return set.contains(o);
    }

    public List<T> getSortedList() {
        val result = Lists.<T> newArrayList();
        val iterator = iterator();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }

        int capacityForNewHashSet = capacityForNewHashSet();
        return result.stream()
                .sorted((cuboid1, cuboid2) -> ComparisonChain.start()
                        .compare(hash(cuboid1, capacityForNewHashSet), hash(cuboid2, capacityForNewHashSet)).result())
                .collect(Collectors.toList());
    }

    private int hash(T cuboidDecimal, int capacity) {
        int hashSetTableSize = hashSetTableSize(capacity);
        int hashcode = cuboidDecimal == null ? 0 : cuboidDecimal.hashCode();
        return (hashSetTableSize - 1) & (hashcode ^ (hashcode >>> 16));
    }

    private int capacity() {
        return Math.max((int) (size() / DEFAULT_LOAD_FACTOR), 16);
    }

    private int capacityForNewHashSet() {
        return Math.max((int) (size() / DEFAULT_LOAD_FACTOR) + 1, 16);
    }

    // the minimum 2^n value greater than size
    private int hashSetTableSize(int capacity) {
        int n = capacity - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

}
