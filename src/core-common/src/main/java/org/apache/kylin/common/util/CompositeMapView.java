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

package org.apache.kylin.common.util;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.collections.collection.CompositeCollection;
import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.commons.collections.iterators.IteratorChain;

/**
 * <pre>
 * Decorates a map of some maps to provide a single unified view.
 * 1. The updatable methods for CompositeMapView is not supported.
 * 2. The changes of the composite maps can be reflected in this view
 * </pre>
 */
public class CompositeMapView implements Map {

    /**
     * Array of all maps in the composite,
     * latter element has higher priority to be read and write
     * */
    private final Map[] composite;

    public CompositeMapView(Map one, Map two) {
        this(new Map[] { one, two });
    }

    public CompositeMapView(Map[] composite) {
        this.composite = composite;
    }


    @Override
    public int size() {
        throw new UnsupportedOperationException("Use size(boolean precise) instead.");
    }

    public int size(boolean precise) {
        if (!precise) {
            Set<Object> set = new HashSet<>();
            for (int i = this.composite.length - 1; i >= 0; --i) {
                set.addAll(this.composite[i].keySet());
            }
            return set.size();
        } else {
            int count = 0;
            for (int i = this.composite.length - 1; i >= 0; --i) {
                count += this.composite[i].size();
            }
            return count;
        }
    }

    @Override
    public boolean isEmpty() {
        for (int i = this.composite.length - 1; i >= 0; --i) {
            if (!this.composite[i].isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean containsKey(Object key) {
        for (int i = this.composite.length - 1; i >= 0; --i) {
            if (this.composite[i].containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        for (int i = this.composite.length - 1; i >= 0; --i) {
            if (this.composite[i].containsValue(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object get(Object key) {
        for (int i = this.composite.length - 1; i >= 0; --i) {
            if (this.composite[i].containsKey(key)) {
                return this.composite[i].get(key);
            }
        }
        return null;
    }

    @Override
    public Object put(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object clone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Object> keySet() {
        Set[] keys = new Set[composite.length];
        for (int i = 0; i < composite.length; i++) {
            keys[i] = this.composite[i].keySet();
        }
        return new CompositeSetView(keys);
    }

    @Override
    public Set<Map.Entry<Object, Object>> entrySet() {
        Set[] entries = new Set[composite.length];
        for (int i = 0; i < composite.length; i++) {
            entries[i] = this.composite[i].entrySet();
        }
        return new CompositeSetView(entries);
    }

    @Override
    public Collection<Object> values() {
        Collection[] values = new Collection[composite.length];
        for (int i = 0; i < composite.length; i++) {
            values[i] = this.composite[i].values();
        }
        return new CompositeCollectionView(values);
    }

    @Override
    public void forEach(BiConsumer action) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(BiFunction function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object replace(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object computeIfAbsent(Object key, Function mappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object computeIfPresent(Object key, BiFunction remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized Object compute(Object key, BiFunction remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized Object merge(Object key, Object value, BiFunction remappingFunction) {
        throw new UnsupportedOperationException();
    }

    private static class CompositeCollectionView implements Collection {
        protected Collection[] all;

        public CompositeCollectionView(Collection[] colls) {
            this.all = colls;
        }

        @Override
        public int size() {
            int size = 0;
            for (int i = this.all.length - 1; i >= 0; i--) {
                size += this.all[i].size();
            }
            return size;
        }

        @Override
        public boolean isEmpty() {
            for (int i = this.all.length - 1; i >= 0; i--) {
                if (!this.all[i].isEmpty()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean contains(Object obj) {
            for (int i = this.all.length - 1; i >= 0; i--) {
                if (this.all[i].contains(obj)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Iterator iterator() {
            if (this.all.length == 0) {
                return EmptyIterator.INSTANCE;
            }
            IteratorChain chain = new IteratorChain();
            for (int i = 0; i < this.all.length; ++i) {
                chain.addIterator(this.all[i].iterator());
            }
            return chain;
        }

        @Override
        public Object[] toArray() {
            final Object[] result = new Object[this.size()];
            int i = 0;
            for (Iterator it = this.iterator(); it.hasNext(); i++) {
                result[i] = it.next();
            }
            return result;
        }

        @Override
        public boolean add(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray(Object[] array) {
            int size = this.size();
            Object[] result = null;
            if (array.length >= size) {
                result = array;
            } else {
                result = (Object[]) Array.newInstance(array.getClass().getComponentType(), size);
            }

            int offset = 0;
            for (int i = 0; i < this.all.length; ++i) {
                for (Iterator it = this.all[i].iterator(); it.hasNext();) {
                    result[offset++] = it.next();
                }
            }
            if (result.length > size) {
                result[size] = null;
            }
            return result;
        }
    }

    private static class CompositeSetView extends CompositeCollection implements Set {
        public CompositeSetView(Collection[] colls) {
            super(colls);
        }
    }
}
