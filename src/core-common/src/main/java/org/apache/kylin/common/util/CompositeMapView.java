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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.EqualsAndHashCode;
import lombok.val;

/**
 * <pre>
 * Decorates a map of some maps to provide a single unified view.
 * 1. The updatable methods for CompositeMapView is not supported.
 * 2. The changes of the composite maps can be reflected in this view
 * </pre>
 */
@EqualsAndHashCode(callSuper = false)
public class CompositeMapView<K, V> extends AbstractMap<K, V> {

    /**
     * Array of all maps in the composite,
     * latter element has higher priority to be read and write
     * */
    @EqualsAndHashCode.Include
    private final Map<K, V>[] composite;

    @SuppressWarnings("unchecked")
    public CompositeMapView(@NotNull Map<K, V>... maps) {
        for (Map<K, V> map : maps) {
            Preconditions.checkNotNull(map);
        }
        this.composite = maps;
    }

    @Override
    public int size() {
        return this.keySet().size();
    }

    @Override
    public boolean isEmpty() {
        return Arrays.stream(composite).allMatch(Map::isEmpty);
    }

    @Override
    public boolean containsKey(Object key) {
        return Arrays.stream(composite).anyMatch(map -> map.containsKey(key));
    }

    @Override
    public boolean containsValue(Object value) {
        return Arrays.stream(composite).anyMatch(map -> map.containsValue(value));
    }

    @Override
    public V get(final Object key) {
        for (int i = this.composite.length - 1; i >= 0; --i) {
            if (this.composite[i].containsKey(key)) {
                return this.composite[i].get(key);
            }
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
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
    public String toString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        val keySet = Sets.<K> newHashSet();
        Arrays.stream(composite).map(Map::keySet).forEach(keySet::addAll);
        return keySet;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        val compositeMap = Maps.<K, V> newHashMapWithExpectedSize(this.size());
        Arrays.stream(composite).forEach(compositeMap::putAll);
        return compositeMap.entrySet();
    }

    @Override
    public Collection<V> values() {
        return this.entrySet().stream().map(Entry::getValue).collect(Collectors.toList());
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
    public V putIfAbsent(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V replace(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException();
    }
}
