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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

public class CollectionUtil {

    public static <T> T findElement(Collection<T> collection, Predicate<T> predicate) {
        if (collection != null && predicate != null) {
            for (Iterator<T> iter = collection.iterator(); iter.hasNext();) {
                T item = iter.next();
                if (predicate.test(item)) {
                    return item;
                }
            }
        }
        return null;
    }

    public static <T> Optional<T> find(Collection<T> collection, Predicate<T> predicate) {
        if (collection != null && predicate != null) {
            for (Iterator<T> iter = collection.iterator(); iter.hasNext();) {
                T item = iter.next();
                if (predicate.test(item)) {
                    return Optional.of(item);
                }
            }
        }
        return Optional.empty();
    }

    public static <T, V> void distinct(Collection<T> collection, Function<T, V> distinctBy) {
        Set<V> values = new HashSet<>();
        collection.removeIf(el -> !values.add(distinctBy.apply(el)));
    }

    public static List<Integer> intersection(List<Integer> list1, List<Integer> list2) {
        return Sets.intersection(Sets.newHashSet(list1), Sets.newHashSet(list2)).stream().sorted()
                .collect(Collectors.toList());
    }

    public static List<Integer> difference(List<Integer> list1, List<Integer> list2) {
        return Sets.difference(Sets.newHashSet(list1), Sets.newHashSet(list2)).stream().sorted()
                .collect(Collectors.toList());
    }
}
