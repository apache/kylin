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

package org.apache.kylin.cube.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class FuzzyValueCombination {

    private static class Dim<K, V> {
        K col;
        Set<V> values;
    }

    private static final Set SINGLE_NULL_SET = Sets.newHashSet();

    static {
        SINGLE_NULL_SET.add(null);
    }

    public static <K, V> List<Map<K, V>> calculate(Map<K, Set<V>> fuzzyValues, long cap) {
        Dim<K, V>[] dims = toDims(fuzzyValues);
        // If a query has many IN clause and each IN clause has many values, then it will easily generate 
        // thousands of fuzzy keys. When there are lots of fuzzy keys, the scan performance is bottle necked 
        // on it. So simply choose to abandon all fuzzy keys in this case.
        if (exceedCap(dims, cap)) {
            return Lists.newArrayList();
        } else {
            return combination(dims);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> List<Map<K, V>> combination(Dim<K, V>[] dims) {

        List<Map<K, V>> result = Lists.newArrayList();

        int emptyDims = 0;
        for (Dim dim : dims) {
            if (dim.values.isEmpty()) {
                dim.values = SINGLE_NULL_SET;
                emptyDims++;
            }
        }
        if (emptyDims == dims.length) {
            return result;
        }

        Map<K, V> r = Maps.newHashMap();
        Iterator<V>[] iters = new Iterator[dims.length];
        int level = 0;
        while (true) {
            Dim<K, V> dim = dims[level];
            if (iters[level] == null) {
                iters[level] = dim.values.iterator();
            }

            Iterator<V> it = iters[level];
            if (it.hasNext() == false) {
                if (level == 0)
                    break;
                r.remove(dim.col);
                iters[level] = null;
                level--;
                continue;
            }

            r.put(dim.col, it.next());
            if (level == dims.length - 1) {
                result.add(new HashMap<K, V>(r));
            } else {
                level++;
            }
        }
        return result;
    }

    private static <K, V> Dim<K, V>[] toDims(Map<K, Set<V>> fuzzyValues) {
        Dim[] dims = new Dim[fuzzyValues.size()];
        int i = 0;
        for (Entry<K, Set<V>> entry : fuzzyValues.entrySet()) {
            dims[i] = new Dim<K, V>();
            dims[i].col = entry.getKey();
            dims[i].values = entry.getValue();
            if (dims[i].values == null)
                dims[i].values = Collections.emptySet();
            i++;
        }
        return dims;
    }

    private static boolean exceedCap(Dim[] dims, long cap) {
        return combCount(dims) > cap;
    }

    private static long combCount(Dim[] dims) {
        long count = 1;
        for (Dim dim : dims) {
            count *= Math.max(dim.values.size(), 1);
        }
        return count;
    }

}
