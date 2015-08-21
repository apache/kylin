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

package org.apache.kylin.storage.hbase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author yangli9
 * 
 */
public class FuzzyValueCombination {

    private static class Dim {
        TblColRef col;
        Set<String> values;
    }

    private static final Set<String> SINGLE_NULL_SET = Sets.newHashSet();
    static {
        SINGLE_NULL_SET.add(null);
    }

    public static List<Map<TblColRef, String>> calculate(Map<TblColRef, Set<String>> fuzzyValues, long cap) {
        Dim[] dims = toDims(fuzzyValues);
        capDims(dims, cap);
        return combination(dims);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<TblColRef, String>> combination(Dim[] dims) {

        List<Map<TblColRef, String>> result = Lists.newArrayList();

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

        Map<TblColRef, String> r = Maps.newHashMap();
        Iterator<String>[] iters = new Iterator[dims.length];
        int level = 0;
        while (true) {
            Dim dim = dims[level];
            if (iters[level] == null) {
                iters[level] = dim.values.iterator();
            }

            Iterator<String> it = iters[level];
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
                result.add(new HashMap<TblColRef, String>(r));
            } else {
                level++;
            }
        }
        return result;
    }

    private static Dim[] toDims(Map<TblColRef, Set<String>> fuzzyValues) {
        Dim[] dims = new Dim[fuzzyValues.size()];
        int i = 0;
        for (Entry<TblColRef, Set<String>> entry : fuzzyValues.entrySet()) {
            dims[i] = new Dim();
            dims[i].col = entry.getKey();
            dims[i].values = entry.getValue();
            if (dims[i].values == null)
                dims[i].values = Collections.emptySet();
            i++;
        }
        return dims;
    }

    private static void capDims(Dim[] dims, long cap) {
        Arrays.sort(dims, new Comparator<Dim>() {
            @Override
            public int compare(Dim o1, Dim o2) {
                return -(o1.values.size() - o2.values.size());
            }
        });

        for (Dim dim : dims) {
            if (combCount(dims) < cap)
                break;
            dim.values = Collections.emptySet();
        }
    }

    private static long combCount(Dim[] dims) {
        long count = 1;
        for (Dim dim : dims) {
            count *= Math.max(dim.values.size(), 1);
        }
        return count;
    }

}
