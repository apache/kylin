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

package org.apache.kylin.cube.gridtable;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

public abstract class ComparatorEx<T> implements Comparator<T> {

    public T min(Collection<T> v) {
        if (v.size() <= 0) {
            return null;
        }

        Iterator<T> iterator = v.iterator();
        T min = iterator.next();
        while (iterator.hasNext()) {
            min = min(min, iterator.next());
        }
        return min;
    }

    public T max(Collection<T> v) {
        if (v.size() <= 0) {
            return null;
        }

        Iterator<T> iterator = v.iterator();
        T max = iterator.next();
        while (iterator.hasNext()) {
            max = max(max, iterator.next());
        }
        return max;
    }

    public T min(T a, T b) {
        return compare(a, b) <= 0 ? a : b;
    }

    public T max(T a, T b) {
        return compare(a, b) >= 0 ? a : b;
    }

    public boolean between(T v, T start, T end) {
        return compare(start, v) <= 0 && compare(v, end) <= 0;
    }
}
