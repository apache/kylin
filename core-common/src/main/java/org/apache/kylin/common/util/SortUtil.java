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

import java.util.Iterator;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.TreeMultimap;

/**
 */
public class SortUtil {

    private SortUtil() {
        throw new IllegalStateException("Class SortUtil is an utility class !");
    }

    public static <T extends Comparable, E extends Comparable> Iterator<T> extractAndSort(Iterator<T> input, Function<T, E> extractor) {
        TreeMultimap<E, T> reorgnized = TreeMultimap.create();
        while (input.hasNext()) {
            T t = input.next();
            E e = extractor.apply(t);
            reorgnized.put(e, t);
        }
        return reorgnized.values().iterator();
    }
}
