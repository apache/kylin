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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;

/**
 *
 * This utility class sorts only the specified part of a list
 */
public class PartialSorter {
    public static <T> void partialSort(List<T> list, List<Integer> items, Comparator<? super T> c) {
        List<T> temp = Lists.newLinkedList();
        for (int index : items) {
            temp.add(list.get(index));
        }
        Collections.sort(temp, c);
        for (int i = 0; i < temp.size(); ++i) {
            list.set(items.get(i), temp.get(i));
        }
    }
}
