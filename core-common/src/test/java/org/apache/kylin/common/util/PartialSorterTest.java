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

import static org.junit.Assert.assertArrayEquals;

import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

/**
 */
public class PartialSorterTest {
    @Test
    public void basicTest() {
        List<Integer> a = Lists.newArrayList();
        a.add(100);
        a.add(2);
        a.add(92);
        a.add(1);
        a.add(0);
        PartialSorter.partialSort(a, Lists.newArrayList(1, 3, 4), new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });
        assertArrayEquals(a.toArray(), new Integer[] { 100, 0, 92, 1, 2 });
    }

}
