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

package org.apache.kylin.storage.gtrecord;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class SortedIteratorMergerTest {

    private Comparator<Integer> getComp() {
        return new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        };
    }

    @Test
    public void basic1() {

        List<Integer> a = Lists.newArrayList(1, 2, 3);
        List<Integer> b = Lists.newArrayList(1, 2, 3);
        List<Integer> c = Lists.newArrayList(1, 2, 5);
        List<Iterator<Integer>> input = Lists.newArrayList();
        input.add(a.iterator());
        input.add(b.iterator());
        input.add(c.iterator());
        SortedIteratorMerger<Integer> merger = new SortedIteratorMerger<Integer>(input.iterator(), getComp());
        Iterator<Integer> iterator = merger.getIterator();
        List<Integer> result = Lists.newArrayList();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        Assert.assertEquals(Lists.newArrayList(1, 1, 1, 2, 2, 2, 3, 3, 5), result);
    }

    @Test
    public void basic2() {

        List<Integer> a = Lists.newArrayList(2);
        List<Integer> b = Lists.newArrayList();
        List<Integer> c = Lists.newArrayList(1, 2, 5);
        List<Iterator<Integer>> input = Lists.newArrayList();
        input.add(a.iterator());
        input.add(b.iterator());
        input.add(c.iterator());
        SortedIteratorMerger<Integer> merger = new SortedIteratorMerger<Integer>(input.iterator(), getComp());
        Iterator<Integer> iterator = merger.getIterator();
        List<Integer> result = Lists.newArrayList();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        Assert.assertEquals(Lists.newArrayList(1, 2, 2, 5), result);
    }

    @Test
    public void basic3() {

        List<Integer> a = Lists.newArrayList();
        List<Integer> b = Lists.newArrayList();
        List<Integer> c = Lists.newArrayList();
        List<Iterator<Integer>> input = Lists.newArrayList();
        input.add(a.iterator());
        input.add(b.iterator());
        input.add(c.iterator());
        SortedIteratorMerger<Integer> merger = new SortedIteratorMerger<Integer>(input.iterator(), getComp());
        Iterator<Integer> iterator = merger.getIterator();
        List<Integer> result = Lists.newArrayList();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        Assert.assertEquals(Lists.newArrayList(), result);
    }
}