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

public class SortedIteratorMergerWithLimitTest {
    class CloneableInteger implements Cloneable {
        int value;

        public CloneableInteger(int value) {
            this.value = value;
        }

        @Override
        public Object clone() {
            return new CloneableInteger(value);
        }

        @Override
        public int hashCode() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            CloneableInteger that = (CloneableInteger) o;

            return value == that.value;

        }
    }

    private Comparator<CloneableInteger> getComp() {
        return new Comparator<CloneableInteger>() {
            @Override
            public int compare(CloneableInteger o1, CloneableInteger o2) {
                return o1.value - o2.value;
            }
        };
    }

    @Test
    public void basic1() {

        List<CloneableInteger> a = Lists.newArrayList(new CloneableInteger(1), new CloneableInteger(2), new CloneableInteger(3));
        List<CloneableInteger> b = Lists.newArrayList(new CloneableInteger(1), new CloneableInteger(2), new CloneableInteger(3));
        List<CloneableInteger> c = Lists.newArrayList(new CloneableInteger(1), new CloneableInteger(2), new CloneableInteger(5));
        List<Iterator<CloneableInteger>> input = Lists.newArrayList();
        input.add(a.iterator());
        input.add(b.iterator());
        input.add(c.iterator());
        SortedIteratorMergerWithLimit<CloneableInteger> merger = new SortedIteratorMergerWithLimit<CloneableInteger>(input.iterator(), 3, getComp());
        Iterator<CloneableInteger> iterator = merger.getIterator();
        List<CloneableInteger> result = Lists.newArrayList();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        Assert.assertEquals(Lists.newArrayList(new CloneableInteger(1), new CloneableInteger(1), new CloneableInteger(1), new CloneableInteger(2), new CloneableInteger(2), new CloneableInteger(2), new CloneableInteger(3), new CloneableInteger(3)), result);
    }

    @Test
    public void basic2() {

        List<CloneableInteger> a = Lists.newArrayList(new CloneableInteger(2));
        List<CloneableInteger> b = Lists.newArrayList();
        List<CloneableInteger> c = Lists.newArrayList(new CloneableInteger(1), new CloneableInteger(2), new CloneableInteger(5));
        List<Iterator<CloneableInteger>> input = Lists.newArrayList();
        input.add(a.iterator());
        input.add(b.iterator());
        input.add(c.iterator());
        SortedIteratorMergerWithLimit<CloneableInteger> merger = new SortedIteratorMergerWithLimit<CloneableInteger>(input.iterator(), 3, getComp());
        Iterator<CloneableInteger> iterator = merger.getIterator();
        List<CloneableInteger> result = Lists.newArrayList();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        Assert.assertEquals(Lists.newArrayList(new CloneableInteger(1), new CloneableInteger(2), new CloneableInteger(2), new CloneableInteger(5)), result);
    }

    @Test(expected = IllegalStateException.class)
    public void basic3() {

        List<CloneableInteger> a = Lists.newArrayList(new CloneableInteger(2), new CloneableInteger(1));
        List<CloneableInteger> b = Lists.newArrayList();
        List<CloneableInteger> c = Lists.newArrayList(new CloneableInteger(1), new CloneableInteger(2), new CloneableInteger(5));
        List<Iterator<CloneableInteger>> input = Lists.newArrayList();
        input.add(a.iterator());
        input.add(b.iterator());
        input.add(c.iterator());
        SortedIteratorMergerWithLimit<CloneableInteger> merger = new SortedIteratorMergerWithLimit<CloneableInteger>(input.iterator(), 3, getComp());
        Iterator<CloneableInteger> iterator = merger.getIterator();
        List<CloneableInteger> result = Lists.newArrayList();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
    }

}