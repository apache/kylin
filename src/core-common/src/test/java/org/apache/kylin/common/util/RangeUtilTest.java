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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;

/**
 */
public class RangeUtilTest {
    @Test
    public void testFilter() {
        NavigableMap<Integer, Integer> map = new TreeMap<>();
        map.put(3, 3);
        map.put(1, 1);
        map.put(2, 2);
        Map<Integer, Integer> subMap = RangeUtil.filter(map, Range.<Integer> all());
        Assert.assertEquals(subMap.size(), 3);

        subMap = RangeUtil.filter(map, Range.atLeast(2));
        Assert.assertEquals(subMap.size(), 2);

        subMap = RangeUtil.filter(map, Range.greaterThan(2));
        Assert.assertEquals(subMap.size(), 1);

        subMap = RangeUtil.filter(map, Range.greaterThan(0));
        Assert.assertEquals(subMap.size(), 3);

        subMap = RangeUtil.filter(map, Range.greaterThan(5));
        Assert.assertEquals(subMap.size(), 0);

        subMap = RangeUtil.filter(map, Range.atMost(2));
        Assert.assertEquals(subMap.size(), 2);

        subMap = RangeUtil.filter(map, Range.lessThan(2));
        Assert.assertEquals(subMap.size(), 1);

        subMap = RangeUtil.filter(map, Range.lessThan(5));
        Assert.assertEquals(subMap.size(), 3);

        subMap = RangeUtil.filter(map, Range.lessThan(0));
        Assert.assertEquals(subMap.size(), 0);
    }

    @Test
    public void testBuildRanges() {
        int[] test1 = { 1, 2, 3, 5, 7, 8, 10, 4 };
        TreeSet<Integer> treeSet = Sets.newTreeSet();
        for (int t : test1) {
            treeSet.add(t);
        }
        List<Range<Integer>> ranges = RangeUtil.buildRanges(new TreeSet<Integer>(treeSet));
        Assert.assertEquals(3, ranges.size());
    }
}
