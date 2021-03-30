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

package org.apache.kylin.measure.map.bitmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class BitmapMapAggregatorTest {
    private static final BitmapCounterMapFactory factory = RoaringBitmapCounterMapFactory.INSTANCE;

    @Test
    public void testAggregator() {
        BitmapMapAggregator aggregator = new BitmapMapAggregator();
        assertNull(aggregator.getState());

        Long key1 = 300L;

        BitmapCounterMap counterMap = factory.newBitmapMap();
        counterMap.add(key1, 10);
        counterMap.add(key1, 20);
        counterMap.add(key1, 30);
        counterMap.add(key1, 40);
        aggregator.aggregate(counterMap);
        assertEquals(4, aggregator.getState().getCount());

        BitmapCounterMap anotherMap = factory.newBitmapMap();
        anotherMap.add(key1, 25);
        anotherMap.add(key1, 30);
        anotherMap.add(key1, 35);
        anotherMap.add(key1, 40);
        anotherMap.add(key1, 45);
        aggregator.aggregate(anotherMap);

        assertEquals(7, aggregator.getState().getCount());

        aggregator.reset();
        assertNull(aggregator.getState());
    }
}
