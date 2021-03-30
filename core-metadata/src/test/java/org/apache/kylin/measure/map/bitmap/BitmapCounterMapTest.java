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

import org.junit.Test;

public class BitmapCounterMapTest {

    private static final BitmapCounterMapFactory factory = RoaringBitmapCounterMapFactory.INSTANCE;

    @Test
    public void testBitmapCounter() {

        BitmapCounterMap counterMap = factory.newBitmapMap();
        assertEquals(0, counterMap.getCount());

        Long key1 = 300L;
        counterMap.add(key1, 10);
        counterMap.add(key1, 20);
        counterMap.add(key1, 30);
        counterMap.add(key1, 1000);
        assertEquals(4, counterMap.getCount());

        Long key2 = 400L;
        counterMap.add(key2, 10);
        counterMap.add(key2, 30);
        counterMap.add(key2, 40);
        counterMap.add(key2, 2000);
        assertEquals(8, counterMap.getCount());

        BitmapCounterMap anotherMap = factory.newBitmapMap();
        anotherMap.add(key1, 30);
        anotherMap.add(key1, 40);

        anotherMap.add(key2, 40);
        anotherMap.add(key2, 50);

        counterMap.orWith(anotherMap);
        assertEquals(10, counterMap.getCount());
    }
}
