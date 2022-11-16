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

package org.apache.kylin.measure.bitmap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BitmapCounterTest {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;

    @Test
    public void testBitmapCounter() {
        BitmapCounter counter = factory.newBitmap(10, 20, 30, 1000);
        assertEquals(4, counter.getCount());
        assertTrue(counter.getMemBytes() > 0);

        BitmapCounter counter2 = factory.newBitmap();
        assertEquals(0, counter2.getCount());
        counter2.add(10);
        counter2.add(30);
        counter2.add(40);
        counter2.add(2000);
        assertEquals(4, counter2.getCount());

        counter2.orWith(counter);
        assertEquals(4, counter.getCount());
        assertEquals(6, counter2.getCount()); // in-place change

        int i = 0;
        long[] values = new long[Math.toIntExact(counter2.getCount())];
        for (long value : counter2) {
            values[i++] = value;
        }
        assertArrayEquals(new long[] { 10, 20, 30, 40, 1000, 2000 }, values);

        counter2.clear();
        assertEquals(0, counter2.getCount());
    }

}