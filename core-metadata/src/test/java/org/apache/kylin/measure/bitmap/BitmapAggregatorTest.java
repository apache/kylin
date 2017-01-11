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

import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BitmapAggregatorTest {

    @Test
    public void testAggregator() {
        BitmapAggregator aggregator = new BitmapAggregator();
        assertNull(null, aggregator.getState());

        aggregator.aggregate(new ImmutableBitmapCounter(
                ImmutableRoaringBitmap.bitmapOf(10, 20, 30, 40)
        ));
        assertEquals(4, aggregator.getState().getCount());

        aggregator.aggregate(new ImmutableBitmapCounter(
                ImmutableRoaringBitmap.bitmapOf(25, 30, 35, 40, 45)
        ));
        assertEquals(7, aggregator.getState().getCount());

        aggregator.reset();
        assertNull(aggregator.getState());
    }

}