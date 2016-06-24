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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Created by sunyerui on 15/12/31.
 */
public class BitmapAggregatorTest {

    @Test
    public void testAggregator() {
        BitmapCounter counter = new BitmapCounter();
        counter.add(1);
        counter.add(3333);
        counter.add("123".getBytes());
        counter.add(123);
        assertEquals(3, counter.getCount());

        BitmapCounter counter2 = new BitmapCounter();
        counter2.add("23456");
        counter2.add(12273456);
        counter2.add("4258");
        counter2.add(123);
        assertEquals(4, counter2.getCount());

        BitmapAggregator aggregator = new BitmapAggregator();
        assertNull(aggregator.getState());
        assertEquals(Integer.MIN_VALUE, aggregator.getMemBytesEstimate());

        aggregator.aggregate(counter);
        aggregator.aggregate(counter2);
        assertEquals(6, aggregator.getState().getCount());
        aggregator.reset();
        assertNull(aggregator.getState());
    }

}