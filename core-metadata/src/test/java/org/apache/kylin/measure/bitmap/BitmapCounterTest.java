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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Created by sunyerui on 15/12/31.
 */
public class BitmapCounterTest {

    @Test
    public void testAddAndMergeValues() {
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

        counter.merge(counter2);
        assertEquals(6, counter.getCount());
        System.out.print("counter size: " + counter.getMemBytes() + ", counter2 size: " + counter2.getMemBytes());
    }

    @Test
    public void testSerDeCounter() throws IOException {
        BitmapCounter counter = new BitmapCounter();
        for (int i = 1; i < 1000; i++) {
            counter.add(i);
        }
        ByteBuffer buffer = ByteBuffer.allocate(10 * 1024 * 1024);
        counter.writeRegisters(buffer);
        int len = buffer.position();

        buffer.position(0);
        assertEquals(len, counter.peekLength(buffer));
        assertEquals(0, buffer.position());

        BitmapCounter counter2 = new BitmapCounter();
        counter2.readRegisters(buffer);
        assertEquals(999, counter2.getCount());
    }

}