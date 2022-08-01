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
package org.apache.kylin.measure.topn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class DoubleDeltaSerializerTest {

    ByteBuffer buf = ByteBuffer.allocate(8192);
    DoubleDeltaSerializer dds = new DoubleDeltaSerializer();

    @Test
    public void testEmpty() {
        buf.clear();
        dds.serialize(new double[] {}, buf);
        buf.flip();
        double[] r = dds.deserialize(buf);
        assertTrue(r.length == 0);
    }

    @Test
    public void testSingle() {
        buf.clear();
        dds.serialize(new double[] { 1.2 }, buf);
        buf.flip();
        double[] r = dds.deserialize(buf);
        assertArrayEquals(new double[] { 1.2 }, r);
    }

    @Test
    public void testRounding() {
        buf.clear();
        dds.serialize(new double[] { 1.234, 2.345 }, buf);
        buf.flip();
        double[] r = dds.deserialize(buf);
        assertArrayEquals(new double[] { 1.23, 2.35 }, r);
    }

    @Test
    public void testRandom() {
        Random rand = new Random();
        int n = 1000;

        double[] nums = new double[n];
        for (int i = 0; i < n; i++) {
            nums[i] = rand.nextDouble() * 1000000;
        }
        Arrays.sort(nums);

        buf.clear();
        dds.serialize(nums, buf);
        buf.flip();
        double[] r = dds.deserialize(buf);
        assertArrayEquals(nums, r);
        System.out.println("doubles size of " + (n * 8) + " bytes serialized to " + buf.limit() + " bytes");
    }

    @Test
    public void testRandom2() {
        Random rand = new Random();
        int n = 1000;

        double[] nums = new double[n];
        for (int i = 0; i < n; i++) {
            nums[i] = rand.nextInt();
        }
        Arrays.sort(nums);

        buf.clear();
        dds.serialize(nums, buf);
        buf.flip();
        double[] r = dds.deserialize(buf);
        assertArrayEquals(nums, r);
        System.out.println("doubles size of " + (n * 8) + " bytes serialized to " + buf.limit() + " bytes");
    }

    private static void assertArrayEquals(double[] expected, double[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i], 0.02);
        }
    }
}
