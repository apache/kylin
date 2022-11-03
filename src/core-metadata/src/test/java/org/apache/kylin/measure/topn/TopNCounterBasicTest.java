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

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TopNCounterBasicTest {

    @Test
    public void testTopNCounter() {
        TopNCounter<String> vs = new TopNCounter<String>(3);
        String[] stream = { "X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "A", "A", "Y" };
        for (String i : stream) {
            vs.offer(i);
        }

        List<Counter<String>> topk = vs.topK(6);

        for (Counter<String> top : topk) {
            System.out.println(top.getItem() + ":" + top.getCount());
        }

    }

    @Test
    public void testTopK() {
        TopNCounter<String> vs = new TopNCounter<>(3);
        String[] stream = { "X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A" };
        for (String i : stream) {
            vs.offer(i);
        }
        List<Counter<String>> topK = vs.topK(3);
        for (Counter<String> c : topK) {
            assertTrue(Arrays.asList("A", "C", "X").contains(c.getItem()));
        }
    }

    @Test
    public void testTopKWithIncrement() {
        TopNCounter<String> vs = new TopNCounter<String>(3);
        String[] stream = { "X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A" };
        for (String i : stream) {
            vs.offer(i, 10.0);
        }
        List<Counter<String>> topK = vs.topK(3);
        for (Counter<String> c : topK) {
            assertTrue(Arrays.asList("A", "C", "X").contains(c.getItem()));
        }
    }

    @Test
    public void testTopKWithIncrementOutOfOrder() {
        TopNCounter<String> vs_increment = new TopNCounter<String>(3);
        TopNCounter<String> vs_single = new TopNCounter<String>(3);
        String[] stream = { "A", "B", "C", "D", "A", "A", "E", "E", "A" };
        Double[] increments = { 15d, 20d, 25d, -30d, 1d, null, null, null, 0d };

        for (int i = 0; i < stream.length; i++) {
            vs_increment.offer(stream[i], increments[i]);
            for (int k = 0; increments[i] != null && k < increments[i]; k++) {
                vs_single.offer(stream[i]);
            }
        }
        System.out.println("Insert with counts vs. single inserts:");
        System.out.println(vs_increment);
        System.out.println(vs_single);

        List<Counter<String>> topK_increment = vs_increment.topK(3);
        List<Counter<String>> topK_single = vs_single.topK(3);

        for (int i = 0; i < topK_increment.size(); i++) {
            assertEquals(topK_increment.get(i).getItem(), topK_single.get(i).getItem());
        }
    }

    @Test
    public void testRetain() {
        TopNCounter<String> vs = new TopNCounter<String>(10);
        String[] stream = { "X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A" };
        for (String i : stream) {
            vs.offer(i);
        }

        vs.retain(5);
        assertTrue(vs.size() <= 5);
        assertTrue(vs.getCapacity() <= 5);
    }

    @Test
    public void testMerge() {

        TopNCounter<String> vs = new TopNCounter<String>(10);
        String[] stream = { "X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "B", "A" };
        for (String i : stream) {
            vs.offer(i);
        }

        String[] stream2 = { "B", "B", "Z", "Z", "B", "C", "X", "X" };
        TopNCounter<String> vs2 = new TopNCounter<String>(10);
        for (String i : stream2) {
            vs2.offer(i);
        }
        // X: 4+2, C: 2+1, A: 3+0, B: 2 +3, Y: 1+0 Z: 1 +0
        vs.merge(vs2);
        List<Counter<String>> topK = vs.topK(3);
        for (Counter<String> c : topK) {
            assertTrue(Arrays.asList("A", "B", "X").contains(c.getItem()));
        }
    }

    @Test
    public void testMergeWithNullValue() {
        TopNCounter<String> vs = new TopNCounter<String>(5);
        String[] stream = { "X", "X", "Y", "Z", "A", "A", "A" };
        Double[] count = { 1d, null, null, 4d, null, null, 0d };
        for (int i = 0; i < stream.length; i++) {
            vs.offer(stream[i], count[i]);
        }
        List<Counter<String>> topK = vs.topK(4);
        List<String> topResult = Lists.newArrayList();
        topK.forEach(counter -> topResult.add(counter.getItem()));
        Assert.assertArrayEquals(new String[] { "Z", "X", "A", "Y" }, topResult.toArray());
    }
}
