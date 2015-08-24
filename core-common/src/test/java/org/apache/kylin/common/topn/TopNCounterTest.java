/*
 * Copyright (C) 2011 Clearspring Technologies, Inc. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.topn;

import org.apache.kylin.common.topn.Counter;
import org.apache.kylin.common.topn.TopNCounter;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TopNCounterTest {

    private static final int NUM_ITERATIONS = 100000;

    @Test
    public void testTopNCounter() {
        TopNCounter<String> vs = new TopNCounter<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "A", "A", "Y"};
        for (String i : stream) {
                vs.offer(i);
            /*
        for(String s : vs.poll(3))
        System.out.print(s+" ");
             */
            System.out.println(vs);
        }

        List<Counter<String>> topk = vs.topK(6);
        
        for(Counter<String> top : topk) {
            System.out.println(top.getItem() + ":" + top.getCount() + ":" + top.getError());
        }
        
    }

    @Test
    public void testTopK() {
        TopNCounter<String> vs = new TopNCounter<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
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
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream) {
            vs.offer(i, 10);
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
        String[] stream = {"A", "B", "C", "D", "A"};
        Integer[] increments = {15, 20, 25, 30, 1};

        for (int i = 0; i < stream.length; i++) {
            vs_increment.offer(stream[i], increments[i]);
            for (int k = 0; k < increments[i]; k++) {
                vs_single.offer(stream[i]);
            }
        }
        System.out.println("Insert with counts vs. single inserts:");
        System.out.println(vs_increment);
        System.out.println(vs_single);

        List<Counter<String>> topK_increment = vs_increment.topK(3);
        List<Counter<String>> topK_single = vs_single.topK(3);

        for (int i = 0; i < topK_increment.size(); i++) {
            assertEquals(topK_increment.get(i).getItem(),
                    topK_single.get(i).getItem());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCounterSerialization() throws IOException, ClassNotFoundException {
        TopNCounter<String> vs = new TopNCounter<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream) {
            vs.offer(i);
        }
        List<Counter<String>> topK = vs.topK(3);
        for (Counter<String> c : topK) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutput oo = new ObjectOutputStream(baos);
            oo.writeObject(c);
            oo.close();

            ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            Counter<String> clone = (Counter<String>) oi.readObject();
            assertEquals(c.getCount(), clone.getCount(), 0.0001);
            assertEquals(c.getError(), clone.getError(), 0.0001);
            assertEquals(c.getItem(), clone.getItem());
        }
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        TopNCounter<String> vs = new TopNCounter<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream) {
            vs.offer(i);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutput oo = new ObjectOutputStream(baos);
        oo.writeObject(vs);
        oo.close();

        ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        TopNCounter<String> clone = (TopNCounter<String>) oi.readObject();

        assertEquals(vs.toString(), clone.toString());
    }


    @Test
    public void testByteSerialization() throws IOException, ClassNotFoundException {
        TopNCounter<String> vs = new TopNCounter<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream) {
            vs.offer(i);
        }

        testSerialization(vs);

        // Empty
        vs = new TopNCounter<String>(0);
        testSerialization(vs);
    }

    private void testSerialization(TopNCounter<?> vs) throws IOException, ClassNotFoundException {
        byte[] bytes = vs.toBytes();
        TopNCounter<String> clone = new TopNCounter<String>(bytes);

        assertEquals(vs.toString(), clone.toString());
    }
}
