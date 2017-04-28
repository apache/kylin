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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Modified from the StreamSummary.java in https://github.com/addthis/stream-lib
 *
 * Based on the <i>Space-Saving</i> algorithm and the <i>Stream-Summary</i>
 * data structure as described in:
 * <i>Efficient Computation of Frequent and Top-k Elements in Data Streams</i>
 * by Metwally, Agrawal, and Abbadi
 *
 * @param <T> type of data in the stream to be summarized
 */
public class TopNCounter<T> implements Iterable<Counter<T>>, java.io.Serializable {

    public static final int EXTRA_SPACE_RATE = 50;

    protected int capacity;
    private HashMap<T, Counter<T>> counterMap;
    protected LinkedList<Counter<T>> counterList; //a linked list, first the is the toppest element
    private boolean ordered = true;
    private boolean descending = true;

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public TopNCounter(int capacity) {
        this.capacity = capacity;
        counterMap = Maps.newHashMap();
        counterList = Lists.newLinkedList();
    }

    public int getCapacity() {
        return capacity;
    }

    public LinkedList<Counter<T>> getCounterList() {
        return counterList;
    }

    public void offer(T item) {
        offer(item, 1.0);
    }

    /**
     * Algorithm: <i>Space-Saving</i>
     *
     * @param item stream element (<i>e</i>)
     * @return false if item was already in the stream summary, true otherwise
     */
    public void offer(T item, double incrementCount) {
        Counter<T> counterNode = counterMap.get(item);
        if (counterNode == null) {
            counterNode = new Counter<T>(item, incrementCount);
            counterMap.put(item, counterNode);
            counterList.add(counterNode);
        } else {
            counterNode.setCount(counterNode.getCount() + incrementCount);
        }
        ordered = false;
    }

    /**
     * Sort and keep the expected size;
     */
    public void sortAndRetain() {
        Collections.sort(counterList, this.descending ? DESC_COMPARATOR : ASC_COMPARATOR);
        retain(capacity);
        ordered = true;
    }

    public List<Counter<T>> topK(int k) {
        if (ordered == false) {
            sortAndRetain();
        }
        List<Counter<T>> topK = new ArrayList<>(k);
        Iterator<Counter<T>> iterator = counterList.iterator();
        while (iterator.hasNext()) {
            Counter<T> b = iterator.next();
            if (topK.size() == k) {
                return topK;
            }
            topK.add(b);
        }

        return topK;
    }

    /**
     * @return number of items stored
     */
    public int size() {
        return counterMap.size();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        Iterator<Counter<T>> iterator = counterList.iterator();
        while (iterator.hasNext()) {
            Counter<T> b = iterator.next();
            sb.append(b.item);
            sb.append(':');
            sb.append(b.count);
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * Put element to the head position;
     * The consumer should call this method with count in ascending way; the item will be directly put to the head of the list, without comparison for best performance;
     * @param item
     * @param count
     */
    public void offerToHead(T item, double count) {
        Counter<T> c = new Counter<T>(item, count);
        counterList.addFirst(c);
        counterMap.put(c.item, c);
    }

    /**
     * Merge another counter into this counter;
     * @param another
     * @return
     */
    public TopNCounter<T> merge(TopNCounter<T> another) {
        boolean thisFull = this.size() >= this.capacity;
        boolean anotherFull = another.size() >= another.capacity;
        double m1 = thisFull ? this.counterList.getLast().count : 0.0;
        double m2 = anotherFull ? another.counterList.getLast().count : 0.0;

        if (anotherFull == true) {
            for (Counter<T> entry : this.counterMap.values()) {
                entry.count += m2;
            }
        }

        for (Map.Entry<T, Counter<T>> entry : another.counterMap.entrySet()) {
            if (this.counterMap.containsKey(entry.getKey())) {
                this.offer(entry.getValue().getItem(), (entry.getValue().count - m2));
            } else {
                this.offer(entry.getValue().getItem(), entry.getValue().count + m1);
            }
        }

        this.sortAndRetain();
        return this;
    }

    /**
     * Retain the capacity to the given number; The extra counters will be cut off
     * @param newCapacity
     */
    public void retain(int newCapacity) {
        this.capacity = newCapacity;
        if (this.size() > newCapacity) {
            Counter<T> toRemoved;
            for (int i = 0, n = this.size() - newCapacity; i < n; i++) {
                toRemoved = counterList.pollLast();
                this.counterMap.remove(toRemoved.item);
            }
        }

    }

    /**
     * Get the counter values in ascending order
     * @return
     */
    public double[] getCounters() {
        double[] counters = new double[size()];
        int index = 0;

        if (this.descending == true) {
            Iterator<Counter<T>> iterator = counterList.descendingIterator();
            while (iterator.hasNext()) {
                Counter<T> b = iterator.next();
                counters[index] = b.count;
                index++;
            }
        } else {
            throw new IllegalStateException(); // support in future
        }

        assert index == size();
        return counters;
    }

    @Override
    public Iterator<Counter<T>> iterator() {
        if (this.descending == true) {
            return this.counterList.descendingIterator();
        } else {
            throw new IllegalStateException(); // support in future
        }
    }

    private static final Comparator ASC_COMPARATOR = new Comparator<Counter>() {
        @Override
        public int compare(Counter o1, Counter o2) {
            return o1.getCount() > o2.getCount() ? 1 : o1.getCount() == o2.getCount() ? 0 : -1;
        }

    };

    private static final Comparator DESC_COMPARATOR = new Comparator<Counter>() {
        @Override
        public int compare(Counter o1, Counter o2) {
            return o1.getCount() > o2.getCount() ? -1 : o1.getCount() == o2.getCount() ? 0 : 1;
        }

    };

}
