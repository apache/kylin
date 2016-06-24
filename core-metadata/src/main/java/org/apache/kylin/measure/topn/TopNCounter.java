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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
public class TopNCounter<T> implements Iterable<Counter<T>> {

    public static final int EXTRA_SPACE_RATE = 50;

    protected int capacity;
    private HashMap<T, ListNode2<Counter<T>>> counterMap;
    protected DoublyLinkedList<Counter<T>> counterList;

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public TopNCounter(int capacity) {
        this.capacity = capacity;
        counterMap = new HashMap<T, ListNode2<Counter<T>>>();
        counterList = new DoublyLinkedList<Counter<T>>();
    }

    public int getCapacity() {
        return capacity;
    }

    /**
     * Algorithm: <i>Space-Saving</i>
     *
     * @param item stream element (<i>e</i>)
     * @return false if item was already in the stream summary, true otherwise
     */
    public boolean offer(T item) {
        return offer(item, 1.0);
    }

    /**
     * Algorithm: <i>Space-Saving</i>
     *
     * @param item stream element (<i>e</i>)
     * @return false if item was already in the stream summary, true otherwise
     */
    public boolean offer(T item, double incrementCount) {
        return offerReturnAll(item, incrementCount).getFirst();
    }

    /**
     * @param item stream element (<i>e</i>)
     * @return item dropped from summary if an item was dropped, null otherwise
     */
    public T offerReturnDropped(T item, double incrementCount) {
        return offerReturnAll(item, incrementCount).getSecond();
    }

    /**
     * @param item stream element (<i>e</i>)
     * @return Pair<isNewItem, itemDropped> where isNewItem is the return value of offer() and itemDropped is null if no item was dropped
     */
    public Pair<Boolean, T> offerReturnAll(T item, double incrementCount) {
        ListNode2<Counter<T>> counterNode = counterMap.get(item);
        boolean isNewItem = (counterNode == null);
        T droppedItem = null;
        if (isNewItem) {

            if (size() < capacity) {
                counterNode = counterList.enqueue(new Counter<T>(item));
            } else {
                counterNode = counterList.tail();
                Counter<T> counter = counterNode.getValue();
                droppedItem = counter.item;
                counterMap.remove(droppedItem);
                counter.item = item;
                counter.count = 0.0;
            }
            counterMap.put(item, counterNode);
        }

        incrementCounter(counterNode, incrementCount);

        return Pair.newPair(isNewItem, droppedItem);
    }

    protected void incrementCounter(ListNode2<Counter<T>> counterNode, double incrementCount) {
        Counter<T> counter = counterNode.getValue();
        counter.count += incrementCount;

        ListNode2<Counter<T>> nodeNext;

        if (incrementCount > 0) {
            nodeNext = counterNode.getNext();
        } else {
            nodeNext = counterNode.getPrev();
        }
        counterList.remove(counterNode);
        counterNode.prev = null;
        counterNode.next = null;

        if (incrementCount > 0) {
            while (nodeNext != null && counter.count >= nodeNext.getValue().count) {
                nodeNext = nodeNext.getNext();
            }
            if (nodeNext != null) {
                counterList.addBefore(nodeNext, counterNode);
            } else {
                counterList.add(counterNode);
            }

        } else {
            while (nodeNext != null && counter.count < nodeNext.getValue().count) {
                nodeNext = nodeNext.getPrev();
            }
            if (nodeNext != null) {
                counterList.addAfter(nodeNext, counterNode);
            } else {
                counterList.enqueue(counterNode);
            }
        }

    }

    public List<T> peek(int k) {
        List<T> topK = new ArrayList<T>(k);

        for (ListNode2<Counter<T>> bNode = counterList.head(); bNode != null; bNode = bNode.getPrev()) {
            Counter<T> b = bNode.getValue();
            if (topK.size() == k) {
                return topK;
            }
            topK.add(b.item);
        }

        return topK;
    }

    public List<Counter<T>> topK(int k) {
        List<Counter<T>> topK = new ArrayList<Counter<T>>(k);

        for (ListNode2<Counter<T>> bNode = counterList.head(); bNode != null; bNode = bNode.getPrev()) {
            Counter<T> b = bNode.getValue();
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
        for (ListNode2<Counter<T>> bNode = counterList.head(); bNode != null; bNode = bNode.getPrev()) {
            Counter<T> b = bNode.getValue();
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
        Counter<T> c = new Counter<T>(item);
        c.count = count;
        ListNode2<Counter<T>> node = counterList.add(c);
        counterMap.put(c.item, node);
    }

    /**
     * Merge another counter into this counter;
     * @param another
     * @return
     */
    public TopNCounter<T> merge(TopNCounter<T> another) {
        double m1 = 0.0, m2 = 0.0;
        if (this.size() >= this.capacity) {
            m1 = this.counterList.tail().getValue().count;
        }

        if (another.size() >= another.capacity) {
            m2 = another.counterList.tail().getValue().count;
        }

        Set<T> duplicateItems = Sets.newHashSet();
        List<T> notDuplicateItems = Lists.newArrayList();

        for (Map.Entry<T, ListNode2<Counter<T>>> entry : this.counterMap.entrySet()) {
            T item = entry.getKey();
            ListNode2<Counter<T>> existing = another.counterMap.get(item);
            if (existing != null) {
                duplicateItems.add(item);
            } else {
                notDuplicateItems.add(item);
            }
        }

        for (T item : duplicateItems) {
            this.offer(item, another.counterMap.get(item).getValue().count);
        }

        for (T item : notDuplicateItems) {
            this.offer(item, m2);
        }

        for (Map.Entry<T, ListNode2<Counter<T>>> entry : another.counterMap.entrySet()) {
            T item = entry.getKey();
            if (duplicateItems.contains(item) == false) {
                double counter = entry.getValue().getValue().count;
                this.offer(item, counter + m1);
            }
        }

        return this;
    }

    /**
     * Retain the capacity to the given number; The extra counters will be cut off
     * @param newCapacity
     */
    public void retain(int newCapacity) {
        assert newCapacity > 0;
        this.capacity = newCapacity;
        if (newCapacity < this.size()) {
            ListNode2<Counter<T>> tail = counterList.tail();
            while (tail != null && this.size() > newCapacity) {
                Counter<T> bucket = tail.getValue();
                this.counterMap.remove(bucket.getItem());
                this.counterList.remove(tail);
                tail = this.counterList.tail();
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

        for (ListNode2<Counter<T>> bNode = counterList.tail(); bNode != null; bNode = bNode.getNext()) {
            Counter<T> b = bNode.getValue();
            counters[index] = b.count;
            index++;
        }

        assert index == size();
        return counters;
    }

    @Override
    public Iterator<Counter<T>> iterator() {
        return new TopNCounterIterator();
    }

    /**
     * Iterator from the tail (smallest) to head (biggest);
     */
    private class TopNCounterIterator implements Iterator<Counter<T>> {

        private ListNode2<Counter<T>> currentBNode;

        private TopNCounterIterator() {
            currentBNode = counterList.tail();
        }

        @Override
        public boolean hasNext() {
            return currentBNode != null;

        }

        @Override
        public Counter<T> next() {
            Counter<T> counter = currentBNode.getValue();
            currentBNode = currentBNode.getNext();
            return counter;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
