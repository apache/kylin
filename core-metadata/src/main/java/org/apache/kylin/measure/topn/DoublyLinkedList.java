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

/**
 * Modified from DoublyLinkedList.java in https://github.com/addthis/stream-lib
 * 
 * @param <T>
 */
public class DoublyLinkedList<T> {

    private int size = 0;
    private ListNode2<T> tail;
    private ListNode2<T> head;

    /**
     * Append to head of list
     */
    public ListNode2<T> add(T value) {
        ListNode2<T> node = new ListNode2<T>(value);
        add(node);

        return node;
    }

    /**
     * Prepend to tail of list
     */
    public ListNode2<T> enqueue(T value) {
        ListNode2<T> node = new ListNode2<T>(value);

        return enqueue(node);
    }

    public ListNode2<T> enqueue(ListNode2<T> node) {
        if (size++ == 0) {
            head = node;
        } else {
            node.next = tail;
            tail.prev = node;
        }

        tail = node;

        return node;
    }

    public void add(ListNode2<T> node) {
        node.prev = head;
        node.next = null;

        if (size++ == 0) {
            tail = node;
        } else {
            head.next = node;
        }

        head = node;
    }

    public ListNode2<T> addAfter(ListNode2<T> node, T value) {
        ListNode2<T> newNode = new ListNode2<T>(value);
        addAfter(node, newNode);
        return newNode;
    }

    public void addAfter(ListNode2<T> node, ListNode2<T> newNode) {
        newNode.next = node.next;
        newNode.prev = node;
        node.next = newNode;
        if (newNode.next == null) {
            head = newNode;
        } else {
            newNode.next.prev = newNode;
        }
        size++;
    }

    public void addBefore(ListNode2<T> node, ListNode2<T> newNode) {
        newNode.prev = node.prev;
        newNode.next = node;
        node.prev = newNode;
        if (newNode.prev == null) {
            tail = newNode;
        } else {
            newNode.prev.next = newNode;
        }
        size++;
    }

    public void remove(ListNode2<T> node) {
        if (node == tail) {
            tail = node.next;
        } else {
            node.prev.next = node.next;
        }

        if (node == head) {
            head = node.prev;
        } else {
            node.next.prev = node.prev;
        }
        size--;
    }

    public int size() {
        return size;
    }

    public ListNode2<T> head() {
        return head;
    }

    public ListNode2<T> tail() {
        return tail;
    }

    public boolean isEmpty() {
        return size == 0;
    }

}
