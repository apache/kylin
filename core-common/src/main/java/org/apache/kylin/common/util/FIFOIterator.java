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
package org.apache.kylin.common.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 *
 * Normal iterators in Collections are fail-safe,
 * i.e. adding elements to a queue will break current iterator.
 * The FIFOIterator is stateless, it only check the first element of a Queue
 */
public class FIFOIterator<T> implements Iterator<T> {
    private Queue<T> q;

    public FIFOIterator(Queue<T> q) {
        this.q = q;
    }

    @Override
    public boolean hasNext() {
        return !q.isEmpty();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return q.poll();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
