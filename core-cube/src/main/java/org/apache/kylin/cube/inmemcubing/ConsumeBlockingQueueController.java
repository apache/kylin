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

package org.apache.kylin.cube.inmemcubing;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class ConsumeBlockingQueueController<T> implements Iterator<T> {
    public final static int DEFAULT_BATCH_SIZE = 1000;

    private volatile boolean hasException = false;
    private final BlockingQueue<T> input;
    private final int batchSize;
    private final List<T> batchBuffer;
    private Iterator<T> internalIT;

    private AtomicInteger outputRowCount = new AtomicInteger();

    public ConsumeBlockingQueueController(BlockingQueue<T> input, int batchSize) {
        this.input = input;
        this.batchSize = batchSize;
        this.batchBuffer = Lists.newArrayListWithExpectedSize(batchSize);
        this.internalIT = batchBuffer.iterator();
    }

    @Override
    public boolean hasNext() {
        if (hasException) {
            return false;
        }
        if (internalIT.hasNext()) {
            return true;
        } else {
            batchBuffer.clear();
            try {
                batchBuffer.add(input.take());
                outputRowCount.incrementAndGet();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            outputRowCount.addAndGet(input.drainTo(batchBuffer, batchSize - 1));
            internalIT = batchBuffer.iterator();
        }
        return true;
    }

    @Override
    public T next() {
        return internalIT.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void findException() {
        hasException = true;
    }

    public int getOutputRowCount() {
        return outputRowCount.get();
    }
}
