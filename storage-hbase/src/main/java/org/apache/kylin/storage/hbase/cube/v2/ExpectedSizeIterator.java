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

package org.apache.kylin.storage.hbase.cube.v2;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.NotImplementedException;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.gridtable.GTScanRequest;

import org.apache.kylin.shaded.com.google.common.base.Throwables;

class ExpectedSizeIterator implements Iterator<byte[]> {
    private final QueryContext queryContext;
    private final int expectedSize;
    private final BlockingQueue<byte[]> queue;
    private final long coprocessorTimeout;
    private final long deadline;
    private int current = 0;

    public ExpectedSizeIterator(QueryContext queryContext, int expectedSize, long coprocessorTimeout) {
        this.queryContext = queryContext;
        this.expectedSize = expectedSize;
        this.queue = new ArrayBlockingQueue<byte[]>(expectedSize);

        this.coprocessorTimeout = coprocessorTimeout;
        //longer timeout than coprocessor so that query thread will not timeout faster than coprocessor
        this.deadline = System.currentTimeMillis() + coprocessorTimeout * 10;
    }

    @Override
    public boolean hasNext() {
        return (current < expectedSize);
    }

    @Override
    public byte[] next() {
        if (current >= expectedSize) {
            throw new NoSuchElementException("Won't have more data");
        }
        try {
            current++;
            byte[] ret = null;

            while (ret == null && deadline > System.currentTimeMillis()) {
                checkState();
                ret = queue.poll(1000, TimeUnit.MILLISECONDS);
            }

            if (ret == null) {
                throw new RuntimeException("Timeout visiting cube! Check why coprocessor exception is not sent back? In coprocessor Self-termination is checked every " + //
                        GTScanRequest.terminateCheckInterval + " scanned rows, the configured timeout(" + coprocessorTimeout + ") cannot support this many scans?");
            }

            return ret;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error when waiting queue", e);
        }
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    public void append(byte[] data) {
        checkState();

        try {
            queue.put(data);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("error when waiting queue", e);
        }
    }

    private void checkState() {
        if (queryContext.isStopped()) {
            Throwable throwable = queryContext.getThrowable();
            if (throwable != null) {
                throw Throwables.propagate(throwable);
            } else {
                throw new IllegalStateException("the query is stopped: " + queryContext.getStopReason());
            }
        }
    }
}
