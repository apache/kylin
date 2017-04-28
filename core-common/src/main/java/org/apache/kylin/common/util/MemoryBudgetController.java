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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class MemoryBudgetController {

    private static final boolean debug = true;

    public interface MemoryConsumer {
        // return number MB released
        int freeUp(int mb);
    }

    @SuppressWarnings("serial")
    public static class NotEnoughBudgetException extends IllegalStateException {

        public NotEnoughBudgetException() {
            super();
        }

        public NotEnoughBudgetException(Throwable cause) {
            super(cause);
        }
    }

    private static class ConsumerEntry {
        final MemoryConsumer consumer;
        int reservedMB;

        ConsumerEntry(MemoryConsumer consumer) {
            this.consumer = consumer;
        }
    }

    public static final MemoryBudgetController ZERO_BUDGET = new MemoryBudgetController(0);
    public static final int ONE_MB = 1024 * 1024;
    public static final long ONE_GB = 1024 * 1024 * 1024L;

    private static final Logger logger = LoggerFactory.getLogger(MemoryBudgetController.class);

    // all budget numbers are in MB
    private final int totalBudgetMB;
    private final ConcurrentMap<MemoryConsumer, ConsumerEntry> booking = new ConcurrentHashMap<MemoryConsumer, ConsumerEntry>();
    private int totalReservedMB;
    private final ReentrantLock lock = new ReentrantLock();

    public MemoryBudgetController(int totalBudgetMB) {
        Preconditions.checkArgument(totalBudgetMB >= 0);
        Preconditions.checkState(totalBudgetMB <= getSystemAvailMB());
        this.totalBudgetMB = totalBudgetMB;
        this.totalReservedMB = 0;
    }

    public int getTotalBudgetMB() {
        return totalBudgetMB;
    }

    public int getTotalReservedMB() {
        lock.lock();
        try {
            return totalReservedMB;
        } finally {
            lock.unlock();
        }
    }

    public int getRemainingBudgetMB() {
        lock.lock();
        try {
            return totalBudgetMB - totalReservedMB;
        } finally {
            lock.unlock();
        }
    }

    public void reserveInsist(MemoryConsumer consumer, int requestMB) {
        if (requestMB > totalBudgetMB)
            throw new NotEnoughBudgetException();

        long waitStart = 0;
        while (true) {
            try {
                reserve(consumer, requestMB);
                if (debug && waitStart > 0)
                    logger.debug(consumer + " waited " + (System.currentTimeMillis() - waitStart) + " ms on the " + requestMB + " MB request");
                return;
            } catch (NotEnoughBudgetException ex) {
                // retry
            }

            if (waitStart == 0)
                waitStart = System.currentTimeMillis();

            synchronized (lock) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new NotEnoughBudgetException(e);
                }
            }
        }
    }

    /** reserve without wait, fail with NotEnoughBudgetException immediately if no mem */
    public void reserve(MemoryConsumer consumer, int requestMB) {
        if (totalBudgetMB == 0 && requestMB > 0)
            throw new NotEnoughBudgetException();

        boolean ok = false;
        while (!ok) {
            int gap = calculateGap(consumer, requestMB);
            if (gap > 0) {
                // to void deadlock, don't hold lock when invoking consumer.freeUp()
                tryFreeUp(gap);
            }
            ok = updateBooking(consumer, requestMB);
        }
    }

    private int calculateGap(MemoryConsumer consumer, int requestMB) {
        lock.lock();
        try {
            ConsumerEntry entry = booking.get(consumer);
            int curMB = entry == null ? 0 : entry.reservedMB;
            int delta = requestMB - curMB;
            return delta - (totalBudgetMB - totalReservedMB);
        } finally {
            lock.unlock();
        }
    }

    private void tryFreeUp(int gap) {
        // note don't hold lock when calling consumer.freeUp(), that method holding lock for itself and may cause deadlock
        for (ConsumerEntry entry : booking.values()) {
            int mb = entry.consumer.freeUp(gap);
            if (mb > 0) {
                lock.lock();
                try {
                    updateBookingWithDelta(entry.consumer, -mb);
                } finally {
                    lock.unlock();
                }
                gap -= mb;
                if (gap <= 0)
                    break;
            }
        }
        if (gap > 0)
            throw new NotEnoughBudgetException();

        if (debug) {
            if (getSystemAvailMB() < getRemainingBudgetMB()) {
                logger.debug("Remaining budget is " + getRemainingBudgetMB() + " MB free, but system only has " + getSystemAvailMB() + " MB free. If this persists, some memory calculation must be wrong.");
            }
        }
    }

    private boolean updateBooking(MemoryConsumer consumer, int requestMB) {
        lock.lock();
        try {
            ConsumerEntry entry = booking.get(consumer);
            if (entry == null) {
                if (requestMB == 0)
                    return true;

                entry = new ConsumerEntry(consumer);
                booking.put(consumer, entry);
            }

            int delta = requestMB - entry.reservedMB;
            return updateBookingWithDelta(consumer, delta);
        } finally {
            lock.unlock();
        }
    }

    // lock MUST be obtained before entering
    private boolean updateBookingWithDelta(MemoryConsumer consumer, int delta) {
        if (delta == 0)
            return true;

        ConsumerEntry entry = booking.get(consumer);
        if (entry == null) {
            if (delta <= 0)
                return true;

            entry = new ConsumerEntry(consumer);
            booking.put(consumer, entry);
        }

        // double check gap again, it may be changed by other concurrent requests
        if (delta > 0) {
            int gap = delta - (totalBudgetMB - totalReservedMB);
            if (gap > 0)
                return false;
        }

        totalReservedMB += delta;
        entry.reservedMB += delta;
        if (entry.reservedMB == 0) {
            booking.remove(entry.consumer);
        }
        if (debug) {
            logger.debug(entry.consumer + " reserved " + entry.reservedMB + " MB, total reserved " + totalReservedMB + " MB, remaining budget " + getRemainingBudgetMB() + " MB");
        }

        if (delta < 0) {
            synchronized (lock) {
                lock.notifyAll();
            }
        }

        return true;
    }

    public static int gcAndGetSystemAvailMB() {
        final int tolerance = 1;
        try {
            int lastMB = -1;
            while (true) {
                Runtime.getRuntime().gc();
                Thread.sleep(1000);
                int thisMB = getSystemAvailMB();

                if (lastMB < 0) {
                    lastMB = thisMB;
                    continue;
                }
                if (lastMB - thisMB < tolerance) {
                    return thisMB;
                }
                lastMB = thisMB;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("", e);
            return getSystemAvailMB();
        }
    }

    public static long getSystemAvailBytes() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory(); // current heap allocated to the VM process
        long freeMemory = runtime.freeMemory(); // out of the current heap, how much is free
        long maxMemory = runtime.maxMemory(); // Max heap VM can use e.g. Xmx setting
        long usedMemory = totalMemory - freeMemory; // how much of the current heap the VM is using
        long availableMemory = maxMemory - usedMemory; // available memory i.e. Maximum heap size minus the current amount used
        return availableMemory;
    }

    public static int getSystemAvailMB() {
        return (int) (getSystemAvailBytes() / ONE_MB);
    }

    // protective estimate of memory usage, prefer overestimate rather than underestimate
    public static class MemoryWaterLevel {
        int lowAvail = Integer.MAX_VALUE;
        int highAvail = Integer.MIN_VALUE;

        public void markHigh() {
            // get avail mem without gc
            int mb = MemoryBudgetController.getSystemAvailMB();
            if (mb < lowAvail) {
                lowAvail = mb;
                logger.warn("Lower system avail " + lowAvail + " MB in markHigh()");
            }
        }

        public void markLow() {
            // get avail mem after gc
            int mb = MemoryBudgetController.gcAndGetSystemAvailMB();
            if (mb > highAvail) {
                highAvail = mb;
                logger.warn("Higher system avail " + highAvail + " MB in markLow()");
            }
        }

        public int getEstimateMB() {
            return highAvail - lowAvail;
        }
    }
}
