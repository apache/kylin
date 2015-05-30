package org.apache.kylin.common.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryBudgetController {

    public static interface MemoryConsumer {
        // return number MB released
        int freeUp(int mb);
    }

    @SuppressWarnings("serial")
    public static class NotEnoughBudgetException extends IllegalStateException {
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

    private static final Logger logger = LoggerFactory.getLogger(MemoryBudgetController.class);

    // all budget numbers are in MB
    private final int totalBudgetMB;
    private final AtomicInteger totalReservedMB;
    private final ConcurrentHashMap<MemoryConsumer, ConsumerEntry> booking = new ConcurrentHashMap<MemoryConsumer, ConsumerEntry>();

    public MemoryBudgetController(int totalBudgetMB) {
        if (totalBudgetMB < 0)
            throw new IllegalArgumentException();
        if (totalBudgetMB > 0 && checkSystemAvailMB(totalBudgetMB) == false)
            throw new IllegalStateException();

        this.totalBudgetMB = totalBudgetMB;
        this.totalReservedMB = new AtomicInteger();
    }

    public int getTotalBudgetMB() {
        return totalBudgetMB;
    }

    public int getTotalReservedMB() {
        return totalReservedMB.get();
    }

    public int getRemainingBudgetMB() {
        return totalBudgetMB - totalReservedMB.get();
    }

    public void reserve(MemoryConsumer consumer, int requestMB) {
        if (totalBudgetMB == 0 && requestMB > 0)
            throw new NotEnoughBudgetException();

        ConsumerEntry entry = booking.get(consumer);
        if (entry == null) {
            booking.putIfAbsent(consumer, new ConsumerEntry(consumer));
            entry = booking.get(consumer);
        }

        int delta = requestMB - entry.reservedMB;

        if (delta > 0) {
            checkFreeMemoryAndUpdateBooking(entry, delta);
        } else {
            updateBooking(entry, delta);
        }
    }

    synchronized private void updateBooking(ConsumerEntry entry, int delta) {
        totalReservedMB.addAndGet(delta);
        entry.reservedMB += delta;
        if (entry.reservedMB == 0) {
            booking.remove(entry.consumer);
        }
        if (delta < 0) {
            this.notify();
        }
        if (delta != 0) {
            logger.debug(entry.consumer + " reserved " + entry.reservedMB + " MB, total reserved " + totalReservedMB + " MB, remaining budget " + getRemainingBudgetMB() + " MB");
        }
    }

    private void checkFreeMemoryAndUpdateBooking(ConsumerEntry consumer, int delta) {
        while (true) {
            // if budget is not enough, try free up 
            while (delta > totalBudgetMB - totalReservedMB.get()) {
                int freeUpToGo = delta;
                for (ConsumerEntry entry : booking.values()) {
                    int mb = entry.consumer.freeUp(freeUpToGo);
                    updateBooking(entry, -mb);
                    freeUpToGo -= mb;
                    if (freeUpToGo <= 0)
                        break;
                }
                if (freeUpToGo > 0)
                    throw new NotEnoughBudgetException();
            }

            if (checkSystemAvailMB(delta))
                break;

            try {
                synchronized (this) {
                    logger.debug("Remaining budget is " + getRemainingBudgetMB() + " MB free, but system only has " + getSystemAvailMB() + " MB free. If this persists, some memory calculation must be wrong.");
                    this.wait(200);
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while wait free memory", e);
            }
        }

        updateBooking(consumer, delta);
    }

    private boolean checkSystemAvailMB(int mb) {
        return getSystemAvailMB() >= mb;
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

}
