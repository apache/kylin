package org.apache.kylin.storage.gridtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public final class MemoryChecker {

    private static Logger logger = LoggerFactory.getLogger(MemoryChecker.class);

    private static final int MEMORY_THRESHOLD = 80 << 20;

    private MemoryChecker() {
    }

    public static final void checkMemory() {
        if (!Thread.currentThread().isInterrupted()) {
            final long freeMem = Runtime.getRuntime().freeMemory();
            if (freeMem <= MEMORY_THRESHOLD) {
                throw new OutOfMemoryError("free memory:" + freeMem + " is lower than " + MEMORY_THRESHOLD);
            }
        } else {
            logger.info("thread interrupted");
            throw new OutOfMemoryError("thread interrupted");
        }
    }

}
