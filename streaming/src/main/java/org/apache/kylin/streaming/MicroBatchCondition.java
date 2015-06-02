package org.apache.kylin.streaming;

/**
 */
public final class MicroBatchCondition {

    private final int batchSize;
    private final int batchInterval;

    public MicroBatchCondition(int batchSize, int batchInterval) {
        this.batchSize = batchSize;
        this.batchInterval = batchInterval;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBatchInterval() {
        return batchInterval;
    }
}
