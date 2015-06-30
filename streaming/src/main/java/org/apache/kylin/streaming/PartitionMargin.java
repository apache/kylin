package org.apache.kylin.streaming;

/**
 */
public class PartitionMargin {
    public long leftMargin;
    public long rightMargin;

    public PartitionMargin(long leftMargin, long rightMargin) {
        this.leftMargin = leftMargin;
        this.rightMargin = rightMargin;
    }
}
