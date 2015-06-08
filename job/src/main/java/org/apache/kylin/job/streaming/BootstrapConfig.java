package org.apache.kylin.job.streaming;

/**
 */
public class BootstrapConfig {

    private boolean oneOff = false;
    private long start = 0L;
    private long end = 0L;

    public boolean isOneOff() {
        return oneOff;
    }

    public void setOneOff(boolean oneOff) {
        this.oneOff = oneOff;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }
}
