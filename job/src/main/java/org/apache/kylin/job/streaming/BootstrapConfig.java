package org.apache.kylin.job.streaming;

/**
 */
public class BootstrapConfig {

    private String streaming;
    private int partitionId = -1;

    private boolean oneOff = false;
    private long start = 0L;
    private long end = 0L;
    private long margin = 0L;

    private boolean fillGap;


    public long getMargin() {
        return margin;
    }

    public void setMargin(long margin) {
        this.margin = margin;
    }

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

    public String getStreaming() {
        return streaming;
    }

    public void setStreaming(String streaming) {
        this.streaming = streaming;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public boolean isFillGap() {
        return fillGap;
    }

    public void setFillGap(boolean fillGap) {
        this.fillGap = fillGap;
    }
}
