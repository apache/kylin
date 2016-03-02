package org.apache.kylin.engine.streaming;

/**
 */
public class BootstrapConfig {

    private String cubeName;
    private long start = 0L;
    private long end = 0L;

    private boolean fillGap;

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

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public boolean isFillGap() {
        return fillGap;
    }

    public void setFillGap(boolean fillGap) {
        this.fillGap = fillGap;
    }
}
