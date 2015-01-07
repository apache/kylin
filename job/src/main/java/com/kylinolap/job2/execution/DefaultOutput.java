package com.kylinolap.job2.execution;

import java.util.Map;

/**
 * Created by qianzhou on 1/6/15.
 */
public class DefaultOutput implements Output {

    private ExecutableState state;
    private Map<String, String> extra;
    private String verboseMsg;
    private long lastModified;

    @Override
    public Map<String, String> getExtra() {
        return extra;
    }

    @Override
    public String getVerboseMsg() {
        return verboseMsg;
    }

    @Override
    public ExecutableState getState() {
        return state;
    }

    @Override
    public long getLastModified() {
        return lastModified;
    }

    public void setState(ExecutableState state) {
        this.state = state;
    }

    public void setExtra(Map<String, String> extra) {
        this.extra = extra;
    }

    public void setVerboseMsg(String verboseMsg) {
        this.verboseMsg = verboseMsg;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }
}
