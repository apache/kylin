package org.apache.kylin.job.execution;

import org.apache.commons.lang3.StringUtils;

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

    @Override
    public int hashCode() {
        final int prime = 31;
        int hashCode = state.hashCode();
        hashCode = hashCode * prime + extra.hashCode();
        hashCode = hashCode * prime + verboseMsg.hashCode();
        hashCode = hashCode * prime + Long.valueOf(lastModified).hashCode();
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DefaultOutput)) {
            return false;
        }
        DefaultOutput another = ((DefaultOutput) obj);
        if (this.state != another.state) {
            return false;
        }
        if (!extra.equals(another.extra)) {
            return false;
        }
        if (this.lastModified != another.lastModified) {
            return false;
        }
        return StringUtils.equals(verboseMsg, another.verboseMsg);
    }
}
