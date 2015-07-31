package org.apache.kylin.engine.streaming;

import java.util.List;

/**
 */
public class StreamingMessage {

    private final List<String> data;

    private long offset;

    private long timestamp;

    public StreamingMessage(List<String> data, long offset, long timestamp) {
        this.data = data;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public final List<String> getData() {
        return data;
    }

    public final long getOffset() {
        return offset;
    }

    public final long getTimestamp() {
        return timestamp;
    }

}
