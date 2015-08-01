package org.apache.kylin.engine.streaming;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public class StreamingMessage {

    private final List<String> data;

    private long offset;

    private long timestamp;
    
    private Map<String, Object> params;
    
    public static final StreamingMessage EOF = new StreamingMessage(Collections.<String>emptyList(), 0L, 0L, Collections.<String, Object>emptyMap());

    public StreamingMessage(List<String> data, long offset, long timestamp, Map<String, Object> params) {
        this.data = data;
        this.offset = offset;
        this.timestamp = timestamp;
        this.params = params;
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

    public Map<String, Object> getParams() {
        return params;
    }
}
