package org.apache.kylin.streaming;

import java.util.List;

/**
 */
public class ParsedStreamMessage {

    private final List<String> streamMessage;

    private long offset;

    private long timestamp;

    public ParsedStreamMessage(List<String> streamMessage, long offset, long timestamp) {
        this.streamMessage = streamMessage;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public final List<String> getStreamMessage() {
        return streamMessage;
    }

    public final long getOffset() {
        return offset;
    }

    public final long getTimestamp() {
        return timestamp;
    }
}
