package org.apache.kylin.streaming;

import java.util.List;

/**
 */
public class ParsedStreamMessage {

    private final List<String> streamMessage;

    private long offset;

    private long timestamp;

    /**
     * false for pruned messages
     */
    private boolean accepted;

    public ParsedStreamMessage(List<String> streamMessage, long offset, long timestamp, boolean accepted) {
        this.streamMessage = streamMessage;
        this.offset = offset;
        this.timestamp = timestamp;
        this.accepted = accepted;
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

    public final boolean isAccepted() {
        return accepted;
    }
}
