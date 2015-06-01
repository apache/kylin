package org.apache.kylin.streaming;

import org.apache.kylin.common.util.Pair;

import java.util.List;

/**
 */
public final class MicroStreamBatch {

    private final List<List<String>> streams;

    private final Pair<Long, Long> timestamp;

    private final Pair<Long, Long> offset;

    public MicroStreamBatch(List<List<String>> streams, Pair<Long, Long> timestamp, Pair<Long, Long> offset) {
        this.streams = streams;
        this.timestamp = timestamp;
        this.offset = offset;
    }

    public final List<List<String>> getStreams() {
        return streams;
    }

    public final Pair<Long, Long> getTimestamp() {
        return timestamp;
    }

    public final Pair<Long, Long> getOffset() {
        return offset;
    }

    public final int size() {
        return streams.size();
    }
}
