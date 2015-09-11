package org.apache.kylin.streaming;

import java.util.List;

import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Lists;

/**
 */
public final class MicroStreamBatch {

    private final int partitionId;

    private final List<List<String>> streams;

    private final Pair<Long, Long> timestamp;

    private final Pair<Long, Long> offset;

    private int rawMessageCount;

    public MicroStreamBatch(int partitionId) {
        this.partitionId = partitionId;
        this.streams = Lists.newLinkedList();
        this.timestamp = Pair.newPair(Long.MAX_VALUE, Long.MIN_VALUE);
        this.offset = Pair.newPair(Long.MAX_VALUE, Long.MIN_VALUE);
    }

    private MicroStreamBatch(MicroStreamBatch batch) {
        this.partitionId = batch.partitionId;
        this.streams = Lists.newLinkedList(batch.streams);
        this.timestamp = Pair.newPair(batch.timestamp.getFirst(), batch.timestamp.getSecond());
        this.offset = Pair.newPair(batch.offset.getFirst(), batch.offset.getSecond());
    }

    public int getPartitionId() {
        return partitionId;
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

    public final void incRawMessageCount() {
        this.rawMessageCount++;
    }

    public final int getRawMessageCount() {
        return this.rawMessageCount;
    }

    public final int getFilteredMessageCount() {
        return this.streams.size();
    }

    public final void add(ParsedStreamMessage parsedStreamMessage) {
        if (offset.getFirst() > parsedStreamMessage.getOffset()) {
            offset.setFirst(parsedStreamMessage.getOffset());
        }
        if (offset.getSecond() < parsedStreamMessage.getOffset()) {
            offset.setSecond(parsedStreamMessage.getOffset());
        }
        if (timestamp.getFirst() > parsedStreamMessage.getTimestamp()) {
            timestamp.setFirst(parsedStreamMessage.getTimestamp());
        }
        if (timestamp.getSecond() < parsedStreamMessage.getTimestamp()) {
            timestamp.setSecond(parsedStreamMessage.getTimestamp());
        }
        this.streams.add(parsedStreamMessage.getStreamMessage());
    }

    public static MicroStreamBatch union(MicroStreamBatch one, MicroStreamBatch another) {
        MicroStreamBatch result = new MicroStreamBatch(one);
        result.streams.addAll(another.streams);
        result.offset.setFirst(Math.min(result.offset.getFirst(), another.offset.getFirst()));
        result.offset.setSecond(Math.max(result.offset.getSecond(), another.offset.getSecond()));
        result.timestamp.setFirst(Math.min(result.timestamp.getFirst(), another.timestamp.getFirst()));
        result.timestamp.setSecond(Math.max(result.timestamp.getSecond(), another.timestamp.getSecond()));
        result.rawMessageCount = one.rawMessageCount + another.rawMessageCount;
        return result;
    }

    public boolean isEmpty() {
        return streams.isEmpty();
    }
}
