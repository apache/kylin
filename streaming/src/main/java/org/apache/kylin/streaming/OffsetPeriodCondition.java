package org.apache.kylin.streaming;

/**
 */
public class OffsetPeriodCondition implements BatchCondition {

    private final long startOffset;
    private final long endOffset;

    public OffsetPeriodCondition(long startOffset, long endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    @Override
    public Result apply(ParsedStreamMessage message) {
        final long offset = message.getOffset();
        if (offset < startOffset) {
            return Result.DISCARD;
        } else if (offset < endOffset) {
            return Result.ACCEPT;
        } else {
            return Result.REJECT;
        }
    }

}
