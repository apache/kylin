package org.apache.kylin.streaming;

/**
 */
public class LimitedSizeCondition implements BatchCondition {

    private final int limit;
    private int count;

    public LimitedSizeCondition(int limit) {
        this.limit = limit;
        this.count = 0;
    }

    @Override
    public Result apply(ParsedStreamMessage message) {
        if (count < limit) {
            count++;

            if (count == limit) {
                return Result.LAST_ACCEPT_FOR_BATCH;
            } else {
                return Result.ACCEPT;
            }
        } else {
            return Result.REJECT;
        }
    }
}
