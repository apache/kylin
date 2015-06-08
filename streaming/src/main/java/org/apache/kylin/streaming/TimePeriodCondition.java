package org.apache.kylin.streaming;

/**
 */
public class TimePeriodCondition implements BatchCondition {

    private final long startTime;
    private final long endTime;

    public TimePeriodCondition(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    @Override
    public Result apply(ParsedStreamMessage message) {
        if (message.getTimestamp() < startTime) {
            return Result.DISCARD;
        } else if (message.getTimestamp() < endTime) {
            return Result.ACCEPT;
        } else {
            return Result.REJECT;
        }
    }

}
