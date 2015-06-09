package org.apache.kylin.streaming;

/**
 */
public class TimePeriodCondition implements BatchCondition {

    private final long startTime;
    private final long endTime;
    private final long margin;

    public TimePeriodCondition(long startTime, long endTime) {
        this(startTime, endTime, 0L);
    }

    public TimePeriodCondition(long startTime, long endTime, long margin) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.margin = margin;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    @Override
    public Result apply(ParsedStreamMessage message) {
        final long timestamp = message.getTimestamp();
        if (timestamp < startTime) {
            return Result.DISCARD;
        } else if (timestamp < endTime) {
            return Result.ACCEPT;
        } else if (timestamp < endTime + margin) {
            return Result.DISCARD;
        } else {
            return Result.REJECT;
        }
    }

}
