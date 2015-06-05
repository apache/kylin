package org.apache.kylin.streaming;

/**
 */
public interface BatchCondition {

    enum Result {
        ACCEPT,
        REJECT,
        DISCARD
    }

    Result apply(ParsedStreamMessage message);

    BatchCondition copy();
}
