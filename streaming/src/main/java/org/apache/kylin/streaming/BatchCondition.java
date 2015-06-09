package org.apache.kylin.streaming;

/**
 */
public interface BatchCondition {

    enum Result {
        ACCEPT, REJECT, DISCARD,LAST_ACCEPT_FOR_BATCH
    }

    Result apply(ParsedStreamMessage message);

}
