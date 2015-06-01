package org.apache.kylin.streaming;

/**
 */
public interface StreamFilter {

    boolean filter(ParsedStreamMessage streamMessage);
}
