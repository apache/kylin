package org.apache.kylin.streaming;

/**
 */
public final class DefaultStreamFilter implements StreamFilter {

    public static final DefaultStreamFilter instance = new DefaultStreamFilter();

    private DefaultStreamFilter() {}

    @Override
    public boolean filter(ParsedStreamMessage streamMessage) {
        return streamMessage != null;
    }
}
