package org.apache.kylin.common.persistence;

import java.io.InputStream;

/**
 */
public class RawResource {

    public final InputStream inputStream;
    public final long timestamp;

    public RawResource(InputStream resource, long timestamp) {
        this.inputStream = resource;
        this.timestamp = timestamp;
    }
}
