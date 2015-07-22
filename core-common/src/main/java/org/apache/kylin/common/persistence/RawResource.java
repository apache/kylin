package org.apache.kylin.common.persistence;

import java.io.InputStream;

/**
 */
public class RawResource {

    public final InputStream resource;
    public final long timestamp;

    public RawResource(InputStream resource, long timestamp) {
        this.resource = resource;
        this.timestamp = timestamp;
    }
}
