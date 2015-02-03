package org.apache.kylin.job.execution;

import java.util.Map;

/**
 * Created by qianzhou on 1/6/15.
 */
public interface Output {

    Map<String, String> getExtra();

    String getVerboseMsg();

    ExecutableState getState();

    long getLastModified();
}
