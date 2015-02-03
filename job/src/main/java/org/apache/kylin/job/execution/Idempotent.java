package org.apache.kylin.job.execution;

import org.apache.kylin.job.exception.ExecuteException;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Idempotent {

    void cleanup() throws ExecuteException;
}
