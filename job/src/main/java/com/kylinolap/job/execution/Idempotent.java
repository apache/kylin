package com.kylinolap.job.execution;

import com.kylinolap.job.exception.ExecuteException;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Idempotent {

    void cleanup() throws ExecuteException;
}
