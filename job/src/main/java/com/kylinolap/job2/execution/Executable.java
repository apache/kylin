package com.kylinolap.job2.execution;

import com.kylinolap.job2.exception.ExecuteException;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Executable {

    ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException;

    void stop();

    ExecuteStatus getStatus();
}
