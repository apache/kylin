package com.kylinolap.job.execution;

import com.kylinolap.job.exception.ExecuteException;

import java.util.Map;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Executable {

    String getId();

    String getName();

    ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException;

    ExecutableState getStatus();

    Output getOutput();

    boolean isRunnable();

    Map<String, String> getParams();
}
