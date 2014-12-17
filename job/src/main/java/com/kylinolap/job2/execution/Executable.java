package com.kylinolap.job2.execution;

import com.kylinolap.job2.exception.ExecuteException;

import java.util.Map;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Executable {

    String getId();

    ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException;

    void stop() throws ExecuteException;

    ExecuteStatus getStatus();

    boolean isRunnable();

    boolean isAsync();

    Map<String, String> getExtra();
}
