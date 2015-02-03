package org.apache.kylin.job.execution;

import org.apache.kylin.job.exception.ExecuteException;

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
