package com.kylinolap.job2.impl.threadpool;

import com.google.common.base.Preconditions;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.*;

/**
 * Created by qianzhou on 12/16/14.
 */
public abstract class AbstractExecutable implements Executable, Idempotent {

    private String uuid;
    private ExecuteStatus status;
    private boolean isAsync;

    protected void beforeExecute(ExecutableContext executableContext) throws ExecuteException {

    }
    protected void afterExecute(ExecutableContext executableContext) throws ExecuteException {

    }

    @Override
    public final ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException {
        Preconditions.checkArgument(executableContext instanceof DefaultContext);
        try {
            beforeExecute(executableContext);
            return doWork(executableContext);
        } finally {
            afterExecute(executableContext);
        }
    }

    protected abstract ExecuteResult doWork(ExecutableContext context) throws ExecuteException;

    @Override
    public void stop() throws ExecuteException {

    }

    @Override
    public void cleanup() throws ExecuteException {

    }


    @Override
    public final String getId() {
        return uuid;
    }

    public final void setId(String id) {
        this.uuid = id;
    }

    @Override
    public final ExecuteStatus getStatus() {
        return status;
    }

    public final void setStatus(ExecuteStatus status) {
        this.status = status;
    }

    @Override
    public final boolean isAsync() {
        return isAsync;
    }

    public final void setAsync(boolean isAsync) {
        this.isAsync = isAsync;
    }
}
