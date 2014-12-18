package com.kylinolap.job2.impl.threadpool;

import com.google.common.base.Preconditions;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.*;

import java.util.Map;

/**
 * Created by qianzhou on 12/16/14.
 */
public abstract class AbstractExecutable implements Executable, Idempotent {

    private String uuid;
    private ExecutableStatus status;
    private Map<String, String> extra;
    private String output;

    protected void onExecuteStart(ExecutableContext executableContext) {

    }
    protected void onExecuteSucceed(ExecuteResult result, ExecutableContext executableContext) {

    }

    protected void onExecuteException(Exception exception, ExecutableContext executableContext) {

    }

    @Override
    public final ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException {
        Preconditions.checkArgument(executableContext instanceof DefaultContext);
        ExecuteResult result;
        try {
            onExecuteStart(executableContext);
            result = doWork(executableContext);
        } catch (Exception e) {
            onExecuteException(e, executableContext);
            throw new ExecuteException(e);
        }
        onExecuteSucceed(result, executableContext);
        return result;
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
    public final ExecutableStatus getStatus() {
        return status;
    }

    public final void setStatus(ExecutableStatus status) {
        this.status = status;
    }

    @Override
    public Map<String, String> getExtra() {
        return extra;
    }

    public void setExtra(Map<String, String> extra) {
        this.extra = extra;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    @Override
    public String getOutput() {
        return output;
    }
}
